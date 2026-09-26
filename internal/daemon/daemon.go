// Package daemon is the local control plane between the scheduling CLI
// and THE daemon: exactly one long-lived process per store collects
// submissions over a Unix socket and is the only process that ever fires
// jobs (see jobber.Scheduler for the firing engine and its run lock).
//
// The socket is owner-only (0600): requests carry the mailbox
// credentials of the submitting user, exactly like the store itself.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"gomailer/internal/jobber"
	"gomailer/internal/mailer"
)

// ProtocolVersion tags the request/response envelope, leaving room for
// evolution without guessing. Version 2 adds the idempotent submit: the
// request carries a client-generated job_id and the ack carries the
// persisted job's state. Clients and daemons should be upgraded together —
// a mixed pair still works, but only the matching pair gets the no-duplicate
// guarantee on ack loss.
const ProtocolVersion = 2

// Request ops.
const (
	// OpSchedule submits one job: the daemon validates, persists and arms
	// it, and answers with the persisted job's identifiers.
	OpSchedule = "schedule"
	// OpPing probes liveness and reports how many jobs are pending.
	OpPing = "ping"
)

// Request is one client ask: one JSON value per connection.
type Request struct {
	Op      string             `json:"op"`
	JobID   string             `json:"job_id,omitempty"` // client-chosen idempotency key: a retry with the same ID converges on the same job
	FireAt  time.Time          `json:"fire_at,omitempty"`
	Config  mailer.MailConfig  `json:"config,omitempty"`
	Content mailer.MailContent `json:"content,omitempty"`
}

// JobAck echoes the persisted job back to the client WITHOUT the mailbox
// credentials the full record carries. State lets the client tell a fresh
// scheduling from an idempotent replay of a job that already fired.
type JobAck struct {
	ID        string          `json:"id"`
	MessageID string          `json:"message_id"`
	FireAt    time.Time       `json:"fire_at"`
	State     jobber.JobState `json:"state,omitempty"`
}

// Response is the daemon's answer to one Request.
type Response struct {
	OK      bool    `json:"ok"`
	Error   string  `json:"error,omitempty"`
	Job     *JobAck `json:"job,omitempty"`
	Pending int     `json:"pending,omitempty"`
	Version int     `json:"version,omitempty"`
}

// DefaultSock is where the daemon for a store listens (and clients
// submit): next to the store file, overridable with -sock.
func DefaultSock(storePath string) string {
	return filepath.Join(filepath.Dir(storePath), "gomailer.sock")
}

// Server accepts submissions for one scheduler.
type Server struct {
	// Path is the Unix socket to listen on.
	Path string
	// Scheduler collects the submissions (must be the one being Served).
	Scheduler *jobber.Scheduler
}

// Listen binds the socket, replacing any file a dead daemon left behind.
// The caller must already hold the store's run lock — the Serve loop
// guarantees that — which proves no live daemon owns the socket, so any
// leftover file is stale. The socket is owner-only: requests carry the
// submitting user's mailbox credentials.
func (s *Server) Listen() (net.Listener, error) {
	if err := os.Remove(s.Path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("daemon: removing stale socket %s: %w", s.Path, err)
	}
	ln, err := net.Listen("unix", s.Path)
	if err != nil {
		return nil, fmt.Errorf("daemon: listening on %s: %w", s.Path, err)
	}
	if err := os.Chmod(s.Path, 0o600); err != nil {
		ln.Close()
		return nil, fmt.Errorf("daemon: restricting socket %s: %w", s.Path, err)
	}
	return ln, nil
}

// Serve accepts connections until ctx is canceled, then DRAINS: in-flight
// exchanges are waited for — and their connections closed, so a wedged
// client cannot stretch the stop — because a reply killed mid-write is
// indistinguishable from a dead daemon on the client side. Idempotent
// submissions make the client's fallback safe even then, but completing
// the exchange is strictly better: Serve returns only after the last
// handler finished.
//
// A submission accepted in the stop's drain window is persisted and
// answered, but its firing waits for the next start (the tending loop has
// already returned): the store is the truth and re-arms it at boot. Each
// connection is one Request/Response exchange, handled in its own
// goroutine: Schedule is safe to call concurrently with the firing loop,
// and a submission that races shutdown is simply persisted and stays
// pending — nothing is ever lost.
func (s *Server) Serve(ctx context.Context, ln net.Listener) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	conns := make(map[net.Conn]struct{})
	shutdown := func() {
		ln.Close()
		mu.Lock()
		for c := range conns {
			c.Close()
		}
		mu.Unlock()
	}
	go func() {
		<-ctx.Done()
		shutdown()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() == nil {
				// The socket broke (not a shutdown): stop accepting but keep
				// firing — clients degrade to their persist-only fallback and
				// nothing is lost. Loud, because it needs a human glance.
				log.Printf("daemon: accepting on %s stopped: %v (firing continues; submissions fall back to persist-only)", s.Path, err)
			}
			break
		}
		wg.Add(1)
		mu.Lock()
		conns[conn] = struct{}{}
		mu.Unlock()
		go func() {
			defer wg.Done()
			defer func() {
				mu.Lock()
				delete(conns, conn)
				mu.Unlock()
			}()
			s.handle(conn)
		}()
	}
	wg.Wait()
}

// handle serves exactly one request, then closes the connection.
func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	var req Request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		s.reply(conn, Response{OK: false, Error: "bad request: " + err.Error()})
		return
	}

	switch req.Op {
	case OpPing:
		s.reply(conn, Response{OK: true, Version: ProtocolVersion, Pending: s.Scheduler.Pending()})
	case OpSchedule:
		// All validation, persistence and arming happen in the daemon —
		// one implementation, one process, exactly one firing authority.
		// The client-chosen JobID makes the submission idempotent: a retry
		// whose first attempt was persisted but never acknowledged (the
		// daemon died mid-reply) converges on that job instead of
		// duplicating it.
		job, err := s.Scheduler.ScheduleWithID(req.JobID, req.Config, &req.Content, req.FireAt)
		if err != nil {
			s.reply(conn, Response{OK: false, Error: err.Error()})
			return
		}
		s.reply(conn, Response{OK: true, Job: &JobAck{
			ID:        job.ID,
			MessageID: job.MessageID,
			FireAt:    job.FireAt,
			State:     job.State,
		}})
	default:
		s.reply(conn, Response{OK: false, Error: "unknown op " + strconv.Quote(req.Op)})
	}
}

// reply writes one response and lets the deferred close flush it.
func (s *Server) reply(conn net.Conn, resp Response) {
	_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if err := json.NewEncoder(conn).Encode(resp); err != nil {
		log.Printf("daemon: replying to %s: %v", conn.RemoteAddr(), err)
	}
}

// Submit sends one request to the daemon and returns its response.
// A transport error means the daemon is not reachable — the CLI then
// degrades to its persist-only fallback, so a scheduled job never depends
// on the daemon being up at scheduling time.
func Submit(sock string, req Request) (Response, error) {
	return SubmitTimeout(sock, req, 30*time.Second)
}

// SubmitTimeout is Submit with a caller-chosen overall deadline (the
// CLI's idempotent retry after a failed first attempt uses a short one: a
// daemon that just failed to answer should not be waited on at length
// again).
func SubmitTimeout(sock string, req Request, timeout time.Duration) (Response, error) {
	conn, err := net.DialTimeout("unix", sock, min(timeout, 2*time.Second))
	if err != nil {
		return Response{}, fmt.Errorf("daemon: reaching %s: %w", sock, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	if err := json.NewEncoder(conn).Encode(&req); err != nil {
		return Response{}, fmt.Errorf("daemon: sending request: %w", err)
	}
	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return Response{}, fmt.Errorf("daemon: reading response: %w", err)
	}
	return resp, nil
}
