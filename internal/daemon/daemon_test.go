package daemon

// End-to-end tests of the control plane: clients submit over the socket,
// THE daemon validates/persists/arms and fires exactly once, ping reports
// the queue, validation errors travel back to the client, and an
// unreachable daemon surfaces as a transport error (the CLI's cue to
// degrade to persist-only).

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gomailer/internal/jobber"
	"gomailer/internal/mailer"
)

// fakeCourier records what the daemon actually delivered.
type fakeCourier struct {
	mu    sync.Mutex
	sends [][]byte
}

func (f *fakeCourier) Prepare(cfg mailer.MailConfig, c *mailer.MailContent) ([]byte, error) {
	return []byte(c.Body), nil
}

func (f *fakeCourier) Warm(cfg mailer.MailConfig) (any, error) { return nil, nil }

func (f *fakeCourier) Send(cfg mailer.MailConfig, session any, msg []byte, to []string) error {
	f.mu.Lock()
	f.sends = append(f.sends, msg)
	f.mu.Unlock()
	return nil
}

func (f *fakeCourier) Discard(session any) {}

func (f *fakeCourier) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sends)
}

func testConfig() mailer.MailConfig {
	return mailer.MailConfig{
		Hostname: "smtp.pec.test",
		Port:     "465",
		Username: "sender@pec.test",
		Password: "secret",
	}
}

func testContent(body string) mailer.MailContent {
	return mailer.MailContent{
		From:    "sender@pec.test",
		To:      []string{"dest@pec.test"},
		Subject: "test",
		Body:    body,
	}
}

// startDaemon brings up a serving scheduler plus its socket, mimicking
// what main.runDaemon wires together. The returned cancel stops it.
func startDaemon(t *testing.T) (*jobber.Store, *fakeCourier, string, context.CancelFunc) {
	t.Helper()
	dir := t.TempDir()
	store, err := jobber.OpenStore(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	s := jobber.NewScheduler(store)
	s.WarmLead = 0
	c := &fakeCourier{}
	s.Courier = c

	sock := filepath.Join(dir, "gomailer.sock")
	srv := &Server{Path: sock, Scheduler: s}

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	serveErr := make(chan error, 2)
	go func() {
		serveErr <- s.Serve(ctx, func() {
			ln, lerr := srv.Listen()
			if lerr != nil {
				serveErr <- lerr
				cancel()
				return
			}
			go srv.Serve(ctx, ln)
			close(ready)
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-serveErr
	})
	select {
	case <-ready:
	case err := <-serveErr:
		t.Fatalf("daemon failed to start: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("daemon did not become ready")
	}
	return store, c, sock, cancel
}

func waitSends(t *testing.T, c *fakeCourier, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for c.count() < want {
		if time.Now().After(deadline) {
			t.Fatalf("send calls = %d, want %d", c.count(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDaemonEndToEnd: two clients schedule at different times through the
// ONE daemon; the daemon fires each exactly once, in order, and the acks
// match the persisted records.
func TestDaemonEndToEnd(t *testing.T) {
	store, c, sock, _ := startDaemon(t)
	cfg := testConfig()

	ca := testContent("first")
	respA, err := Submit(sock, Request{Op: OpSchedule, FireAt: time.Now().Add(300 * time.Millisecond), Config: cfg, Content: ca})
	if err != nil || !respA.OK {
		t.Fatalf("submit A: err=%v resp=%+v", err, respA)
	}
	if respA.Job == nil || respA.Job.MessageID == "" || respA.Job.ID == "" {
		t.Fatalf("no ack for A: %+v", respA)
	}
	cb := testContent("second")
	respB, err := Submit(sock, Request{Op: OpSchedule, FireAt: time.Now().Add(700 * time.Millisecond), Config: cfg, Content: cb})
	if err != nil || !respB.OK {
		t.Fatalf("submit B: err=%v resp=%+v", err, respB)
	}
	if respA.Job.MessageID == respB.Job.MessageID {
		t.Fatal("the two jobs share a Message-ID")
	}

	// Both pending right after submission.
	ping, err := Submit(sock, Request{Op: OpPing})
	if err != nil || !ping.OK {
		t.Fatalf("ping: err=%v resp=%+v", err, ping)
	}
	if ping.Pending != 2 || ping.Version != ProtocolVersion {
		t.Fatalf("ping = %+v, want 2 pending and the protocol version", ping)
	}

	// The single daemon fires both, exactly once, in order.
	waitSends(t, c, 2, 4*time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 2 || string(c.sends[0]) != "first" || string(c.sends[1]) != "second" {
		t.Fatalf("sends = %q, want first then second", c.sends)
	}

	// The store records the acked Message-IDs, terminal states only.
	jobs, err := store.Load()
	if err != nil || len(jobs) != 2 {
		t.Fatalf("store: err=%v jobs=%d", err, len(jobs))
	}
	for _, j := range jobs {
		if j.State != jobber.StateSent {
			t.Fatalf("job %s: state=%q, want sent", j.ID, j.State)
		}
		if j.MessageID != respA.Job.MessageID && j.MessageID != respB.Job.MessageID {
			t.Fatalf("job %s carries unexpected Message-ID %s", j.ID, j.MessageID)
		}
	}

	// Empty queue afterwards.
	ping, err = Submit(sock, Request{Op: OpPing})
	if err != nil || !ping.OK || ping.Pending != 0 {
		t.Fatalf("ping after fires = %+v err=%v, want empty queue", ping, err)
	}
}

// TestDaemonRejectsInvalidJob: validation stays on the daemon and the
// error travels back to the client; nothing is persisted.
func TestDaemonRejectsInvalidJob(t *testing.T) {
	store, _, sock, _ := startDaemon(t)

	bad := testContent("") // empty body: rejected at scheduling time
	resp, err := Submit(sock, Request{Op: OpSchedule, FireAt: time.Now().Add(time.Hour), Config: testConfig(), Content: bad})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if resp.OK {
		t.Fatal("invalid job accepted by the daemon")
	}
	if !strings.Contains(resp.Error, "body") {
		t.Fatalf("error = %q, want it to mention the body", resp.Error)
	}
	jobs, err := store.Load()
	if err != nil || len(jobs) != 0 {
		t.Fatalf("store: err=%v jobs=%d, want nothing persisted", err, len(jobs))
	}
}

// TestDaemonRefusesSecondFiringProcess: a second scheduler on the same
// store cannot even start serving — the run lock rejects it.
func TestDaemonRefusesSecondFiringProcess(t *testing.T) {
	_, _, sock, _ := startDaemon(t)

	// A second daemon (or a resume-drain) on the same store must be
	// refused: it would fire the same pending jobs twice.
	dir := filepath.Dir(sock)
	s2 := jobber.NewScheduler(mustOpen(t, filepath.Join(dir, "jobs.json")))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := s2.Serve(ctx, nil)
	if err == nil || !strings.Contains(err.Error(), "another scheduler") {
		t.Fatalf("second daemon Serve = %v, want run-lock refusal", err)
	}
}

// TestSubmitUnreachable: a dead socket is a transport error — the CLI's
// cue to degrade to its persist-only fallback.
func TestSubmitUnreachable(t *testing.T) {
	_, err := Submit(filepath.Join(t.TempDir(), "missing.sock"), Request{Op: OpPing})
	if err == nil {
		t.Fatal("submit to a dead socket must fail")
	}
	if !strings.Contains(err.Error(), "reaching") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func mustOpen(t *testing.T, path string) *jobber.Store {
	t.Helper()
	st, err := jobber.OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return st
}

// TestSubmitIdempotentOverSocket: the same job_id submitted twice through
// the socket converges on ONE job — the ack-loss retry case. The second
// ack reports the same identifiers, and exactly one delivery happens.
func TestSubmitIdempotentOverSocket(t *testing.T) {
	store, c, sock, _ := startDaemon(t)
	cfg := testConfig()
	content := testContent("idempotent")
	req := Request{
		Op: OpSchedule, JobID: "fixed-job-id",
		FireAt: time.Now().Add(200 * time.Millisecond), Config: cfg, Content: content,
	}
	r1, err := Submit(sock, req)
	if err != nil || !r1.OK {
		t.Fatalf("first submit: err=%v resp=%+v", err, r1)
	}
	r2, err := Submit(sock, req)
	if err != nil || !r2.OK {
		t.Fatalf("replay submit: err=%v resp=%+v", err, r2)
	}
	if r1.Job.ID != r2.Job.ID || r1.Job.MessageID != r2.Job.MessageID {
		t.Fatalf("replay created a second job: %s/%s vs %s/%s", r2.Job.ID, r2.Job.MessageID, r1.Job.ID, r1.Job.MessageID)
	}
	if r2.Job.State != jobber.StatePending {
		t.Fatalf("replay ack state = %q, want pending", r2.Job.State)
	}

	waitSends(t, c, 1, 3*time.Second)
	time.Sleep(100 * time.Millisecond) // let any (wrong) second send show up
	if n := c.count(); n != 1 {
		t.Fatalf("send calls = %d, want exactly 1", n)
	}
	jobs, err := store.Load()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("store rows = %d (err=%v), want 1", len(jobs), err)
	}
	if jobs[0].State != jobber.StateSent || jobs[0].ID != r1.Job.ID {
		t.Fatalf("job after fire: state=%q id=%s", jobs[0].State, jobs[0].ID)
	}
}

// TestServeDrainsLiveConnectionsOnStop: on cancellation the daemon must
// close every live connection (a wedged client cannot stretch the stop)
// and return promptly — Serve waits for handlers, not for deadlines.
func TestServeDrainsLiveConnectionsOnStop(t *testing.T) {
	dir := t.TempDir()
	store, err := jobber.OpenStore(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	s := jobber.NewScheduler(store)
	s.WarmLead = 0
	c := &fakeCourier{}
	s.Courier = c
	sock := filepath.Join(dir, "gomailer.sock")
	srv := &Server{Path: sock, Scheduler: s}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	sockDone := make(chan struct{})
	serveErr := make(chan error, 2)
	go func() {
		serveErr <- s.Serve(ctx, func() {
			ln, lerr := srv.Listen()
			if lerr != nil {
				serveErr <- lerr
				return
			}
			go func() {
				srv.Serve(ctx, ln)
				close(sockDone)
			}()
			close(ready)
		})
	}()
	select {
	case <-ready:
	case err := <-serveErr:
		t.Fatalf("daemon failed to start: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("daemon did not start")
	}

	// A client that connects and sends nothing: its handler blocks on the
	// read deadline (30s). Without connection-closing on stop, Serve would
	// hang for that full deadline during shutdown.
	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	time.Sleep(100 * time.Millisecond) // let the daemon accept it

	cancel()
	select {
	case <-sockDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not drain promptly: a live connection stretched the stop")
	}
	if err := <-serveErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve returned %v, want context.Canceled", err)
	}

	// The idle connection was closed: the read unblocks immediately.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("idle connection was not closed at shutdown")
	}
}
