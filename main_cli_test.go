package main

// CLI wiring tests for the -ping health check (the probe helper used by
// the runbook's health-check examples).

import (
	"path/filepath"
	"testing"
	"time"

	"gomailer/internal/daemon"
	"gomailer/internal/jobber"
	"gomailer/internal/mailer"
)

// nopCourier accepts every delivery without touching the network.
type nopCourier struct{}

func (nopCourier) Prepare(cfg mailer.MailConfig, c *mailer.MailContent) ([]byte, error) {
	return []byte(c.Body), nil
}
func (nopCourier) Warm(cfg mailer.MailConfig) (any, error) { return nil, nil }
func (nopCourier) Send(cfg mailer.MailConfig, session any, msg []byte, to []string) error {
	return nil
}
func (nopCourier) Discard(session any) {}

// TestRunPingAgainstLiveDaemon wires a minimal daemon (scheduler Serve +
// socket server, as runDaemon does) and checks the ping helper reports it,
// and that a dead socket is an error (the CLI exits 1 on that).
func TestRunPingAgainstLiveDaemon(t *testing.T) {
	dir := t.TempDir()
	store, err := jobber.OpenStore(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	s := jobber.NewScheduler(store)
	s.WarmLead = 0
	s.Courier = nopCourier{}

	sock := filepath.Join(dir, "gomailer.sock")
	srv := &daemon.Server{Path: sock, Scheduler: s}

	ready := make(chan struct{})
	serveErr := make(chan error, 2)
	go func() {
		serveErr <- s.Serve(t.Context(), func() {
			ln, lerr := srv.Listen()
			if lerr != nil {
				serveErr <- lerr
				return
			}
			go srv.Serve(t.Context(), ln)
			close(ready)
		})
	}()
	select {
	case <-ready:
	case err := <-serveErr:
		t.Fatalf("daemon did not start: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("daemon did not start")
	}

	if err := runPing(sock); err != nil {
		t.Fatalf("runPing on a live daemon: %v", err)
	}

	// A submitted job is visible in the pending count.
	content := mailer.MailContent{
		From:    "sender@pec.test",
		To:      []string{"dest@pec.test"},
		Subject: "t",
		Body:    "b",
	}
	cfg := mailer.MailConfig{Hostname: "smtp.pec.test", Port: "465", Username: "sender@pec.test", Password: "x"}
	if _, err := s.Schedule(cfg, &content, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := runPing(sock); err != nil {
		t.Fatalf("runPing after schedule: %v", err)
	}

	// Dead socket: the probe must fail (clients degrade to persist-only).
	if err := runPing(filepath.Join(dir, "missing.sock")); err == nil {
		t.Fatal("ping against a dead socket must fail")
	}
}
