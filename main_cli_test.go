package main

// CLI wiring tests for the -ping health check and the -attach flag
// (the helpers used by the runbook's examples: runPing, loadAttachments
// and the full submitJob path against a live daemon).

import (
	"bytes"
	"os"
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

// TestLoadAttachments: the -attach flag's file plumbing — bytes read
// verbatim at scheduling time, recipient-visible filename is the base
// name (never the sender's directory layout), Content-Type inferred from
// the extension, and a missing file is a scheduling-time error.
func TestLoadAttachments(t *testing.T) {
	dir := t.TempDir()
	pdf := filepath.Join(dir, "contratto.pdf")
	if err := os.WriteFile(pdf, []byte("%PDF-1.4 fake"), 0o600); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(dir, "nested", "appendice.txt")
	if err := os.MkdirAll(filepath.Dir(nested), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nested, []byte("testo"), 0o600); err != nil {
		t.Fatal(err)
	}

	atts, err := loadAttachments([]string{pdf, nested})
	if err != nil {
		t.Fatalf("loadAttachments: %v", err)
	}
	if len(atts) != 2 {
		t.Fatalf("attachments = %d, want 2", len(atts))
	}
	if atts[0].Filename != "contratto.pdf" || atts[1].Filename != "appendice.txt" {
		t.Fatalf("filenames = %q, %q — must be base names, not paths", atts[0].Filename, atts[1].Filename)
	}
	if string(atts[0].Data) != "%PDF-1.4 fake" || string(atts[1].Data) != "testo" {
		t.Fatal("attachment bytes differ from the file contents")
	}
	// NewAttachment infers or falls back to application/octet-stream:
	// never empty, whatever the machine's mime table knows.
	if atts[0].ContentType == "" || atts[1].ContentType == "" {
		t.Fatal("content types must be inferred (or the octet-stream fallback)")
	}

	// A missing file is a scheduling-time error, never a fire-time surprise.
	if _, err := loadAttachments([]string{filepath.Join(dir, "nope.pdf")}); err == nil {
		t.Fatal("missing attachment file accepted")
	}
}

// TestSubmitJobWithAttachments: the full -attach wiring — loadAttachments
// plus submitJob against a live daemon — must land the exact attachment
// bytes in the store. The daemon needs no attachment support of its own:
// MailContent travels over the socket verbatim, and ScheduleWithID
// persists it whole.
func TestSubmitJobWithAttachments(t *testing.T) {
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

	pdf := filepath.Join(dir, "contratto.pdf")
	pdfData := []byte("%PDF-1.4 fake contract bytes")
	if err := os.WriteFile(pdf, pdfData, 0o600); err != nil {
		t.Fatal(err)
	}
	atts, err := loadAttachments([]string{pdf})
	if err != nil {
		t.Fatalf("loadAttachments: %v", err)
	}

	content := mailer.MailContent{
		From:         "sender@pec.test",
		To:           []string{"dest@pec.test"},
		Subject:      "con allegato",
		Body:         "vedi allegato",
		TipoRicevuta: mailer.RicevutaCompleta,
		Attachments:  atts,
	}
	cfg := mailer.MailConfig{Hostname: "smtp.pec.test", Port: "465", Username: "sender@pec.test", Password: "x"}

	// submitJob is the client path: it prints the ack; the proof is the
	// store — the daemon must have collected, validated and persisted the
	// job with its attachment untouched.
	submitJob(sock, s, cfg, &content, time.Now().Add(time.Hour))

	jobs, err := store.Load()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("store: err=%v jobs=%d, want the submitted job", err, len(jobs))
	}
	got := jobs[0].Content.Attachments
	if len(got) != 1 || got[0].Filename != "contratto.pdf" || !bytes.Equal(got[0].Data, pdfData) {
		t.Fatalf("persisted attachment = %+v, want contratto.pdf with the exact bytes", got)
	}
}
