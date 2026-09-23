package jobber

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gomailer/internal/mailer"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	st, err := OpenStore(filepath.Join(t.TempDir(), "jobs.json"))
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return st
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

// warmToken is the fake session returned by a successful fakeCourier.Warm.
type warmToken struct{}

type sendCall struct {
	at      time.Time
	session any
	msg     []byte
}

// fakeCourier is the test Courier: it records the instant of every phase
// and, by returning the body as the prepared message, lets tests identify
// which job each send belongs to.
type fakeCourier struct {
	mu         sync.Mutex
	prepareErr error
	warmErr    error
	sendErr    error
	prepares   []time.Time
	warms      []time.Time
	sends      []sendCall
}

func (f *fakeCourier) Prepare(cfg mailer.MailConfig, c *mailer.MailContent) ([]byte, error) {
	f.mu.Lock()
	f.prepares = append(f.prepares, time.Now())
	f.mu.Unlock()
	return []byte(c.Body), f.prepareErr
}

func (f *fakeCourier) Warm(cfg mailer.MailConfig) (any, error) {
	f.mu.Lock()
	f.warms = append(f.warms, time.Now())
	f.mu.Unlock()
	if f.warmErr != nil {
		return nil, f.warmErr
	}
	return &warmToken{}, nil
}

func (f *fakeCourier) Send(cfg mailer.MailConfig, session any, msg []byte, to []string) error {
	f.mu.Lock()
	f.sends = append(f.sends, sendCall{at: time.Now(), session: session, msg: msg})
	f.mu.Unlock()
	return f.sendErr
}

func (f *fakeCourier) Discard(session any) {}

// coldCourier is a fakeCourier with warmup disabled.
func coldCourier(s *Scheduler) *fakeCourier {
	c := &fakeCourier{}
	s.WarmLead = 0
	s.Courier = c
	return c
}

func TestScheduleValidation(t *testing.T) {
	s := NewScheduler(testStore(t))
	cfg := testConfig()

	// Empty body must be rejected at scheduling time, never at fire time.
	c := testContent("")
	if _, err := s.Schedule(cfg, &c, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("empty body accepted")
	}

	// No recipients.
	c = testContent("hi")
	c.To = nil
	if _, err := s.Schedule(cfg, &c, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("missing recipients accepted")
	}

	// From must be the certified identity.
	c = testContent("hi")
	c.From = "other@pec.test"
	if _, err := s.Schedule(cfg, &c, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("mismatched From accepted")
	}

	// A deadline in the past is rejected even with MinLead=0.
	c = testContent("hi")
	if _, err := s.Schedule(cfg, &c, time.Now().Add(-time.Minute)); err == nil {
		t.Fatal("past deadline accepted")
	}
}

func TestMinLead(t *testing.T) {
	s := NewScheduler(testStore(t))
	s.MinLead = 24 * time.Hour
	c := testContent("hi")
	if _, err := s.Schedule(testConfig(), &c, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("job scheduled within MinLead")
	}
}

func TestSchedulePersistsPending(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)

	content := testContent("hello")
	job, err := s.Schedule(testConfig(), &content, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	jobs, err := store.Load()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("store after schedule: err=%v jobs=%d", err, len(jobs))
	}
	if jobs[0].State != StatePending {
		t.Fatalf("state = %q, want pending", jobs[0].State)
	}
	if jobs[0].MessageID == "" || jobs[0].Content.MessageID != jobs[0].MessageID {
		t.Fatal("Message-ID not pre-generated/persisted")
	}
	if jobs[0].ID != job.ID {
		t.Fatal("ID mismatch")
	}

	// The store contains credentials: it must be 0600.
	fi, err := os.Stat(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("store mode = %o, want 600", fi.Mode().Perm())
	}
}

func TestStoreRoundTrip(t *testing.T) {
	store := testStore(t)
	base := Job{
		ID:        "j1",
		MessageID: "<j1@pec.test>",
		FireAt:    time.Now().Add(time.Hour),
		CreatedAt: time.Now(),
		Content:   testContent("one"),
		Config:    testConfig(),
		State:     StatePending,
	}
	j2 := base
	j2.ID = "j2"

	if err := store.Add(base); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := store.Add(j2); err != nil {
		t.Fatalf("Add: %v", err)
	}

	jobs, err := store.Load()
	if err != nil || len(jobs) != 2 {
		t.Fatalf("Load: err=%v jobs=%d", err, len(jobs))
	}

	sent := base
	sent.State = StateSent
	sent.Result = &JobResult{FiredAt: time.Now(), SendDuration: time.Second}
	if err := store.Update(sent); err != nil {
		t.Fatalf("Update: %v", err)
	}

	jobs, err = store.Load()
	if err != nil {
		t.Fatalf("Load after update: %v", err)
	}
	for _, j := range jobs {
		switch j.ID {
		case "j1":
			if j.State != StateSent || j.Result == nil {
				t.Fatalf("j1 after update: %+v", j)
			}
		case "j2":
			if j.State != StatePending {
				t.Fatalf("j2 must remain pending, got %q", j.State)
			}
		}
	}
}

func TestFireTimingPrecision(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := coldCourier(s)
	fireAt := time.Now().Add(150 * time.Millisecond)

	content := testContent("ping")
	if _, err := s.Schedule(testConfig(), &content, fireAt); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(c.sends) != 1 {
		t.Fatalf("send calls = %d, want 1", len(c.sends))
	}
	if lateness := c.sends[0].at.Sub(fireAt); lateness < 0 || lateness > 500*time.Millisecond {
		t.Fatalf("lateness = %v, want 0..500ms", lateness)
	}
	if c.sends[0].session != nil {
		t.Fatal("cold path must pass a nil session")
	}

	jobs, _ := store.Load()
	if jobs[0].State != StateSent || jobs[0].Result == nil || jobs[0].Result.Err != "" {
		t.Fatalf("terminal state = %q result=%+v, want sent with clean result", jobs[0].State, jobs[0].Result)
	}
	if jobs[0].Result.Lateness < 0 {
		t.Fatalf("negative recorded lateness: %+v", jobs[0].Result)
	}
}

func TestCatchUpFiresImmediately(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := coldCourier(s)
	due := time.Now().Add(-30 * time.Second)
	err := store.Add(Job{
		ID:        "late",
		MessageID: "<late@pec.test>",
		FireAt:    due,
		CreatedAt: due.Add(-time.Minute),
		Content:   testContent("late"),
		Config:    testConfig(),
		State:     StatePending,
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(c.sends) != 1 {
		t.Fatalf("send calls = %d, want 1 (catch-up)", len(c.sends))
	}
	if lateness := c.sends[0].at.Sub(due); lateness < 25*time.Second {
		t.Fatalf("catch-up lateness = %v, want >= 25s", lateness)
	}

	jobs, _ := store.Load()
	if jobs[0].State != StateSent || jobs[0].Result.Lateness < 25*time.Second {
		t.Fatalf("result = %+v, want sent with recorded lateness", jobs[0].Result)
	}
}

func TestInflightNeverAutoResent(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := coldCourier(s)
	t0 := time.Now().Add(-time.Minute)
	err := store.Add(Job{
		ID:        "crashed",
		MessageID: "<crashed@pec.test>",
		FireAt:    t0,
		CreatedAt: t0,
		Content:   testContent("ambiguous"),
		Config:    testConfig(),
		State:     StateInflight,
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(c.sends) != 0 {
		t.Fatalf("send calls = %d, want 0 (never auto-resend)", len(c.sends))
	}
	jobs, _ := store.Load()
	if jobs[0].State != StateFailed || jobs[0].Result == nil || jobs[0].Result.Err == "" {
		t.Fatalf("state = %q result=%+v, want failed with manual-verification note", jobs[0].State, jobs[0].Result)
	}
}

// TestLongLeadDriftBounded guards the chunked re-arm: a single long-armed
// timer accumulates wake drift proportional to the armed duration (measured
// ~1ms/s on virtualized clocks — a 5s single arm wakes ~5ms late here),
// while chunked re-arming keeps lateness at the short-arm jitter level.
//
// FiredAt (the wake decision) is asserted tightly: it isolates the timer
// from everything else. The send itself starts a few ms after FiredAt —
// the inflight fsync deliberately precedes the send — so the end-to-end
// bound for the actual send call is correspondingly looser.
func TestLongLeadDriftBounded(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := coldCourier(s)
	fireAt := time.Now().Add(5 * time.Second)

	content := testContent("long lead")
	if _, err := s.Schedule(testConfig(), &content, fireAt); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(c.sends) != 1 {
		t.Fatalf("send calls = %d, want 1", len(c.sends))
	}

	jobs, err := store.Load()
	if err != nil || len(jobs) != 1 || jobs[0].State != StateSent || jobs[0].Result == nil {
		t.Fatalf("store after run: err=%v jobs=%d state=%q", err, len(jobs), jobs[0].State)
	}
	// Wake-decision drift: chunked re-arm keeps this at sub-ms wake jitter
	// (a regressed single-arm timer drifts ~5ms over a 5s lead here).
	if lateness := jobs[0].Result.FiredAt.Sub(fireAt); lateness < 0 || lateness > 2*time.Millisecond {
		t.Fatalf("5s lead woke %v late, want <= 2ms (chunked re-arm regression)", lateness)
	}
	// End-to-end: the send additionally waits out the inflight fsync.
	if lateness := c.sends[0].at.Sub(fireAt); lateness < 0 || lateness > 50*time.Millisecond {
		t.Fatalf("5s lead sent %v late, want <= 50ms", lateness)
	}
}

func TestWakeRearmsForEarlierJob(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := coldCourier(s)

	// Job A is due in 300ms and gets armed first...
	contentA := testContent("A")
	if _, err := s.Schedule(testConfig(), &contentA, time.Now().Add(300*time.Millisecond)); err != nil {
		t.Fatalf("Schedule A: %v", err)
	}
	// ...the loop starts sleeping on it...
	done := make(chan struct{})
	go func() {
		if err := s.Run(); err != nil {
			t.Errorf("Run: %v", err)
		}
		close(done)
	}()
	// ...and job B sneaks in mid-sleep with an earlier deadline.
	time.Sleep(60 * time.Millisecond)
	contentB := testContent("B")
	fireB := time.Now().Add(120 * time.Millisecond)
	if _, err := s.Schedule(testConfig(), &contentB, fireB); err != nil {
		t.Fatalf("Schedule B: %v", err)
	}
	<-done

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 2 {
		t.Fatalf("send calls = %d, want 2", len(c.sends))
	}
	// B must fire FIRST and on ITS deadline — not A's: this proves the
	// sleep was interrupted and the timer re-armed (without the wake
	// channel B would fire at A's deadline, ~120ms late).
	if string(c.sends[0].msg) != "B" {
		t.Fatalf("first fired = %q, want B", c.sends[0].msg)
	}
	if lateness := c.sends[0].at.Sub(fireB); lateness < 0 || lateness > 60*time.Millisecond {
		t.Fatalf("job B lateness = %v, want 0..60ms (proves re-arm)", lateness)
	}
}

// TestWarmupOffCriticalPath verifies the warm phase: the provider session
// opens WarmLead before the fire time, and the fire itself only delivers.
func TestWarmupOffCriticalPath(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	s.WarmLead = 2 * time.Second
	c := &fakeCourier{}
	s.Courier = c
	fireAt := time.Now().Add(3 * time.Second)

	content := testContent("warm me")
	if _, err := s.Schedule(testConfig(), &content, fireAt); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 1 || len(c.warms) != 1 {
		t.Fatalf("sends=%d warms=%d, want 1 and 1", len(c.sends), len(c.warms))
	}
	// The warm session must be the one used at fire time.
	if _, ok := c.sends[0].session.(*warmToken); !ok {
		t.Fatal("fire must deliver over the warmed session")
	}
	// Warmup happened inside the lead window: at least ~800ms before fire.
	if early := fireAt.Sub(c.warms[0]); early < 800*time.Millisecond {
		t.Fatalf("warmup only %v before fire, want >= 800ms (lead window)", early)
	}
	// Build precedes the session open.
	if len(c.prepares) != 1 || c.prepares[0].After(c.warms[0]) {
		t.Fatal("prepare must run exactly once, before warm")
	}
	// And the delivery itself still fires on time.
	if lateness := c.sends[0].at.Sub(fireAt); lateness < 0 || lateness > 500*time.Millisecond {
		t.Fatalf("warm-path lateness = %v, want 0..500ms", lateness)
	}
}

// TestWarmupFailureFallsBackCold: a failed warmup must neither delay nor
// cancel the job — it degrades to a cold send at the fire time.
func TestWarmupFailureFallsBackCold(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	s.WarmLead = 2 * time.Second
	c := &fakeCourier{warmErr: errors.New("provider unreachable")}
	s.Courier = c
	fireAt := time.Now().Add(3 * time.Second)

	content := testContent("cold fallback")
	if _, err := s.Schedule(testConfig(), &content, fireAt); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 1 {
		t.Fatalf("send calls = %d, want 1", len(c.sends))
	}
	if c.sends[0].session != nil {
		t.Fatal("failed warmup must degrade to a cold (nil) session")
	}
	if lateness := c.sends[0].at.Sub(fireAt); lateness < 0 || lateness > 500*time.Millisecond {
		t.Fatalf("cold fallback lateness = %v, want 0..500ms", lateness)
	}

	jobs, _ := store.Load()
	if jobs[0].State != StateSent {
		t.Fatalf("state = %q, want sent", jobs[0].State)
	}
}

// TestWarmSessionSurvivesRequeue: when an earlier job interrupts the sleep
// of a warmed job, the warm state must be kept and reused when the warmed
// job's turn comes — not reopened.
func TestWarmSessionSurvivesRequeue(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	s.WarmLead = 2500 * time.Millisecond
	c := &fakeCourier{}
	s.Courier = c

	// Job A: fires at +3s, warmup window opens at +0.5s.
	contentA := testContent("A")
	fireA := time.Now().Add(3 * time.Second)
	if _, err := s.Schedule(testConfig(), &contentA, fireA); err != nil {
		t.Fatalf("Schedule A: %v", err)
	}
	done := make(chan struct{})
	go func() {
		if err := s.Run(); err != nil {
			t.Errorf("Run: %v", err)
		}
		close(done)
	}()

	// Job B sneaks in AFTER A warmed up, with an earlier deadline (B's
	// own warm window is already gone: B fires cold).
	time.Sleep(1200 * time.Millisecond)
	contentB := testContent("B")
	fireB := time.Now().Add(400 * time.Millisecond)
	if _, err := s.Schedule(testConfig(), &contentB, fireB); err != nil {
		t.Fatalf("Schedule B: %v", err)
	}
	<-done

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 2 || len(c.warms) != 1 {
		t.Fatalf("sends=%d warms=%d, want 2 sends and 1 warm (A only)", len(c.sends), len(c.warms))
	}
	// B fired first, cold.
	if string(c.sends[0].msg) != "B" || c.sends[0].session != nil {
		t.Fatal("B must fire first, cold (no warm window)")
	}
	// A fired second, over its surviving warm session.
	if string(c.sends[1].msg) != "A" {
		t.Fatal("A must fire second")
	}
	if _, ok := c.sends[1].session.(*warmToken); !ok {
		t.Fatal("A's warm session must survive the requeue and be reused")
	}
	if lateness := c.sends[1].at.Sub(fireA); lateness < 0 || lateness > 500*time.Millisecond {
		t.Fatalf("A lateness = %v, want 0..500ms", lateness)
	}
}
