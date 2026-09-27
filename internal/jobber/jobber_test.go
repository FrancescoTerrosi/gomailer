package jobber

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// stagedToken is the fake held transaction returned by a successful
// fakeCourier.Stage when staging is enabled (staged == nil means the
// courier opts out: the job then delivers cold over the warm session,
// the pre-staging behavior).
type stagedToken struct{}

type sendCall struct {
	at      time.Time
	session any
	msg     []byte
}

// fakeCourier is the test Courier: it records the instant of every phase
// and, by returning the body as the prepared message, lets tests identify
// which job each send belongs to.
type fakeCourier struct {
	mu          sync.Mutex
	prepareErr  error
	warmErr     error
	stageErr    error
	sendErr     error
	commitErr   error
	commitDelay time.Duration // how long Commit takes before its verdict
	staged      any           // nil = opt out of staging (pre-staging behavior)
	prepares    []time.Time
	warms       []time.Time
	stages      []time.Time
	commits     []time.Time
	sends       []sendCall
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

func (f *fakeCourier) Stage(cfg mailer.MailConfig, session any, msg []byte, to []string) (any, error) {
	f.mu.Lock()
	f.stages = append(f.stages, time.Now())
	f.mu.Unlock()
	if f.stageErr != nil {
		return nil, f.stageErr
	}
	return f.staged, nil
}

func (f *fakeCourier) Commit(cfg mailer.MailConfig, staged any) error {
	f.mu.Lock()
	f.commits = append(f.commits, time.Now())
	f.mu.Unlock()
	if f.commitDelay > 0 {
		time.Sleep(f.commitDelay)
	}
	return f.commitErr
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

// TestPayloadQuota: the per-job payload admission — over-quota dies at
// scheduling time with the limit named and nothing persisted; the
// boundary is inclusive; 0 disables the check; and a replay of an
// already-persisted job converges even when the quota would now reject
// it (idempotency outranks admission control).
func TestPayloadQuota(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	if s.MaxPayload != defaultMaxPayload {
		t.Fatalf("default quota = %d, want the provider-mirrored %d", s.MaxPayload, defaultMaxPayload)
	}
	s.MaxPayload = 1000

	big := testContent("hi")
	big.Attachments = []mailer.Attachment{{Filename: "big.bin", ContentType: "application/octet-stream", Data: make([]byte, 2000)}}
	if _, err := s.ScheduleWithID("quota-over", testConfig(), &big, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("over-quota payload accepted")
	} else if !strings.Contains(err.Error(), "payload") || !strings.Contains(err.Error(), humanBytes(1000)) {
		t.Fatalf("error must name the payload and the limit: %v", err)
	}
	if jobs, _ := store.Load(); len(jobs) != 0 {
		t.Fatalf("over-quota submission persisted %d row(s)", len(jobs))
	}

	// Exactly at the boundary: accepted (the rejection is strictly above).
	edge := testContent("hi") // body: 2 bytes
	edge.Attachments = []mailer.Attachment{{Filename: "edge.bin", ContentType: "application/octet-stream", Data: make([]byte, 998)}}
	if _, err := s.ScheduleWithID("quota-edge", testConfig(), &edge, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("at-the-boundary payload rejected: %v", err)
	}

	// Disabled: no check at all.
	s.MaxPayload = 0
	if _, err := s.ScheduleWithID("quota-off", testConfig(), &big, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("quota disabled but the big payload was rejected: %v", err)
	}

	// Idempotency outranks admission: re-enabling the quota must not
	// break the replay of the job persisted while it was off.
	s.MaxPayload = 1000
	again, err := s.ScheduleWithID("quota-off", testConfig(), &big, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("replay of a now-over-quota job must converge: %v", err)
	}
	if again.ID != "quota-off" || again.State != StatePending {
		t.Fatalf("replay = %+v; want the existing pending job returned as-is", again)
	}
	if jobs, _ := store.Load(); len(jobs) != 2 {
		t.Fatalf("store rows = %d, want 2 (the over-quota attempt was never persisted)", len(jobs))
	}
}

// TestUpdateStripsTerminalCredentials: a row that will never fire again
// carries no credential — Update is the single choke point every state
// write flows through, so the strip is mechanical. It must NOT fire
// early: pending and inflight rows keep theirs, the firing loop
// replays them at fire time.
func TestUpdateStripsTerminalCredentials(t *testing.T) {
	store := testStore(t)
	base := Job{
		ID:        "strip",
		MessageID: "<strip@pec.test>",
		FireAt:    time.Now().Add(time.Hour),
		CreatedAt: time.Now(),
		Content:   testContent("x"),
		Config:    testConfig(), // carries Password "secret"
	}
	for _, st := range []JobState{StatePending, StateInflight} {
		j := base
		j.State = st
		if err := store.Update(j); err != nil {
			t.Fatalf("Update(%s): %v", st, err)
		}
		if got, ok, _ := store.GetByID("strip"); !ok || got.Config.Password != "secret" {
			t.Fatalf("%s row lost its credential — fire time still needs it", st)
		}
	}
	for _, st := range []JobState{StateSent, StateFailed} {
		j := base
		j.State = st
		if err := store.Update(j); err != nil {
			t.Fatalf("Update(%s): %v", st, err)
		}
		if got, ok, _ := store.GetByID("strip"); !ok || got.Config.Password != "" {
			t.Fatalf("%s row still carries its credential — it will never fire again", st)
		}
	}
}

// TestFiredJobLandsCredentialless: end to end — a job that fired leaves
// a sent row with no credential behind.
func TestFiredJobLandsCredentialless(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	coldCourier(s)
	content := testContent("bye bye credential")
	if _, err := s.ScheduleWithID("e2e-strip", testConfig(), &content, time.Now().Add(100*time.Millisecond)); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	jobs, _ := store.Load()
	if len(jobs) != 1 || jobs[0].State != StateSent {
		t.Fatalf("state = %q, want sent", jobs[0].State)
	}
	if jobs[0].Config.Password != "" {
		t.Fatal("the sent row still carries the mailbox password")
	}
}

// TestFailedBeforeSendAlsoStripped: a job that never reached the wire
// (prepare failed) is failed-and-credentialless too — never auto-resent,
// so the credential is dead weight there as well.
func TestFailedBeforeSendAlsoStripped(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	s.WarmLead = 0
	s.Courier = &fakeCourier{prepareErr: errors.New("broken build")}
	content := testContent("never sent")
	if _, err := s.ScheduleWithID("fail-strip", testConfig(), &content, time.Now().Add(100*time.Millisecond)); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	jobs, _ := store.Load()
	if jobs[0].State != StateFailed || jobs[0].Config.Password != "" {
		t.Fatalf("failed row: state=%q pass=%q", jobs[0].State, jobs[0].Config.Password)
	}
}

// TestBlankTerminalPasswordsSweep: rows written by an older gomailer
// (terminal, still credentialed) are swept in ONE atomic rewrite;
// pending rows keep their live credential; a second sweep rewrites
// nothing.
func TestBlankTerminalPasswordsSweep(t *testing.T) {
	store := testStore(t)
	old := Job{
		ID:        "old-sent",
		MessageID: "<old-sent@pec.test>",
		FireAt:    time.Now().Add(-time.Hour),
		CreatedAt: time.Now().Add(-2 * time.Hour),
		Content:   testContent("old"),
		Config:    testConfig(),
		State:     StateSent,
		Result:    &JobResult{FiredAt: time.Now().Add(-time.Hour)},
	}
	older := old
	older.ID, older.MessageID = "old-failed", "<old-failed@pec.test>"
	older.State = StateFailed
	older.Result = &JobResult{FiredAt: time.Now().Add(-time.Hour), Err: "x"}
	pending := Job{
		ID:        "live",
		MessageID: "<live@pec.test>",
		FireAt:    time.Now().Add(time.Hour),
		CreatedAt: time.Now(),
		Content:   testContent("live"),
		Config:    testConfig(),
		State:     StatePending,
	}
	for _, j := range []Job{old, older, pending} {
		if err := store.Add(j); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	swept, err := store.BlankTerminalPasswords()
	if err != nil || swept != 2 {
		t.Fatalf("sweep = %d, err = %v; want 2, nil", swept, err)
	}
	jobs, _ := store.Load()
	for _, j := range jobs {
		want := ""
		if j.ID == "live" {
			want = "secret" // pending: the credential is replayed at fire time
		}
		if j.Config.Password != want {
			t.Fatalf("job %s password = %q, want %q", j.ID, j.Config.Password, want)
		}
	}

	// Idempotent: nothing left to sweep, nothing rewritten.
	before, err := os.Stat(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if swept, err := store.BlankTerminalPasswords(); err != nil || swept != 0 {
		t.Fatalf("second sweep = %d, err = %v; want 0, nil", swept, err)
	}
	after, err := os.Stat(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("an empty sweep rewrote the store")
	}
}

// TestBootSweepsOldTerminalCredentials: the firing process sweeps stale
// credentials at boot (before the queue starts or the socket accepts).
func TestBootSweepsOldTerminalCredentials(t *testing.T) {
	store := testStore(t)
	if err := store.Add(Job{
		ID:        "stale",
		MessageID: "<stale@pec.test>",
		FireAt:    time.Now().Add(-time.Hour),
		CreatedAt: time.Now().Add(-2 * time.Hour),
		Content:   testContent("stale"),
		Config:    testConfig(),
		State:     StateSent,
		Result:    &JobResult{FiredAt: time.Now().Add(-time.Hour)},
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	s := NewScheduler(store)
	coldCourier(s)
	if err := s.Run(); err != nil { // empty queue: load, sweep, done
		t.Fatalf("run: %v", err)
	}
	jobs, _ := store.Load()
	if len(jobs) != 1 || jobs[0].Config.Password != "" {
		t.Fatalf("boot must sweep the stale credential: state=%q pass=%q", jobs[0].State, jobs[0].Config.Password)
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

// countWarmSends returns how many sends went over a warm (non-nil) session.
func countWarmSends(sends []sendCall) int {
	n := 0
	for _, s := range sends {
		if s.session != nil {
			n++
		}
	}
	return n
}

// TestSameDeadlineAllWarm: jobs that share a deadline must EACH get their
// warm session, not just the one that happened to be head when the window
// opened. Under the old strict "warm only if still before the window"
// rule the loop dispatched the first job at T-WarmLead and then judged
// every follower — evaluated a microsecond later — as already past the
// window, firing it cold at FireAt. Eager dispatch hands off every job
// whose window is open with the full lead still ahead of it.
func TestSameDeadlineAllWarm(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	s.WarmLead = 2 * time.Second
	c := &fakeCourier{}
	s.Courier = c

	fireAt := time.Now().Add(3 * time.Second)
	for i := 0; i < 3; i++ {
		content := testContent("same")
		if _, err := s.Schedule(testConfig(), &content, fireAt); err != nil {
			t.Fatalf("Schedule %d: %v", i, err)
		}
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 3 || len(c.warms) != 3 {
		t.Fatalf("sends=%d warms=%d, want 3 and 3 (every same-deadline job warms)", len(c.sends), len(c.warms))
	}
	if got := countWarmSends(c.sends); got != 3 {
		t.Fatalf("warm sends = %d, want 3 (followers must not fall to the cold path)", got)
	}
}

// TestLateScheduledStaysCold: eager dispatch must NOT warm a job that was
// scheduled inside its warm window with too little time left to finish
// warmup before the deadline — it fires cold, exactly as before.
func TestLateScheduledStaysCold(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	s.WarmLead = 2 * time.Second
	c := &fakeCourier{}
	s.Courier = c

	// 500ms from now: inside the 2s window, below the 1s warm budget.
	content := testContent("late")
	if _, err := s.Schedule(testConfig(), &content, time.Now().Add(800*time.Millisecond)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 1 {
		t.Fatalf("sends = %d, want 1", len(c.sends))
	}
	if c.sends[0].session != nil || len(c.warms) != 0 {
		t.Fatalf("late-scheduled job warmed (warms=%d): too little time remained", len(c.warms))
	}
}

// TestColdJobDoesNotShadowFollowers: a cold job must not drag later jobs
// whose warm windows opened before its FireAt down with it. Under the old
// rule the loop parked on the cold job until its FireAt, so every follower
// evaluated then was already inside its own window and also fired cold.
func TestColdJobDoesNotShadowFollowers(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	s.WarmLead = 2 * time.Second
	c := &fakeCourier{}
	s.Courier = c

	// A: 500ms out, inside its window and below the warm budget -> cold.
	// B and C are far enough out that, once A is dispatched, they are
	// still evaluated before their windows open -> they must warm.
	ca := testContent("A")
	if _, err := s.Schedule(testConfig(), &ca, time.Now().Add(800*time.Millisecond)); err != nil {
		t.Fatalf("Schedule A: %v", err)
	}
	cb := testContent("B")
	if _, err := s.Schedule(testConfig(), &cb, time.Now().Add(2500*time.Millisecond)); err != nil {
		t.Fatalf("Schedule B: %v", err)
	}
	cc := testContent("C")
	if _, err := s.Schedule(testConfig(), &cc, time.Now().Add(4500*time.Millisecond)); err != nil {
		t.Fatalf("Schedule C: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 3 || len(c.warms) != 2 {
		t.Fatalf("sends=%d warms=%d, want 3 sends and 2 warm (B and C; A cold)", len(c.sends), len(c.warms))
	}
	// A fires first (cold); B and C follow over warm sessions.
	if string(c.sends[0].msg) != "A" || c.sends[0].session != nil {
		t.Fatalf("A must fire first, cold: msg=%q session=%v", c.sends[0].msg, c.sends[0].session)
	}
	if _, ok := c.sends[1].session.(*warmToken); !ok || string(c.sends[1].msg) != "B" {
		t.Fatalf("B must fire warm after A: msg=%q session=%v", c.sends[1].msg, c.sends[1].session)
	}
	if _, ok := c.sends[2].session.(*warmToken); !ok || string(c.sends[2].msg) != "C" {
		t.Fatalf("C must fire warm: msg=%q session=%v", c.sends[2].msg, c.sends[2].session)
	}
}

// --- Staged delivery: hold the dot, "just go" (TODO.md item 6) ---

// TestStagedCommitFiresAtDeadline: a warmed job stages its whole
// transaction early, then holds the dot and commits it AT the fire time —
// the Send path is never taken, and the staging happened off the critical
// path (well before the deadline).
func TestStagedCommitFiresAtDeadline(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := &fakeCourier{staged: &stagedToken{}}
	s.Courier = c
	fireAt := time.Now().Add(2500 * time.Millisecond)

	content := testContent("hold the dot")
	if _, err := s.Schedule(testConfig(), &content, fireAt); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.warms) != 1 || len(c.stages) != 1 || len(c.commits) != 1 {
		t.Fatalf("warms=%d stages=%d commits=%d, want 1/1/1", len(c.warms), len(c.stages), len(c.commits))
	}
	if len(c.sends) != 0 {
		t.Fatalf("sends = %d, want 0 (a staged job commits the dot; it never takes the cold Send path)", len(c.sends))
	}
	if lateness := c.commits[0].Sub(fireAt); lateness < 0 || lateness > 500*time.Millisecond {
		t.Fatalf("dot committed %v off the deadline, want 0..500ms", lateness)
	}
	// The whole transaction was at the provider well before the fire
	// time: staging ran off the critical path.
	if early := fireAt.Sub(c.stages[0]); early < time.Second {
		t.Fatalf("staged only %v before fire, want >= 1s (off the critical path)", early)
	}

	jobs, _ := store.Load()
	if jobs[0].State != StateSent || jobs[0].Result == nil || jobs[0].Result.Err != "" {
		t.Fatalf("result = %+v, want a clean sent", jobs[0].Result)
	}
}

// TestStagedDotOutIsTerminalNeverResent: a commit whose dot left the wire
// but whose answer was lost is TERMINAL ambiguity — recorded failed with
// the manual-verification rule, and never retried (a second attempt could
// duplicate a certified send).
func TestStagedDotOutIsTerminalNeverResent(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := &fakeCourier{staged: &stagedToken{}, commitErr: fmt.Errorf("%w: reading the answer: EOF", mailer.ErrDotOut)}
	s.Courier = c

	content := testContent("ambiguous dot")
	// >= 1s out: inside the warm budget, so the job stages and commits.
	if _, err := s.Schedule(testConfig(), &content, time.Now().Add(1200*time.Millisecond)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.commits) != 1 {
		t.Fatalf("commits = %d, want exactly 1", len(c.commits))
	}
	if len(c.sends) != 0 {
		t.Fatalf("sends = %d, want 0 (an uncertain dot must never be re-attempted)", len(c.sends))
	}
	jobs, _ := store.Load()
	if jobs[0].State != StateFailed || jobs[0].Result == nil {
		t.Fatalf("state = %q, want failed", jobs[0].State)
	}
	if err := jobs[0].Result.Err; !strings.Contains(err, "NOT auto-resent") || !strings.Contains(err, "verify the recipient inbox") {
		t.Fatalf("the ambiguity must carry the manual-verification rule: %q", err)
	}
}

// TestStagingFailureFiresColdAtDeadline: a staging failure is pre-dot by
// construction — discard the session, deliver cold at the fire time
// ("just go"), never late by more than the fallback itself.
func TestStagingFailureFiresColdAtDeadline(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := &fakeCourier{stageErr: errors.New("upload died")}
	s.Courier = c
	fireAt := time.Now().Add(2 * time.Second)

	content := testContent("cold after staging")
	if _, err := s.Schedule(testConfig(), &content, fireAt); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.warms) != 1 || len(c.stages) != 1 {
		t.Fatalf("warms=%d stages=%d, want 1/1 (staging was attempted)", len(c.warms), len(c.stages))
	}
	if len(c.sends) != 1 || c.sends[0].session != nil {
		t.Fatalf("sends = %d (session=%v), want exactly one COLD send over a discarded session", len(c.sends), c.sends[0].session)
	}
	if lateness := c.sends[0].at.Sub(fireAt); lateness < 0 || lateness > 500*time.Millisecond {
		t.Fatalf("cold fallback lateness = %v, want 0..500ms", lateness)
	}
	jobs, _ := store.Load()
	if jobs[0].State != StateSent {
		t.Fatalf("state = %q, want sent", jobs[0].State)
	}
}

// TestCommitDotWriteFailureFallsBackCold: a held transaction lost BEFORE
// the dot (reset connection, truncated record) left nothing on the wire —
// "just go" sends cold at the fire time, exactly once.
func TestCommitDotWriteFailureFallsBackCold(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := &fakeCourier{staged: &stagedToken{}, commitErr: errors.New("connection reset by peer")}
	s.Courier = c
	fireAt := time.Now().Add(2 * time.Second)

	content := testContent("held, then lost")
	if _, err := s.Schedule(testConfig(), &content, fireAt); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.commits) != 1 || len(c.sends) != 1 {
		t.Fatalf("commits=%d sends=%d, want 1/1 (one failed dot attempt, one cold send)", len(c.commits), len(c.sends))
	}
	if c.sends[0].session != nil {
		t.Fatal("the fallback must be cold: the held session is dead")
	}
	if lateness := c.sends[0].at.Sub(fireAt); lateness < 0 || lateness > 500*time.Millisecond {
		t.Fatalf("fallback lateness = %v, want 0..500ms (it starts AT the fire time)", lateness)
	}
	jobs, _ := store.Load()
	if jobs[0].State != StateSent {
		t.Fatalf("state = %q, want sent", jobs[0].State)
	}
}

// TestFallbackStampCarriesTheLostHold: when the held commit fails before
// the dot, the delivery that actually runs is the cold fallback — and the
// record must stamp THAT attempt (JobResult.FiredAt: "the instant the
// delivery attempt that produced the outcome began"). A commit that burns
// its dot budget before failing pushes the fallback late by exactly that
// much, and the recorded lateness must show the cost, not the entry
// instant (regression: the stamp used to be taken at fire entry, silently
// dropping the dead held transaction's time).
func TestFallbackStampCarriesTheLostHold(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := &fakeCourier{
		staged:      &stagedToken{},
		commitErr:   errors.New("connection reset by peer"),
		commitDelay: 300 * time.Millisecond,
	}
	s.Courier = c
	fireAt := time.Now().Add(2 * time.Second)

	content := testContent("lost hold")
	if _, err := s.Schedule(testConfig(), &content, fireAt); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.commits) != 1 || len(c.sends) != 1 {
		t.Fatalf("commits=%d sends=%d, want 1/1 (lost hold, then one cold send)", len(c.commits), len(c.sends))
	}
	jobs, _ := store.Load()
	res := jobs[0].Result
	if res == nil || jobs[0].State != StateSent {
		t.Fatalf("state=%q result=%v, want a clean sent", jobs[0].State, res)
	}
	// The fallback began ~300ms after the fire time (the lost hold's
	// cost): the recorded lateness must show it, not the entry instant.
	if res.Lateness < 250*time.Millisecond {
		t.Fatalf("recorded lateness = %v, want >= 250ms (the failed commit's cost must be visible)", res.Lateness)
	}
	// FiredAt is the fallback send's start, not the fire-phase entry.
	if d := c.sends[0].at.Sub(res.FiredAt); d < -50*time.Millisecond || d > 50*time.Millisecond {
		t.Fatalf("FiredAt is %v off the fallback send's start; it must stamp the attempt that actually ran", d)
	}
}

// overrunningCourier simulates an upload that overruns the fire time:
// Stage blocks until released, past the deadline. "Just go" then demands
// finish-then-dot: exactly one commit, after staging completes — never an
// abandoned upload, never a second attempt.
type overrunningCourier struct {
	mu      sync.Mutex
	stages  []time.Time
	commits []time.Time
	sends   []sendCall
	release chan struct{}
}

func (c *overrunningCourier) Prepare(cfg mailer.MailConfig, ct *mailer.MailContent) ([]byte, error) {
	return []byte(ct.Body), nil
}
func (c *overrunningCourier) Warm(cfg mailer.MailConfig) (any, error) { return &warmToken{}, nil }
func (c *overrunningCourier) Stage(cfg mailer.MailConfig, session any, msg []byte, to []string) (any, error) {
	c.mu.Lock()
	c.stages = append(c.stages, time.Now())
	c.mu.Unlock()
	<-c.release // the "upload" keeps streaming past FireAt
	return &stagedToken{}, nil
}
func (c *overrunningCourier) Commit(cfg mailer.MailConfig, staged any) error {
	c.mu.Lock()
	c.commits = append(c.commits, time.Now())
	c.mu.Unlock()
	return nil
}
func (c *overrunningCourier) Send(cfg mailer.MailConfig, session any, msg []byte, to []string) error {
	c.mu.Lock()
	c.sends = append(c.sends, sendCall{at: time.Now(), session: session, msg: msg})
	c.mu.Unlock()
	return nil
}
func (c *overrunningCourier) Discard(session any) {}

func TestStagedOverrunFinishThenDot(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := &overrunningCourier{release: make(chan struct{})}
	s.Courier = c
	fireAt := time.Now().Add(2 * time.Second)

	content := testContent("fat upload")
	if _, err := s.Schedule(testConfig(), &content, fireAt); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Run() }()

	// Let the deadline pass mid-upload, then let the upload finish.
	time.Sleep(time.Until(fireAt) + 300*time.Millisecond)
	close(c.release)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.commits) != 1 || len(c.sends) != 0 {
		t.Fatalf("commits=%d sends=%d, want 1/0 (finish-then-dot, never an abandoned upload)", len(c.commits), len(c.sends))
	}
	if d := c.commits[0].Sub(fireAt); d < 250*time.Millisecond {
		t.Fatalf("commit %v after fire; want the overrun (~300ms) — never an early dot", d)
	}
	jobs, _ := store.Load()
	if jobs[0].State != StateSent || jobs[0].Result == nil || jobs[0].Result.Lateness < 250*time.Millisecond {
		t.Fatalf("result = %+v, want sent with the overrun recorded as lateness", jobs[0].Result)
	}
}

// TestStagedWindowIsPayloadAware: warm windows are per job. With the hold
// budget zeroed and a slow assumed uplink, the fat job's window opens
// long before the tiny one's — the tending loop must dispatch (and
// stage) the fat job FIRST even though FIFO order puts the tiny job at
// the head of the queue.
func TestStagedWindowIsPayloadAware(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	s.HoldBudget = 0         // windows = pure staging estimate
	s.UplinkRate = 1000      // 1 kB/s: the fat upload dominates its window
	s.WarmLead = time.Second // the floor for the tiny job
	c := &fakeCourier{staged: &stagedToken{}}
	s.Courier = c

	t0 := time.Now()
	fireAt := t0.Add(4 * time.Second)
	tiny := testContent("tiny") // ~1s window: session + one RTT
	if _, err := s.Schedule(testConfig(), &tiny, fireAt); err != nil {
		t.Fatalf("Schedule tiny: %v", err)
	}
	fat := testContent(strings.Repeat("x", 6000))
	fat.Attachments = []mailer.Attachment{{Filename: "fat.bin", ContentType: "application/octet-stream", Data: make([]byte, 4000)}}
	if _, err := s.Schedule(testConfig(), &fat, fireAt); err != nil { // ~11s window
		t.Fatalf("Schedule fat: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.stages) != 2 || len(c.commits) != 2 {
		t.Fatalf("stages=%d commits=%d, want 2/2", len(c.stages), len(c.commits))
	}
	fatStage, tinyStage := c.stages[0], c.stages[1]
	if fatStage.Sub(t0) > 800*time.Millisecond {
		t.Fatalf("the fat job staged %v after start; its larger window must open immediately", fatStage.Sub(t0))
	}
	if tinyStage.Sub(t0) < 1500*time.Millisecond {
		t.Fatalf("the tiny job staged %v after start; its ~1s window must not open before ~3s", tinyStage.Sub(t0))
	}
	for i, cm := range c.commits {
		if d := cm.Sub(fireAt); d < 0 || d > 500*time.Millisecond {
			t.Fatalf("commit %d landed %v off the deadline", i, d)
		}
	}
}

// TestWarmLeadZeroDisablesStaging: warmup disabled means no window, no
// session, no staging — every job fires cold, exactly as before.
func TestWarmLeadZeroDisablesStaging(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := &fakeCourier{staged: &stagedToken{}}
	s.WarmLead = 0
	s.Courier = c

	content := testContent("no warmup")
	if _, err := s.Schedule(testConfig(), &content, time.Now().Add(200*time.Millisecond)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.stages) != 0 || len(c.commits) != 0 {
		t.Fatalf("stages=%d commits=%d, want 0/0 (WarmLead=0 disables the warm path entirely)", len(c.stages), len(c.commits))
	}
	if len(c.sends) != 1 {
		t.Fatalf("sends = %d, want 1 (cold)", len(c.sends))
	}
	jobs, _ := store.Load()
	if jobs[0].State != StateSent {
		t.Fatalf("state = %q, want sent", jobs[0].State)
	}
}
