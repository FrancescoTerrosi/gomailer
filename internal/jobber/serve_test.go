package jobber

// Tests for the one-daemon era of the scheduler: concurrent firing (one
// runner per job), the run lock (exactly one firing process per store),
// submission-while-serving, and the persist-without-firing fallback the
// CLI client uses when the daemon is unreachable. The timing-precision
// suite lives in jobber_test.go and must keep passing unchanged.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gomailer/internal/mailer"
)

// slowSendCourier records every send and takes a fixed send duration, to
// prove that fires are concurrent: with a serial firing loop, the second
// of two same-deadline jobs would start one full send-duration late.
type slowSendCourier struct {
	mu    sync.Mutex
	sends []sendCall
	sleep time.Duration
}

func (f *slowSendCourier) Prepare(cfg mailer.MailConfig, c *mailer.MailContent) ([]byte, error) {
	return []byte(c.Body), nil
}

func (f *slowSendCourier) Warm(cfg mailer.MailConfig) (any, error) { return nil, nil }

func (f *slowSendCourier) Send(cfg mailer.MailConfig, session any, msg []byte, to []string) error {
	f.mu.Lock()
	f.sends = append(f.sends, sendCall{at: time.Now(), session: session, msg: msg})
	f.mu.Unlock()
	time.Sleep(f.sleep)
	return nil
}

func (f *slowSendCourier) Discard(session any) {}

// waitSends polls a fake courier until want sends were recorded.
func waitSends(t *testing.T, c *fakeCourier, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		n := len(c.sends)
		c.mu.Unlock()
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("send calls = %d, want %d", n, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestConcurrentSameDeadline: two jobs sharing a deadline must fire
// concurrently, each on time — a serial loop would delay the second by a
// full SMTP send (150ms here).
func TestConcurrentSameDeadline(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := &slowSendCourier{sleep: 150 * time.Millisecond}
	s.Courier = c
	s.WarmLead = 0

	fireAt := time.Now().Add(400 * time.Millisecond)
	ca := testContent("A")
	cb := testContent("B")
	if _, err := s.Schedule(testConfig(), &ca, fireAt); err != nil {
		t.Fatalf("Schedule A: %v", err)
	}
	if _, err := s.Schedule(testConfig(), &cb, fireAt); err != nil {
		t.Fatalf("Schedule B: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 2 {
		t.Fatalf("send calls = %d, want 2", len(c.sends))
	}
	for i, call := range c.sends {
		if lateness := call.at.Sub(fireAt); lateness < 0 || lateness >= 150*time.Millisecond {
			t.Fatalf("send %d started %v late, want < 150ms (fires must be concurrent, not serialized)", i, lateness)
		}
	}
	// Both recorded sent in the store, exactly once.
	jobs, err := store.Load()
	if err != nil || len(jobs) != 2 {
		t.Fatalf("store after run: err=%v jobs=%d", err, len(jobs))
	}
	for _, j := range jobs {
		if j.State != StateSent || j.Result == nil || j.Result.Err != "" {
			t.Fatalf("job %s: state=%q result=%+v", j.ID, j.State, j.Result)
		}
	}
}

// TestRunLockRejectsSecondFiringProcess: while one firing loop owns the
// store, a second one must be refused — it would load and re-send every
// pending job (the duplicate-certified-send hazard the run lock exists
// for). The first loop is unaffected.
func TestRunLockRejectsSecondFiringProcess(t *testing.T) {
	store := testStore(t)

	// First firing process: holds the run lock while its only job waits.
	s1 := NewScheduler(store)
	coldCourier(s1)
	content := testContent("held")
	if _, err := s1.Schedule(testConfig(), &content, time.Now().Add(300*time.Millisecond)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s1.Run() }()
	time.Sleep(100 * time.Millisecond) // let the first loop take the lock

	// Second firing process on the same store: refused, fast.
	s2 := NewScheduler(store)
	err := s2.Run()
	if err == nil {
		t.Fatal("second firing process was accepted (it would duplicate certified sends)")
	}
	if !strings.Contains(err.Error(), "another scheduler") {
		t.Fatalf("unexpected error: %v", err)
	}
	if serr := <-done; serr != nil {
		t.Fatalf("first run must complete cleanly: %v", serr)
	}
}

// TestScheduleWithoutRunPersistsUntilDrained: the client fallback path.
// Schedule persists the job; nothing fires while no firing loop runs —
// not even past the deadline — and a later firing loop (daemon boot /
// resume) sends it exactly once.
func TestScheduleWithoutRunPersistsUntilDrained(t *testing.T) {
	store := testStore(t)
	sClient := NewScheduler(store)
	content := testContent("fallback")
	job, err := sClient.Schedule(testConfig(), &content, time.Now().Add(200*time.Millisecond))
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	time.Sleep(350 * time.Millisecond) // deadline passes with no firing loop

	jobs, _ := store.Load()
	if len(jobs) != 1 || jobs[0].State != StatePending {
		t.Fatalf("job must stay pending without a firing loop: %+v", jobs)
	}

	sFire := NewScheduler(store)
	c := coldCourier(sFire)
	if err := sFire.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(c.sends) != 1 || string(c.sends[0].msg) != "fallback" {
		t.Fatalf("sends after drain = %v, want exactly one fallback", c.sends)
	}
	jobs, _ = store.Load()
	if jobs[0].State != StateSent || jobs[0].MessageID != job.MessageID {
		t.Fatalf("store after fire: state=%q msgid=%q", jobs[0].State, jobs[0].MessageID)
	}
}

// TestServeCollectsAndFires: the daemon loop accepts submissions while
// serving (as the socket handler delivers them), fires each exactly once
// and on time, and stops cleanly on cancellation.
func TestServeCollectsAndFires(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := coldCourier(s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.Serve(ctx, func() { close(ready) })
	}()
	<-ready

	// Submissions arriving while the daemon serves. Schedule is safe
	// from any goroutine (the socket handler calls it concurrently).
	a := testContent("A")
	if _, err := s.Schedule(testConfig(), &a, time.Now().Add(150*time.Millisecond)); err != nil {
		t.Fatalf("Schedule A: %v", err)
	}
	b := testContent("B")
	fireB := time.Now().Add(400 * time.Millisecond)
	if _, err := s.Schedule(testConfig(), &b, fireB); err != nil {
		t.Fatalf("Schedule B: %v", err)
	}

	waitSends(t, c, 2, 3*time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 2 {
		t.Fatalf("send calls = %d, want 2", len(c.sends))
	}
	if string(c.sends[0].msg) != "A" || string(c.sends[1].msg) != "B" {
		t.Fatalf("fired %q then %q, want A then B", c.sends[0].msg, c.sends[1].msg)
	}
	if lateness := c.sends[1].at.Sub(fireB); lateness < 0 || lateness > 100*time.Millisecond {
		t.Fatalf("job B lateness = %v, want 0..100ms while serving", lateness)
	}

	// Stop: runners drained, Serve returns canceled.
	cancel()
	if err := <-serveErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve returned %v, want context.Canceled", err)
	}
}

// TestServeStopLeavesPendingArmed: on shutdown, a job that was never
// handed off stays PENDING in the store — safe to re-arm at the next
// start — while an imminent job that was handed off still fires before
// Serve returns (its runner is committed).
func TestServeStopLeavesPendingArmed(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := coldCourier(s)

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.Serve(ctx, func() { close(ready) })
	}()
	<-ready

	// One imminent job (handed off and fired) and one far-future job
	// that must stay untouched when the daemon stops.
	near := testContent("near")
	if _, err := s.Schedule(testConfig(), &near, time.Now().Add(150*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	far := testContent("far")
	if _, err := s.Schedule(testConfig(), &far, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	waitSends(t, c, 1, 3*time.Second)
	time.Sleep(50 * time.Millisecond) // the far job is (still) pending
	cancel()
	if err := <-serveErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve returned %v, want context.Canceled", err)
	}

	c.mu.Lock()
	nearFired := len(c.sends) == 1 && string(c.sends[0].msg) == "near"
	c.mu.Unlock()
	if !nearFired {
		t.Fatalf("sends = %d, want the imminent job fired exactly once", len(c.sends))
	}
	jobs, _ := store.Load()
	states := map[string]JobState{}
	for _, j := range jobs {
		states[string(j.Content.Body)] = j.State
	}
	if states["near"] != StateSent {
		t.Fatalf("near job state = %q, want sent", states["near"])
	}
	if states["far"] != StatePending {
		t.Fatalf("far job state = %q, want pending (re-arms at next start)", states["far"])
	}
}

// --- Idempotent submission (daemon death between persist and ack) ---

// TestScheduleWithIDIdempotentReplay: re-submitting with the same job ID
// converges on the already-persisted job — no second store row, no second
// queue entry. This is the daemon-died-mid-ack case: the client's fallback
// must never be able to turn one logical submission into two certified
// sends.
func TestScheduleWithIDIdempotentReplay(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	content := testContent("only once")

	first, err := s.ScheduleWithID("fixed-id", testConfig(), &content, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("first ScheduleWithID: %v", err)
	}
	again, err := s.ScheduleWithID("fixed-id", testConfig(), &content, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("replay ScheduleWithID: %v", err)
	}
	if again.ID != first.ID || again.MessageID != first.MessageID {
		t.Fatalf("replay returned a different job: %s/%s vs %s/%s", again.ID, again.MessageID, first.ID, first.MessageID)
	}
	jobs, err := store.Load()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("store rows = %d (err=%v), want 1", len(jobs), err)
	}
	if n := s.Pending(); n != 1 {
		t.Fatalf("queue entries = %d, want 1 (replay must not double-arm)", n)
	}
}

// TestReplayRearmsJobTheDaemonMissed: a pending job persisted by a process
// whose firing loop is gone (crashed daemon mid-ack, or a client while the
// daemon was booting) is re-armed when a live scheduler that never loaded
// it receives the idempotent replay — and then fires exactly once.
func TestReplayRearmsJobTheDaemonMissed(t *testing.T) {
	store := testStore(t)

	// "The daemon that died": persisted the job, never answered, never fired.
	sDead := NewScheduler(store)
	content := testContent("missed me")
	job, err := sDead.ScheduleWithID("missed-id", testConfig(), &content, time.Now().Add(200*time.Millisecond))
	if err != nil {
		t.Fatalf("persist: %v", err)
	}

	// A live scheduler that booted BEFORE the persist (empty queue, no load
	// of the store): the replay must re-arm the job it never saw.
	sLive := NewScheduler(store)
	c := coldCourier(sLive)
	again, err := sLive.ScheduleWithID("missed-id", testConfig(), &content, time.Now().Add(200*time.Millisecond))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if again.ID != job.ID {
		t.Fatalf("replay job = %s, want %s", again.ID, job.ID)
	}
	if n := sLive.Pending(); n != 1 {
		t.Fatalf("replay must re-arm the missed job, queue = %d", n)
	}
	if err := sLive.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	c.mu.Lock()
	sends := len(c.sends)
	c.mu.Unlock()
	if sends != 1 {
		t.Fatalf("sends = %d, want exactly 1", sends)
	}
	jobs, _ := store.Load()
	if len(jobs) != 1 || jobs[0].State != StateSent {
		t.Fatalf("store after fire: %d rows, state=%q", len(jobs), jobs[0].State)
	}
}

// TestReplayNeverResendsTerminalJob: an idempotent replay of a job that
// already reached a terminal state returns it untouched — a retry after
// the job fired must never schedule a second certified send.
func TestReplayNeverResendsTerminalJob(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	c := coldCourier(s)
	content := testContent("already done")
	if _, err := s.ScheduleWithID("done-id", testConfig(), &content, time.Now().Add(100*time.Millisecond)); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	again, err := s.ScheduleWithID("done-id", testConfig(), &content, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("replay after completion: %v", err)
	}
	if again.State != StateSent {
		t.Fatalf("replay state = %q, want sent (returned as-is)", again.State)
	}
	if n := s.Pending(); n != 0 {
		t.Fatalf("replay of a sent job re-armed it: queue = %d", n)
	}
	c.mu.Lock()
	sends := len(c.sends)
	c.mu.Unlock()
	if sends != 1 {
		t.Fatalf("sends = %d, want 1 (terminal jobs are never re-sent)", sends)
	}
}

// TestConcurrentTwinSubmitsCreateOneJob: N goroutines submitting with the
// SAME id (a client retry racing its own first attempt) still end up with
// exactly one job: AddNew's atomic create-if-absent closes the TOCTOU the
// pre-check cannot.
func TestConcurrentTwinSubmitsCreateOneJob(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := testContent("twin")
			if _, err := s.ScheduleWithID("twin-id", testConfig(), &c, time.Now().Add(time.Hour)); err != nil {
				t.Errorf("twin submit: %v", err)
			}
		}()
	}
	wg.Wait()
	jobs, err := store.Load()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("store rows = %d (err=%v), want 1", len(jobs), err)
	}
	if n := s.Pending(); n != 1 {
		t.Fatalf("queue entries = %d, want 1", n)
	}
}

// TestReplayBypassesStaleValidation: a retry can arrive after the fire time
// has passed (MinLead would reject it afresh); it must still converge on
// the persisted job instead of failing.
func TestReplayBypassesStaleValidation(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	s.MinLead = time.Hour
	content := testContent("late retry")
	job, err := s.ScheduleWithID("late-id", testConfig(), &content, time.Now().Add(2*time.Hour))
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	// Simulate the deadline having passed by the time the retry lands:
	// flip the stored FireAt into the past, then replay.
	jobs, _ := store.Load()
	jobs[0].FireAt = time.Now().Add(-time.Minute)
	if err := store.Update(jobs[0]); err != nil {
		t.Fatal(err)
	}
	again, err := s.ScheduleWithID("late-id", testConfig(), &content, time.Now().Add(2*time.Hour))
	if err != nil {
		t.Fatalf("replay with a past persisted deadline must not fail: %v", err)
	}
	if again.ID != job.ID {
		t.Fatalf("replay job = %s, want %s", again.ID, job.ID)
	}
}

// TestInvalidRecipientRejectedAtScheduling: a malformed recipient address
// is rejected at scheduling time — the "typo never surfaces at fire time"
// promise must cover the address syntax too.
func TestInvalidRecipientRejectedAtScheduling(t *testing.T) {
	s := NewScheduler(testStore(t))
	c := testContent("hi")
	c.To = []string{"not-an-address"}
	if _, err := s.ScheduleWithID("bad-rcpt", testConfig(), &c, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("malformed recipient accepted")
	}
}
