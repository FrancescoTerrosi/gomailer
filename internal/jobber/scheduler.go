package jobber

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/mail"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"gomailer/internal/mailer"
)

// Courier abstracts the delivery primitives the scheduler needs to run a
// warmup phase off the fire-time critical path:
//
//   - Prepare renders the wire message (pure CPU);
//   - Warm opens an authenticated provider session (network);
//   - Send delivers the prepared message over that session;
//   - Discard releases a session that will no longer be used.
//
// Production wires RealCourier (the PEC mailer); tests inject fakes that
// record call instants. The session value is opaque to the scheduler.
type Courier interface {
	Prepare(cfg mailer.MailConfig, content *mailer.MailContent) ([]byte, error)
	Warm(cfg mailer.MailConfig) (any, error)
	Send(cfg mailer.MailConfig, session any, msg []byte, to []string) error
	Discard(session any)
}

// RealCourier delivers through the PEC mailer.
type RealCourier struct{}

func (RealCourier) Prepare(cfg mailer.MailConfig, c *mailer.MailContent) ([]byte, error) {
	return cfg.Prepare(c)
}

func (RealCourier) Warm(cfg mailer.MailConfig) (any, error) {
	return cfg.WarmUp()
}

func (RealCourier) Send(cfg mailer.MailConfig, session any, msg []byte, to []string) error {
	sess, _ := session.(*mailer.Session)
	return cfg.SendOn(sess, msg, to)
}

func (RealCourier) Discard(session any) {
	if sess, ok := session.(*mailer.Session); ok {
		sess.Close()
	}
}

// catchupWarn is the lateness above which a firing is flagged as catch-up
// (the process/machine was not running at the fire time).
const catchupWarn = 2 * time.Second

// maxArm caps each timer arm while waiting for a deadline (see
// Scheduler.waitForUntil and sleepUntil). A single long-armed timer
// accumulates wake drift proportional to its armed duration (measured
// ~1ms/s on virtualized clocks: a 60s arm fires ~59ms late), whereas
// re-arming in bounded chunks — recomputing the remaining wait against
// the absolute instant on every tick — flushes any drift before it can
// compound: only the final arm's sub-millisecond jitter survives,
// whatever the lead.
const maxArm = time.Second

// defaultWarmLead is how far before FireAt the warmup phase starts: ~10x
// the measured connection+auth cost against the real provider
// (~100-300ms), with slack for one retry, yet far below any provider
// idle-timeout so the warm session is still alive at FireAt.
const defaultWarmLead = 5 * time.Second

// errJobTerminal signals that a job reached a terminal state during
// warmup (e.g. prepare failed): it must NOT be re-queued.
var errJobTerminal = errors.New("jobber: job reached a terminal state during warmup")

// warmState is the runtime-only warmup result of a handed-off job. It is
// never persisted (it is meaningless across process restarts) and never
// shared: the runner that owns the job owns its warm state from hand-off
// to terminal state, so it survives any interleaving with other jobs by
// construction — no cross-job bookkeeping, no requeue.
type warmState struct {
	msg     []byte
	session any
}

// Scheduler fires pending jobs from a Store at their exact FireAt.
//
// ARCHITECTURE — one collector, concurrent senders. A Scheduler (Run, or
// Serve in daemon mode) is the SINGLE firing authority for its store,
// enforced by an exclusive run lock (see acquireRunLock): no second
// process may ever fire the same pending jobs, which would duplicate
// certified sends — with one firing process per scheduling invocation,
// every daemon loaded the whole store and re-sent every pending job.
//
// The tending loop owns the queue and nothing else: it sleeps toward the
// next hand-off instant and hands each job to a dedicated runner
// goroutine when its warmup window opens (WarmLead before FireAt) or when
// it is due. Runners are fully independent: each builds its message,
// opens its own provider session, waits out its own chunked timer to the
// exact fire instant and delivers. Jobs that share a deadline fire
// CONCURRENTLY, each over its own session, and a slow SMTP send can never
// delay another job's fire: the strict per-job time budget holds no
// matter how many jobs are in flight. Concurrency is bounded by the
// workload: only jobs within WarmLead of their fire time are handed off,
// so one session per imminent fire.
//
// Ownership: a popped job belongs to exactly one runner until it reaches
// a terminal state — it is never re-queued. That keeps the exactly-one-
// delivery-attempt discipline under concurrency and makes warm state
// runner-local.
//
// Timing design: both the loop and every runner re-arm their waits in
// chunks of at most maxArm, recomputing against the absolute deadline
// (see maxArm). The loop wakes to hand a job off, to re-arm because an
// earlier job was scheduled, or on a chunk tick — no polling, no
// cumulative drift, even over long leads.
//
// Warmup: WarmLead before FireAt the message is built, the crash-safety
// "inflight" marker is persisted and an authenticated provider session is
// opened, so at FireAt only the envelope/data round-trips remain.
// Warmup is best-effort: any failure degrades to a cold send at FireAt
// and never costs the job its deadline. Jobs handed off with less than
// WarmLead remaining (scheduled late, catch-up, or the loop was busy
// with earlier jobs) fire cold.
type Scheduler struct {
	store *Store

	// Courier is the delivery seam. Production leaves it as RealCourier.
	Courier Courier

	// MinLead is the smallest allowed scheduling horizon: Schedule rejects
	// fire times closer than MinLead from now, which by construction
	// eliminates jobs with a deadline already in the past. The future
	// 24/7 service sets this to 24h; the CLI test tool defaults to 0.
	MinLead time.Duration

	// WarmLead is how far before FireAt the warmup phase starts. Jobs
	// with less than WarmLead remaining fire cold. Zero disables warmup.
	WarmLead time.Duration

	mu      sync.Mutex
	pending SortedQueue
	// active tracks the IDs of jobs handed off to runners and not yet
	// terminal. It bounds the drain on stop and disambiguates idempotent
	// replays: a job already in a runner's hands must never be re-queued
	// (see ScheduleWithID).
	active map[string]struct{}

	// wake, runnerDone and fatal are re-check HINTS, not signals that
	// carry meaning: sends are non-blocking into a size-1 buffer and may
	// drop, which is safe because every consumer re-reads the
	// authoritative state (queue, active count) under mu after waking.
	// A hint can only drop while another one sits unconsumed, and
	// consuming that one triggers the same re-check — so a state change
	// can never go unnoticed.
	wake       chan struct{}
	runnerDone chan struct{}
	fatal      chan error
}

// NewScheduler creates a scheduler backed by the given store.
func NewScheduler(store *Store) *Scheduler {
	return &Scheduler{
		store:      store,
		Courier:    RealCourier{},
		WarmLead:   defaultWarmLead,
		wake:       make(chan struct{}, 1),
		runnerDone: make(chan struct{}, 1),
		fatal:      make(chan error, 1),
		active:     make(map[string]struct{}),
	}
}

// Schedule validates and persists a new job, then arms the run loop when
// it fires earlier than everything pending. It returns the persisted job.
// It is safe to call from any goroutine while Run/Serve is running (the
// daemon's socket handler does exactly that).
//
// All validation happens HERE, at scheduling time — never at fire time:
// discovering a typo minutes or hours later, when the message is already
// due, would defeat the whole point of scheduling.
func (s *Scheduler) Schedule(cfg mailer.MailConfig, content *mailer.MailContent, fireAt time.Time) (Job, error) {
	id, err := NewJobID()
	if err != nil {
		return Job{}, fmt.Errorf("jobber: generating job id: %w", err)
	}
	return s.ScheduleWithID(id, cfg, content, fireAt)
}

// ScheduleWithID is Schedule with a caller-chosen job ID, and that ID is
// what makes submission idempotent: the client pre-generates it and sends
// it with every attempt of ONE logical submission. If a job with this ID
// is already persisted — the daemon died between persisting the original
// and writing the acknowledgment, or a concurrent twin raced us — the
// EXISTING job is returned and nothing is duplicated: a lost ack can
// never become a duplicate certified send. A pending job that no live
// firing loop has in its queue (persisted by a previous daemon, or by a
// client while this daemon was booting) is re-armed here; terminal jobs
// are returned untouched — a replay must never re-send. An empty id
// behaves exactly like Schedule.
//
// Callers that choose their own IDs own their uniqueness per logical
// submission (the CLI generates a fresh random one for every invocation).
func (s *Scheduler) ScheduleWithID(id string, cfg mailer.MailConfig, content *mailer.MailContent, fireAt time.Time) (Job, error) {
	if content == nil {
		return Job{}, fmt.Errorf("jobber: nil content")
	}
	if id == "" {
		var err error
		if id, err = NewJobID(); err != nil {
			return Job{}, fmt.Errorf("jobber: generating job id: %w", err)
		}
	}

	// Idempotent replay check FIRST, before any validation: a retry can
	// arrive after the fire time already passed (MinLead would reject it
	// afresh) and must still converge on the one already-persisted job.
	if existing, ok, err := s.store.GetByID(id); err != nil {
		return Job{}, fmt.Errorf("jobber: checking store for job %s: %w", id, err)
	} else if ok {
		return s.replay(existing), nil
	}

	c := *content
	if c.From == "" {
		c.From = cfg.Username
	}
	if c.From != cfg.Username {
		return Job{}, fmt.Errorf("jobber: From %q must equal the certified PEC address %q", c.From, cfg.Username)
	}
	if len(c.To) == 0 {
		return Job{}, fmt.Errorf("jobber: no recipients")
	}
	for _, rcpt := range c.To {
		if _, err := mail.ParseAddress(rcpt); err != nil {
			return Job{}, fmt.Errorf("jobber: invalid recipient address %q", rcpt)
		}
	}
	if _, err := mail.ParseAddress(cfg.Username); err != nil {
		return Job{}, fmt.Errorf("jobber: invalid certified address %q", cfg.Username)
	}
	if c.Body == "" {
		return Job{}, fmt.Errorf("jobber: empty body")
	}
	if fireAt.IsZero() {
		return Job{}, fmt.Errorf("jobber: zero fire time")
	}
	now := time.Now()
	if lead := fireAt.Sub(now); lead < s.MinLead {
		if fireAt.Before(now) {
			return Job{}, fmt.Errorf("jobber: fire time %s is already in the past", fireAt.Format("2006-01-02 15:04:05 -0700"))
		}
		return Job{}, fmt.Errorf("jobber: fire time is %s away, but jobs must be scheduled at least %s in advance", lead.Round(time.Second), s.MinLead)
	}

	// The Message-ID is pre-generated and persisted so that, even if the
	// process dies mid-send, the recipient inbox can be searched for it.
	mid, err := mailer.GenerateMessageID(domainOf(cfg.Username))
	if err != nil {
		return Job{}, fmt.Errorf("jobber: generating message-id: %w", err)
	}
	c.MessageID = mid

	job := Job{
		ID:        id,
		MessageID: mid,
		FireAt:    fireAt,
		CreatedAt: now,
		Content:   c,
		Config:    cfg,
		State:     StatePending,
	}

	// Persist BEFORE acknowledging the job: a scheduled job must never
	// exist only in RAM. AddNew, not Add: the atomic create-if-absent is
	// what closes the twin race — two identical submissions passing the
	// pre-check above still end up with exactly one job.
	existing, created, err := s.store.AddNew(job)
	if err != nil {
		return Job{}, fmt.Errorf("jobber: persisting job: %w", err)
	}
	if !created {
		// A twin won the race between the pre-check and here.
		return s.replay(existing), nil
	}

	s.mu.Lock()
	s.pending.Insert(job)
	wasHead := s.pending[0].ID == job.ID
	s.mu.Unlock()
	if wasHead {
		// The new job fires before everything else: hint the run loop to
		// re-evaluate its head and re-arm on the new earliest deadline.
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}

	log.Printf("SCHEDULED job=%s msgid=%s to=%s fire=%s (in %s)",
		job.ID, job.MessageID, strings.Join(c.To, ","),
		fireAt.Format("2006-01-02 15:04:05.000000 -0700"),
		fireAt.Sub(now).Round(time.Millisecond))
	return job, nil
}

// replay converges an idempotent re-submission on the job that is already
// persisted. The existing job is re-armed only when it is pending AND no
// live firing loop owns it (not in the queue, not handed to a runner) —
// the case of a job the current daemon never loaded: persisted by a
// previous daemon that died mid-ack, or by a client while this daemon was
// still booting. Everything else is returned untouched; in particular a
// terminal job must never be re-sent.
func (s *Scheduler) replay(j Job) Job {
	s.mu.Lock()
	_, running := s.active[j.ID]
	queued := false
	for _, q := range s.pending {
		if q.ID == j.ID {
			queued = true
			break
		}
	}
	arm := !running && !queued && j.State == StatePending
	if arm {
		s.pending.Insert(j)
	}
	wasHead := arm && s.pending[0].ID == j.ID
	s.mu.Unlock()

	if !arm {
		log.Printf("SCHEDULED job=%s msgid=%s — idempotent replay: already persisted (state=%s), left untouched",
			j.ID, j.MessageID, j.State)
		return j
	}
	if wasHead {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	log.Printf("SCHEDULED job=%s msgid=%s to=%s fire=%s (in %s) — idempotent replay: re-armed",
		j.ID, j.MessageID, strings.Join(j.Content.To, ","),
		j.FireAt.Format("2006-01-02 15:04:05.000000 -0700"),
		time.Until(j.FireAt).Round(time.Millisecond))
	return j
}

// Pending returns how many jobs are waiting to fire (status aid).
func (s *Scheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// Run drains the store: it recovers interrupted jobs, then fires
// everything pending and returns once the queue is empty AND no runner is
// active. This is the resume mode (bare CLI invocation after a reboot)
// and the test entry point; the daemon uses Serve instead. A second
// firing process is refused by the run lock — it would duplicate
// certified sends.
func (s *Scheduler) Run() error {
	return s.run(context.Background(), false, nil)
}

// Serve is the daemon loop: the same firing engine, but it idles when the
// queue is empty instead of returning, and it stops only when ctx is
// canceled (e.g. SIGTERM) or a runner reports a fatal store error.
//
// ready, when non-nil, is invoked after boot recovery and queue load and
// BEFORE the loop starts: the control-socket server starts accepting
// exactly then, so no submission can race the initial queue build and be
// dropped from RAM.
//
// Stopping is graceful by construction: a handed-off runner is never
// canceled (its inflight marker is already persisted — abandoning it
// would poison the job for manual verification), and each runner is
// within WarmLead of its fire time or mid-send, so Serve returns
// promptly. Jobs still waiting in the queue stay pending in the store and
// re-arm at the next start.
func (s *Scheduler) Serve(ctx context.Context, ready func()) error {
	return s.run(ctx, true, ready)
}

// run is the shared firing engine behind Run and Serve.
func (s *Scheduler) run(ctx context.Context, serve bool, ready func()) error {
	lock, err := acquireRunLock(s.store.path)
	if err != nil {
		return err
	}
	defer lock.release()

	jobs, err := s.store.Load()
	if err != nil {
		return fmt.Errorf("jobber: loading store: %w", err)
	}

	// Boot recovery: a job found inflight means the previous process died
	// during its SMTP send. Whether the PEC actually went out is
	// unknowable, and a duplicate certified message is worse than a late
	// manual decision, so such jobs are NEVER auto-resent.
	for i := range jobs {
		if jobs[i].State != StateInflight {
			continue
		}
		jobs[i].State = StateFailed
		jobs[i].Result = &JobResult{
			Err: "interrupted during send (previous process died mid-send): NOT auto-resent; verify the recipient inbox for the Message-ID",
		}
		if err := s.store.Update(jobs[i]); err != nil {
			return fmt.Errorf("jobber: recovering inflight job %s: %w", jobs[i].ID, err)
		}
		log.Printf("RECOVER job=%s msgid=%s was inflight at boot: interrupted during send, NOT auto-resent — verify the recipient inbox for %s",
			jobs[i].ID, jobs[i].MessageID, jobs[i].MessageID)
	}

	var pending SortedQueue
	var sent, failed int
	for _, j := range jobs {
		switch j.State {
		case StatePending:
			pending.Insert(j)
		case StateSent:
			sent++
		case StateFailed:
			failed++
		}
	}
	s.mu.Lock()
	s.pending = pending
	s.mu.Unlock()

	log.Printf("queue: %d pending, %d sent, %d failed (history kept in the store)", len(pending), sent, failed)

	// The socket may start accepting only now (see Serve).
	if ready != nil {
		ready()
	}

	for {
		s.mu.Lock()
		if len(s.pending) == 0 {
			active := len(s.active)
			s.mu.Unlock()
			if active == 0 && !serve {
				log.Printf("queue empty; done")
				return nil
			}
			// Nothing to tend: wait until something changes. Every case
			// below is just a hint to re-check (see the chan fields).
			select {
			case <-s.wake: // a new job became the head
			case <-s.runnerDone: // a runner reached a terminal state
			case err := <-s.fatal:
				return s.stop(err)
			case <-ctx.Done():
				return s.stop(ctx.Err())
			}
			continue
		}

		head := s.pending[0]
		now := time.Now()
		// Hand-off decision, made at peek time exactly as the hand-off is
		// executed: a job whose warmup window has not opened yet is handed
		// off WARM, at the window opening; a job whose window is already
		// open (scheduled late, catch-up, or the loop was busy with
		// earlier jobs) fires COLD — no warmup could still beat the
		// deadline.
		warm := s.WarmLead > 0 && now.Before(head.FireAt.Add(-s.WarmLead))
		handoff := head.FireAt
		if warm {
			handoff = head.FireAt.Add(-s.WarmLead)
		}
		if !now.Before(handoff) {
			// Warm window open, due or overdue: hand the job to its own
			// runner and immediately tend the next deadline.
			s.pending = s.pending[1:]
			s.active[head.ID] = struct{}{}
			s.mu.Unlock()
			go s.runJob(head, warm)
			continue
		}
		s.mu.Unlock()

		due, err := s.waitForUntil(ctx, handoff)
		if err != nil {
			return s.stop(err)
		}
		if !due {
			continue // an earlier job became the head: re-evaluate
		}
		// The hand-off instant is due. Pop and hand off — but only if the
		// head is still the job we slept for: an earlier submission can
		// have raced the timer without its hint being consumed yet.
		s.mu.Lock()
		if len(s.pending) == 0 || s.pending[0].ID != head.ID {
			s.mu.Unlock()
			continue
		}
		s.pending = s.pending[1:]
		s.active[head.ID] = struct{}{}
		s.mu.Unlock()
		go s.runJob(head, warm)
	}
}

// stop drains the active runners and returns the cause. Runners are
// committed once handed off (their inflight marker is on disk), so they
// are waited for, never canceled; the wait is bounded by WarmLead plus
// one SMTP send. Jobs still pending stay safely in the store.
func (s *Scheduler) stop(cause error) error {
	s.drainRunners()
	log.Printf("run stopped (%v): %d job(s) left pending in the store; they re-arm at the next start",
		cause, s.Pending())
	return cause
}

// drainRunners waits until every handed-off job reached a terminal state.
func (s *Scheduler) drainRunners() {
	for {
		s.mu.Lock()
		active := len(s.active)
		s.mu.Unlock()
		if active == 0 {
			return
		}
		<-s.runnerDone
	}
}

// runJob owns one job from hand-off to its terminal state: warmup (when a
// warm window was handed over), the chunked wait to the exact fire
// instant, and the delivery. Single ownership is what preserves the
// at-most-one-delivery-attempt discipline under concurrency.
func (s *Scheduler) runJob(j Job, warm bool) {
	defer func() {
		s.mu.Lock()
		delete(s.active, j.ID)
		s.mu.Unlock()
		select {
		case s.runnerDone <- struct{}{}:
		default:
		}
	}()

	st := &warmState{}
	if warm {
		if err := s.prewarm(j, st); err != nil {
			if errors.Is(err, errJobTerminal) {
				return // already recorded failed on the store
			}
			s.Courier.Discard(st.session)
			s.reportFatal(err)
			return
		}
	}

	// The exact-instant wait: the same chunked re-arm discipline as the
	// tending loop (see maxArm), so a long lead costs no precision. It
	// is deliberately not interruptible — a handed-off job is committed.
	sleepUntil(j.FireAt)

	if err := s.fire(j, st); err != nil {
		if errors.Is(err, errJobTerminal) {
			return // already recorded failed on the store
		}
		// fire only fails on store errors that PRECEDE the send: the
		// on-disk state is authoritative and never lies — still pending
		// means the wire was never touched, so the job is safe to re-fire
		// at the next start.
		s.Courier.Discard(st.session)
		s.reportFatal(err)
	}
}

// sleepUntil blocks until the absolute instant, re-armed in chunks of at
// most maxArm recomputed against the deadline (see maxArm).
func sleepUntil(until time.Time) {
	for wait := time.Until(until); wait > 0; wait = time.Until(until) {
		time.Sleep(min(wait, maxArm))
	}
}

// waitForUntil blocks until the given instant. It returns (true, nil) when
// the instant is due and (false, nil) when the loop must re-evaluate its
// head (an earlier job was scheduled mid-sleep); an error is returned
// only when the run must stop (fatal store error or cancellation).
//
// The sleep is re-armed in chunks of at most maxArm, recomputing the
// remaining wait against the absolute instant on every tick (see maxArm).
func (s *Scheduler) waitForUntil(ctx context.Context, until time.Time) (bool, error) {
	for wait := time.Until(until); wait > 0; wait = time.Until(until) {
		arm := min(wait, maxArm)
		timer := time.NewTimer(arm)
		select {
		case <-timer.C:
		case <-s.wake:
			timer.Stop()
			return false, nil
		case err := <-s.fatal:
			timer.Stop()
			return false, err
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		}
	}
	return true, nil
}

// prewarm moves everything except the delivery off the fire-time critical
// path: message build, the crash-safety inflight persist (the fsync leaves
// the critical window too), and the provider session (measured ~100-300ms
// against the real provider).
//
// Failures are best-effort: a session failure merely degrades the job to a
// cold send at FireAt; only a prepare failure (the job can never be sent)
// or a store failure terminates the job early.
func (s *Scheduler) prewarm(j Job, st *warmState) error {
	msg, err := s.Courier.Prepare(j.Config, &j.Content)
	if err != nil {
		if ferr := s.failNow(j, "prepare: "+err.Error()); ferr != nil {
			return ferr
		}
		return errJobTerminal
	}
	st.msg = msg

	// Crash-safety marker, absorbed into the warmup window: inflight is
	// persisted BEFORE the wire can ever be touched (see Job.State).
	j.State = StateInflight
	if err := s.store.Update(j); err != nil {
		return fmt.Errorf("jobber: persisting inflight state for job %s: %w", j.ID, err)
	}

	t0 := time.Now()
	st.session, err = s.Courier.Warm(j.Config)
	if err != nil || st.session == nil {
		log.Printf("WARM job=%s msgid=%s failed (%v): cold fallback at fire time",
			j.ID, j.MessageID, err)
		st.session = nil
	} else {
		log.Printf("WARM job=%s msgid=%s session ready in %s (%s before fire)",
			j.ID, j.MessageID,
			time.Since(t0).Round(time.Millisecond),
			time.Until(j.FireAt).Round(time.Millisecond))
	}
	return nil
}

// fire delivers one job, recording the full timing paper trail. It returns
// an error only when the store itself failed BEFORE the send (a broken
// store must stop the queue); send failures are recorded and reported,
// not fatal, and a result that cannot be persisted is logged loudly
// instead of failing the job.
func (s *Scheduler) fire(j Job, st *warmState) error {
	now := time.Now()
	lateness := now.Sub(j.FireAt)
	if lateness < 0 {
		lateness = 0
	}
	stamp := now.Format("2006-01-02 15:04:05.000000 -0700")

	var msg []byte
	var sess any
	if st != nil {
		msg, sess = st.msg, st.session
	}

	if lateness >= catchupWarn {
		log.Printf("WARNING job=%s msgid=%s FIRED %s LATE (process/machine was not running at the fire time) fired=%s",
			j.ID, j.MessageID, lateness.Round(time.Millisecond), stamp)
	} else {
		cold := ""
		if sess == nil {
			cold = " (cold)"
		}
		log.Printf("FIRE job=%s msgid=%s fired=%s lateness=%s%s",
			j.ID, j.MessageID, stamp, lateness, cold)
	}

	// Cold path (no warm window existed): build and persist inflight here
	// — still before any network I/O, as the crash-safety marker requires.
	if msg == nil {
		var err error
		msg, err = s.Courier.Prepare(j.Config, &j.Content)
		if err != nil {
			if ferr := s.failNow(j, "prepare: "+err.Error()); ferr != nil {
				return ferr
			}
			return errJobTerminal
		}
		j.State = StateInflight
		if err := s.store.Update(j); err != nil {
			return fmt.Errorf("jobber: persisting inflight state for job %s: %w", j.ID, err)
		}
	}

	t0 := time.Now()
	err := s.Courier.Send(j.Config, sess, msg, j.Content.To)
	sendDur := time.Since(t0)

	j.Result = &JobResult{FiredAt: now, Lateness: lateness, SendDuration: sendDur}
	if err != nil {
		j.State = StateFailed
		j.Result.Err = err.Error()
	} else {
		j.State = StateSent
	}
	if uerr := s.store.Update(j); uerr != nil {
		// The send already happened, but its outcome could not be
		// recorded. The store still says "inflight", so the next boot
		// will flag it for manual verification. Loud, not queue-fatal.
		log.Printf("CRITICAL job=%s send result could not be persisted: %v (next boot will mark it for manual verification)", j.ID, uerr)
	}

	if err != nil {
		log.Printf("FAILED job=%s msgid=%s send=%s err=%v",
			j.ID, j.MessageID, sendDur.Round(time.Millisecond), err)
	} else {
		log.Printf("SENT job=%s msgid=%s send=%s",
			j.ID, j.MessageID, sendDur.Round(time.Millisecond))
	}
	return nil
}

// failNow records a terminal failure for a job that never reached the
// send (e.g. prepare failed), keeping the paper trail complete.
func (s *Scheduler) failNow(j Job, reason string) error {
	j.State = StateFailed
	j.Result = &JobResult{FiredAt: time.Now(), Err: reason}
	if err := s.store.Update(j); err != nil {
		return fmt.Errorf("jobber: persisting failure for job %s: %w", j.ID, err)
	}
	log.Printf("FAILED job=%s msgid=%s before send: %s", j.ID, j.MessageID, reason)
	return nil
}

// reportFatal hands the first fatal store error to the tending loop: a
// broken store must stop the whole run (pending jobs stay safely
// persisted; the process owner — e.g. systemd — restarts it).
func (s *Scheduler) reportFatal(err error) {
	select {
	case s.fatal <- err:
	default:
	}
}

// runLock is the exclusive, non-blocking hold on <store>.runlock that
// grants ONE process the right to fire jobs from a store. It is the hard
// guarantee behind the one-daemon architecture: a second firing process —
// a second daemon, or a resume-drain racing the daemon — fails fast
// instead of ever risking a duplicate certified send (each would load and
// re-send every pending job). The kernel releases the flock when the
// holder exits for any reason, so a crashed daemon leaves no stale lock
// behind. Advisory flock: Linux-only, like the systemd deployment.
type runLock struct{ f *os.File }

// acquireRunLock takes the firing right for the store, or explains why
// this process must stay a non-firing one.
func acquireRunLock(storePath string) (*runLock, error) {
	f, err := os.OpenFile(storePath+".runlock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("jobber: opening run lock for %s: %w", storePath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("jobber: another scheduler is already firing jobs from store %s — refusing to start a second one (it would duplicate certified sends); stop it first or inspect with -list", storePath)
	}
	return &runLock{f: f}, nil
}

// release drops the firing right (nil-safe).
func (l *runLock) release() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
}

// domainOf extracts the domain of a mailbox address, used to build the
// pre-generated Message-ID.
func domainOf(addr string) string {
	if i := strings.LastIndexByte(addr, '@'); i >= 0 && i+1 < len(addr) {
		return addr[i+1:]
	}
	return "localhost"
}
