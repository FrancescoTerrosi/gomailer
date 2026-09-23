package jobber

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
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
// Scheduler.waitForUntil). A single long-armed timer accumulates wake drift
// proportional to its armed duration (measured ~1ms/s on virtualized
// clocks: a 60s arm fires ~59ms late), whereas re-arming in bounded
// chunks — recomputing the remaining wait against the absolute instant on
// every tick — flushes any drift before it can compound: only the final
// arm's sub-millisecond jitter survives, whatever the lead.
const maxArm = time.Second

// defaultWarmLead is how far before FireAt the warmup phase starts: ~10x
// the measured connection+auth cost against the real provider
// (~100-300ms), with slack for one retry, yet far below any provider
// idle-timeout so the warm session is still alive at FireAt.
const defaultWarmLead = 5 * time.Second

// errJobTerminal signals that a job reached a terminal state during
// warmup (e.g. prepare failed): it must NOT be re-queued.
var errJobTerminal = errors.New("jobber: job reached a terminal state during warmup")

// warmState is the runtime-only warmup result of a pending job. It is
// never persisted: it is meaningless across process restarts.
type warmState struct {
	msg     []byte
	session any
}

// Scheduler fires pending jobs from a Store at their exact FireAt.
//
// Timing design: a single timer tends the earliest deadline, re-armed in
// chunks of at most maxArm (see waitForUntil). The loop wakes to fire a
// due job, to re-arm because an earlier job was scheduled, or on a chunk
// tick to recompute the remaining wait against the absolute FireAt —
// there is no polling and no cumulative drift, even over long leads.
//
// Warmup: WarmLead before FireAt the message is built, the crash-safety
// "inflight" marker is persisted, and an authenticated provider session
// is opened, so at FireAt only the envelope/data round-trips remain.
// Warmup is best-effort: any failure degrades to a cold send at FireAt
// and never costs the job its deadline.
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
	wake    chan struct{}

	// warm holds the warmup state of popped jobs, keyed by job ID: it
	// survives a requeue (an earlier job interrupting the sleep) so the
	// session is reused, not reopened. Only the Run loop touches it.
	warm map[string]*warmState
}

// NewScheduler creates a scheduler backed by the given store.
func NewScheduler(store *Store) *Scheduler {
	return &Scheduler{
		store:    store,
		Courier:  RealCourier{},
		WarmLead: defaultWarmLead,
		wake:     make(chan struct{}, 1),
		warm:     make(map[string]*warmState),
	}
}

// Schedule validates and persists a new job, then arms the run loop when
// it fires earlier than everything pending. It returns the persisted job.
//
// All validation happens HERE, at scheduling time — never at fire time:
// discovering a typo minutes or hours later, when the message is already
// due, would defeat the whole point of scheduling.
func (s *Scheduler) Schedule(cfg mailer.MailConfig, content *mailer.MailContent, fireAt time.Time) (Job, error) {
	if content == nil {
		return Job{}, fmt.Errorf("jobber: nil content")
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

	id, err := newJobID()
	if err != nil {
		return Job{}, fmt.Errorf("jobber: generating job id: %w", err)
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
	// exist only in RAM.
	if err := s.store.Add(job); err != nil {
		return Job{}, fmt.Errorf("jobber: persisting job: %w", err)
	}

	s.mu.Lock()
	s.pending.Insert(job)
	wasHead := s.pending[0].ID == job.ID
	s.mu.Unlock()
	if wasHead {
		// The new job fires before everything else: interrupt the run
		// loop so it re-arms on the new earliest deadline.
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

// Run loads the store, recovers jobs that were interrupted mid-send, then
// fires everything pending. It returns when the queue is empty
// (test-friendly); the future 24/7 service will idle instead of exiting.
func (s *Scheduler) Run() error {
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

	for {
		s.mu.Lock()
		if len(s.pending) == 0 {
			s.mu.Unlock()
			log.Printf("queue empty; done")
			s.drainWarm()
			return nil
		}
		head := s.pending[0]
		s.pending = s.pending[1:]
		s.mu.Unlock()

		// Warmup window: build + crash-safety persist + provider session
		// — everything except the delivery itself, moved off the fire
		// critical path. Skipped when disabled (WarmLead 0) or when the lead
		// was too short for a window to exist (e.g. catch-up jobs due now).
		if warmAt := head.FireAt.Add(-s.WarmLead); s.WarmLead > 0 && time.Now().Before(warmAt) {
			if !s.waitForUntil(warmAt) {
				s.requeue(head)
				continue
			}
			if _, ok := s.warm[head.ID]; !ok {
				if err := s.prewarm(head); err != nil {
					if errors.Is(err, errJobTerminal) {
						continue // already recorded failed on the store
					}
					s.requeue(head)
					s.drainWarm()
					return err
				}
			}
		}

		// Fire moment.
		if !s.waitForUntil(head.FireAt) {
			s.requeue(head)
			continue
		}

		if err := s.fire(head); err != nil {
			if errors.Is(err, errJobTerminal) {
				continue
			}
			s.requeue(head)
			s.drainWarm()
			return err
		}
	}
}

// requeue puts a job back into the pending queue, keeping its warm state.
func (s *Scheduler) requeue(j Job) {
	s.mu.Lock()
	s.pending.Insert(j)
	s.mu.Unlock()
}

// drainWarm releases leftover warm sessions (e.g. when a fatal store error
// aborts the run with a job re-queued).
func (s *Scheduler) drainWarm() {
	for id, st := range s.warm {
		s.Courier.Discard(st.session)
		delete(s.warm, id)
	}
}

// waitForUntil blocks until the given instant. It returns true when the
// instant is due, and false when an earlier job was scheduled mid-sleep
// (the caller must requeue the head and re-evaluate the queue).
//
// The sleep is re-armed in chunks of at most maxArm, recomputing the
// remaining wait against the absolute instant on every tick: each chunk
// is a fresh, closed-loop measurement, so any drift from one chunk is
// absorbed into the next instead of compounding (see maxArm).
func (s *Scheduler) waitForUntil(until time.Time) bool {
	for wait := time.Until(until); wait > 0; {
		arm := min(wait, maxArm)
		timer := time.NewTimer(arm)
		select {
		case <-timer.C:
			wait = time.Until(until)
		case <-s.wake:
			timer.Stop()
			return false
		}
	}
	return true
}

// prewarm moves everything except the delivery off the fire-time critical
// path: message build, the crash-safety inflight persist (the fsync leaves
// the critical window too), and the provider session (measured ~100-300ms
// against the real provider).
//
// Failures are best-effort: a session failure merely degrades the job to a
// cold send at FireAt; only a prepare failure (the job can never be sent)
// or a store failure terminates the job early.
func (s *Scheduler) prewarm(j Job) error {
	st := &warmState{}

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

	s.warm[j.ID] = st
	return nil
}

// fire delivers one job, recording the full timing paper trail. It returns
// an error only when the store itself failed (a broken store must stop
// the queue); send failures are recorded and reported, not fatal.
func (s *Scheduler) fire(j Job) error {
	now := time.Now()
	lateness := now.Sub(j.FireAt)
	if lateness < 0 {
		lateness = 0
	}
	stamp := now.Format("2006-01-02 15:04:05.000000 -0700")

	st := s.warm[j.ID]
	delete(s.warm, j.ID)

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

// domainOf extracts the domain of a mailbox address, used to build the
// pre-generated Message-ID.
func domainOf(addr string) string {
	if i := strings.LastIndexByte(addr, '@'); i >= 0 && i+1 < len(addr) {
		return addr[i+1:]
	}
	return "localhost"
}
