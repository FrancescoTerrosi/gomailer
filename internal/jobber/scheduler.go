package jobber

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/mail"
	"os"
	"slices"
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
	// Prepare renders the wire message (pure CPU).
	Prepare(cfg mailer.MailConfig, content *mailer.MailContent) ([]byte, error)
	// Warm opens an authenticated provider session (network).
	Warm(cfg mailer.MailConfig) (any, error)
	// Stage moves everything except the delivery act off the fire-time
	// critical path: envelope + DATA + the full message bytes, leaving
	// the transaction ONE DOT short of delivery. It is called immediately
	// after Warm (no idle gap to probe). The returned handle is opaque;
	// nil means "not staged" — the job then delivers at the fire time over
	// the warm session, exactly as before staging existed. The session is
	// consumed either way (RealCourier's StageOn closes it on failure: a
	// session that failed to stage must never accept another command, see
	// mailer.StageOn — so a Discard after a failed Stage is idempotent,
	// kept for couriers that release lazily). Every Stage failure precedes
	// the dot by construction, so the caller must degrade to a cold send
	// at the fire time ("just go"), never abandon the job.
	Stage(cfg mailer.MailConfig, session any, msg []byte, to []string) (any, error)
	// Commit performs the delivery act (the terminating dot) and reads
	// the provider's answer; call it at or after the fire time, exactly
	// once, never before. Errors wrapping mailer.ErrDotOut mean the dot
	// left the wire: terminal ambiguity, never re-attempted — only
	// verified against the recipient inbox. Any other error means
	// nothing was delivered: the caller falls back to one cold full send
	// at the fire time.
	Commit(cfg mailer.MailConfig, staged any) error
	// Send is the cold path: one full delivery attempt — connect, auth,
	// envelope, upload, dot, answer — back to back.
	Send(cfg mailer.MailConfig, session any, msg []byte, to []string) error
	// Discard releases a session that will no longer be used.
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

func (RealCourier) Stage(cfg mailer.MailConfig, session any, msg []byte, to []string) (any, error) {
	sess, _ := session.(*mailer.Session)
	return cfg.StageOn(sess, msg, to)
}

func (RealCourier) Commit(cfg mailer.MailConfig, staged any) error {
	st, _ := staged.(*mailer.Staged)
	if st == nil {
		return fmt.Errorf("jobber: no staged transaction to commit")
	}
	return st.Commit()
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

// minWarmBudget is the least time-to-fire at which an eagerly dispatched
// job still attempts warmup — the boundary between "dispatched at the
// window opening" (warm) and "scheduled inside the window, or catching
// up" (cold). It is ~3x the measured connect+auth cost against the real
// provider (~100-300ms), so a warmup that starts with this much time
// left comfortably finishes before the deadline. Capped at half the
// job's warm window (see stagedLead) so a job dispatched at its own
// window opening always warms, whatever the window's size.
const minWarmBudget = time.Second

// Staging estimates for sizing a job's warm window (see stagedLead).
const (
	// connectAuthBudget: open + authenticate the session. Measured
	// ~100-300ms against the real provider; one second is generous.
	connectAuthBudget = time.Second
	// rcptBudget: one envelope round trip per recipient — net/smtp is
	// lock-step (no pipelining), so a long recipient list pays RTT each.
	rcptBudget = 50 * time.Millisecond
)

// defaultHoldBudget is the slack added on top of the staging estimate
// when sizing a staged job's warm window: an upload that finishes on
// estimate holds its dot this long before FireAt; one that overruns
// shrinks the hold (down to zero, into finish-then-dot — never an
// abandoned upload). 20s sits 15x inside the 300s server-timeout floor
// (RFC 5321 §4.5.3.2.7, a SHOULD) and ~4x inside the unit's stop budget
// (TimeoutStopSec=90s), and under the idle timeouts of common
// middleboxes — see TODO.md item 6 for the full clock ladder.
const defaultHoldBudget = 20 * time.Second

// defaultUplinkRate is the assumed payload throughput (decimal bytes/s)
// used to estimate the upload leg of a staged window: a conservative
// floor of the measured fat workload (~20MB payload in 9.5s ≈ 2 MB/s).
// The logged STAGED upload= durations are the data for tightening it.
const defaultUplinkRate = 2 * 1000 * 1000

// defaultMaxPayload is the per-job payload admission default (body +
// attachment bytes). It mirrors the default provider's published
// guarantee — Legalmail certifies delivery only for messages whose
// attachments total up to 70 MB (Manuale Operativo §5.1; max message
// 100 MB) — so a message the provider will not carry dies at
// scheduling time, never at fire time. Decimal MB, like the provider's
// own number; `-max-payload 0` disables the check. The RAM window this
// leaves is bounded: ~2.4x the payload at fire time (decoded job +
// built multipart, base64 ≈ 1.37x), ~16x measured end-to-end with the
// JSON store's rewrite buffers.
const defaultMaxPayload int64 = 70 * 1000 * 1000

// maxRecipients caps the recipient list per job: the default provider
// refuses more than 1000 recipients between To and Cc, and every
// ricevuta echoes the full original message back into the sender's
// mailbox — a list past the cap is a guaranteed fire-time refusal (and
// a mailbox-filling hazard for the sender's own receipts), so it dies
// at scheduling time.
const maxRecipients = 1000

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
	// staged is the held DATA transaction (see Courier.Stage): envelope
	// and full message already at the provider, one dot short of
	// delivery. nil means "deliver cold" — over the warm session when
	// one exists (couriers that do not stage), or from scratch when even
	// that failed. It is runner-local like the rest of warmState.
	staged any
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
// goroutine when its warm window opens (payload-aware, WarmLead is the
// floor — see stagedLead) or when it is due. Runners are fully independent: each builds its message,
// opens its own provider session, waits out its own chunked timer to the
// exact fire instant and delivers. Jobs that share a deadline fire
// CONCURRENTLY, each over its own session, and a slow SMTP send can never
// delay another job's fire: the strict per-job time budget holds no
// matter how many jobs are in flight. Concurrency is bounded by the
// workload: only jobs inside their warm window are handed off, so one
// held session per imminent fire.
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
// Warmup: the job's window opens stagedLead before FireAt. The message
// is built, the crash-safety "inflight" marker is persisted, an
// authenticated provider session is opened, and the whole transaction —
// envelope and full DATA upload — is STAGED and held one dot short of
// delivery, so at FireAt only the terminating dot (the delivery act)
// and the provider's answer remain. Every step is best-effort: any
// failure degrades to a cold send at FireAt and never costs the job its
// deadline ("just go"). Every job is handed off as its warm window
// opens; one that reaches the loop with too little time left to finish
// warmup (scheduled inside the window, or catching up) fires cold — see
// minWarmBudget.
type Scheduler struct {
	store *Store

	// Courier is the delivery seam. Production leaves it as RealCourier.
	Courier Courier

	// MinLead is the smallest allowed scheduling horizon: Schedule rejects
	// fire times closer than MinLead from now, which by construction
	// eliminates jobs with a deadline already in the past. The future
	// 24/7 service sets this to 24h; the CLI test tool defaults to 0.
	MinLead time.Duration

	// WarmLead is the FLOOR of a job's warm window in time. Windows are
	// payload-aware (see stagedLead): a job whose staging needs longer —
	// many recipients, a fat upload — opens earlier, by its own
	// estimate plus HoldBudget; every warmed job is then staged and holds
	// its dot until FireAt (see Courier.Stage). WarmLead 0 disables
	// warmup — and with it staging — entirely: every job fires cold.
	WarmLead time.Duration

	// HoldBudget is the slack added on top of the staging estimate when
	// sizing a job's warm window: an upload that finishes on estimate
	// holds its dot this long before FireAt; one that overruns shrinks
	// the hold, down to zero and into finish-then-dot ("just go": the
	// dot is floor-anchored, never before FireAt, never delayed past the
	// earliest instant possible after it). Default 20s (see
	// defaultHoldBudget for the clock-ladder rationale).
	HoldBudget time.Duration

	// UplinkRate is the assumed payload throughput (bytes per second)
	// used to estimate the upload leg of a staged window. Zero or negative
	// disables the upload term — the window then covers only session,
	// envelope and hold, and an overrunning upload falls to finish-then-dot
	// (correct, just later). Default 2 MB/s (see defaultUplinkRate).
	UplinkRate int64

	// MaxPayload is the largest accepted message payload: body bytes
	// plus attachment bytes. Jobs above it are rejected at scheduling
	// time — admission control: the message must be materialized in RAM
	// at fire time, and the provider does not guarantee delivery beyond
	// its own published bound anyway. Zero or negative disables the
	// check (see defaultMaxPayload for the default's rationale).
	MaxPayload int64

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
		HoldBudget: defaultHoldBudget,
		UplinkRate: defaultUplinkRate,
		MaxPayload: defaultMaxPayload,
		wake:       make(chan struct{}, 1),
		runnerDone: make(chan struct{}, 1),
		fatal:      make(chan error, 1),
		active:     make(map[string]struct{}),
	}
}

// stagedLead sizes one job's warm window: the time its runner needs to
// open and authenticate a session (connectAuthBudget), run the lock-step
// envelope (rcptBudget per recipient), stream the payload at the assumed
// uplink rate, and then hold the dot for HoldBudget before FireAt. Only
// jobs inside their window are handed off, so a provider session is held
// no longer than this. WarmLead floors the result (short windows stay
// as configured); WarmLead 0 means no warmup at all, hence no window.
func (s *Scheduler) stagedLead(j Job) time.Duration {
	if s.WarmLead <= 0 {
		return 0
	}
	est := connectAuthBudget + time.Duration(len(j.Content.To))*rcptBudget
	if s.UplinkRate > 0 {
		payload := int64(len(j.Content.Body)) + attachmentBytes(j.Content.Attachments)
		est += time.Duration(payload) * time.Second / time.Duration(s.UplinkRate)
	}
	lead := est + s.HoldBudget
	if lead < s.WarmLead {
		lead = s.WarmLead
	}
	return lead
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
	if len(c.To) > maxRecipients {
		return Job{}, fmt.Errorf("jobber: %d recipients, but a single certified send is capped at %d (the default provider refuses more, and every ricevuta echoes the whole message back into the sender's mailbox); split the mailing into several jobs", len(c.To), maxRecipients)
	}
	for _, rcpt := range c.To {
		a, err := mail.ParseAddress(rcpt)
		if err != nil {
			return Job{}, fmt.Errorf("jobber: invalid recipient address %q", rcpt)
		}
		// Bare-form rule: the envelope carries the address verbatim
		// (RCPT TO:<%s>), so a display-name or bracketed form — which
		// ParseAddress happily accepts — would go out as
		// RCPT TO:<Name <a@b>>, a wire-level syntax error. Reject it here,
		// at scheduling time, never at fire time.
		if a.Address != rcpt {
			return Job{}, fmt.Errorf("jobber: recipient %q must be the bare address %q (display names break the SMTP envelope)", rcpt, a.Address)
		}
		// RFC 5321 caps the whole RCPT path (surrounding angle brackets
		// included) at 256 octets: anything longer is refused by every
		// conformant provider — reject it here, at scheduling time.
		if len(rcpt) > 254 {
			return Job{}, fmt.Errorf("jobber: recipient %q is too long for the SMTP envelope (RFC 5321 caps the path at 256 octets)", rcpt)
		}
	}
	sender, err := mail.ParseAddress(cfg.Username)
	if err != nil {
		return Job{}, fmt.Errorf("jobber: invalid certified address %q", cfg.Username)
	}
	// The certified identity must be bare too: it becomes MAIL FROM
	// verbatim, and a display-name or bracketed form — accepted by
	// ParseAddress — would break the envelope and poison the
	// pre-generated Message-ID's domain.
	if sender.Address != cfg.Username {
		return Job{}, fmt.Errorf("jobber: certified address %q must be the bare address %q (display names break the SMTP envelope)", cfg.Username, sender.Address)
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

	// Payload admission, fail-early like everything above: the message
	// must be fully materialized in RAM at fire time (decoded job +
	// built multipart, base64 ≈ 1.37x), and the default provider does
	// not guarantee delivery above its published attachment bound at
	// all — a message that big is a scheduling mistake, and it must die
	// here, never at fire time. Zero or negative disables the check.
	if s.MaxPayload > 0 {
		if payload := int64(len(c.Body)) + attachmentBytes(c.Attachments); payload > s.MaxPayload {
			return Job{}, fmt.Errorf("jobber: message payload is %s, above the %s per-job limit (-max-payload, in MB; 0 disables it — the default mirrors the provider's guaranteed delivery bound)", humanBytes(payload), humanBytes(s.MaxPayload))
		}
	}

	// The Message-ID is pre-generated and persisted so that, even if the
	// process dies mid-send, the recipient inbox can be searched for it.
	mid, err := mailer.GenerateMessageID(mailer.DomainOf(cfg.Username))
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
	s.mu.Unlock()
	// Always hint the loop, not only for a new head: warm windows are
	// payload-aware, so a job deep in the queue can carry the EARLIEST
	// window (a fat job opens long before a thin head) — the loop must
	// re-evaluate the whole queue on every new pending job. The hint may
	// drop; every consumer re-reads the authoritative queue after
	// waking, so a dropped hint only delays the re-evaluation to the
	// next one, never loses it.
	select {
	case s.wake <- struct{}{}:
	default:
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
	s.mu.Unlock()

	if !arm {
		log.Printf("SCHEDULED job=%s msgid=%s — idempotent replay: already persisted (state=%s), left untouched",
			j.ID, j.MessageID, j.State)
		return j
	}
	// Always hint (see ScheduleWithID): a re-armed job can carry the
	// earliest warm window even when it is not the queue's head.
	select {
	case s.wake <- struct{}{}:
	default:
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

	// Boot sweep: rows persisted by an older gomailer can be terminal
	// and still carry their mailbox credential (the strip-on-terminal
	// invariant in Store.Update postdates them). Blank those in ONE
	// atomic rewrite; pending and inflight rows keep theirs — the
	// firing loop replays them at fire time.
	if swept, err := s.store.BlankTerminalPasswords(); err != nil {
		return fmt.Errorf("jobber: sweeping stale credentials from terminal jobs: %w", err)
	} else if swept > 0 {
		log.Printf("SECURITY swept the stored credential of %d terminal job(s) (they will never fire again; written by an older gomailer); pending jobs keep theirs", swept)
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

		// Hand-off decision, queue-wide: dispatch every job whose warm
		// window has opened (or that is due). Windows are payload-aware
		// (stagedLead), so a fat job deep in the queue can open long
		// BEFORE the head's — a head-only loop would dispatch it late and
		// shrink its hold into an overrun. Dispatching EAGERLY — rather
		// than only while the window is still strictly in the future — is
		// what lets every job sharing a deadline get its own warm session:
		// under the old rule the loop handed the first job off at the
		// window opening, then judged every follower — evaluated a
		// microsecond later — "already past the window" and fired it cold
		// at FireAt. Warmup is still skipped when too little time remains
		// to finish it before the deadline (a job scheduled inside its
		// window, or catching up) — decided from the remaining time, not
		// from which side of the window edge the loop happens to be on.
		now := time.Now()
		var nextWindow time.Time
		dispatched := 0
		for i := 0; i < len(s.pending); {
			j := s.pending[i]
			lead := s.stagedLead(j)
			windowStart := j.FireAt
			if lead > 0 {
				windowStart = j.FireAt.Add(-lead)
			}
			windowOpen := lead > 0 && !now.Before(windowStart)
			if windowOpen || !now.Before(j.FireAt) {
				warm := lead > 0 && j.FireAt.Sub(now) >= min(minWarmBudget, lead/2)
				s.pending = slices.Delete(s.pending, i, i+1)
				s.active[j.ID] = struct{}{}
				go s.runJob(j, warm)
				dispatched++
				continue // re-examine the slot: the queue shifted under it
			}
			if nextWindow.IsZero() || windowStart.Before(nextWindow) {
				nextWindow = windowStart
			}
			i++
		}
		if dispatched > 0 {
			s.mu.Unlock()
			continue // re-scan: state may have changed while dispatching
		}
		s.mu.Unlock()

		// No window is open yet: sleep until the earliest one does and
		// re-evaluate. The wait is chunked and recomputed against the
		// absolute instant (see maxArm); an earlier submission wakes it
		// early to re-arm (the wake hint may drop, so every path re-reads
		// the queue after waking).
		if err := s.waitForUntil(ctx, nextWindow); err != nil {
			return s.stop(err)
		}
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

// runJob owns one job from hand-off to its terminal state: warmup and
// staging (when a warm window was handed over), the chunked wait to the
// exact fire instant, and the delivery. Single ownership is what preserves
// the at-most-one-completed-delivery discipline under concurrency.
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

// waitForUntil blocks until the given instant, or returns early so the
// tender can re-evaluate (an earlier job was scheduled mid-sleep, which
// arrives as a wake hint). Either way the caller re-reads the queue and
// recomputes the next wait, so the distinction is not reported. An error
// is returned only when the run must stop (fatal store error or
// cancellation).
//
// The sleep is re-armed in chunks of at most maxArm, recomputing the
// remaining wait against the absolute instant on every tick (see maxArm).
func (s *Scheduler) waitForUntil(ctx context.Context, until time.Time) error {
	for wait := time.Until(until); wait > 0; wait = time.Until(until) {
		arm := min(wait, maxArm)
		timer := time.NewTimer(arm)
		select {
		case <-timer.C:
		case <-s.wake:
			timer.Stop()
			return nil
		case err := <-s.fatal:
			timer.Stop()
			return err
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	return nil
}

// prewarm moves everything except the delivery act off the fire-time
// critical path: message build, the crash-safety inflight persist (the
// fsync leaves the critical window too), the provider session (measured
// ~100-300ms against the real provider), and — staging — the whole
// transaction: envelope and full DATA upload, held ONE DOT short of
// delivery (see Courier.Stage). At FireAt only the dot and the
// provider's answer remain.
//
// Failures are best-effort — "just go": a session or staging failure
// merely degrades the job to a cold send at FireAt; only a prepare
// failure (the job can never be sent) or a store failure terminates the
// job early.
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

	// Staging ("hold the dot"): envelope + DATA + the full message go
	// onto the fresh session NOW, leaving the transaction one dot short
	// of delivery. A staged job's FIRE is the dot write — everything
	// else already happened off the critical path. Failure degrades to
	// a cold send at FireAt ("just go"): a staging error is pre-dot by
	// construction (see StageOn), so nothing was delivered and nothing
	// can be duplicated.
	if st.session != nil {
		t1 := time.Now()
		st.staged, err = s.Courier.Stage(j.Config, st.session, st.msg, j.Content.To)
		if err != nil {
			s.Courier.Discard(st.session) // idempotent: the failed Stage consumed it (see Courier.Stage)
			st.session, st.staged = nil, nil
			log.Printf("WARM job=%s msgid=%s staging failed (%v): cold fallback at fire time",
				j.ID, j.MessageID, err)
		} else if st.staged != nil {
			log.Printf("STAGED job=%s msgid=%s upload=%s (%s before fire; dot held)",
				j.ID, j.MessageID,
				time.Since(t1).Round(time.Millisecond),
				time.Until(j.FireAt).Round(time.Millisecond))
		}
		// staged == nil with no error: the courier opted out of staging
		// (the job delivers at the fire time over the warm session — the
		// pre-staging behavior). Fakes use this; RealCourier never does.
	}
	return nil
}

// fire delivers one job, recording the full timing paper trail. The
// paths, per the "just go" policy (TODO.md item 6): a staged job writes
// the dot — the delivery act — over its held transaction; a dot write
// that fails left nothing on the wire, so exactly ONE cold full send
// starts at the fire time, never later; a failure at or after the dot
// wraps mailer.ErrDotOut and is TERMINAL (verify manually, never
// re-attempted — a duplicate certified message is worse than a late
// one). fire returns an error only when the store itself failed BEFORE
// the send (a broken store must stop the queue); send failures are
// recorded and reported, not fatal, and a result that cannot be persisted
// is logged loudly instead of failing the job.
//
// The timing stamps (FiredAt/Lateness/SendDuration) bracket the
// delivery attempt that produced the outcome: the Commit for a staged
// job (its first wire act is the dot, so FiredAt IS the dot time), the
// wake instant for a cold job — the drift observable the record pins.
// On the "just go" fallback the stamp MOVES to the cold send that
// actually delivered, so the dead held transaction's cost lands in
// Lateness, never in silence.
func (s *Scheduler) fire(j Job, st *warmState) error {
	started := time.Now()
	enteredLate := started.Sub(j.FireAt)
	if enteredLate < 0 {
		enteredLate = 0
	}
	stamp := started.Format("2006-01-02 15:04:05.000000 -0700")

	var msg []byte
	var sess any
	if st != nil {
		msg, sess = st.msg, st.session
	}

	if enteredLate >= catchupWarn {
		log.Printf("WARNING job=%s msgid=%s FIRED %s LATE (process/machine was not running at the fire time) fired=%s",
			j.ID, j.MessageID, enteredLate.Round(time.Millisecond), stamp)
	} else {
		cold := ""
		if sess == nil {
			cold = " (cold)"
		}
		// For a staged job this line stamps the DOT: the delivery act
		// and the only thing that still had to happen at the fire time.
		log.Printf("FIRE job=%s msgid=%s fired=%s lateness=%s%s",
			j.ID, j.MessageID, stamp, enteredLate, cold)
	}

	// firedAt stamps the delivery attempt that produced the outcome (see
	// JobResult.FiredAt): the Commit for a staged job — its first wire act
	// is the dot, so firedAt IS the dot time — and the wake instant for a
	// cold job (the record's drift observable: nothing else pins the wake
	// decision). On the "just go" fallback the stamp MOVES to the cold
	// send's start, so Lateness carries the time the dead held transaction
	// consumed instead of silently dropping it.
	firedAt := started
	var sendErr error
	var sendDur time.Duration
	switch {
	case st != nil && st.staged != nil:
		// Staged: everything but the dot is already at the provider.
		firedAt = time.Now()
		sendErr = s.Courier.Commit(j.Config, st.staged)
		sendDur = time.Since(firedAt)
		if sendErr != nil && !errors.Is(sendErr, mailer.ErrDotOut) {
			// The dot never left the wire. "Just go": one cold full send
			// starting NOW — at the fire time, never later, never an
			// abandoned job. (An ErrDotOut error takes the other branch of
			// the contract below: terminal, because the dot may have
			// gone out.)
			log.Printf("WARNING job=%s msgid=%s held transaction lost before the dot after %s (%v): cold send at fire time",
				j.ID, j.MessageID, sendDur.Round(time.Millisecond), sendErr)
			firedAt = time.Now()
			sendErr = s.Courier.Send(j.Config, nil, msg, j.Content.To)
			sendDur = time.Since(firedAt)
		}
	default:
		// Cold path (no staged transaction): build and persist inflight
		// here — still before any network I/O, as the crash-safety marker
		// requires — then deliver, over the warm session when one exists
		// (a courier that does not stage, or a staging failure).
		if msg == nil {
			prepared, err := s.Courier.Prepare(j.Config, &j.Content)
			if err != nil {
				if ferr := s.failNow(j, "prepare: "+err.Error()); ferr != nil {
					return ferr
				}
				return errJobTerminal
			}
			msg = prepared
			j.State = StateInflight
			if err := s.store.Update(j); err != nil {
				return fmt.Errorf("jobber: persisting inflight state for job %s: %w", j.ID, err)
			}
		}
		firedAt = started
		sendStart := time.Now()
		sendErr = s.Courier.Send(j.Config, sess, msg, j.Content.To)
		sendDur = time.Since(sendStart)
	}

	lateness := firedAt.Sub(j.FireAt)
	if lateness < 0 {
		lateness = 0
	}
	j.Result = &JobResult{FiredAt: firedAt, Lateness: lateness, SendDuration: sendDur}
	if sendErr != nil {
		j.State = StateFailed
		j.Result.Err = sendErr.Error()
		if errors.Is(sendErr, mailer.ErrDotOut) {
			// The dot left the wire but the outcome is unknown — the same
			// manual-verification rule as a crash during the send.
			j.Result.Err += " — NOT auto-resent; verify the recipient inbox for the Message-ID"
		}
	} else {
		j.State = StateSent
	}
	if uerr := s.store.Update(j); uerr != nil {
		// The send already happened, but its outcome could not be
		// recorded. The store still says "inflight", so the next boot
		// will flag it for manual verification. Loud, not queue-fatal.
		log.Printf("CRITICAL job=%s send result could not be persisted: %v (next boot will mark it for manual verification)", j.ID, uerr)
	}

	if sendErr != nil {
		log.Printf("FAILED job=%s msgid=%s send=%s err=%v",
			j.ID, j.MessageID, sendDur.Round(time.Millisecond), sendErr)
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

// attachmentBytes totals the raw bytes of every attachment — the
// scheduling-time snapshot that must live in the store until the fire
// time and be materialized in RAM when the message is built.
func attachmentBytes(atts []mailer.Attachment) int64 {
	var n int64
	for _, a := range atts {
		n += int64(len(a.Data))
	}
	return n
}

// humanBytes renders a byte count in decimal units (MB = 10^6), the way
// the -max-payload flag and provider limits are expressed.
func humanBytes(n int64) string {
	d := float64(n)
	for _, unit := range []string{"B", "kB", "MB", "GB", "TB"} {
		if d < 1000 {
			if unit == "B" {
				return fmt.Sprintf("%d B", n)
			}
			return fmt.Sprintf("%.1f %s", d, unit)
		}
		d /= 1000
	}
	return fmt.Sprintf("%.1f PB", d)
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
