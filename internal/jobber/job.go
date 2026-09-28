package jobber

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"gomailer/internal/mailer"
)

// JobState is the durable lifecycle state of a scheduled job. Every
// transition is persisted in the store before the corresponding real-world
// action happens (see Scheduler.fire for the inflight transition), so a
// crash at any point leaves an unambiguous, recoverable state behind.
type JobState string

const (
	// StatePending: scheduled, waiting for FireAt.
	StatePending JobState = "pending"
	// StateInflight: the SMTP send has started. Written to the store
	// BEFORE the send begins, so a crash mid-send is detectable at the
	// next boot (and never auto-resent).
	StateInflight JobState = "inflight"
	// StateSent: the PEC was accepted by the provider.
	StateSent JobState = "sent"
	// StateFailed: the send failed, or was interrupted by a crash and
	// requires manual verification (see JobResult.Err).
	StateFailed JobState = "failed"
)

// JobResult records what actually happened when the job fired. It is
// persisted together with the terminal state as a timing paper trail for
// human-side verification.
type JobResult struct {
	// FiredAt is the instant the delivery attempt began: the Commit
	// (whose first wire act is the dot) for a staged job, the wake
	// instant for a cold job — and, on the "just go" fallback, the cold
	// send that actually delivered.
	FiredAt time.Time
	// Lateness is how much later than FireAt the delivery attempt began
	// (>= 0). Non-zero lateness means catch-up — the process/machine was
	// not running at FireAt — or, for a fallback send, the time the dead
	// held transaction consumed before the cold retry began.
	Lateness time.Duration
	// SendDuration is the wall-clock length of the SMTP session.
	SendDuration time.Duration
	// Err is the send error, "" on success.
	Err string
}

// Job is one scheduled certified email, and the unit of persistence: a job
// is never "scheduled" until it is on disk, and every state transition is
// written before the next step happens.
type Job struct {
	ID        string
	MessageID string // pre-generated at schedule time; correlates the inbox and the ricevuta
	FireAt    time.Time
	CreatedAt time.Time
	Content   mailer.MailContent
	Config    mailer.MailConfig
	State     JobState
	Result    *JobResult

	// Split-store fields (see store.go): ContentFile names the per-job
	// blob holding the fat half of the payload (body + attachments);
	// ContentSHA256 is that blob's sha256; PayloadBytes is the raw
	// payload size snapshotted at scheduling. In RAM Content stays the
	// whole payload whenever its holder needs it; on disk the row
	// carries only the slim half (To/Subject/receipt type inline), so
	// index rewrites are O(rows), never O(payload) — the fat workload
	// made whole-file rewrites of a 70MB+ history cost seconds per
	// mutation, right on the warm-window critical path.
	//
	// A settled row carries the RELEASED shape: ContentFile == "" with
	// ContentSHA256 != "" — the payload was released at the terminal
	// persist (see Store.releasePayload); the digest remains as the
	// permanent fingerprint of what crossed the wire. No other write
	// path produces this shape.
	ContentFile   string
	ContentSHA256 string
	PayloadBytes  int64
}

// NewJobID returns a short random hex identifier. Clients pre-generate it
// before submitting so the submission can be retried idempotently (see
// Scheduler.ScheduleWithID): a lost acknowledgment must never be able to
// turn into a second, duplicate certified send.
func NewJobID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
