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
	// FiredAt is the instant the SMTP send actually began.
	FiredAt time.Time
	// Lateness is how much later than FireAt the send began (>= 0).
	// Non-zero lateness means catch-up: the process/machine was not
	// running at FireAt.
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
}

// newJobID returns a short random hex identifier.
func newJobID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
