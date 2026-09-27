package mailer

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/smtp"
	"time"
)

// Per-command deadlines. RFC 5321 §4.5.3.2 requires an SMTP client to time
// out per command (and per data block), never per whole transaction — and
// a firing runner lives inside the unit's stop budget (TimeoutStopSec=90s),
// so a wedged provider must fail the job loudly instead of hanging a
// graceful stop (the stop-hang corner previously documented in RUNBOOK §8).
// They are vars so the wire tests can shorten them.
var (
	dialBudget    = 30 * time.Second // TCP connect
	commandBudget = 30 * time.Second // any single command exchange or data chunk
	dotBudget     = 30 * time.Second // writing the terminating dot
	// answerBudget bounds reading the 250 after the dot. §4.5.3.2.6
	// SHOULDs ten minutes for the client; that cannot fit the stop
	// budget, and §6.1 obliges the receiver to answer the final dot
	// promptly — 60s is generous, and terminal either way: the dot is
	// out, so a timeout here is ambiguity to verify, never a retry.
	answerBudget = 60 * time.Second
)

// deadlineConn arms a sliding deadline on the transport: every successful
// read or write re-arms it commandBudget into the future. That is exactly
// the §4.5.3.2 discipline — a stalled peer (or wedged middlebox) is
// detected within one budget, while a slow-but-flowing transfer of any
// length is never killed. Explicit SetDeadline calls pass through to the
// raw conn: StageOn CLEARS the deadline for the hold (idle time must not
// fire one), and Commit re-arms it for the dot write and the answer read.
type deadlineConn struct{ net.Conn }

func newDeadlineConn(c net.Conn) *deadlineConn {
	d := &deadlineConn{Conn: c}
	_ = d.SetDeadline(time.Now().Add(commandBudget))
	return d
}

func (d *deadlineConn) Read(b []byte) (int, error) {
	n, err := d.Conn.Read(b)
	if err == nil {
		_ = d.SetDeadline(time.Now().Add(commandBudget))
	}
	return n, err
}

func (d *deadlineConn) Write(b []byte) (int, error) {
	n, err := d.Conn.Write(b)
	if err == nil {
		_ = d.SetDeadline(time.Now().Add(commandBudget))
	}
	return n, err
}

// MailConfig holds the connection and authentication parameters of the
// sender's PEC mailbox.
type MailConfig struct {
	// Hostname is the PEC provider SMTP host, e.g. "smtp.pec.example.it".
	Hostname string
	// Port is "465" for implicit TLS (SMTPS) or "587" for STARTTLS.
	Port string
	// Username is the certified PEC mailbox address. It is used both as the
	// SMTP authentication identity and as the envelope sender (MAIL FROM).
	Username string
	// Password is the mailbox password (or app-specific password).
	Password string
	// InsecureSkipVerify disables TLS certificate verification. It is intended
	// for local/testing only and MUST NOT be enabled in production, where PEC
	// requires a fully verified TLS channel.
	InsecureSkipVerify bool
}

// Send builds a PEC-compliant message and delivers it over TLS to the
// configured PEC provider. It is the cold path: connection setup, auth and
// delivery happen back to back. Scheduled sends with a warmup lead use
// Prepare + WarmUp + StageOn/Commit (or SendOn) instead, so the connection
// and upload cost lands before the fire time.
func (c MailConfig) Send(content *MailContent) error {
	msg, err := c.Prepare(content)
	if err != nil {
		return err
	}
	return c.SendOn(nil, msg, content.To)
}

// Prepare validates the certified identity, fills in the Message-ID when
// absent, and renders the full wire message. It performs no network I/O:
// it is the build half of warmup and may run ahead of the fire time.
func (c MailConfig) Prepare(content *MailContent) ([]byte, error) {
	if content == nil {
		return nil, fmt.Errorf("mailer: nil content")
	}
	// The certified identity is the mailbox itself: the From header and the
	// envelope sender must both be the authenticated PEC address.
	if content.From == "" {
		content.From = c.Username
	}
	if content.From != c.Username {
		return nil, fmt.Errorf("mailer: From %q must equal the certified PEC address %q", content.From, c.Username)
	}
	if content.MessageID == "" {
		id, err := GenerateMessageID(DomainOf(c.Username))
		if err != nil {
			return nil, fmt.Errorf("mailer: generating message-id: %w", err)
		}
		content.MessageID = id
	}
	return content.build(c.Username, content.MessageID)
}

// Session is an open, authenticated SMTP session with the PEC provider.
// Sessions exist so connection setup can be moved off the fire-time
// critical path (warmup). They are safe to hold only for short leads;
// their liveness is re-probed (NOOP) before any delivery command when
// they had time to idle. A session is CONSUMED by its delivery path:
// StageOn either hands it to the returned Staged (whose Commit closes
// it) or closes it on failure — a session that entered a delivery
// attempt must never accept an unrelated command again (see StageOn).
type Session struct {
	client *smtp.Client
	// conn is the transport under the client, carrying the sliding
	// deadline (see deadlineConn). Staging clears its deadline for the
	// hold; Commit re-arms it.
	conn net.Conn
}

// WarmUp opens and authenticates a provider session. PEC mandates an
// encrypted transport: implicit TLS (SMTPS) on port 465, STARTTLS
// otherwise; no plaintext SMTP is ever used.
func (c MailConfig) WarmUp() (*Session, error) {
	client, conn, err := c.connect()
	if err != nil {
		return nil, err
	}
	auth := smtp.PlainAuth("", c.Username, c.Password, c.Hostname)
	if err := client.Auth(auth); err != nil {
		client.Close()
		return nil, fmt.Errorf("mailer: auth: %w", err)
	}
	return &Session{client: client, conn: conn}, nil
}

// Close terminates the session (nil-safe).
func (s *Session) Close() {
	if s != nil && s.client != nil {
		s.client.Close()
	}
}

// ErrDotOut marks every error raised once the terminating dot has left
// the wire (or may have): the delivery act happened, but its outcome is
// unknown. A job that fails this way is TERMINAL — never re-attempted,
// only verified against the recipient inbox for its Message-ID (a
// duplicate certified message is worse than a late manual decision).
var ErrDotOut = errors.New("mailer: delivery dot was written, outcome uncertain")

// Staged is a DATA transaction held one dot short of delivery: the
// envelope and the full message bytes are already at the provider, and
// only the terminating dot — the delivery act — is missing. Nothing
// recipient-visible exists before it: the server processes the stored
// transaction only at the end-of-data indicator (RFC 5321 §4.1.1.4) —
// no queue entry, no accettazione, no ricevuta — so a staged job still
// honors the no-early-deposit floor by writing the dot at the fire time.
//
// While held, nothing may touch the client: textproto auto-closes a
// pending dot-writer on the next command (a NOOP "liveness probe" would
// silently COMMIT the message early), and no probe exists inside a DATA
// transaction anyway. A session that dies during the hold is discovered
// at the dot (Commit), which is precisely what its error contract splits.
type Staged struct {
	sess *Session
	w    io.WriteCloser // the textproto dot-writer; Close writes the dot
}

// StageOn opens the DATA phase over the given session and streams the
// whole message, leaving the transaction one dot short of delivery (see
// Staged). The transport deadline is cleared at the end: the hold is
// deliberate idle time, and Commit re-arms it.
//
// The session is CONSUMED either way: success hands it to the returned
// Staged (whose Commit closes it); ANY failure closes it here. That is
// defense in depth for a textproto trap: a staging failure leaves the
// session with a half-open envelope — and, from the DATA step on, an
// abandoned dot-writer — and textproto auto-closes a pending dot-writer
// when the next command is written (PrintfLine → closeDot), so a NOOP
// "liveness probe" against the leftover session would write the DOT
// and commit a partial message early. A closed session makes that
// misuse impossible: the caller's one safe move — close and retry with
// a single cold full send ("just go") — becomes the only move. Close is
// idempotent, so an extra Discard on the failed session is harmless.
//
// The caller guarantees a live session: the scheduler stages immediately
// after warming (no idle gap), and SendOn probes first (a session that
// had time to idle is detected — and replaced cold — BEFORE any delivery
// command; NOOP has no delivery semantics). Every failure here precedes
// the dot by construction, hence is never ambiguous: the caller may
// safely fall back to a cold full send at the fire time ("just go").
func (c MailConfig) StageOn(sess *Session, msg []byte, to []string) (st *Staged, err error) {
	if sess == nil || sess.client == nil {
		return nil, errors.New("mailer: staging needs a live session")
	}
	// A session that failed to stage must never accept another command
	// (see the contract above).
	defer func() {
		if err != nil {
			sess.Close()
		}
	}()
	// Envelope sender is the certified address.
	if err := sess.client.Mail(c.Username); err != nil {
		return nil, fmt.Errorf("mailer: mail from: %w", err)
	}
	for _, rcpt := range to {
		if err := sess.client.Rcpt(rcpt); err != nil {
			return nil, fmt.Errorf("mailer: rcpt to %q: %w", rcpt, err)
		}
	}
	w, err := sess.data()
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(msg); err != nil {
		return nil, fmt.Errorf("mailer: uploading message: %w", err)
	}
	// Push the bufio tail NOW: the hold must leave the wire idle, so the
	// dot write at the fire time is five bytes, not tail+dot — the µs
	// fire discipline survives the staging (the bufio default would sit
	// on up to 4KB of message tail until the dot flushed it out).
	if err := sess.client.Text.W.Flush(); err != nil {
		return nil, fmt.Errorf("mailer: uploading message: %w", err)
	}
	// The hold begins: no deadline may fire on an idle connection.
	_ = sess.conn.SetDeadline(time.Time{})
	return &Staged{sess: sess, w: w}, nil
}

// data issues the DATA command and returns the RAW dot-writer — not
// smtp.Client.Data's dataCloser, which conflates the terminating dot with
// the 250 read. Splitting the two is what puts the dot on the fire-time
// critical path and everything before it in the warm window, and what
// lets Commit tell "the dot never left" (safe to fall back cold) from
// "the dot left, the answer is lost" (terminal, verify manually). It
// replicates smtp's own cmd/StartResponse/EndResponse bookkeeping.
func (s *Session) data() (io.WriteCloser, error) {
	id, err := s.client.Text.Cmd("DATA")
	if err != nil {
		return nil, fmt.Errorf("mailer: data: %w", err)
	}
	s.client.Text.StartResponse(id)
	defer s.client.Text.EndResponse(id)
	if _, _, err := s.client.Text.ReadResponse(354); err != nil {
		return nil, fmt.Errorf("mailer: data: %w", err)
	}
	return s.client.Text.DotWriter(), nil
}

// Commit writes the terminating dot — THE delivery act — and reads the
// provider's answer. Call it at or after the fire time, exactly once,
// and never before it (the no-early-deposit floor: the dot is what makes
// the bytes a message, RFC 5321 §3.3).
//
// Error contract ("just go"): an error that does NOT wrap ErrDotOut means
// the dot never left the wire as a complete line — the dot rides in one
// small TLS record, and a failed write means a truncated record, which
// the peer discards — so the caller may safely fall back to one cold
// full send. An ErrDotOut error means the dot left but the outcome is
// unknown: terminal, never re-attempted, verify the recipient inbox.
//
// One honest asymmetry: a server that closes GRACEFULLY during the hold
// (FIN, not RST) lets the dot write succeed into the local socket buffer
// and then fails the answer read — that lands on the safe ErrDotOut side
// (terminal + verify), not the cold-retry side, which is the conservative
// answer for an undetectable case.
func (st *Staged) Commit() error {
	defer st.sess.Close()
	// Re-arm after the deadline-free hold: the dot write and the answer
	// read each get their own budget.
	if err := st.sess.conn.SetDeadline(time.Now().Add(dotBudget)); err != nil {
		return fmt.Errorf("mailer: writing the delivery dot: %w", err)
	}
	if err := st.w.Close(); err != nil {
		return fmt.Errorf("mailer: writing the delivery dot: %w", err)
	}
	// The dot is out (Close flushes): everything from here on is
	// ambiguous on failure.
	_ = st.sess.conn.SetDeadline(time.Now().Add(answerBudget))
	if _, _, err := st.sess.client.Text.ReadResponse(250); err != nil {
		return fmt.Errorf("%w: reading the provider's answer: %v", ErrDotOut, err)
	}
	if err := st.sess.client.Quit(); err != nil {
		// The 250 was received: the provider accepted the message. A QUIT
		// failure after it is recorded as a failure (SendOn's existing
		// behavior), but flagged terminal — never retried.
		return fmt.Errorf("%w: accepted, but QUIT failed: %v", ErrDotOut, err)
	}
	return nil
}

// SendOn delivers a prepared message over the given session, then closes
// it — the cold path: everything, including the dot, happens back to
// back at the fire time. It is the composition StageOn + Commit with a
// zero-length hold, and it consumes the session on every path: StageOn
// closes it on failure (see its contract), Commit on success.
//
// A nil session — or one whose NOOP liveness probe fails (the probe runs
// BEFORE any delivery command; NOOP has no delivery semantics) — makes
// SendOn open a fresh one. Either way at most ONE completed delivery is
// ever attempted per call: errors raised after the dot left the wire wrap
// ErrDotOut and are terminal ambiguity (never retried); every other error
// means nothing was delivered.
func (c MailConfig) SendOn(sess *Session, msg []byte, to []string) error {
	if sess != nil && sess.client != nil {
		if sess.client.Noop() != nil {
			sess.Close()
			sess = nil
		}
	}
	if sess == nil {
		var err error
		if sess, err = c.WarmUp(); err != nil {
			return err
		}
	}
	staged, err := c.StageOn(sess, msg, to)
	if err != nil {
		return err // StageOn already consumed (closed) the failed session
	}
	return staged.Commit()
}

// connect establishes the TLS-protected SMTP session (implicit TLS on port
// 465, STARTTLS otherwise) and returns the client together with the
// deadline-carrying transport underneath it.
func (c MailConfig) connect() (*smtp.Client, net.Conn, error) {
	addr := net.JoinHostPort(c.Hostname, c.Port)

	raw, err := net.DialTimeout("tcp", addr, dialBudget)
	if err != nil {
		return nil, nil, fmt.Errorf("mailer: dial: %w", err)
	}
	// net.DialTimeout leaves TCP keepalives on (15s default): a session
	// that dies during a hold is likely flagged by the kernel before the
	// dot — narrowing detection, never carrying correctness.
	conn := newDeadlineConn(raw)

	if c.Port == "465" {
		// Implicit TLS (SMTPS).
		tlsConn := tls.Client(conn, c.tlsConfig())
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("mailer: tls dial: %w", err)
		}
		client, err := smtp.NewClient(tlsConn, c.Hostname)
		if err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("mailer: smtp session: %w", err)
		}
		return client, conn, nil
	}

	// Plain connection, then upgrade with STARTTLS. net/smtp wraps the
	// deadline conn in TLS internally, so every syscall — before and
	// after the upgrade — slides the same deadline.
	client, err := smtp.NewClient(conn, c.Hostname)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("mailer: smtp session: %w", err)
	}
	if err := client.StartTLS(c.tlsConfig()); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("mailer: starttls: %w", err)
	}
	return client, conn, nil
}

func (c MailConfig) tlsConfig() *tls.Config {
	return &tls.Config{
		ServerName:         c.Hostname,
		InsecureSkipVerify: c.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}
}
