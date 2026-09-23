package mailer

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
)

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
// Prepare + WarmUp + SendOn instead, so the connection cost lands before
// the fire time.
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
		id, err := GenerateMessageID(domainOf(c.Username))
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
// their liveness is re-probed before any delivery command.
type Session struct {
	client *smtp.Client
}

// WarmUp opens and authenticates a provider session. PEC mandates an
// encrypted transport: implicit TLS (SMTPS) on port 465, STARTTLS
// otherwise; no plaintext SMTP is ever used.
func (c MailConfig) WarmUp() (*Session, error) {
	client, err := c.connect()
	if err != nil {
		return nil, err
	}
	auth := smtp.PlainAuth("", c.Username, c.Password, c.Hostname)
	if err := client.Auth(auth); err != nil {
		client.Close()
		return nil, fmt.Errorf("mailer: auth: %w", err)
	}
	return &Session{client: client}, nil
}

// Close terminates the session (nil-safe).
func (s *Session) Close() {
	if s != nil && s.client != nil {
		s.client.Close()
	}
}

// SendOn delivers a prepared message over the given session, then closes it.
//
// A nil session (cold path) makes SendOn open one first. A session that
// died while being held (provider dropped the idle connection) is detected
// with a liveness probe BEFORE any delivery command — NOOP has no delivery
// semantics — and replaced with a fresh one. Either way at most ONE
// delivery is ever attempted per call: a warm failure can never duplicate
// a message, and a send failure after MAIL/DATA is a real error, never
// silently retried.
func (c MailConfig) SendOn(sess *Session, msg []byte, to []string) error {
	if sess != nil && sess.client.Noop() != nil {
		sess.Close()
		sess = nil
	}
	if sess == nil {
		var err error
		sess, err = c.WarmUp()
		if err != nil {
			return err
		}
	}
	defer sess.client.Close()

	// Envelope sender is the certified address.
	if err := sess.client.Mail(c.Username); err != nil {
		return fmt.Errorf("mailer: mail from: %w", err)
	}
	for _, rcpt := range to {
		if err := sess.client.Rcpt(rcpt); err != nil {
			return fmt.Errorf("mailer: rcpt to %q: %w", rcpt, err)
		}
	}

	w, err := sess.client.Data()
	if err != nil {
		return fmt.Errorf("mailer: data: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("mailer: write message: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("mailer: close message: %w", err)
	}

	return sess.client.Quit()
}

// connect establishes the TLS-protected SMTP session (implicit TLS on port
// 465, STARTTLS otherwise).
func (c MailConfig) connect() (*smtp.Client, error) {
	addr := net.JoinHostPort(c.Hostname, c.Port)

	if c.Port == "465" {
		// Implicit TLS (SMTPS).
		conn, err := tls.Dial("tcp", addr, c.tlsConfig())
		if err != nil {
			return nil, fmt.Errorf("mailer: tls dial: %w", err)
		}
		client, err := smtp.NewClient(conn, c.Hostname)
		if err != nil {
			return nil, fmt.Errorf("mailer: smtp session: %w", err)
		}
		return client, nil
	}

	// Plain connection, then upgrade with STARTTLS.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mailer: dial: %w", err)
	}
	client, err := smtp.NewClient(conn, c.Hostname)
	if err != nil {
		return nil, fmt.Errorf("mailer: smtp session: %w", err)
	}
	if err := client.StartTLS(c.tlsConfig()); err != nil {
		return nil, fmt.Errorf("mailer: starttls: %w", err)
	}
	return client, nil
}

func (c MailConfig) tlsConfig() *tls.Config {
	return &tls.Config{
		ServerName:         c.Hostname,
		InsecureSkipVerify: c.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}
}
