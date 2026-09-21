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
// configured PEC provider.
func (c MailConfig) Send(content *MailContent) error {
	if content == nil {
		return fmt.Errorf("mailer: nil content")
	}

	// The certified identity is the mailbox itself: the From header and the
	// envelope sender must both be the authenticated PEC address.
	if content.From == "" {
		content.From = c.Username
	}
	if content.From != c.Username {
		return fmt.Errorf("mailer: From %q must equal the certified PEC address %q", content.From, c.Username)
	}

	if content.MessageID == "" {
		id, err := GenerateMessageID(domainOf(c.Username))
		if err != nil {
			return fmt.Errorf("mailer: generating message-id: %w", err)
		}
		content.MessageID = id
	}

	msg, err := content.build(c.Username, content.MessageID)
	if err != nil {
		return err
	}

	return c.deliver(c.Username, content.To, msg)
}

// deliver establishes a TLS-protected SMTP session (implicit TLS on port 465,
// STARTTLS otherwise), authenticates, and transfers the message. PEC mandates
// an encrypted transport; no plaintext SMTP is ever used.
func (c MailConfig) deliver(from string, to []string, msg []byte) error {
	addr := net.JoinHostPort(c.Hostname, c.Port)

	var (
		client *smtp.Client
		err    error
	)

	if c.Port == "465" {
		// Implicit TLS (SMTPS).
		conn, derr := tls.Dial("tcp", addr, c.tlsConfig())
		if derr != nil {
			return fmt.Errorf("mailer: tls dial: %w", derr)
		}
		client, err = smtp.NewClient(conn, c.Hostname)
	} else {
		// Plain connection, then upgrade with STARTTLS.
		conn, derr := net.Dial("tcp", addr)
		if derr != nil {
			return fmt.Errorf("mailer: dial: %w", derr)
		}
		client, err = smtp.NewClient(conn, c.Hostname)
		if err == nil {
			err = client.StartTLS(c.tlsConfig())
		}
	}
	if err != nil {
		return fmt.Errorf("mailer: smtp session: %w", err)
	}
	defer client.Close()

	auth := smtp.PlainAuth("", c.Username, c.Password, c.Hostname)
	if err := client.Auth(auth); err != nil {
		return fmt.Errorf("mailer: auth: %w", err)
	}

	// Envelope sender is the certified address.
	if err := client.Mail(from); err != nil {
		return fmt.Errorf("mailer: mail from: %w", err)
	}
	for _, rcpt := range to {
		if err := client.Rcpt(rcpt); err != nil {
			return fmt.Errorf("mailer: rcpt to %q: %w", rcpt, err)
		}
	}

	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("mailer: data: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("mailer: write message: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("mailer: close message: %w", err)
	}

	return client.Quit()
}

func (c MailConfig) tlsConfig() *tls.Config {
	return &tls.Config{
		ServerName:         c.Hostname,
		InsecureSkipVerify: c.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}
}
