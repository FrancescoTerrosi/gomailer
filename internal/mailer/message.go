package mailer

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"mime"
	"net/mail"
	"strings"
	"time"
)

// StdNewline is the line terminator mandated by RFC 5322 for the message
// body and headers as transported over SMTP.
const StdNewline = "\r\n"

// Attachment is a binary document attached to the certified message.
type Attachment struct {
	Filename    string // as it appears to the recipient
	ContentType string // e.g. "application/pdf"; defaults to application/octet-stream
	Data        []byte
}

// NewAttachment builds an Attachment, inferring the Content-Type from the
// filename extension. Unknown extensions fall back to application/octet-stream.
func NewAttachment(filename string, data []byte) Attachment {
	ct := mime.TypeByExtension(ext(filename))
	if ct == "" {
		ct = "application/octet-stream"
	}
	return Attachment{Filename: filename, ContentType: ct, Data: data}
}

// ext returns the filename extension including the leading dot ("" when
// absent).
func ext(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i:]
	}
	return ""
}

// MailContent is the certified message payload. For PEC, From must be the
// certified mailbox address (the same identity used to authenticate) and To
// must contain the recipient certified addresses.
type MailContent struct {
	From         string   // certified sender address
	To           []string // certified recipient addresses
	Subject      string
	Body         string
	MessageID    string       // auto-generated when empty
	TipoRicevuta TipoRicevuta // receipt type requested (defaults to completa)

	// RiferimentoMessageID references the Message-ID of the certified message
	// being replied to. Set it only when this is a reply to a PEC.
	RiferimentoMessageID string

	Attachments []Attachment
}

// build assembles the full RFC 5322 message (headers + MIME body) that is
// handed to the SMTP DATA command.
func (c *MailContent) build(from, messageID string) ([]byte, error) {
	if from == "" {
		return nil, fmt.Errorf("mailer: empty sender address")
	}
	if len(c.To) == 0 {
		return nil, fmt.Errorf("mailer: no recipients")
	}

	var b strings.Builder
	line := func(format string, args ...any) {
		fmt.Fprintf(&b, format+StdNewline, args...)
	}

	line("From: %s", formatAddress(from))
	line("To: %s", strings.Join(mapStrings(c.To, formatAddress), ", "))
	line("Subject: %s", encodeHeaderWord(c.Subject))
	line("Date: %s", time.Now().Format(time.RFC1123Z))
	line("Message-ID: %s", messageID)
	line("MIME-Version: 1.0")

	// PEC transport-envelope headers.
	line("%s: %s", HeaderTrasporto, TrasportoPEC)
	if c.RiferimentoMessageID != "" {
		line("%s: %s", HeaderRiferimentoMessageID, c.RiferimentoMessageID)
	}
	line("%s: %s", HeaderTipoRicevuta, c.tipoRicevuta())

	// MIME body: single-part text when there are no attachments, otherwise
	// a multipart/mixed structure with the text first and each attachment
	// after it.
	encoding, text := encodeText(c.Body)

	if len(c.Attachments) == 0 {
		line("Content-Type: text/plain; charset=UTF-8")
		line("Content-Transfer-Encoding: %s", encoding)
		line("")
		b.WriteString(text)
		b.WriteString(StdNewline)
		return []byte(b.String()), nil
	}

	boundary := randomBoundary()
	line("Content-Type: multipart/mixed; boundary=%q", boundary)
	line("")

	// Text part.
	b.WriteString("--" + boundary + StdNewline)
	b.WriteString("Content-Type: text/plain; charset=UTF-8" + StdNewline)
	b.WriteString("Content-Transfer-Encoding: " + encoding + StdNewline)
	b.WriteString(StdNewline)
	b.WriteString(text)
	b.WriteString(StdNewline)

	// Attachment parts.
	for _, a := range c.Attachments {
		ct := a.ContentType
		if ct == "" {
			ct = "application/octet-stream"
		}
		b.WriteString("--" + boundary + StdNewline)
		b.WriteString("Content-Type: " + ct + StdNewline)
		b.WriteString("Content-Transfer-Encoding: base64" + StdNewline)
		b.WriteString("Content-Disposition: attachment; filename=" + encodeFilename(a.Filename) + StdNewline)
		b.WriteString(StdNewline)
		b.WriteString(b64(a.Data))
		b.WriteString(StdNewline)
	}

	b.WriteString("--" + boundary + "--" + StdNewline)
	return []byte(b.String()), nil
}

func (c *MailContent) tipoRicevuta() TipoRicevuta {
	if c.TipoRicevuta == "" {
		return RicevutaCompleta
	}
	return c.TipoRicevuta
}

// randomBoundary returns a unique MIME multipart boundary string.
func randomBoundary() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return fmt.Sprintf("pec_%x", raw[:])
}

// GenerateMessageID builds an RFC 5322 Message-ID using a random UUIDv4 and
// the supplied domain as the right-hand side.
func GenerateMessageID(domain string) (string, error) {
	uuid := make([]byte, 16)
	if _, err := rand.Read(uuid); err != nil {
		return "", err
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // RFC 4122 variant

	id := fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
	if domain == "" {
		domain = "localhost"
	}
	return fmt.Sprintf("<%s@%s>", id, domain), nil
}

// domainOf returns the domain part of a mailbox address.
func domainOf(addr string) string {
	if a, err := mail.ParseAddress(addr); err == nil {
		if i := strings.LastIndexByte(a.Address, '@'); i >= 0 {
			return a.Address[i+1:]
		}
	}
	return "localhost"
}

// formatAddress renders a mailbox address for use in a header. When a display
// name is present it is RFC 2047 encoded, otherwise the bare address is used
// (which is the strict requirement for the certified identity in a PEC).
func formatAddress(addr string) string {
	a, err := mail.ParseAddress(addr)
	if err != nil || a.Address == "" {
		return addr
	}
	if a.Name == "" {
		return a.Address
	}
	return encodeHeaderWord(a.Name) + " <" + a.Address + ">"
}

// encodeHeaderWord RFC 2047 encodes a header value that contains non-ASCII
// characters, leaving already-ASCII values untouched.
func encodeHeaderWord(s string) string {
	if isASCII(s) {
		return s
	}
	return mime.BEncoding.Encode("UTF-8", s)
}

func encodeFilename(name string) string {
	if name == "" {
		return `""`
	}
	if isASCII(name) && !strings.ContainsAny(name, "\"\\\r\n") {
		return `"` + name + `"`
	}
	return mime.BEncoding.Encode("UTF-8", name)
}

// encodeText returns a Content-Transfer-Encoding and the encoded text body.
// ASCII bodies stay 7-bit; anything else is base64 encoded (and line wrapped).
func encodeText(body string) (string, string) {
	if isASCII(body) {
		return "7bit", body
	}
	return "base64", b64([]byte(body))
}

func b64(data []byte) string {
	var b strings.Builder
	enc := base64.NewEncoder(base64.StdEncoding, &b)
	_, _ = enc.Write(data)
	_ = enc.Close()
	return b.String()
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > 0x7f {
			return false
		}
	}
	return true
}

func mapStrings(in []string, f func(string) string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = f(s)
	}
	return out
}
