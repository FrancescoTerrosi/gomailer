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
	line("To: %s", foldAddressList(mapStrings(c.To, formatAddress)))
	line("Subject: %s", renderSubject(c.Subject))
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

// smtpLineLimit is the maximum line length, excluding CRLF, that RFC 5321
// §4.5.3.1.6 obliges every SMTP receiver to accept. Staying inside it (and
// inside RFC 2045's 76-character base64 lines) keeps long text and large
// attachments from being a delivery risk on a strict MTA — a PEC provider
// rejecting the busta over line length would be a silent, late failure.
const smtpLineLimit = 998

// headerWidth is the per-line budget for folded headers: comfortably inside
// smtpLineLimit with room for the "Subject: " prefix and continuation spaces.
const headerWidth = 900

// renderSubject renders the Subject value, folding anything that would
// overflow the SMTP line limit. Folding an unstructured header at
// whitespace is lossless: unfolding removes only the CRLF, so a compliant
// reader sees the identical value. Values that cannot fold at whitespace
// (one huge token, or non-ASCII long enough that its encoded form
// overflows) are emitted as several RFC 2047 B-words joined by folds —
// adjacent encoded words concatenate on decoding, so that is lossless too.
func renderSubject(subj string) string {
	subj = stripNewlines(subj)
	if v := encodeHeaderWord(subj); len(v) <= headerWidth && !strings.ContainsAny(v, "\r\n") {
		return v
	}
	if isASCII(subj) && longestToken(subj) <= headerWidth {
		return foldAtSpaces(subj)
	}
	return foldBWords(subj)
}

// foldAddressList joins and folds an address list at its ", " separators,
// so a long recipient list cannot overflow the line limit. The fold sits
// between addresses, where RFC 5322 allows FWS: unfolding restores the
// identical list.
func foldAddressList(addrs []string) string {
	var b strings.Builder
	n := 0
	for i, a := range addrs {
		if i > 0 {
			b.WriteString(",")
			if n+len(a)+2 > headerWidth {
				b.WriteString("\r\n ") // fold between addresses
				n = 1
			} else {
				b.WriteString(" ")
				n++
			}
		}
		b.WriteString(a)
		n += len(a)
	}
	return b.String()
}

// longestToken returns the longest space-free run in s.
func longestToken(s string) int {
	longest, n := 0, 0
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' || s[i] == '\t' {
			n = 0
		} else {
			n++
			if n > longest {
				longest = n
			}
		}
	}
	return longest
}

// foldAtSpaces folds s at its spaces so no line exceeds headerWidth. Each
// fold replaces a space with CRLF+space — unfolding restores the exact
// original byte for byte.
func foldAtSpaces(s string) string {
	var b strings.Builder
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' && n >= headerWidth {
			b.WriteString("\r\n ")
			n = 1
			continue
		}
		b.WriteByte(s[i])
		n++
	}
	return b.String()
}

// foldBWords encodes s as a run of small RFC 2047 B-words separated by
// folds. Adjacent encoded words are concatenated (the separating
// whitespace dropped) on decoding, so the decoded value is exact. The
// words are built directly rather than through mime.BEncoding, which
// passes pure-ASCII text through unencoded — this path exists exactly
// for tokens that cannot be folded any other way.
func foldBWords(s string) string {
	var words []string
	for start := 0; start < len(s); {
		end := start + 24
		if end > len(s) {
			end = len(s)
		} else if end < len(s) {
			for end > start+1 && s[end]&0xC0 == 0x80 {
				end-- // keep UTF-8 runes whole
			}
		}
		words = append(words, "=?UTF-8?B?"+base64.StdEncoding.EncodeToString([]byte(s[start:end]))+"?=")
		start = end
	}
	return strings.Join(words, "\r\n ")
}

// stripNewlines removes CR and LF from a header value: embedded line
// breaks would split (and so forge) headers.
func stripNewlines(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	return strings.NewReplacer("\r\n", " ", "\r", " ", "\n", " ").Replace(s)
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
// Values that fail address parsing are passed through with any CR/LF
// stripped — they will be rejected by the provider, but must never be able
// to forge headers.
func formatAddress(addr string) string {
	a, err := mail.ParseAddress(addr)
	if err != nil || a.Address == "" {
		return stripNewlines(addr)
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
// ASCII bodies whose lines fit the SMTP limit stay 7-bit (bare LF/CR line
// endings are normalized to the CRLF RFC 5322 mandates on the wire);
// anything else — non-ASCII, or one line longer than a receiver must
// accept — is base64 encoded byte-exact, in RFC 2045 76-character lines.
func encodeText(body string) (string, string) {
	text := normalizeCRLF(body)
	if isASCII(text) && maxLineLen(text) <= smtpLineLimit {
		return "7bit", text
	}
	return "base64", b64([]byte(text))
}

// normalizeCRLF rewrites bare LF and bare CR line endings into the CRLF
// that RFC 5322 mandates. Already-CRLF text is untouched.
func normalizeCRLF(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return strings.ReplaceAll(s, "\n", StdNewline)
}

// maxLineLen returns the longest line length, excluding CRLF.
func maxLineLen(s string) int {
	longest, n := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\r':
		case '\n':
			if n > longest {
				longest = n
			}
			n = 0
		default:
			n++
		}
	}
	if n > longest {
		longest = n
	}
	return longest
}

// b64 encodes data as RFC 2045 base64 wrapped at 76 characters per line
// with CRLF: an unwrapped encoder emits one line of unbounded length, which
// RFC 2045 forbids and strict SMTP receivers may reject.
func b64(data []byte) string {
	enc := base64.StdEncoding.EncodeToString(data)
	var b strings.Builder
	for len(enc) > 76 {
		b.WriteString(enc[:76])
		b.WriteString(StdNewline)
		enc = enc[76:]
	}
	b.WriteString(enc)
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

// randomBoundary returns a unique MIME multipart boundary string.
func randomBoundary() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return fmt.Sprintf("pec_%x", raw[:])
}

func mapStrings(in []string, f func(string) string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = f(s)
	}
	return out
}
