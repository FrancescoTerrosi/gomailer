package mailer

// Wire-format tests for the built PEC message: the busta headers, RFC 2045
// line-wrapped base64 (long text and attachments were previously emitted as
// single over-long lines — an SMTP 998-octet violation that a strict MTA
// may reject), header folding for long Subject and To values, and exact
// round-trips of everything the builder encodes.

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

func testBuild(t *testing.T, c *MailContent) (map[string]string, string, string) {
	t.Helper()
	cfg := MailConfig{Hostname: "smtp.pec.test", Port: "465", Username: c.From, Password: "x"}
	msg, err := cfg.Prepare(c)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	headers, body := parseMessage(t, string(msg))
	return headers, body, string(msg)
}

// parseMessage splits a built message into its unfolded headers and body.
func parseMessage(t *testing.T, msg string) (map[string]string, string) {
	t.Helper()
	head, body, ok := strings.Cut(msg, "\r\n\r\n")
	if !ok {
		t.Fatalf("no header/body break in message:\n%s", msg)
	}
	headers := map[string]string{}
	name := ""
	for _, ln := range strings.Split(head, "\r\n") {
		if strings.HasPrefix(ln, " ") { // folded continuation: unfold (RFC 5322 removes the CRLF, keeps the WSP)
			headers[name] += " " + strings.TrimLeft(ln, " ")
			continue
		}
		var v string
		var found bool
		name, v, found = strings.Cut(ln, ": ")
		if !found {
			t.Fatalf("malformed header line %q", ln)
		}
		headers[name] = v
	}
	return headers, body
}

func longestLine(s string) int {
	longest := 0
	for _, ln := range strings.Split(s, "\r\n") {
		if len(ln) > longest {
			longest = len(ln)
		}
	}
	return longest
}

func decodeBody(t *testing.T, cte, body string) string {
	t.Helper()
	switch cte {
	case "7bit":
		return body
	case "base64":
		clean := strings.TrimSuffix(body, "\r\n")
		out, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(clean, "\r\n", ""))
		if err != nil {
			t.Fatalf("decoding base64 body: %v", err)
		}
		return string(out)
	default:
		t.Fatalf("unexpected Content-Transfer-Encoding %q", cte)
		return ""
	}
}

var bwordRe = regexp.MustCompile(`=\?UTF-8\?B\?([A-Za-z0-9+/=]+)\?=`)

// decodeSubject unfolds the header and decodes RFC 2047 B-words, dropping
// the whitespace between adjacent encoded words as a compliant reader does.
func decodeSubject(v string) string {
	unfolded := strings.ReplaceAll(v, "\r\n ", " ")
	unfolded = strings.ReplaceAll(unfolded, "?= =?", "?==?")
	return bwordRe.ReplaceAllStringFunc(unfolded, func(m string) string {
		b, err := base64.StdEncoding.DecodeString(bwordRe.FindStringSubmatch(m)[1])
		if err != nil {
			return m
		}
		return string(b)
	})
}

func TestBuildMandatoryAndPECHeaders(t *testing.T) {
	c := MailContent{From: "sender@pec.test", To: []string{"dest@pec.test"}, Subject: "contratto", Body: "testo"}
	headers, body, _ := testBuild(t, &c)

	for _, h := range []string{"From", "To", "Subject", "Date", "Message-ID", "MIME-Version"} {
		if _, ok := headers[h]; !ok {
			t.Fatalf("missing mandatory header %q: %v", h, headers)
		}
	}
	if headers["X-Trasporto"] != TrasportoPEC {
		t.Fatalf("X-Trasporto = %q, want %q", headers["X-Trasporto"], TrasportoPEC)
	}
	if headers["X-TipoRicevuta"] != string(RicevutaCompleta) {
		t.Fatalf("X-TipoRicevuta = %q, want %q (default)", headers["X-TipoRicevuta"], RicevutaCompleta)
	}
	if _, ok := headers["X-Riferimento-Message-ID"]; ok {
		t.Fatal("X-Riferimento-Message-ID must be absent outside replies")
	}
	if _, err := time.Parse(time.RFC1123Z, headers["Date"]); err != nil {
		t.Fatalf("Date %q is not RFC 5322 form: %v", headers["Date"], err)
	}
	if !regexp.MustCompile(`^<[^<>@\s]+@[^<>@\s]+>$`).MatchString(headers["Message-ID"]) {
		t.Fatalf("Message-ID %q is not RFC 5322 form", headers["Message-ID"])
	}
	if headers["Content-Transfer-Encoding"] != "7bit" {
		t.Fatalf("short ASCII body must stay 7bit, got %q", headers["Content-Transfer-Encoding"])
	}
	if body != "testo\r\n" {
		t.Fatalf("7bit body = %q", body)
	}
}

func TestReplyCarriesRiferimento(t *testing.T) {
	c := MailContent{
		From: "sender@pec.test", To: []string{"dest@pec.test"}, Subject: "re", Body: "x",
		RiferimentoMessageID: "<orig@pec.test>",
	}
	headers, _, _ := testBuild(t, &c)
	if headers["X-Riferimento-Message-ID"] != "<orig@pec.test>" {
		t.Fatalf("X-Riferimento-Message-ID = %q", headers["X-Riferimento-Message-ID"])
	}
}

func TestCRLFLineEndingsEverywhere(t *testing.T) {
	c := MailContent{
		From:    "sender@pec.test",
		To:      []string{"d1@pec.test", "d2@pec.test"},
		Subject: strings.Repeat("soggetto molto lungo ", 80), // forces folding
		Body:    "prima riga\nseconda riga",                  // forces normalization
	}
	_, _, msg := testBuild(t, &c)
	for i := 0; i < len(msg); i++ {
		switch msg[i] {
		case '\n':
			if i == 0 || msg[i-1] != '\r' {
				t.Fatalf("bare LF at offset %d", i)
			}
		case '\r':
			if i+1 >= len(msg) || msg[i+1] != '\n' {
				t.Fatalf("bare CR at offset %d", i)
			}
		}
	}
}

func TestBareLFBodyNormalized(t *testing.T) {
	c := MailContent{From: "sender@pec.test", To: []string{"dest@pec.test"}, Subject: "s", Body: "prima\nseconda"}
	headers, body, _ := testBuild(t, &c)
	if headers["Content-Transfer-Encoding"] != "7bit" {
		t.Fatalf("ASCII multiline body must stay 7bit, got %q", headers["Content-Transfer-Encoding"])
	}
	if body != "prima\r\nseconda\r\n" {
		t.Fatalf("normalized body = %q", body)
	}
}

func TestNonASCIIBodyBase64Wrapped(t *testing.T) {
	orig := strings.Repeat("àèìòù corpo di prova con accenti ", 100) // >998 encoded on one line before the fix
	c := MailContent{From: "sender@pec.test", To: []string{"dest@pec.test"}, Subject: "s", Body: orig}
	headers, body, msg := testBuild(t, &c)

	if headers["Content-Transfer-Encoding"] != "base64" {
		t.Fatalf("non-ASCII body must be base64, got %q", headers["Content-Transfer-Encoding"])
	}
	for _, ln := range strings.Split(strings.TrimSuffix(body, "\r\n"), "\r\n") {
		if len(ln) > 76 {
			t.Fatalf("base64 line is %d chars, RFC 2045 wants <= 76", len(ln))
		}
	}
	if longestLine(msg) > smtpLineLimit {
		t.Fatalf("longest message line = %d, want <= %d", longestLine(msg), smtpLineLimit)
	}
	if got := decodeBody(t, headers["Content-Transfer-Encoding"], body); got != orig {
		t.Fatalf("base64 round-trip mismatch: got %d bytes, want %d", len(got), len(orig))
	}
}

func TestOverlongASCIILineFallsBackToBase64(t *testing.T) {
	orig := strings.Repeat("x", 5000) // single ASCII line way past the 998 limit
	c := MailContent{From: "sender@pec.test", To: []string{"dest@pec.test"}, Subject: "s", Body: orig}
	headers, body, msg := testBuild(t, &c)

	if headers["Content-Transfer-Encoding"] != "base64" {
		t.Fatal("an over-long ASCII line must switch the body to base64")
	}
	if longestLine(msg) > smtpLineLimit {
		t.Fatalf("longest message line = %d, want <= %d", longestLine(msg), smtpLineLimit)
	}
	if got := decodeBody(t, "base64", body); got != orig {
		t.Fatal("base64 round-trip mismatch")
	}
}

func TestAttachmentWrappedAndDecoded(t *testing.T) {
	data := make([]byte, 100_000) // a real document, not a token attachment
	for i := range data {
		data[i] = byte(i * 7)
	}
	c := MailContent{
		From: "sender@pec.test", To: []string{"dest@pec.test"}, Subject: "allegato", Body: "vedi allegato",
		Attachments: []Attachment{NewAttachment("contratto.pdf", data)},
	}
	headers, body, msg := testBuild(t, &c)

	if !strings.Contains(headers["Content-Type"], "multipart/mixed") {
		t.Fatalf("expected multipart/mixed, got %q", headers["Content-Type"])
	}
	if longestLine(msg) > smtpLineLimit {
		t.Fatalf("attachment produced a %d-char line, want <= %d", longestLine(msg), smtpLineLimit)
	}

	m := regexp.MustCompile(`boundary="([^"]+)"`).FindStringSubmatch(headers["Content-Type"])
	if m == nil {
		t.Fatalf("no boundary in %q", headers["Content-Type"])
	}
	parts := strings.Split(body, "--"+m[1])
	if len(parts) < 3 {
		t.Fatalf("multipart body parts = %d", len(parts))
	}
	attPart := parts[2]
	if !strings.Contains(attPart, "Content-Type: application/pdf") {
		t.Fatalf("attachment part lost its content type: %q", attPart[:80])
	}
	if !strings.Contains(attPart, `filename="contratto.pdf"`) {
		t.Fatalf("attachment disposition wrong: %q", attPart[:120])
	}
	_, b64body, _ := strings.Cut(attPart, "\r\n\r\n")
	b64body = strings.TrimSuffix(b64body, "\r\n--")
	got, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(b64body, "\r\n", ""))
	if err != nil {
		t.Fatalf("decoding attachment: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("attachment decoded to %d bytes, want %d", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("attachment round-trip mismatch at byte %d", i)
		}
	}
}

func TestLongSubjectFoldsLosslessly(t *testing.T) {
	orig := strings.Repeat("clausola contrattuale ", 100) // ~2300 chars, spaces to fold at
	c := MailContent{From: "sender@pec.test", To: []string{"dest@pec.test"}, Subject: orig, Body: "x"}
	headers, _, msg := testBuild(t, &c)

	if longestLine(msg) > smtpLineLimit {
		t.Fatalf("folded subject produced a %d-char line", longestLine(msg))
	}
	if unfold := strings.ReplaceAll(headers["Subject"], "\r\n ", " "); unfold != orig {
		t.Fatalf("unfolded subject differs:\n got  %q\n want %q", unfold, orig)
	}
}

func TestUnbrokenLongSubjectEncodes(t *testing.T) {
	orig := strings.Repeat("x", 3000) // no spaces to fold at
	c := MailContent{From: "sender@pec.test", To: []string{"dest@pec.test"}, Subject: orig, Body: "x"}
	headers, _, msg := testBuild(t, &c)

	if longestLine(msg) > smtpLineLimit {
		t.Fatalf("encoded subject produced a %d-char line", longestLine(msg))
	}
	if got := decodeSubject(headers["Subject"]); got != orig {
		t.Fatalf("decoded subject = %d bytes, want %d", len(got), len(orig))
	}
}

func TestLongNonASCIISubjectEncodes(t *testing.T) {
	orig := strings.Repeat("à", 400) // B-encoded to ~2100 chars: must chunk into several words
	c := MailContent{From: "sender@pec.test", To: []string{"dest@pec.test"}, Subject: orig, Body: "x"}
	headers, _, msg := testBuild(t, &c)

	if longestLine(msg) > smtpLineLimit {
		t.Fatalf("encoded subject produced a %d-char line", longestLine(msg))
	}
	if got := decodeSubject(headers["Subject"]); got != orig {
		t.Fatalf("decoded subject = %d bytes, want %d", len(got), len(orig))
	}
}

func TestManyRecipientsFoldToHeader(t *testing.T) {
	recipients := make([]string, 60)
	for i := range recipients {
		recipients[i] = fmt.Sprintf("destinatario%03d@pec-certificata.test", i)
	}
	c := MailContent{From: "sender@pec.test", To: recipients, Subject: "s", Body: "x"}
	headers, _, msg := testBuild(t, &c)

	if longestLine(msg) > smtpLineLimit {
		t.Fatalf("long To list produced a %d-char line", longestLine(msg))
	}
	for i, r := range recipients {
		if !strings.Contains(headers["To"], r) {
			t.Fatalf("recipient %d lost from folded To header: %q", i, headers["To"])
		}
	}
	// Order survives the fold.
	pos := 0
	for _, r := range recipients {
		p := strings.Index(headers["To"][pos:], r)
		if p < 0 {
			t.Fatalf("recipient %s out of order in To header", r)
		}
		pos += p
	}
}

func TestNewlinesInHeadersCannotForge(t *testing.T) {
	c := MailContent{
		From: "sender@pec.test", To: []string{"dest@pec.test"},
		Subject: "legittimo\r\nBcc: attaccante@evil.test", Body: "x",
	}
	_, _, msg := testBuild(t, &c)
	if strings.Contains(msg, "\r\nBcc:") {
		t.Fatal("CR/LF in the subject forged an extra header line")
	}
}

func TestGenerateMessageIDShapeAndUniqueness(t *testing.T) {
	a, err := GenerateMessageID("pec.test")
	if err != nil {
		t.Fatal(err)
	}
	b, err := GenerateMessageID("pec.test")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("two Message-IDs are identical")
	}
	if !regexp.MustCompile(`^<[0-9a-f-]+@pec\.test>$`).MatchString(a) {
		t.Fatalf("Message-ID %q has unexpected shape", a)
	}
	if id, _ := GenerateMessageID(""); !strings.HasSuffix(id, "@localhost>") {
		t.Fatalf("empty domain must fall back to localhost: %q", id)
	}
}
