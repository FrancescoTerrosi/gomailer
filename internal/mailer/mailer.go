package mailer

import (
	"crypto/rand"
	"fmt"
	"log"
	"net/smtp"
	"strings"
)

var (
	StdNewline      = "\r\n"
	FromHeader      = "From: "
	ToHeader        = "To: "
	SubjectHeader   = "Subject: "
	MessageIDHeader = "Message-ID: "
	MimeHeader      = "MIME-version: 1.0;" + StdNewline +"Content-Type: text/plain; charset=\"UTF-8\";"
)

type MailConfig struct {
	Hostname string
	Port     string
	Key      string
	Sender   string
}

type MailContent struct {
	From      string
	To        []string
	Subject   string
	Body      string
	MessageID string
}

func (c *MailConfig) Send(m *MailContent) error {

	auth := smtp.PlainAuth(
		"",
		c.Sender,
		c.Key,
		c.Hostname,
	)

	messageID, err := GenerateMessageID(c.Hostname)

	if err != nil {
		return err
	}

	m.MessageID = messageID

	mailMsg := m.ToBytes()
	addr := c.Hostname + ":" + c.Port

	return smtp.SendMail(addr, auth, m.From, m.To, mailMsg)

}

func (m *MailContent) ToBytes() []byte {
	var stringbuilder strings.Builder

	stringbuilder.WriteString(FromHeader)
	stringbuilder.WriteString(m.From)
	stringbuilder.WriteString(StdNewline)

	stringbuilder.WriteString(ToHeader)
	stringbuilder.WriteString(strings.Join(m.To, ","))
	stringbuilder.WriteString(StdNewline)

	stringbuilder.WriteString(SubjectHeader)
	stringbuilder.WriteString(m.Subject)
	stringbuilder.WriteString(StdNewline)

	stringbuilder.WriteString(MessageIDHeader)
	stringbuilder.WriteString(m.MessageID)
	stringbuilder.WriteString(StdNewline)

	stringbuilder.WriteString(MimeHeader)
	stringbuilder.WriteString(StdNewline)
	stringbuilder.WriteString(StdNewline)

	stringbuilder.WriteString(m.Body)

	return []byte(stringbuilder.String())

}

func GenerateMessageID(domain string) (string, error) {
	uuid := make([]byte, 16)

	_, err := rand.Read(uuid)

	if err != nil {
		return "", err
	}

	// UUID compliance
	// clear top 4 bits and set version
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	// clear top 2
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	uuidStr := fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
	log.Printf("SUCCESFULLY GENERATED UUID: %s", uuidStr)

	return fmt.Sprintf("<%s@%s>", uuidStr, domain), nil

}
