package main

import (
	"log"

	"gomailer/internal/mailer"
)

func main() {
	cfg := mailer.MailConfig{
		Hostname: "sendm.cert.legalmail.it",
		Port:     "465",                     // implicit TLS (SMTPS)
		Username: "massmailer@legalmail.it", // certified PEC mailbox
		Password: "mySecretPassword123",
	}

	content := &mailer.MailContent{
		From:         "massmailer@legalmail.it", // MUST equal Username
		To:           []string{"receiver@pect.it"},
		Subject:      "Oggetto della PEC",
		Body:         "Corpo del messaggio certificato.",
		TipoRicevuta: mailer.RicevutaCompleta,

		// Attachments: the message becomes multipart/mixed, with the text
		// body first and each attachment base64-encoded after it.
		Attachments: []mailer.Attachment{
			mailer.NewAttachment("contratto.pdf", []byte("%PDF-1.4 ...")),
		},
	}

	if err := cfg.Send(content); err != nil {
		log.Fatalf("> COULD NOT SEND EMAIL: %v", err)
	}

	log.Println("> EMAIL SENT!")
}
