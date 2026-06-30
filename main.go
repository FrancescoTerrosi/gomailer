package main

import (
	"fmt"
	"gomailer/internal/mailer"
	"log"
	"net/smtp"
)

func main() {

	emailConfig := mailer.MailConfig{
		Hostname: "hostname",
		Port:     "587",
		Key:      "myKey",
		Sender:   "sender",
	}

	emailContent := &mailer.MailContent{
		From:    "senderNick",
		To:      []string{"recipient1, recipient2"},
		Subject: "Test",
		Body:    "Hello world! I'm working!",
	}

	err := emailConfig.Send(emailContent)

	if err != nil {
		err = fmt.Errorf("> COULD NOT SEND EMAIL\n %w", err)
		log.Fatal(err)
	}

	log.Println("> EMAIL SENT!")

}

