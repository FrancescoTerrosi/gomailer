package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gomailer/internal/jobber"
	"gomailer/internal/mailer"
)

// gomailer — PEC scheduled sender (CLI).
//
// Two modes:
//
//	Scheduling:  -at/-in + -to/-subject/-body schedules one job and runs
//	              it to completion (exit when the queue is empty).
//	Resume:      bare invocation loads the store and fires everything
//	              pending — this is what you run after a reboot (see the
//	              systemd unit for automatic resume).
func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		host      = flag.String("host", "sendm.cert.legalmail.it", "PEC provider SMTP hostname")
		port      = flag.String("port", "465", `SMTP port: "465" implicit TLS, "587" STARTTLS`)
		user      = flag.String("user", "massmailer@legalmail.it", "certified PEC mailbox address (the certified identity)")
		pass      = flag.String("pass", os.Getenv("PEC_PASSWORD"), "mailbox password (falls back to $PEC_PASSWORD)")
		storePath = flag.String("store", "gomailer-jobs.json", "job store (JSON file; survives reboot)")

		at      = flag.String("at", "", `absolute fire time: "2006-01-02 15:04:05", "15:04:05" (today, local), or RFC3339`)
		in      = flag.String("in", "", `relative fire time: "90s", "5m", "1h30m" (ignored when -at is set)`)
		minLead = flag.Duration("min-lead", 0, "minimum scheduling horizon; the future 24/7 service will set 24h")
		lead    = flag.Duration("lead", 5*time.Second, "warmup lead: session+auth open this long before the fire time; 0 = cold")

		to      = flag.String("to", "", "comma-separated recipient PEC addresses")
		subject = flag.String("subject", "", "message subject")
		body    = flag.String("body", "", "message body (required)")

		list     = flag.Bool("list", false, "print all stored jobs and exit")
		insecure = flag.Bool("insecure", false, "skip TLS certificate verification (local testing only)")
	)
	flag.Parse()

	store, err := jobber.OpenStore(*storePath)
	if err != nil {
		log.Fatalf("opening store: %v", err)
	}
	scheduler := jobber.NewScheduler(store)
	scheduler.MinLead = *minLead
	scheduler.WarmLead = *lead

	if *list {
		jobs, err := store.Load()
		if err != nil {
			log.Fatalf("loading store: %v", err)
		}
		printJobs(jobs)
		return
	}

	cfg := mailer.MailConfig{
		Hostname:           *host,
		Port:               *port,
		Username:           *user,
		Password:           *pass,
		InsecureSkipVerify: *insecure,
	}

	if *at == "" && *in == "" {
		// Resume mode: fire whatever is pending in the store (e.g. after
		// a reboot). Catch-up applies to jobs due while we were down.
		log.Printf("no -at/-in given: resume mode")
	} else {
		fireAt, err := parseFireTime(*at, *in)
		if err != nil {
			log.Fatalf("%v", err)
		}
		// Log the resolved absolute time WITH its zone: the classic
		// scheduling bug is a silent UTC parse on a local-clock mind.
		log.Printf("fire time resolved: %s", fireAt.Format("2006-01-02 15:04:05.000000 -0700 MST"))
		if *pass == "" {
			log.Printf("WARNING: no password given (set -pass or $PEC_PASSWORD): the send will fail at auth")
		}

		content := mailer.MailContent{
			From:         *user,
			To:           splitAddresses(*to),
			Subject:      *subject,
			Body:         *body,
			TipoRicevuta: mailer.RicevutaCompleta,
		}
		if _, err := scheduler.Schedule(cfg, &content, fireAt); err != nil {
			log.Fatalf("scheduling: %v", err)
		}
	}

	if err := scheduler.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// parseFireTime resolves the -at/-in flags to an absolute time.Time.
// The short clock-time form "15:04:05" means today at that local clock
// time, rolling to tomorrow when already past.
func parseFireTime(at, in string) (time.Time, error) {
	if at == "" && in == "" {
		return time.Time{}, fmt.Errorf("no fire time given: use -at or -in")
	}
	if in != "" {
		d, err := time.ParseDuration(in)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid -in %q: %w", in, err)
		}
		return time.Now().Add(d), nil
	}
	// Unambiguous RFC3339 with explicit offset.
	if t, err := time.Parse(time.RFC3339, at); err == nil {
		return t, nil
	}
	// Local-zone date-time forms.
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if t, err := time.ParseInLocation(layout, at, time.Local); err == nil {
			return t, nil
		}
	}
	// Bare clock time: today at that local clock time (or tomorrow when
	// already past). time.ParseInLocation zero-fills the missing DATE —
	// year 0! — so the calendar day must be built explicitly.
	if _, err := time.ParseInLocation("15:04:05", at, time.Local); err == nil {
		clock, _ := time.ParseInLocation("15:04:05", at, time.Local)
		now := time.Now()
		t := time.Date(now.Year(), now.Month(), now.Day(),
			clock.Hour(), clock.Minute(), clock.Second(), 0, time.Local)
		if t.Before(now) {
			t = t.Add(24 * time.Hour)
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf(`invalid -at %q: use "2006-01-02 15:04:05", "15:04:05", RFC3339, or -in`, at)
}

// splitAddresses splits a comma-separated recipient list.
func splitAddresses(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// printJobs dumps the store as a human-readable table (verification aid).
func printJobs(jobs []jobber.Job) {
	if len(jobs) == 0 {
		fmt.Println("no jobs in store")
		return
	}
	fmt.Printf("%-18s %-8s %-26s %-14s %-12s %s\n", "ID", "STATE", "FIRE AT", "LATE", "SEND", "MESSAGE-ID")
	for _, j := range jobs {
		late, send := "-", "-"
		if j.Result != nil {
			late = j.Result.Lateness.Round(time.Microsecond).String()
			send = j.Result.SendDuration.Round(time.Millisecond).String()
		}
		fmt.Printf("%-18s %-8s %-26s %-14s %-12s %s\n",
			j.ID, j.State, j.FireAt.Format("2006-01-02 15:04:05 -0700"), late, send, j.MessageID)
		if j.Result != nil && j.Result.Err != "" {
			fmt.Printf("  error: %s\n", j.Result.Err)
		}
	}
}
