package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gomailer/internal/daemon"
	"gomailer/internal/jobber"
	"gomailer/internal/mailer"
)

// gomailer — PEC scheduled sender (daemon + client CLI).
//
// ONE daemon fires; clients only schedule:
//
//	-daemon            the single firing process: loads the store, re-arms
//	                   every pending job, collects new schedules over a
//	                   Unix socket and idles (this is what systemd runs).
//	                   A second firing process on the same store is
//	                   refused by the run lock — by construction.
//	-ping              health check: ask the daemon for its protocol
//	                   version and pending count; exit 1 if unreachable.
//	-at/-in + message  a scheduling CLIENT: submits the job to the daemon
//	                   and exits. It never sends anything itself — when
//	                   every scheduling process also ran its own firing
//	                   loop, each one loaded and re-sent every pending
//	                   job, duplicating certified mails. If the daemon
//	                   is unreachable the job is persisted anyway and
//	                   fires when the daemon next starts.
//	bare invocation    resume mode: fire everything pending in the store
//	                   and exit (after a reboot; refused while the daemon
//	                   owns the store — one firing process only).
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
		lead    = flag.Duration("lead", 5*time.Second, "warmup lead: session+auth open this long before the fire time; 0 = cold (daemon and resume modes)")

		to      = flag.String("to", "", "comma-separated recipient PEC addresses")
		subject = flag.String("subject", "", "message subject")
		body    = flag.String("body", "", "message body (required)")

		list       = flag.Bool("list", false, "print all stored jobs and exit")
		insecure   = flag.Bool("insecure", false, "skip TLS certificate verification (local testing only)")
		daemonMode = flag.Bool("daemon", false, "run the scheduling daemon: the ONE process that fires jobs, collects submissions over its socket and idles on an empty queue")
		sock       = flag.String("sock", "", `control socket: the daemon listens here, clients submit here (default: gomailer.sock next to -store)`)
		ping       = flag.Bool("ping", false, "health check: report the daemon's protocol version and pending job count; exits 1 if the daemon is unreachable")
	)
	flag.Parse()

	sockPath := *sock
	if sockPath == "" {
		sockPath = daemon.DefaultSock(*storePath)
	}

	if *ping {
		// Read-only probe: no store is opened or created.
		if err := runPing(sockPath); err != nil {
			log.Fatalf("ping: %v", err)
		}
		return
	}

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

	if *daemonMode {
		runDaemon(scheduler, sockPath)
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
		// a reboot), then exit. Catch-up applies to jobs due while we were
		// down. Refused while the daemon owns the store: there must be
		// exactly one firing process per store.
		log.Printf("no -at/-in given: resume mode")
		if err := scheduler.Run(); err != nil {
			log.Fatalf("run: %v", err)
		}
		return
	}

	// Client mode: schedule ONLY. The job is submitted to the daemon,
	// which validates, persists and arms it; this process exits without
	// ever touching the wire.
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
	submitJob(sockPath, scheduler, cfg, &content, fireAt)
}

// runDaemon runs the single firing process. The scheduler serves the
// store (boot recovery, re-arm, concurrent firing) and, once the queue is
// loaded, the control socket starts accepting submissions. SIGTERM/SIGINT
// stop it gracefully: handed-off runners (within -lead of their fire
// time, or mid-send) run to completion; everything else stays pending in
// the store and re-arms at the next start.
func runDaemon(scheduler *jobber.Scheduler, sockPath string) {
	log.Printf("daemon mode: collecting schedules for the store; submissions on %s (stop with SIGTERM)", sockPath)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &daemon.Server{Path: sockPath, Scheduler: scheduler}
	errCh := make(chan error, 1)
	started := make(chan struct{})
	sockDone := make(chan struct{})
	go func() {
		errCh <- scheduler.Serve(ctx, func() {
			// The socket accepts only after the queue is loaded: no
			// submission can race the boot and be dropped from RAM.
			ln, err := srv.Listen()
			if err != nil {
				log.Fatalf("daemon: %v", err) // nothing is in flight yet
			}
			close(started)
			go func() {
				// Serve drains in-flight exchanges when ctx is canceled:
				// the process must not die mid-reply, because an
				// unacknowledged submission forces the client onto its
				// idempotent fallback path — safe, but a completed
				// exchange is strictly better.
				srv.Serve(ctx, ln)
				close(sockDone)
			}()
			log.Printf("daemon: accepting submissions on %s", sockPath)
		})
	}()
	err := <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("daemon: %v", err) // systemd restarts (Restart=on-failure)
	}
	// Wait for the socket to drain before the process exits.
	select {
	case <-started:
		<-sockDone
	default: // Serve never reached its ready point: nothing to drain.
	}
	log.Printf("daemon: stopped")
}

// submitJob hands a schedule to THE daemon and prints the acknowledgment.
//
// The client never fires anything: that is what fixes the duplicate-send
// bug — when every scheduling process also ran its own firing loop, each
// one loaded and re-sent every pending job in the store.
//
// The submission is IDEMPOTENT: the job ID is generated here and reused by
// every attempt of this invocation. If the daemon dies between persisting
// the job and writing the acknowledgment (a crash, or SIGTERM mid-reply),
// the fallback below re-submits with the SAME ID and the store refuses a
// second copy — a lost acknowledgment can never become a duplicate
// certified message. When no daemon can be reached at all, the job is
// still persisted (a scheduled job must never exist only in RAM) and
// fires when the daemon next starts.
func submitJob(sock string, scheduler *jobber.Scheduler, cfg mailer.MailConfig, content *mailer.MailContent, fireAt time.Time) {
	id, err := jobber.NewJobID()
	if err != nil {
		log.Fatalf("generating job id: %v", err)
	}
	req := daemon.Request{
		Op:      daemon.OpSchedule,
		JobID:   id,
		FireAt:  fireAt,
		Config:  cfg,
		Content: *content,
	}

	resp, err := daemon.Submit(sock, req)
	if err == nil {
		printAck(resp, content, sock, "")
		return
	}
	log.Printf("daemon not reachable on %s: %v", sock, err)

	// Fallback — still with the same ID: this creates the job when the
	// daemon never received it, or converges on the copy it already
	// persisted before dying. Never a second copy either way.
	job, serr := scheduler.ScheduleWithID(id, cfg, content, fireAt)
	if serr != nil {
		log.Fatalf("scheduling: %v", serr)
	}

	// One idempotent retry: a daemon may have (re)started while we were
	// persisting — a live daemon ARMS the job on this replay, which also
	// covers the boot race where the daemon loaded the store just before
	// our persist. Short deadline: it just failed to answer us once.
	if resp, err := daemon.SubmitTimeout(sock, req, 3*time.Second); err == nil {
		printAck(resp, content, sock, " (after one idempotent retry)")
		return
	}
	log.Printf("WARNING: job %s is persisted but NOT armed (no daemon running): it will fire only when the daemon next starts (gomailer -daemon) or resume mode runs — check with -list before re-running this command (a re-run schedules a second job)", job.ID)
}

// printAck reports a daemon-collected job from its acknowledgment. A
// non-pending state means the ack answered an idempotent replay of a job
// that already fired — the store row, not the ack, is the paper trail.
func printAck(resp daemon.Response, content *mailer.MailContent, sock, note string) {
	if !resp.OK {
		log.Fatalf("daemon rejected the job: %s", resp.Error)
	}
	j := resp.Job
	state := ""
	if j.State != "" {
		state = fmt.Sprintf(" state=%s", j.State)
	}
	log.Printf("SCHEDULED job=%s msgid=%s to=%s fire=%s (in %s)%s%s — collected by the daemon on %s",
		j.ID, j.MessageID, strings.Join(content.To, ","),
		j.FireAt.Format("2006-01-02 15:04:05.000000 -0700"),
		time.Until(j.FireAt).Round(time.Millisecond), state, note, sock)
}

// runPing probes the daemon and reports what it finds. Used by -ping and
// by anything scripting around the daemon's health (a nonzero exit means
// "not accepting submissions — clients would degrade to persist-only").
func runPing(sock string) error {
	resp, err := daemon.Submit(sock, daemon.Request{Op: daemon.OpPing})
	if err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("daemon error: %s", resp.Error)
	}
	log.Printf("daemon on %s: protocol %d, %d job(s) pending", sock, resp.Version, resp.Pending)
	return nil
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
