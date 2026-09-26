# RUNBOOK.md — gomailer operations

The operator's manual: what to type, what the output means, and what to
do when things go wrong. Design and rationale live in
[README.md](README.md); PEC compliance in [PEC.md](PEC.md).

All transcripts below were captured on a test machine (UTC clock, a local
SMTPS provider, scratch store paths) — your timestamps, paths, job IDs and
Message-IDs will differ; the shapes are exactly what you will see.

## 1. Cheat sheet

| Task | Command |
|---|---|
| Build | `go build -o gomailer .` |
| Run the daemon | `gomailer -daemon` (systemd in production) |
| Health check | `gomailer -ping` |
| Schedule a mail | `gomailer -at "15:00:00" -to dest@pec.it -subject S -body B` |
| Inspect the store | `gomailer -list` |
| Fire everything pending, no daemon | `gomailer` (bare) |
| Daemon logs | `journalctl -u gomailer -f` (or the foreground console) |
| Stop the daemon | `systemctl stop gomailer` (graceful) |

## 2. Install & first start

    go build -o gomailer .
    sudo install -m755 gomailer /usr/local/bin/gomailer
    sudo mkdir -p /var/lib/gomailer
    sudo install -m600 /dev/null /etc/gomailer.env    # add PEC_PASSWORD=...
    sudo systemctl enable --now gomailer              # unit: gomailer.service

Credentials are captured **per job, at scheduling time**, from the client's
environment (`-pass` or `$PEC_PASSWORD`); the daemon itself carries no
credentials — it only replays job configs. The unit's `EnvironmentFile` is
optional convenience.

## 3. The daemon: run, check, stop

Manual run (foreground, e.g. a one-off box without systemd):

    $ ./gomailer -daemon -store /var/lib/gomailer/jobs.json
    ... daemon mode: collecting schedules for the store; submissions on /var/lib/gomailer/gomailer.sock (stop with SIGTERM)
    ... queue: 0 pending, 0 sent, 0 failed (history kept in the store)
    ... daemon: accepting submissions on /var/lib/gomailer/gomailer.sock

Health check from anywhere — exit 0 means "accepting submissions":

    $ ./gomailer -ping -store /var/lib/gomailer/jobs.json
    ... daemon on /var/lib/gomailer/gomailer.sock: protocol 2, 3 job(s) pending

    $ ./gomailer -ping -store /var/lib/gomailer/jobs.json
    ... ping: daemon: reaching /var/lib/gomailer/gomailer.sock: dial unix ...: connect: no such file or directory
    $ echo $?
    1

Stop is graceful (SIGTERM, or `systemctl stop`): jobs already inside their
warmup window or mid-send run to completion; everything still waiting stays
**pending in the store** and re-arms at the next start:

    ... run stopped (context canceled): 3 job(s) left pending in the store; they re-arm at the next start
    ... daemon: stopped

Restart re-arms everything with no loss:

    ... queue: 3 pending, 0 sent, 0 failed (history kept in the store)
    ... daemon: accepting submissions on /var/lib/gomailer/gomailer.sock

Under systemd: `systemctl status gomailer`, `journalctl -u gomailer -f`.
Crashes are restarted automatically (`Restart=on-failure`); a clean stop
is not.

**A second daemon on the same store is refused by design** — it would
re-send every pending job (duplicate certified messages). Same for a
manual resume while the daemon owns the store:

    $ ./gomailer -daemon -store /var/lib/gomailer/jobs.json
    ... daemon: jobber: another scheduler is already firing jobs from store /var/lib/gomailer/jobs.json — refusing to start a second one (it would duplicate certified sends); stop it first or inspect with -list

When you see this, a firing process already owns the store: find it
(`systemctl status gomailer`, `pgrep -a gomailer`) and use it; `-list` and
`-ping` are always safe. The lock dies with its holder — a crashed daemon
never leaves a stale refusal behind.

## 4. Scheduling certified emails

With the daemon running, every scheduling client only *submits* — it never
sends. The job is persisted **before** the acknowledgment, with a
pre-generated Message-ID for correlating the recipient inbox and the
ricevuta:

    $ export PEC_PASSWORD=...                        # keep it out of shell history
    $ ./gomailer -in 45m -to studio.rossi@pec.it -subject "Contratto v2" \
          -body "In allegato il contratto firmato." -store /var/lib/gomailer/jobs.json
    ... fire time resolved: 2026-09-26 17:06:23.903222 +0000 UTC
    ... SCHEDULED job=c583704f3eeab5ba msgid=<d846c111-797c-4cf4-91b5-37a630169f80@legalmail.it> to=studio.rossi@pec.it fire=2026-09-26 17:06:23.903222 +0000 (in 44m59.994s) state=pending — collected by the daemon on /var/lib/gomailer/gomailer.sock

The `fire time resolved` line always shows the zone — a silent UTC parse on
a local-clock mind is the classic scheduling bug; read it before trusting
it.

Fire-time forms (`-at` wins over `-in`):

| Form | Meaning |
|---|---|
| `-in 90s` / `-in 5m` / `-in 1h30m` | relative to now |
| `-at "15:04:05"` | today at that local clock time; **rolls to tomorrow if already past** |
| `-at "2026-12-24 09:00:00"` | local date and time |
| `-at "2026-12-24T09:00:00+02:00"` | RFC3339 with explicit offset |

    $ ./gomailer -at "09:15:00" ...    # 16:21 on the test box → tomorrow
    ... SCHEDULED job=8d06a1e8f7f436ce ... fire=2026-09-27 09:15:00.000000 +0000 (in 16h53m36.081s) state=pending — collected by the daemon ...

Recipients: comma-separated on `-to`. Attachments, `X-Riferimento-Message-ID`
and receipt-type selection are mailer features reachable through the
socket protocol / library, not the CLI.

**Validation happens at scheduling time** (in the daemon), so a typo never
surfaces at fire time; errors travel back to the client and it exits 1:

    $ ./gomailer -in 10m -to dest@pec.it -subject x -body "" ...
    ... daemon rejected the job: jobber: empty body

    $ ./gomailer -in -5m -to dest@pec.it -subject x -body "troppo tardi" ...
    ... daemon rejected the job: jobber: fire time 2026-09-26 16:16:27 +0000 is already in the past

No password set is only a warning at scheduling — the failure surfaces at
fire time, so take it seriously:

    ... WARNING: no password given (set -pass or $PEC_PASSWORD): the send will fail at auth

Policy flags `-min-lead` and `-lead` are **daemon-side**: when submitting to
a running daemon, the daemon's values apply. The future 24/7 service sets
`-min-lead 24h` so a job with a deadline in the past cannot exist by
construction.

## 5. Manual operation (no daemon)

For a one-off box (no systemd, no resident process) the same binary does
everything, in two steps. **Step 1** — schedule; with no daemon reachable
the client persists anyway and warns:

    $ ./gomailer -in 3s -to studio.bianchi@pec.it -subject "Sollecito" \
          -body "Promemoria: scadenza domani." -store /var/lib/gomailer/jobs.json
    ... fire time resolved: 2026-09-26 16:21:36.887276 +0000 UTC
    ... daemon not reachable on /var/lib/gomailer/gomailer.sock: daemon: reaching ...: connect: no such file or directory
    ... SCHEDULED job=29a729c4633041d2 msgid=<0989bc37-d245-41dc-93a7-374831ea5a45@legalmail.it> to=studio.bianchi@pec.it fire=2026-09-26 16:21:36.887276 +0000 (in 3s)
    ... WARNING: job 29a729c4633041d2 is persisted but NOT armed (no daemon running): it will fire only when the daemon next starts (gomailer -daemon) or resume mode runs — check with -list before re-running this command (a re-run schedules a second job)

The submission is **idempotent within one invocation**: the client
pre-generates the job ID and reuses it for the persist fallback and one
short retry, so a daemon dying between persisting and acknowledging cannot
duplicate the job. A *re-run of the command* is a new invocation with a new
ID — after an ambiguous failure, check `-list` first.

**Step 2** — fire with the bare invocation (resume mode: fires everything
pending and exits when the queue is empty):

    $ ./gomailer -store /var/lib/gomailer/jobs.json
    ... no -at/-in given: resume mode
    ... queue: 1 pending, 0 sent, 0 failed (history kept in the store)
    ... WARNING job=29a729c4633041d2 msgid=<0989bc37-...@legalmail.it> FIRED 2.017s LATE (process/machine was not running at the fire time) fired=2026-09-26 16:21:38.904356 +0000
    ... SENT     job=29a729c4633041d2 msgid=<0989bc37-...@legalmail.it> send=50ms
    ... queue empty; done

The `FIRED … LATE` line is the catch-up path: the deadline passed while no
firing process existed, so the send started as soon as one did. If the
deadline is legally binding, that line is your audit trail.

Two manual-mode facts to plan around:

- Resume mode **holds the firing right** for as long as it runs; while it
  waits for future jobs it blocks the daemon (and vice versa). Choose one.
- It waits for *all* pending jobs: with a job due tomorrow in the store it
  stays up until tomorrow. That is a feature (one-shot "fire everything"
  mode), not a hang.

## 6. Inspecting the store

    $ ./gomailer -list -store /var/lib/gomailer/jobs.json
    ID                 STATE    FIRE AT                    LATE      SEND    MESSAGE-ID
    c583704f3eeab5ba   pending  2026-09-26 17:06:23 +0000  -         -       <d846c111-...@legalmail.it>
    8d06a1e8f7f436ce   pending  2026-09-27 09:15:00 +0000  -         -       <29f6103a-...@legalmail.it>
    a7e5160fb1faf640   pending  2026-12-24 09:00:00 +0000  -         -       <3222e1eb-...@legalmail.it>

After firing, the same table carries the timing paper trail (`LATE` is the
recorded lateness, `SEND` the SMTP session length):

    29a729c4633041d2   sent     2026-09-26 16:21:36 +0000  2.01708s  50ms    <0989bc37-...@legalmail.it>

`-list` is read-only and safe at any time, daemon running or not. The store
is a single human-readable JSON file (mode 0600 — it contains the mailbox
credentials of every job): copy it with `cp` whenever you like; the atomic
rename means a reader always sees the old or the new file, never a torn
one. Completed jobs are kept as history — the file is also the audit log.

Sidecar files beside it implement the process discipline: `jobs.json.lock`
(cross-process write serialization) and `jobs.json.runlock` (the firing
right). Back them up along with the store or accept that a restore needs a
daemon restart, which recreates them.

## 7. Reboots, crashes, and recovery

| Situation | What happens | What you do |
|---|---|---|
| Reboot **before** a fire time | systemd starts the daemon; jobs re-arm and still fire **on time** | nothing |
| Machine down **at** the fire time | job fires at daemon start, logged `FIRED <delay> LATE` | check the delay against your deadline |
| Daemon stopped, restarted later | pending jobs re-arm; if the deadline passed meanwhile → catch-up | prefer `systemctl stop` only when no imminent deadlines |
| Daemon **crashes during the send** | job stays `inflight` on disk; next boot marks it `failed` with a manual-verification note, **never auto-resent** | follow the procedure below |
| Daemon down when a client schedules | job persisted, **not armed**; fires when a daemon next starts (catch-up if late) | start the daemon |

**Crash-during-send procedure** (a duplicate certified message is worse
than a late one, so the tool never guesses):

    $ systemctl start gomailer
    journalctl -u gomailer | grep RECOVER
    ... RECOVER job=b971d39c63f404b1 msgid=<b83dc0dd-...@legalmail.it> was inflight at boot: interrupted during send, NOT auto-resent — verify the recipient inbox for <b83dc0dd-...@legalmail.it>

    $ gomailer -list -store /var/lib/gomailer/jobs.json
    b971d39c63f404b1   failed   2026-09-26 16:00:23 +0000  0s   0s   <b83dc0dd-...@legalmail.it>
      error: interrupted during send (previous process died mid-send): NOT auto-resent; verify the recipient inbox for the Message-ID

1. Search the recipient inbox (and the sender's ricevute) for the logged
   Message-ID.
2. Delivered? Nothing to do — the failure row is just history.
3. Not delivered? A human decides to re-send by scheduling a **new** job
   (fresh Message-ID). Do not edit the store by hand to flip states.

A fire-time send failure (bad credentials, provider outage) follows the
same rule — recorded `failed` with the error, never auto-retried:

    ... FAILED job=... msgid=<...> send=1.767s err=mailer: auth: 535 "invalid user ID or password"

Fix the cause, schedule again.

## 8. Troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| `another scheduler is already firing jobs from store …` | a firing process already owns the store (run lock) | find it: `systemctl status gomailer` / `pgrep -a gomailer`; `-list`/`-ping` are always safe |
| `daemon not reachable …` + `WARNING: … persisted but NOT armed` | daemon down at scheduling time | start it (`systemctl start gomailer`); the job fires at startup, catch-up if late |
| schedule command died ambiguously (daemon crash mid-ack) | the submission is idempotent within one invocation: at most one job exists | `gomailer -list` — one pending row for your message ⇒ nothing to redo; **do not blindly re-run the command** (a new invocation = new job) |
| `daemon rejected the job: jobber: empty body` / `no recipients` / `already in the past` | validation, by design at scheduling time | fix the command; nothing was persisted |
| `WARNING: no password given` | empty `-pass`/`$PEC_PASSWORD` at scheduling | the send WILL fail at auth — re-schedule with credentials |
| `FAILED … err=mailer: auth: …` / `tls dial` / `starttls` | provider rejected at fire time | fix credentials/network; schedule a new job; the failed one stays as history |
| Clients keep falling back although a daemon runs | socket file deleted or `-sock` mismatch | restart the daemon (it recreates the socket); check clients and daemon use the same `-store` (the default socket is `<store dir>/gomailer.sock`) |
| `systemctl stop` hangs past ~a minute | a send is wedged on an unresponsive provider | let systemd's kill timeout (default 90s) fire; that send becomes the crash-during-send case → §7 |
| Everything fires seconds late | machine clock not NTP-synced | `timedatectl`; lateness is measured against the local clock |
| `-ping` fine but you expected more pending jobs | wrong store path | `-ping`/`-list`/daemon must share one `-store` |

## 9. Timing verification

The daemon's log is the evidence: `WARM` (session opened `-lead` before),
`FIRE` (µs-stamped, with measured lateness), `SENT`. Two jobs scheduled for
the same instant fire **in parallel** — one warm, one cold, both
sub-millisecond late:

    ... WARM  job=6fd59f3e757a25e3 msgid=<7d1af80b-...> session ready in 49ms (2.945s before fire)
    ... FIRE  job=6fd59f3e757a25e3 msgid=<7d1af80b-...> fired=2026-09-26 16:02:13.000720 +0000 lateness=720.153µs
    ... FIRE  job=63023c51573b764e msgid=<df4006e2-...> fired=2026-09-26 16:02:13.000711 +0000 lateness=711.42µs (cold)
    ... SENT  job=6fd59f3e757a25e3 msgid=<7d1af80b-...> send=3ms
    ... SENT  job=63023c51573b764e msgid=<df4006e2-...> send=49ms

Human-side verification: compare the `FIRE` instant with the message
`Date:` header and the inbox/ricevuta arrival time. Keep the machine
NTP-synced (`timedatectl`) — sub-millisecond numbers are meaningless
against a drifting clock. Lateness ≥2s is flagged explicitly as catch-up.

## 10. Security checklist

- Store file: mode 0600, contains mailbox credentials per job — restrict
  the directory, never commit it, back it up like a password file.
- Control socket: 0600, owner-only, local machine only; requests carry
  credentials. Do not loosen the permissions.
- `/etc/gomailer.env`: mode 600.
- `-insecure` (skip TLS verification) is for local testing only — PEC
  mandates a verified TLS 1.2+ channel in production.

## 11. Log line glossary

| Line | Meaning |
|---|---|
| `SCHEDULED` | job persisted and armed (client ack mirrors it) |
| `queue: N pending, …` | boot snapshot after recovery |
| `daemon: accepting submissions on …` | socket is live — health checks turn green |
| `WARM … session ready in …` | warmup done off the critical path |
| `WARM … failed …: cold fallback` | warmup failed; job still meets its deadline cold |
| `FIRE … lateness=…` | send starting, µs-stamped; `(cold)` = no warm session |
| `WARNING … FIRED <delay> LATE` | catch-up: no firing process existed at the deadline |
| `SENT … send=…` | provider accepted the message |
| `FAILED …` | terminal failure, never auto-retried — read `err=` |
| `RECOVER …` | boot found a job interrupted mid-send: manual verification |
| `CRITICAL … send result could not be persisted` | send happened but the store write failed; next boot flags it |
| `run stopped (…): N job(s) left pending` | graceful stop; pending jobs re-arm at next start |

## 12. Multiple mailboxes / stores

The run lock, socket default and store are per **store file**. To serve two
mailboxes on one machine, give each its own store *directory* (the default
socket is `<store dir>/gomailer.sock`) or pin `-sock` explicitly:

    gomailer -daemon -store /var/lib/gomailer-a/jobs.json
    gomailer -daemon -store /var/lib/gomailer-b/jobs.json

Two daemons on the *same* store are always refused — that refusal is the
guarantee that no certified mail is ever sent twice.