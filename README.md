# gomailer

Go sender and scheduler for **PEC** (Posta Elettronica Certificata). The
mailer package and its compliance requirements are documented in
[PEC.md](PEC.md); this file covers the daemon, the CLI and the timing.
Hands-on operations — commands, expected output, failure scenarios and
recovery procedures — live in [RUNBOOK.md](RUNBOOK.md).

## Architecture — one daemon fires, clients only schedule

Scheduling a mail must never spawn a *second sending process*: when every
`-at` invocation also ran its own firing loop, each one loaded the whole
store and re-sent **every pending job** — duplicate certified messages,
each legally binding. The process boundary now matches the responsibility:

| Role | Invocation | What it does |
|---|---|---|
| **Daemon** | `gomailer -daemon` | THE one firing process: loads the store, re-arms every pending job, collects submissions over a Unix socket, fires jobs **concurrently**, idles forever |
| **Client** | `gomailer -at/-in ...` | submits the job to the daemon and exits; **never sends anything**. Daemon unreachable ⇒ the job is still persisted and fires when the daemon next starts |
| **Resume** | `gomailer` (bare) | fire everything pending and exit (after a reboot); refused while the daemon owns the store |

Two locks make the "exactly one firing process" property mechanical
instead of conventional:

- **Run lock** (`<store>.runlock`, exclusive flock): a second firing
  process — second daemon, or a resume racing the daemon — fails fast
  with `another scheduler is already firing jobs from this store`. The
  kernel releases the flock when the holder dies, so a crashed daemon
  leaves no stale lock.
- **Mutation lock** (`<store>.lock`, short-lived flock around each
  read-modify-write): the daemon and a persist-only client can never lose
  each other's writes.

## Build

    go build -o gomailer .

## Run the daemon

    export PEC_PASSWORD=...
    ./gomailer -daemon                          # foreground; systemd in production
    ./gomailer -daemon -store /var/lib/gomailer/jobs.json

Install [gomailer.service](gomailer.service) (systemd): it runs the
daemon, reads `PEC_PASSWORD` from `/etc/gomailer.env`, restarts it after
crashes and starts it at boot. Stopping is graceful: jobs already inside
their warmup window or mid-send run to completion (bounded by `-lead`);
everything still waiting stays pending in the store and re-arms at the
next start.

Health check from anywhere:

    ./gomailer -ping
    ... daemon on /var/lib/gomailer/gomailer.sock: protocol 2, 3 job(s) pending

## Schedule a mail

    ./gomailer -at "15:00:00" -to dest@pec.it -subject test -body "hello"
    ./gomailer -in 2m    -to dest@pec.it -subject test -body "hello"

The client resolves the fire time, submits to the daemon and prints the
acknowledgment — the job is **persisted by the daemon before it is
acknowledged**, with a pre-generated `Message-ID`, so you can correlate it
with the recipient inbox and the ricevuta. The daemon's log carries µs
timestamps and the full paper trail:

    ... SCHEDULED job=7f3a... msgid=<...@legalmail.it> fire=2026-07-04 15:00:00.000000 +0700 (in 2m0s)
    ... WARM     job=7f3a... session ready in 284ms (2.7s before fire)
    ... FIRE     job=7f3a... fired=2026-07-04 15:00:00.000312 +0700 lateness=312µs
    ... SENT     job=7f3a... send=41ms

If no daemon answers on the socket, the client persists the job anyway and
warns: it will fire when the daemon next starts. The submission is
**idempotent** end to end: the client pre-generates the job ID and reuses it
for every attempt of one invocation, so a daemon that dies between
persisting the job and writing the acknowledgment (crash, or SIGTERM
mid-reply) cannot turn the lost ack into a duplicate certified send — the
store refuses a second copy with the same ID, and the client's one
idempotent retry after persisting lets a daemon that came back arm the job
without ever re-creating it. `-list` works at any time (read-only):

    ./gomailer -list
    ./gomailer -list -store /var/lib/gomailer/jobs.json

## Timing — the strict constraint

**Warmup** (`-lead`, default 5s): this long before the fire time the
message is built, the crash-safety marker is persisted, and an
authenticated provider session is opened (measured ~100–300ms against the
real provider) — so at the fire time only the envelope/data round-trips
remain. It is best-effort: if the provider is unreachable at warmup, or
drops the warm session while waiting (detected with a NOOP liveness probe
*before* any delivery command), the job degrades to a cold send at the
fire time and never loses its deadline. At most one delivery is ever
attempted per fire. `-lead 0` disables warmup entirely.

**Concurrent firing**: the tending loop hands each job to its own runner
when its warmup window opens (or when due); every runner owns its job
end-to-end — build, session, exact-instant wait, delivery. Jobs that
share a deadline fire **in parallel**, each over its own session, and a
slow SMTP send can never delay another job's fire. Only jobs within
`-lead` of their fire time are handed off, so concurrency costs one
session per imminent fire. A handed-off job is never re-queued: single
ownership keeps the at-most-one-delivery discipline under concurrency.

Timer re-arming is chunked (≤1s arms, each recomputed against the absolute
deadline — the loop and every runner alike), so wake drift cannot
accumulate over long scheduling leads: a 24h-lead job fires with the same
sub-millisecond precision as a 2s one, container or bare metal.

All validation happens at **scheduling** time (empty body, missing
recipients, `From` ≠ certified identity, deadline in the past are all
rejected immediately — by the daemon, and the error travels back to the
client) — a typo must never surface minutes later at fire time.

Human-side verification: compare the logged fire instant with the message
`Date:` header and the inbox/ricevuta arrival time. Keep the machine
NTP-synced (`timedatectl`) for meaningful numbers.

## Survive reboot

| Situation | Behavior |
|---|---|
| Reboot **before** fire time | the daemon re-arms the job at boot → still fires **on time** |
| Machine down **at** fire time | job fires immediately at restart, logged `FIRED <delay> LATE` |
| Daemon stopped, then restarted | same as reboot: pending jobs re-arm; jobs that were warming or mid-send complete before the daemon exits |
| Crash **during** the SMTP send | job marked `failed: interrupted during send`, **never auto-resent** (a duplicate PEC is worse) — verify the recipient inbox for the logged Message-ID |
| Daemon down when a client schedules | job persisted anyway; fires when the daemon next starts (the client warns loudly) |

## The store

A single human-readable JSON file (default `gomailer-jobs.json`), mode
0600 — it contains the mailbox credentials, so keep it private. Every
state transition rewrites it atomically (temp file → fsync → rename), and
completed jobs are kept as history:

    pending → inflight → sent | failed

Two sidecar files implement the process discipline: `<store>.runlock`
(the firing right, held for the daemon's lifetime) and `<store>.lock`
(serializes mutations across processes). The daemon is the only writer
while it runs; scheduling clients write directly only in the
daemon-unreachable fallback — safely, under the mutation lock.

The future 24/7 service will require jobs to be scheduled at least 24h in
advance: that is already enforced by `-min-lead 24h` (rejected at
scheduling time, so a job with a deadline already in the past cannot exist
by construction). `-min-lead` and `-lead` are daemon-side policies; they
apply wherever the firing happens.

## Socket protocol (local automation)

Clients and the daemon speak one JSON request/response exchange per
connection over the Unix socket (default `gomailer.sock` next to the
store; `-sock` to override; owner-only, 0600 — requests carry mailbox
credentials):

    {"op":"schedule","job_id":"<client-generated id>","fire_at":"2026-07-04T15:00:00+02:00","config":{...},"content":{...}}
    {"op":"ping"}

`schedule` answers `{"ok":true,"job":{"id":...,"message_id":...,"fire_at":...,"state":"pending"}}`
or `{"ok":false,"error":"..."}` (the daemon's own validation message);
`ping` answers with the protocol version and the pending count
(`gomailer -ping` is its CLI form). The protocol is local-only by design;
the store remains the integration point for everything else.

**Idempotent submit (protocol 2).** `job_id` is the client-generated
idempotency key: a `schedule` whose `job_id` is already persisted converges
on that job (same ack, no duplicate) — and re-arms it if it is pending and
no live firing loop holds it. Terminal jobs are returned untouched (`state`
reports which): a replay can never re-send a certified message. Clients
and daemons should be upgraded together; a mixed pair still works, but only
the matching pair gets the no-duplicate guarantee on a lost ack.