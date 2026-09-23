# gomailer

Go sender and scheduler for **PEC** (Posta Elettronica Certificata). The
mailer package and its compliance requirements are documented in
[PEC.md](PEC.md); this file covers the scheduler and its CLI.

## Build

    go build -o gomailer .

## Schedule one email and check the timing

    export PEC_PASSWORD=...                     # mailbox password (keep it out of shell history)
    ./gomailer -at "15:00:00" -to dest@pec.it -subject test -body "timing check"
    ./gomailer -in 2m    -to dest@pec.it -subject test -body "timing check"

The job is **persisted before it is acknowledged**, with a pre-generated
`Message-ID`, so you can correlate it with the recipient inbox and the
ricevuta. The process fires it at the exact fire time and exits when the
queue is empty. Logs carry µs timestamps and report the measured drift:

    ... SCHEDULED job=7f3a... msgid=<...@legalmail.it> fire=2026-07-04 15:00:00.000000 +0200 (in 2m0s)
    ... WARM     job=7f3a... session ready in 284ms (2.7s before fire)
    ... FIRE     job=7f3a... fired=2026-07-04 15:00:00.000312 +0200 lateness=312µs
    ... SENT     job=7f3a... send=41ms

**Warmup** (`-lead`, default 5s): this long before the fire time the
message is built, the crash-safety marker is persisted, and an
authenticated provider session is opened (measured ~100–300ms against
the real provider) — so at the fire time only the envelope/data
round-trips remain. It is best-effort: if the provider is unreachable at
warmup, or drops the warm session while waiting (detected with a NOOP
liveness probe *before* any delivery command), the job degrades to a cold
send at the fire time and never loses its deadline. At most one delivery
is ever attempted per fire.
`-lead 0` disables warmup entirely.

Human-side verification: compare the logged fire instant with the message
`Date:` header and the inbox/ricevuta arrival time. Keep the machine
NTP-synced (`timedatectl`) for meaningful numbers.

Timer re-arming is chunked (≤1s arms, each recomputed against the absolute
deadline), so wake drift cannot accumulate over long scheduling leads: a
24h-lead job fires with the same sub-millisecond precision as a 2s one,
container or bare metal.

All validation happens at **scheduling** time (empty body, missing
recipients, `From` ≠ certified identity, deadline in the past are all
rejected immediately) — a typo must never surface minutes later at fire
time.

## Survive reboot

    ./gomailer                 # resume mode: fires everything pending in the store
    ./gomailer -list           # inspect the store: states, timing, errors
    ./gomailer -list -store /var/lib/gomailer/jobs.json

| Situation | Behavior |
|---|---|
| Reboot **before** fire time | resume mode re-arms the job → still fires **on time** |
| Machine down **at** fire time | job fires immediately at restart, logged `FIRED <delay> LATE` |
| Crash **during** the SMTP send | job marked `failed: interrupted during send`, **never auto-resent** (a duplicate PEC is worse) — verify the recipient inbox for the logged Message-ID |

Install [gomailer.service](gomailer.service) (systemd) for automatic
resume after reboot; it reads `PEC_PASSWORD` from `/etc/gomailer.env`.

## The store

A single human-readable JSON file (default `gomailer-jobs.json`), mode
0600 — it contains the mailbox credentials, so keep it private. Every
state transition rewrites it atomically (temp file → fsync → rename), and
completed jobs are kept as history:

    pending → inflight → sent | failed

The future 24/7 service will require jobs to be scheduled at least 24h in
advance: that is already enforced by `-min-lead 24h` (rejected at
scheduling time, so a job with a deadline already in the past cannot
exist by construction).