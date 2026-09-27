# TODO — deferred items, with context

Consciously deferred work. Each item carries the full context needed to
pick it up later: what the corner is, why it is (currently) acceptable,
and the fix sketch. Design/rationale: README.md; PEC compliance: PEC.md;
operations: RUNBOOK.md.

## 1. Attachment filename: no line-length bound at scheduling time

**Why deferred**: every mainstream filesystem caps filenames at 255
bytes, and a 255-byte name leaves a 2–3× margin under the SMTP line
limit (measured: 299 chars ASCII / 467 chars UTF-8 vs the 998 limit).
Files picked from disk cannot trigger this; only a hand-built
`mailer.Attachment` (library / socket client) with an absurd filename
can.

**The corner**: `build()` (message.go) emits the
`Content-Disposition: attachment; filename=…` line unfolded, and it
*cannot* be folded in the general case:

- a filename has no WSP to break at (folding is impossible), and
- multiple RFC 2047 B-words are invalid in a *parameter* value (the
  separator between adjacent words would end up inside the decoded
  filename) — so non-ASCII names emit ONE un-chunked B-word
  (`mime.BEncoding.Encode`), unlike the subject path (`foldBWords`).

Measured consequences (Content-Disposition line length vs 998):

| filename | line |
|---|---|
| typical PDF name | 86 ✓ |
| 255 ASCII bytes (filesystem max) | 299 ✓ |
| 255 UTF-8 bytes | 467 ✓ |
| 1000 ASCII chars | 1044 ✗ |
| 600 × `à` (single B-word) | 2041 ✗ |

The failure mode is the bad part: the message builds fine, the job
passes scheduling validation, and a strict MTA may reject the DATA at
FIRE time (job `failed`, never auto-retried) — precisely the "silent
late failure" PEC.md's line-length discipline exists to prevent.

**Fix sketch (minimal, philosophy-consistent)**: validate in
`Scheduler.ScheduleWithID` using the SAME rendering the builder uses —
measure the emitted `Content-Disposition` line and reject at scheduling
time if it would exceed the 998-octet budget. Reusing the builder's own
renderer (not a parallel length check) keeps validator and builder in
lockstep by construction. The daemon gets it for free — socket
submissions already flow through `ScheduleWithID`.

**Alternative (fully conformant, bigger)**: RFC 2231 parameter
continuations (`filename*0*=UTF-8''…; filename*1*=…`) — the standard
mechanism for long and non-ASCII parameter values, which also fixes the
de-jure wart that RFC 2047 words are technically not a valid parameter
position (PEC.md documents 2047 for filenames; the de-facto acceptance
is broad but not guaranteed). It changes the header shape for EVERY
non-ASCII filename and PEC-provider interop with 2231 is untested — do
not switch without a live provider test.

## 2. Attachment bytes are embedded in the store JSON

**Why deferred**: fine for contract-PDF sizes; a deliberate cost
accepted when `-attach` was added.

Attachment bytes live base64-encoded inside each job's `Content` in the
store JSON. Consequences to remember:

- the store grows by ~1.37× the attachment size **per job, forever**
  (completed jobs are kept as history — by design, the store is also
  the audit log);
- every state transition (`pending → inflight → sent|failed`) rewrites
  the WHOLE file atomically (temp → fsync → rename): a 10 MB attachment
  ⇒ ~13.7 MB base64 ⇒ three full-file rewrites per job lifecycle, and
  every boot/replay re-reads it all;
- the socket request carries the full base64 too (local-only 0600
  socket, 30 s read deadline — fine locally, worth remembering if the
  protocol ever goes remote);
- `build()` at fire time holds the full multipart message in RAM (the
  prepared bytes are also persisted during warmup) — a 10 MB attachment
  costs ~2× that in allocations per job.

**Fix sketch when needed**: first a per-job quota, rejected at
scheduling time above N MB (same fail-early seam as item 1) — cheap and
sufficient for a long time. If blobs ever grow beyond that, move them
out of the store into content-addressed sidecar files
(`<store dir>/blobs/<sha256>`), with the job record carrying hash +
length; the `storeVersion` comment in store.go already anticipates the
SQL-store future where this lands natively.

**Decision (2026-09-26, amended — shipped)**: the per-job quota is needed
regardless of workload data — for two independent reasons:

1. **RAM**: the architecture materializes the payload in memory at
   every stage (client reads the files once — scheduling-time snapshot
   semantics, the file may change afterwards — then JSON → socket →
   store → boot parse → multipart build). The firing process holds the
   decoded job plus the built wire message (base64 ≈ 1.37×): ≥2.4×
   payload minimum, ~16× measured end-to-end with the JSON store's
   rewrite buffers. A streaming redesign would reduce this but cannot
   eliminate a bound: the captured bytes must live somewhere between
   schedule and fire by design.
2. **The provider publishes the cap**: Legalmail (the default `-host`)
   guarantees messages with attachments totaling up to **70 MB** and a
   maximum message size of **100 MB** (Manuale Operativo §5.1); above
   that, delivery is simply not guaranteed. This makes the quota a
   correctness rule — the same fail-at-scheduling-time discipline as
   recipient validation. No workload data is needed to set the
   default: mirror the provider's bound (70 MB payload),
   `-max-payload 0` disables, and real-workload data only ever matters
   to TIGHTEN below the provider cap on small boxes.

Provider facts worth remembering (Legalmail Manuale Operativo / FAQ):

- Max guaranteed attachments per message: **70 MB total** ("peso
  complessivo"); max message **100 MB**; the base64 transport
  expansion (~1.37×) is called out explicitly as the reason.
- Max **1.000 recipients** per send (To + Cc) — gomailer currently has
  no recipient-count validation; same fail-early family as the quota.
- **Ricevuta completa** (gomailer's default receipt type) echoes the
  whole original message back into the SENDER's mailbox per
  recipient; receipts exceeding the mailbox size "non saranno
  recapitate" — large payloads × many recipients can silently lose
  the legal delivery proof.

The structural endgame under consideration is SQLite (single-row
updates, blobs out of the metadata path), which subsumes the
slimming-cleanup design; the quota remains storage-agnostic admission
control in `ScheduleWithID` either way.

**Shipped (2026-09-26)**: `-max-payload` (default 70 MB decimal,
provider-mirrored; `0` disables) and the ≤1000 recipient cap now live
in `ScheduleWithID`, with `-max-payload` as a daemon-side policy flag.
What remains in this item is only the SQLite endgame.

## 3. Credentials: strip at terminal (shipped) + encrypt at rest (designed, not yet built)

Two layers, one threat model.

**Layer 1 — strip on terminal (shipped)**: a `sent`/`failed` job's
`Config.Password` is provably dead weight (never auto-retried; a
re-send is a NEW job with fresh credentials; the replay ack already
excludes credentials). It is stripped inside `Store.Update` — the
single choke point all terminal writes flow through (`fire()`,
`failNow()`, boot recovery) — so the invariant is mechanical, not
conventional. Pending and inflight rows KEEP their credential: the
daemon replays it at fire time. The boot sweep
(`Store.BlankTerminalPasswords`) blanks credentials in terminal rows
persisted by older versions, in ONE guarded rewrite (logged
`SECURITY swept …`).

**Layer 2 — encrypt at rest (requested; design sketched)**: pending
jobs can sit for days (the future service schedules 24h+ ahead), and
the store is explicitly a copy-it-around file ("back it up like a
password file") — backups are the realistic leak vector, which is
exactly what at-rest encryption buys.

- AES-256-GCM, stored as `enc:v1:<base64(nonce||ciphertext+tag)>` —
  stdlib-only, authenticated (tamper = loud error, never a wrong
  password), self-describing, versioned, and distinguishable from
  plaintext so migration is a prefix check.
- Encrypt at the funnel: `ScheduleWithID` before `AddNew` — one seam
  covers the daemon-socket path and the CLI fallback path alike.
- Decrypt in the runner, where the config is materialized for
  `Warm`/`Send` — plaintext exists only in the firing process's
  memory, never written back.
- The key lives OUTSIDE the store: 32 random bytes in a 0600 key file
  (e.g. `/etc/gomailer.key`), loaded via `-keyfile` / env. A key next
  to the data encrypts nothing against file-only leaks — the exact
  threat being bought.
- Migration: a boot sweep encrypts plaintext rows in place when a key
  is configured; no key configured ⇒ today's behavior + a loud warning
  (gradual rollout).
- Failure mode: key lost/mismatched ⇒ decrypt error at warmup ⇒ job
  `failed: credential unreadable`, never auto-retried. The key becomes
  part of the availability story of pending jobs — back it up like the
  store (RUNBOOK security-checklist row).
- Honest threat model: protects store-file-only leaks (backups, wrong
  permissions, accidental commits, future off-box database backups);
  does NOT protect against full machine compromise or a compromised
  daemon (plaintext at fire time is a functional requirement).
- Rotation: out of scope for v1; the `enc:v1:` prefix leaves room
  (rotation = new key + re-encrypt sweep).

Composes with the SQLite direction unchanged: the ciphertext is just a
string in the job row. Composes with Layer 1: terminal jobs carry no
password at all, pending jobs carry ciphertext only.

## 4. Warmup of large bursts: concurrency limit + admission check (deferred)

**What is shipped**: eager dispatch. The tending loop now hands every job
to its runner as soon as its warm window opens, and the runner warms
whenever at least `min(minWarmBudget, WarmLead/2)` remains before FireAt.
This fixed the old per-head rule that made every same-deadline job except
the first — and every follower shadowed by a cold job — fire cold at
FireAt. See the loop in `scheduler.go` (`windowStart`/`windowOpen`) and
`TestSameDeadlineAllWarm` / `TestLateScheduledStaysCold` /
`TestColdJobDoesNotShadowFollowers`.

**What remains**: eager dispatch makes a burst open all its sessions at
`FireAt - WarmLead` at once. For a large same-instant batch that is a
thundering herd — FDs, CPU, and the provider's concurrent-connection
limit — and it also fixes the *timing* of the herd, not its size: N
sessions must still be live at FireAt to fire N jobs simultaneously.

**Fix sketch**:

- a warmup semaphore around `Courier.Warm` (daemon policy, e.g.
  `-max-warm N`), so only N sessions open at a time;
- a scheduling-time capacity check: if a burst needs
  `ceil(n/limit) * warmCost > WarmLead`, reject or warn (same fail-early
  seam as `-max-payload`, and it needs a configured/measured warmCost);
- spread warmups across the window (warm just-in-time, not all at
  `T-WarmLead`) to bound idle time on held sessions — the `NOOP` probe
  already makes a dropped session correct, but a drop costs a reconnect
  on the critical path;
- for genuinely large same-instant sends, prefer one job with many `To:`
  recipients (the provider's 1000 cap) — one session, no herd.

No action planned until a real burst workload needs it; the correctness
half (every imminent job takes the warm path) is done.

## Other known corners (from the audit — cosmetic, no action planned)

- `priorityqueue.go` is 45 lines of commented-out dead code (the
  sorted-slice queue replaced it).
- `Store.Add` is used only by tests; `AddNew`/`Update` are the real
  write paths.
- `-list` creates an empty store file when the store is absent (it
  never writes an *existing* store — "read-only" only in the strictest
  sense).
- Warm sends stamp the `Date:` header at warmup (≤ `-lead` before
  fire), so comparing `Date:` against the FIRE log line is approximate
  for warm jobs.
- The control socket is chmod'ed 0600 right after bind: a brief window
  exists between bind and chmod (umask-dependent perms, local-only).
- No read/write deadlines on the SMTP session: a wedged send blocks a
  graceful stop until systemd's kill timeout (documented in RUNBOOK §8).
- A client persist landing in the daemon's boot window (between store
  load and socket ready) with a failed retry stays dormant until the
  next daemon start; the client's WARNING says so, and the one
  idempotent retry usually heals it.