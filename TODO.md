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
(`<store>.blobs/<sha256>`), with the job record carrying hash +
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

**Coupling with receipt type (2026-09-27; rewritten 2026-09-28 — the
retention policy shipped)**: with ricevuta `breve`, the consegna carries
the original headers and body with each attachment replaced by its
SHA-1 hash — and the Regole Tecniche put the conservation duty on the
SENDER: "è indispensabile che il mittente conservi gli originali
immodificati degli allegati, a cui gli hash fanno riferimento". That is
the sender's legal duty, not the tool's: gomailer is not a compliance
archive, and as of release-at-terminal it does not pretend to be one.
The tool's own commitments are (a) payload bytes live from scheduling
until the job settles — the terminal persist that records the outcome
releases the blob, keeping the row's sha256 as the permanent
fingerprint — and (b) nothing is ever deleted silently: the release is
an explicit, logged, policy-owned event, deletion outside it remains
the operator's own explicit act (the trim recipe, uninstall
`--purge-state`). An org that wants gomailer's store to double as its
conservation copy runs `-keep-payloads` and owns that decision.

**SPEC COUPLING — read before changing at-most-one-send**: the release
policy assumes the current discipline — a settled job never fires
again; a re-send is a NEW job with a fresh submission. If a later spec
lets FAILED jobs be re-fired, re-fire of the SAME row needs bytes that
have been released: revisit `Store.releasePayload` (the one policy
seam) before shipping that change — stop releasing failed rows, or
make re-fire require a fresh submission. Released rows are
self-describing (`ContentFile == ""` with `ContentSHA256 != ""` — a
shape no other write path produces) and `ContentOf` refuses them
loudly, so a future re-fire path fails visibly instead of sending
empty bytes.

**Shipped (2026-09-27, later still — the split store)**: the fat
workload turned the whole-file rewrite tax into the dominant GROWING
latency (a 70MB index: seconds per mutation, serialized under one
lock, on the warm window's critical path — measured as the
`SCHEDULED → STAGED` gap eating ~5s of a 30s window, with one job
staging 105ms before fire). The sidecar sketch above is now the real
layout: `jobs.json` is a slim index (recipients/subject/receipt type
inline — the audit log), the fat half (body + attachments) lives in
ONE write-once blob per job under `<store>.blobs/` (beside the index,
like the lock sidecars), referenced by
name and anchored by its sha256; `ContentOf` verifies the digest before
any byte can reach the wire, and `PayloadBytes` rides the row so
`stagedLead` sizes windows without the payload in RAM. Version-1 files
still load and migrate at boot (one blob write per row, ONE slim
rewrite); the save path carries a never-drop invariant (no write can
drop the payload of a job that has not yet fired — the CLI fallback
writing into a v1 file converts it row by row); orphan blobs are swept
at boot; and the
blob directory is named after the store file (`<store>.blobs/`), so
two stores in one state directory can never sweep each other's
payloads (a boot step adopts the blobs the first split builds kept
in a shared `blobs/` sibling). Index
rewrites are O(rows) forever, whatever the payloads. What remains of
this item is still only the SQLite endgame — now for queryability and
concurrent writers, not for performance.

**Shipped (2026-09-28 — release at settle)**: the store now keeps
payload bytes only for the time it needs them to send the email. Both
terminal persists (sent AND failed — a settled job never fires again
under the current spec) release the blob through the same
`Store.Update` choke point that strips credentials: rows first (the
reference cleared, the digest kept as the fingerprint), then the file
— crash-safe in both directions, with the boot sweep collecting any
stray. A boot step brings pre-policy terminal rows under the policy,
and pre-split settled rows are slimmed by migration instead of
extracted. The blob directory is bounded by the live queue, not by
history; `-keep-payloads` is the operator's escape hatch.

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

## 5. Ricevuta type: selectable per job + the fat-`completa` echo hazard (designed, not yet built)

**Why deferred**: the live workload turned out to be regularly fat (a
14-attachment send measured `send=9.46s`, ~18-25 MB payload), and the
receipt type — which is sender policy per DM 2 nov 2005 — is currently
hardwired: the CLI sets `TipoRicevuta: completa` (main.go), socket/library
clients can already set it (the field rides `Request.Content` and the
store row), and `build()` normalizes empty → `completa`.

**The hazard** (the reason this is not just a flag): ricevuta `completa`
echoes the FULL ORIGINAL MESSAGE back into the SENDER's mailbox PER
RECIPIENT; the provider's published rule is that receipts exceeding the
mailbox quota "non saranno recapitate" — the consegna, i.e. the legal
proof of delivery, is silently lost. For the fat workload that means
~20 MB × recipients re-arriving at the sender mailbox (and a fat receipt
relaying gestore→gestore back). With `breve` the receipt is headers +
digest: full delivery proof, no echo — the recommended org default for
fat sends (pin `-ricevuta breve` in the operator wrapper).

**The gap**: `ScheduleWithID` never validates the value — a socket
submission with `tipoRicevuta: "banana"` persists and fires, emitting
`X-TipoRicevuta: banana` on a legally binding message → undefined receipt
behavior, surfacing (if at all) as missing ricevute long after. Same
fail-early family as recipient/payload validation.

**Verified facts worth keeping** (live probes, 2026-09-27):
- the tipo does NOT affect the recipient deposit time: the consegna is
  generated AFTER the deposit it attests — tipo is downstream. It only
  delays (or loses, via quota) the evidence arriving in the SENDER's
  mailbox;
- `accettazione` is not tipo-selected (envelope + headers always);
- `X-Ricevuta` is set by the RECEIVER when generating receipts — honoring
  of the requested tipo is verifiable post-hoc on received ricevute
  (correlate by Message-ID), never enforceable pre-hoc; no protocol
  mechanism lets a recipient's gestore reject or require a tipo;
- `sintetica` is the least-exercised type in the wild — interop caution,
  live-provider-test discipline applies before relying on it.

**Fix sketch (designed; ~50-70 lines + tests; no protocol bump, no store
schema change — the field has traveled since protocol 1)**:
- CLI flag `-ricevuta completa|breve|sintetica`, default `completa`
  (domain cultural default, backward compatible; orgs pin their policy
  in the client wrapper);
- validation in `ScheduleWithID` (the one seam covering daemon socket +
  CLI fallback): reject values outside the three; resolve empty →
  `completa` AT SCHEDULING so the store row carries the decision, not
  the omission;
- fat-`completa` WARNING at scheduling (client stderr + daemon journal;
  the protocol has no warning channel — `ok:false` is rejection-only):
  state the echo math (payload × recipients) — warning, not rejection,
  because the real limit is the unknowable sender mailbox quota;
- payload bytes into the `SCHEDULED` log line — already computed by the
  `-max-payload` admission check; also makes `send=` decomposable into
  an uplink MB/s baseline. **Shipped with the split store**: `payload=`
  rides `SCHEDULED`, and the snapshot rides the row (`PayloadBytes`)
  so window sizing never needs the payload in RAM.

Also measured while at it: `send=9.46s` for that fat job is irreducible
in-tool — it is bandwidth × (payload × 1.37 base64) + provider-side
accept work; Legalmail advertises no `BINARYMIME` (Aruba does), a single
DATA stream cannot be parallelized, and pre-uploading DATA before
FireAt would be a prefire (forbidden by the no-early-deposit floor).
The only levers live outside gomailer: uplink, payload size, provider.

**Amendment (2026-09-27, later same day)**: the last sentence above is
wrong in one precise way — see item 6. There IS an in-tool lever:
holding the terminating dot. The prefire prohibition applies to
*delivery* (the dot), not to *bytes on the wire*; the two are separable.

## 6. Fat sends: pre-upload at warmup, hold the terminating dot, commit at FireAt (code shipped 2026-09-27; live provider probe pending)

**Shipped (2026-09-27)** — as designed below, with the settled parameters
(20s hold budget, "just go"):

- `mailer.StageOn`/`Staged.Commit` (mailer.go): envelope + full DATA upload
  in the warm window, the dot split from the 250 read (the raw textproto
  dot-writer, not `Client.Data`'s dataCloser), the bufio tail flushed before
  the hold (µs dot precision), `ErrDotOut` marking everything at-or-after
  the dot as terminal ambiguity (verify, never retry).
- Per-command read/write deadlines (§4.5.3.2, the stop-hang corner):
  a sliding per-chunk deadline on the transport (30s), the dot write (30s)
  and the answer read (60s) — all inside TimeoutStopSec=90s.
- Scheduler (scheduler.go): payload-aware warm windows (`stagedLead`:
  connect+auth + RCPT round-trips + payload/UplinkRate + split-row blob
  reassembly (defaultHydrateRate) + HoldBudget, floored at `-lead`); queue-wide dispatch scan (a fat job deep in the queue can
  carry the earliest window); staging in prewarm; the "just go" fire paths
  (finish-then-dot on overrun, cold full send at the fire time when the dot
  write fails, terminal verify when the dot left). Logged `STAGED upload=…`
  is the uplink baseline item 5 wants.
- `StageOn` CONSUMES the session on failure (closes it): a failed staging
  leaves a half-open envelope — and, from DATA on, an abandoned dot-writer
  that textproto auto-closes (writes the DOT) on the next command — so
  closing makes the "probe the leftover session" misuse impossible; the
  caller's fallback — close and retry with one cold full send — is the
  only move left (`TestStageFailureConsumesSession`).
- `fire` stamps `FiredAt`/`Lateness`/`SendDuration` on the delivery
  attempt that produced the outcome: on the "just go" fallback the dead
  held transaction's cost now lands in the recorded lateness (and the
  WARNING log) instead of vanishing (`TestFallbackStampCarriesTheLostHold`).

**Still pending before it runs against the certified provider**: the live
held-dot probe (the item-1 discipline — no wire-behavior change without
one): stage a probe job against Legalmail, hold ~20s, dot, and verify the
250 arrives and no accettanza precedes it. The uplink estimate is a
conservative static constant (`UplinkRate`, 2 MB/s) pending item 5's
logged-upload baseline.

**The question it answers**: the regular-fat workload pays `send=9.5s`
because the whole DATA transfer sits after the fire instant. SMTP has no
stage-then-commit command, but DATA has a mechanical equivalent: issue
DATA and stream the entire message during the warm window, hold back only
the terminating `CRLF.CRLF`, write those bytes at FireAt. The message is
not a message until the dot — no queue entry, no accettazione, no
ricevuta, nothing recipient-visible — so at-most-one-delivery holds (the
dot IS the delivery act) and the no-early-deposit floor holds with the
same causal proof (deposit ≥ dot ≥ FireAt). The µs fire discipline moves
onto the dot.

**What it buys**: fat sends commit at T+RTT (~0.3s 250) instead of
T+upload; and the crash-ambiguity window SHRINKS — a crash before the dot
is provably not-delivered (today a crash mid-fat-upload is genuinely
ambiguous for the whole upload length); only dot→250 stays ambiguous.

**Sharp edges (settle before building)**:
- payload-aware warm windows: `-lead` is a daemon-wide 5s and does not
  fit a 9.5s upload; open the window at `T − (uploadEstimate + slack)`
  per job — payload is known at scheduling time, uplink MB/s baselined
  from logged uploads (needs item 5's payload logging);
- no liveness probe exists inside a DATA transaction: a session silently
  dropped during the hold is discovered at the dot → cold redo → the fat
  job lands ~T+9.5s. Acceptable for the floor workload (late is allowed);
  not for tight "by X" margins. Aggressive TCP keepalives narrow
  detection, cannot prevent;
- the provider's held-dot timeout: the closest RFC 5321 mandate is
  server-side and a SHOULD — §3.8 lets a server close only "after a
  timeout, as specified in Section 4.5.3.2, occurs waiting for the
  client to send a command *or data*", and §4.5.3.2.7 requires the
  server to wait "at least 5 minutes". (The 10-min figure in
  §4.5.3.2.6 is the CLIENT's timeout awaiting the 250 AFTER the dot —
  an earlier version of this note misattributed it to the server.)
  Real-world defaults: Postfix smtpd_timeout 300s per read (including
  DATA), Sendmail Timeout.datablock 1h. A ~20s hold is 15× inside the
  normative floor — but Legalmail's actual config is still unknown:
  live test before trusting long holds (the item 1 precedent: no
  wire-behavior change against the certified provider without one);
- the mechanism is the RFC's own state machine: §4.1.1.4 — "Receipt of
  the end of mail data indication requires the server to process the
  stored mail transaction information"; §3.3 — the dot "confirms the
  mail transaction"; §4.2.5 — responsibility transfers at the 250
  after <CRLF>.<CRLF>. The server is not merely allowed to sit on
  uploaded bytes uncommitted; that is its defined state;
- client-side timeouts are a MUST for us too (§4.5.3.2: per-command
  deadlines): the commit read (awaiting the 250 after the dot) should
  carry the §4.5.3.2.6 SHOULD (10 min) — but pick ≤ TimeoutStopSec=90s
  so a wedged post-dot read never outlives the unit's stop budget;
  today the SMTP session has no read/write deadlines at all (the
  known stop-hang corner), and hold-the-dot must not ship without
  adding them;
- the invariant must be worded deliberately: bytes DO sit on the
  provider's wire before FireAt. Nothing observable exists before the
  dot, but "no bytes leave before X" (vs "recipient cannot receive
  before X") is the stricter reading — decide, then document it in the
  RUNBOOK whichever way;
- FIRE semantics: `fire=` must stamp the dot; log should split
  `upload=` (warmup) vs `commit=` (fire) for the paper trail.

Provider note: Aruba advertises `CHUNKING`/`BDAT` (the standardized form
of this trick — the last chunk completes the message); Legalmail does
NOT (EHLO-verified 2026-09-27) — raw DATA hold-the-dot is the only form
available against the default provider. Implementation seam in today's
code is small: `SendOn`'s `w.Write(msg)` + `w.Close()` split across the
hold — small code change, big semantic change.

**Settled (2026-09-27): the hold budget is ~20s, and the failure policy
is "just go".** The payload-aware window opens at
`T − (uploadEstimate + ~20s)`; an upload that finishes on estimate holds
the dot for ~20s, one that overruns shrinks the hold (down to zero and
into finish-then-dot). 20s sits 15x inside the 300s normative floor
(§4.5.3.2.7, a SHOULD), ~4x inside `TimeoutStopSec=90s`, and under every
middlebox idle default — but deployed configurations below the RFC
floor are real (cPanel-Exim ships `smtp_receive_timeout=165s`; Azure LB
4 min; AWS NLB 350s, fixed for TLS listeners), so the live-provider test
stays mandatory.

"Just go" is one rule: **the dot is floor-anchored — never before
FireAt, and never later than the earliest instant possible at or after
it.** Whatever state the runner finds itself in at FireAt:

- upload complete, session alive → write the dot (the win case);
- upload still streaming → keep streaming and dot at completion — never
  abandon a partial upload (the remaining fraction is always cheaper
  than a fresh full upload; lateness = the overrun, logged);
- session dead (the dot write fails, or keepalive flagged the drop) →
  cold full send STARTING AT FireAt (connect+auth+envelope+upload+dot on
  a fresh session) — never delay FireAt to re-warm;
- warmup never succeeded → today's cold path, unchanged.

"Just go" governs lateness tolerance only; it does NOT override the
no-early-deposit floor: a SIGTERM mid-hold does not pull the dot
earlier (the runner is committed; `sleepUntil(FireAt)` is deliberately
uninterruptible), and no opportunistic re-warm loop is built — a drop
detected before FireAt is still handled at FireAt. One code path, no
budget arithmetic; a cold send for a fat job lands ~T+9.5s, which the
floor workload accepts (late is allowed). The at-most-one invariant is
thereby worded: at most one COMPLETED message (one dot) per fire — a
first attempt that never dotted is not a delivery.

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