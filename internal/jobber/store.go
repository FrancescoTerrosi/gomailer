package jobber

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
	"syscall"
	"time"

	"gomailer/internal/mailer"
)

// storeVersion is the on-disk schema version.
//
// Version 1 carried the WHOLE payload inline in every row — body and
// attachment bytes included — which made the store its own history of
// evidence (TODO.md item 3) but also made every mutation rewrite every
// byte ever stored: the fat workload (~20MB payloads, kept as evidence)
// grew the file past 70MB, and each Add/Update paid read + unmarshal +
// marshal + write + fsync of the whole thing — seconds, on the warm
// window's critical path, growing with every send ("more and more
// delayed").
//
// Version 2 splits the row: the slim half (recipients, subject, receipt
// type — everything a human auditing the file wants inline) stays in the
// index; the fat half (body + attachments) lives in ONE write-once blob
// file per job under <store>.blobs/ (beside the index, like the lock
// sidecars), referenced by name and anchored
// by its sha256. Index rewrites are O(rows); a blob is written exactly
// once (at scheduling, or at the one-time boot migration) and never
// rewritten. Version 1 files still LOAD (rows come back hydrated) and
// are migrated at boot or by the next write, whichever comes first.
// Rows settled by the retention policy carry ContentFile == "" with
// ContentSHA256 != "" — the released shape (see releasePayload): same
// on-disk format, no version bump.
const storeVersion = 2

// storeFile is the on-disk envelope. The field names are load-bearing:
// version-1 files (version + jobs with inline Content) parse unchanged —
// the original schema always tagged the envelope lowercase, and the
// split keeps those keys so operator tooling (jq one-liners, backups)
// written against any gomailer file keeps working.
type storeFile struct {
	Version int        `json:"version"`
	Jobs    []storeRow `json:"jobs"`
}

// storeRow is the on-disk projection of one Job. Content is a pointer so
// the same shape parses both eras: version-1 rows carry the FULL
// MailContent inline (body and attachments included), version-2 rows
// carry only the slim half with the fat fields emptied — never absent,
// so a row always shows its recipients and subject to a human auditing
// the file.
type storeRow struct {
	ID        string
	MessageID string
	FireAt    time.Time
	CreatedAt time.Time
	Config    mailer.MailConfig
	State     JobState
	Result    *JobResult
	Content   *mailer.MailContent

	// The split-store reference: which blob holds this job's fat half,
	// the digest anchoring it, and the raw payload size snapshotted at
	// scheduling (the number stagedLead sizes warm windows from, so the
	// hot loop never needs the payload in RAM).
	ContentFile   string
	ContentSHA256 string
	PayloadBytes  int64
}

// contentBlob is the fat half of a job's payload: the message body and
// the attachments — the only parts that made the version-1 index
// rewrites O(payload). It is written once and never rewritten.
type contentBlob struct {
	Body        string
	Attachments []mailer.Attachment
}

// Store persists jobs as a slim JSON index plus one write-once blob per
// job — for exactly as long as the job can still fire.
//
// RETENTION POLICY (releasePayload): a payload blob lives from
// scheduling until the job settles. The terminal persist that records
// sent/failed also releases the blob: the row is saved WITHOUT its
// reference (keeping the sha256 as the permanent fingerprint of what
// crossed the wire), then the file is deleted — reference cleared on
// disk before the bytes go, so a crash in between leaves an
// unreferenced file for the boot sweep, never a live row without
// bytes. The ROW is the permanent record (identifiers, Message-ID,
// timestamps, lateness, send duration, error text); conserving the
// payload ORIGINALS for legal purposes is the SENDER's duty, not the
// store's — a ricevuta breve binds its hashes to bytes the sender must
// keep (DM 2/11/2005: "è indispensabile che il mittente conservi gli
// originali immodificati degli allegati"). KeepPayloads is the escape
// hatch restoring the pre-policy keep-forever behavior.
//
// Every mutation rewrites the INDEX atomically: content is written to a
// temp file, fsynced, then renamed over the old file (and the directory
// entry is fsynced too), so a reboot mid-write can never expose a torn
// file: a reader sees either the old or the new state, never a half one.
// A pending job's blob is written (and fsynced) BEFORE the row that
// references it is saved, so the only residue a crash can leave between
// the two is an unreferenced blob — pure disk, swept at boot
// (SweepOrphanBlobs); a row referencing missing bytes cannot happen,
// and a blob that fails its recorded sha256 is terminal for the job
// (ContentOf): a certified sender never puts unverified bytes on the
// wire.
//
// SECURITY: pending jobs carry the mailbox credentials (replayed at fire
// time), so both the index and the blobs are created with 0600
// permissions — do not loosen this. Terminal rows are stripped of their
// credential (see Update): a job that will never fire again has no
// business remembering one. The future service should move credentials
// out of the job record entirely (vault/env, or at-rest encryption — see
// TODO.md).
type Store struct {
	path string
	mu   sync.Mutex

	// KeepPayloads is the escape hatch from the retention policy: when
	// set, payload blobs are retained after the job settles too (the
	// pre-policy behavior). See releasePayload — the policy seam.
	KeepPayloads bool
}

// releasePayload is THE retention-policy seam: whether a job in the
// given state has its payload blob released at that state's persist.
//
// POLICY (2026-09-28, product owner's call): both terminal states
// release. A settled job never fires again under the current spec —
// at-most-one-send is absolute, a re-send is a NEW job with a fresh
// submission — so the bytes are operationally dead the moment the
// outcome is recorded, and conserving the originals is the sender's
// duty, not the tool's.
//
// SPEC COUPLING — revisit this predicate BEFORE changing
// at-most-one-send: if a later spec lets FAILED jobs be re-fired
// (today an interrupted send is marked failed at boot and never
// re-sent), re-fire of the SAME row needs the bytes, and they are
// gone. Released rows are self-describing — ContentFile == "" with
// ContentSHA256 != "" (a shape no other write path produces) — and
// ContentOf refuses them loudly, so a future re-fire path fails
// visibly instead of sending empty bytes. Either stop releasing
// failed rows, or make re-fire require a fresh submission.
func (s *Store) releasePayload(state JobState) bool {
	if s.KeepPayloads {
		return false
	}
	return state == StateSent || state == StateFailed
}

// OpenStore opens (creating if absent) the job store at path.
func OpenStore(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("jobber: empty store path")
	}
	// Pre-create with restrictive perms so credentials are never exposed
	// through the umask.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("jobber: opening store %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("jobber: closing store %s: %w", path, err)
	}
	return &Store{path: path}, nil
}

// blobDir is where this store's payload blobs live: a directory NAMED
// AFTER THE STORE FILE (<store>.blobs), beside the index inside the
// state directory systemd already provides, exactly like the .lock and
// .runlock sidecars. Not a generic sibling such as dir/blobs: two store
// files in one state directory would share that, and each store's orphan
// sweep (which deletes what its own rows do not reference) would destroy
// the other's payload evidence — blob ownership must be unambiguous.
func (s *Store) blobDir() string {
	return s.path + ".blobs"
}

// legacyBlobDir is the blob layout of the first split-store builds: one
// shared "blobs" directory beside the index. Nothing writes there
// anymore; AdoptLegacyBlobs moves (only) the blobs this store's rows
// reference into blobDir at boot, and no code path deletes from it.
func (s *Store) legacyBlobDir() string {
	return filepath.Join(filepath.Dir(s.path), "blobs")
}

func (s *Store) blobPath(name string) string {
	return filepath.Join(s.blobDir(), name)
}

// safeBlobID is the charset a job ID may keep verbatim as its blob name.
var safeBlobID = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)

// blobName derives the on-disk name of a job's blob. Job IDs are
// caller-choosable (the idempotency key rides the socket protocol), so
// the name must be safe whatever the ID contains: ids in the safe
// charset keep their readable, greppable form; anything else — path
// separators, spaces, unicode — maps to a deterministic sha256 label.
// No path traversal, and no two ids can ever alias one blob.
func blobName(id string) string {
	if safeBlobID.MatchString(id) {
		return id + ".json"
	}
	sum := sha256.Sum256([]byte("gomailer blob: " + id))
	return hex.EncodeToString(sum[:]) + ".json"
}

// writeBlobLocked persists one job's fat payload as an immutable blob:
// encoded, fsynced, atomically renamed — written once (at scheduling or
// migration) and never rewritten afterwards. The digest of the exact
// bytes is returned alongside the name. Caller holds mu and the flock.
func (s *Store) writeBlobLocked(id string, b contentBlob) (name, sha string, err error) {
	buf, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("jobber: encoding content blob: %w", err)
	}
	buf = append(buf, '\n')
	sum := sha256.Sum256(buf)
	name, sha = blobName(id), hex.EncodeToString(sum[:])

	dir := s.blobDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", fmt.Errorf("jobber: creating blob directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(name)+".tmp-*")
	if err != nil {
		return "", "", fmt.Errorf("jobber: creating blob temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename

	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		return "", "", fmt.Errorf("jobber: writing blob temp file: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return "", "", fmt.Errorf("jobber: restricting blob: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return "", "", fmt.Errorf("jobber: syncing blob: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", "", fmt.Errorf("jobber: closing blob: %w", err)
	}
	if err := os.Rename(tmpName, s.blobPath(name)); err != nil {
		return "", "", fmt.Errorf("jobber: renaming blob: %w", err)
	}
	// Best-effort durability of the rename itself.
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return name, sha, nil
}

// extractLocked moves a job's fat payload (body + attachments) into its
// per-job blob and records the reference — the never-drop invariant of
// the split layout: any row about to be persisted without a blob
// reference gets one HERE, so no write path can ever slim a row into
// evidence loss. This is also the lazy migration: a version-1 file
// written to by an upgraded process (the CLI's persist-only fallback
// counts) converts row by row on its first write, exactly like the boot
// migration. The in-memory Content stays whole — RAM is not the cost;
// only the on-disk row slims down. Caller holds mu and the flock.
func (s *Store) extractLocked(j *Job) error {
	if j.ContentFile != "" {
		return nil // already split
	}
	if s.releasePayload(j.State) {
		// Settled row: the payload is released by policy, not split —
		// persisting it slim (no reference) is the release shape for
		// rows that never had one (pre-split terminal rows). The bytes
		// are the sender's to conserve, not the store's.
		return nil
	}
	name, sha, err := s.writeBlobLocked(j.ID, contentBlob{
		Body:        j.Content.Body,
		Attachments: j.Content.Attachments,
	})
	if err != nil {
		return err
	}
	j.ContentFile, j.ContentSHA256 = name, sha
	if j.PayloadBytes == 0 {
		j.PayloadBytes = payloadBytes(&j.Content)
	}
	return nil
}

// Load returns all jobs currently in the store. Rows come back SLIM in
// the split layout: To/Subject/receipt type are inline, the body and
// attachments are in the blob — use ContentOf to reassemble a full
// payload. Version-1 rows (not yet migrated) come back hydrated, their
// bytes still inline. Loading never hydrates blobs: reading every
// payload would rebuild the very cost the split exists to remove.
func (s *Store) Load() ([]Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

func (s *Store) loadLocked() ([]Job, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("jobber: reading store: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var sf storeFile
	if err := json.Unmarshal(raw, &sf); err != nil {
		return nil, fmt.Errorf("jobber: parsing store %s: %w", s.path, err)
	}
	if sf.Version != 1 && sf.Version != storeVersion {
		return nil, fmt.Errorf("jobber: store version %d not supported (want 1 or %d)", sf.Version, storeVersion)
	}
	jobs := make([]Job, len(sf.Jobs))
	for i := range sf.Jobs {
		r := &sf.Jobs[i]
		j := Job{
			ID:            r.ID,
			MessageID:     r.MessageID,
			FireAt:        r.FireAt,
			CreatedAt:     r.CreatedAt,
			Config:        r.Config,
			State:         r.State,
			Result:        r.Result,
			ContentFile:   r.ContentFile,
			ContentSHA256: r.ContentSHA256,
			PayloadBytes:  r.PayloadBytes,
		}
		if r.Content != nil {
			j.Content = *r.Content
			// Version-1 rows carry the fat payload inline; until the row
			// is extracted, size the window from the bytes at hand.
			if j.ContentFile == "" && j.PayloadBytes == 0 {
				j.PayloadBytes = payloadBytes(&j.Content)
			}
		}
		jobs[i] = j
	}
	return jobs, nil
}

// rowOf projects a job onto its slim on-disk row: every field except the
// fat half of the payload. The slim half is copied, not emptied in
// place, so the in-memory job keeps its full Content for whoever holds
// it (the firing runner included).
func rowOf(j Job) storeRow {
	small := j.Content
	small.Body = ""
	small.Attachments = nil
	return storeRow{
		ID:            j.ID,
		MessageID:     j.MessageID,
		FireAt:        j.FireAt,
		CreatedAt:     j.CreatedAt,
		Config:        j.Config,
		State:         j.State,
		Result:        j.Result,
		Content:       &small,
		ContentFile:   j.ContentFile,
		ContentSHA256: j.ContentSHA256,
		PayloadBytes:  j.PayloadBytes,
	}
}

// Add appends a new job. The whole read-modify-write runs under the
// in-process mutex AND a cross-process flock (see lock), so concurrent
// Adds — from this process or another — cannot lose jobs.
func (s *Store) Add(j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lk, err := s.lock()
	if err != nil {
		return err
	}
	defer unlockStore(lk)
	jobs, err := s.loadLocked()
	if err != nil {
		return err
	}
	return s.saveLocked(append(jobs, j))
}

// Update replaces the job with the same ID, or appends it when absent.
// Every terminal-state write in the system flows through this one choke
// point, which makes both terminal invariants mechanical rather than
// conventional — a future code path cannot forget either:
//
//   - the mailbox credential is stripped (a job that will never fire
//     again has no business remembering one), and
//   - the payload blob is RELEASED (releasePayload): the row is
//     persisted without its reference — the sha256 stays as the
//     permanent fingerprint — and only then is the file deleted.
//     Rows-before-bytes ordering makes the release crash-safe: a crash
//     between the row write and the file delete leaves an unreferenced
//     blob for the boot sweep, never a live row pointing at missing
//     bytes. A crash BEFORE the row write leaves the job inflight with
//     its payload intact, and the next boot's recovery settles it.
func (s *Store) Update(j Job) error {
	release := ""
	if j.ContentFile != "" && s.releasePayload(j.State) {
		release = j.ContentFile
		j.ContentFile = "" // the digest stays: the row's fingerprint
	}
	if j.State == StateSent || j.State == StateFailed {
		j.Config.Password = ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lk, err := s.lock()
	if err != nil {
		return err
	}
	defer unlockStore(lk)
	jobs, err := s.loadLocked()
	if err != nil {
		return err
	}
	for i := range jobs {
		if jobs[i].ID == j.ID {
			jobs[i] = j
			if err := s.saveLocked(jobs); err != nil {
				return err
			}
			s.removeReleasedBlob(release, j)
			return nil
		}
	}
	if err := s.saveLocked(append(jobs, j)); err != nil {
		return err
	}
	s.removeReleasedBlob(release, j)
	return nil
}

// removeReleasedBlob deletes the payload file of a row just persisted
// without its reference, and says so in the log — the release is an
// auditable event, never a silent one. Best-effort by design: a failed
// remove (or a crash before it) leaves an unreferenced file, which the
// boot sweep collects.
func (s *Store) removeReleasedBlob(name string, j Job) {
	if name == "" {
		return
	}
	if err := os.Remove(s.blobPath(name)); err != nil && !os.IsNotExist(err) {
		log.Printf("RELEASED job=%s msgid=%s: payload file %s could not be removed (%v) — the boot sweep will collect it",
			j.ID, j.MessageID, name, err)
		return
	}
	log.Printf("RELEASED job=%s msgid=%s payload=%s (state=%s — bytes released at settle; the row keeps the sha256 fingerprint; conserving the originals is the sender's duty, not the store's)",
		j.ID, j.MessageID, humanBytes(j.PayloadBytes), j.State)
}

// GetByID returns the job with the given ID, if present.
func (s *Store) GetByID(id string) (Job, bool, error) {
	jobs, err := s.Load()
	if err != nil {
		return Job{}, false, err
	}
	for _, j := range jobs {
		if j.ID == id {
			return j, true, nil
		}
	}
	return Job{}, false, nil
}

// AddNew atomically persists j unless a job with the same ID already exists
// — the idempotency seam behind safe submission retries. It returns
// (existing, false, nil) when the ID is taken (a concurrent twin or a
// replay) and (j, true, nil) when j was created. The existence check and
// the append are ONE read-modify-write under the in-process mutex and the
// cross-process mutation lock, so two processes racing the same ID still
// end up with exactly one copy. The returned created job carries its blob
// reference, so its holder can re-persist it without re-extracting.
func (s *Store) AddNew(j Job) (Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lk, err := s.lock()
	if err != nil {
		return Job{}, false, err
	}
	defer unlockStore(lk)
	jobs, err := s.loadLocked()
	if err != nil {
		return Job{}, false, err
	}
	for i := range jobs {
		if jobs[i].ID == j.ID {
			return jobs[i], false, nil
		}
	}
	if err := s.extractLocked(&j); err != nil {
		return Job{}, false, err
	}
	if err := s.saveLocked(append(jobs, j)); err != nil {
		return Job{}, false, err
	}
	return j, true, nil
}

// ContentOf reassembles a job's full payload: the slim half from the
// row, the fat half from its blob — VERIFIED against the recorded
// sha256. That digest is the integrity anchor of the evidence chain: a
// certified sender never puts unverified bytes on the wire, and a
// dispute-time verifier re-checks the stored original against the same
// number (see TODO.md item 3). A missing or corrupt blob is an error the
// caller must treat as terminal for the job — never a blind re-send.
func (s *Store) ContentOf(j Job) (mailer.MailContent, error) {
	// Deliberately LOCK-FREE: a blob is write-once (written and
	// fsynced before the row referencing it is saved, never rewritten),
	// so a reader can never race a writer of the same bytes, and the
	// sweep only removes blobs no row references. Holding the store
	// mutex here would serialize every concurrent runner's hydration
	// (a fat one is seconds of read + sha256 + unmarshal) against
	// socket submissions and other runners' state writes — jobs sharing
	// a deadline need their reassemblies to run in PARALLEL.
	if j.ContentFile == "" {
		if j.ContentSHA256 != "" {
			// The released shape: reference cleared at the terminal
			// persist, digest kept as the fingerprint. No other write
			// path produces it — and the refusal is loud, so a future
			// re-fire path fails visibly instead of sending empty bytes.
			return mailer.MailContent{}, fmt.Errorf("jobber: payload of job %s was released at its terminal state (sha256 %s kept on the row as the fingerprint) — conserving the originals is the sender's duty, not the store's", j.ID, j.ContentSHA256)
		}
		return j.Content, nil // inline: fresh in RAM, or a legacy row
	}
	raw, err := os.ReadFile(s.blobPath(j.ContentFile))
	if err != nil {
		return mailer.MailContent{}, fmt.Errorf("jobber: reading content blob %s: %w", j.ContentFile, err)
	}
	if j.ContentSHA256 != "" {
		if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != j.ContentSHA256 {
			return mailer.MailContent{}, fmt.Errorf("jobber: content blob %s failed its integrity check (sha256 mismatch: the stored payload was corrupted or edited) — never send unverified bytes", j.ContentFile)
		}
	}
	var b contentBlob
	if err := json.Unmarshal(raw, &b); err != nil {
		return mailer.MailContent{}, fmt.Errorf("jobber: parsing content blob %s: %w", j.ContentFile, err)
	}
	c := j.Content
	c.Body = b.Body
	c.Attachments = b.Attachments
	return c, nil
}

// Migrate brings a version-1 store (payload inline in every row) to the
// split layout: one blob write per legacy row, then ONE slim rewrite of
// the index — a bounded, one-time cost at boot, replacing the
// per-mutation rewrite tax the fat rows paid. It is idempotent (rows
// already carrying a blob reference are skipped) and returns the
// canonical job list so the caller builds its queue from the migrated
// view. Migrated rows keep their full content in RAM — hydration is
// free; only the file slims down.
func (s *Store) Migrate() ([]Job, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lk, err := s.lock()
	if err != nil {
		return nil, 0, err
	}
	defer unlockStore(lk)
	jobs, err := s.loadLocked()
	if err != nil {
		return nil, 0, err
	}
	converted := 0
	slimmed := 0
	for i := range jobs {
		if jobs[i].ContentFile == "" {
			if s.releasePayload(jobs[i].State) {
				// Settled row from the pre-split era, payload still
				// inline: extract declines by policy, so slim it here —
				// the retention policy for settled jobs, not a loss.
				// (Pre-split rows carry no fingerprint; the slim half
				// stays for the audit.) Idempotent: an already-slim row
				// has nothing to drop.
				if jobs[i].Content.Body != "" || len(jobs[i].Content.Attachments) > 0 {
					jobs[i].Content.Body = ""
					jobs[i].Content.Attachments = nil
					slimmed++
				}
				continue
			}
			if err := s.extractLocked(&jobs[i]); err != nil {
				return nil, 0, err
			}
			converted++
		}
	}
	if slimmed > 0 {
		log.Printf("STORE: dropped the inline payload of %d settled row(s) — retention policy: payload bytes live until the job settles", slimmed)
	}
	if converted > 0 || slimmed > 0 {
		if err := s.saveLocked(jobs); err != nil {
			return nil, 0, err
		}
	}
	return jobs, converted, nil
}

// AdoptLegacyBlobs moves the payload blobs of the first split-store
// builds — which kept one shared "blobs" directory beside the index
// (see legacyBlobDir) — into this store's own blob directory, so rows
// persisted then keep their evidence readable now that blob directories
// are per store. Only files this store's rows reference are moved: a
// shared directory can hold another store's blobs, and adoption never
// touches bytes it does not own (unreferenced leftovers stay for the
// operator; the sweep never looks there). A blob already present under
// its own name is left alone — the store-named copy wins. Idempotent:
// nothing to adopt returns 0. Called once at boot by the firing process.
func (s *Store) AdoptLegacyBlobs() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lk, err := s.lock()
	if err != nil {
		return 0, err
	}
	defer unlockStore(lk)
	jobs, err := s.loadLocked()
	if err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(s.legacyBlobDir())
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("jobber: reading legacy blob directory %s: %w", s.legacyBlobDir(), err)
	}
	if len(entries) == 0 {
		return 0, nil
	}
	referenced := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		if j.ContentFile != "" {
			referenced[j.ContentFile] = true
		}
	}
	if err := os.MkdirAll(s.blobDir(), 0o700); err != nil {
		return 0, fmt.Errorf("jobber: creating blob directory %s: %w", s.blobDir(), err)
	}
	adopted := 0
	for _, e := range entries {
		if e.IsDir() || !referenced[e.Name()] {
			continue // never ours: another store's blob, or crash residue
		}
		dst := s.blobPath(e.Name())
		if _, err := os.Stat(dst); err == nil {
			continue // already adopted (or re-extracted since): the store-named copy wins
		} else if !os.IsNotExist(err) {
			return adopted, fmt.Errorf("jobber: checking blob %s: %w", dst, err)
		}
		if err := os.Rename(filepath.Join(s.legacyBlobDir(), e.Name()), dst); err != nil {
			if os.IsNotExist(err) {
				continue // vanished mid-adoption: no longer ours to move
			}
			return adopted, fmt.Errorf("jobber: adopting legacy blob %s: %w", e.Name(), err)
		}
		adopted++
	}
	return adopted, nil
}

// ReleaseTerminalPayloads clears the blob references of TERMINAL rows
// that still carry them — rows settled by gomailers older than the
// release-at-terminal policy — and deletes the files. The digest stays
// on each row as its fingerprint. Idempotent: a no-op for stores
// written entirely by release-aware gomailers (their terminal rows
// never persist with a reference). Called once at boot by the firing
// process, after migration and adoption: it is what brings
// pre-existing history under the retention policy. Gated by
// KeepPayloads — the escape hatch keeps legacy blobs, too.
func (s *Store) ReleaseTerminalPayloads() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lk, err := s.lock()
	if err != nil {
		return 0, err
	}
	defer unlockStore(lk)
	jobs, err := s.loadLocked()
	if err != nil {
		return 0, err
	}
	var released []string
	for i := range jobs {
		if jobs[i].ContentFile != "" && s.releasePayload(jobs[i].State) {
			released = append(released, jobs[i].ContentFile)
			jobs[i].ContentFile = "" // the digest stays: the fingerprint
		}
	}
	if len(released) == 0 {
		return 0, nil
	}
	// Rows first, bytes second (see Update): a crash in between leaves
	// unreferenced files for the boot sweep.
	if err := s.saveLocked(jobs); err != nil {
		return 0, err
	}
	for _, name := range released {
		if err := os.Remove(s.blobPath(name)); err != nil && !os.IsNotExist(err) {
			log.Printf("RELEASED: legacy payload file %s could not be removed (%v) — the boot sweep will collect it", name, err)
		}
	}
	return len(released), nil
}

// SweepOrphanBlobs removes blob files no row references — the residue of
// a crash between a blob write and the row save that would have claimed
// it, or between a release's row save and its file delete (the write
// order makes an unreferenced blob the only possible crash leftover in
// both directions, and it is pure disk). Called once at boot by the
// firing process that owns the store. Only this store's OWN directory is
// read: the pre-namespacing shared directory (legacyBlobDir) is never
// swept — another store may own files in it (see AdoptLegacyBlobs).
func (s *Store) SweepOrphanBlobs() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lk, err := s.lock()
	if err != nil {
		return 0, err
	}
	defer unlockStore(lk)
	jobs, err := s.loadLocked()
	if err != nil {
		return 0, err
	}
	referenced := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		if j.ContentFile != "" {
			referenced[j.ContentFile] = true
		}
	}
	entries, err := os.ReadDir(s.blobDir())
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("jobber: reading blob directory %s: %w", s.blobDir(), err)
	}
	swept := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !referenced[e.Name()] {
			if err := os.Remove(s.blobPath(e.Name())); err != nil {
				return swept, fmt.Errorf("jobber: sweeping orphan blob %s: %w", e.Name(), err)
			}
			swept++
		}
	}
	return swept, nil
}

// BlankTerminalPasswords blanks the credential of every TERMINAL job
// still carrying one — rows persisted by an older gomailer, before the
// strip-on-terminal invariant landed in Update — and persists the store
// in ONE atomic rewrite. It returns how many rows were swept;
// zero means nothing was written. Pending and inflight rows are
// untouched: their credential is replayed at fire time.
func (s *Store) BlankTerminalPasswords() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lk, err := s.lock()
	if err != nil {
		return 0, err
	}
	defer unlockStore(lk)
	jobs, err := s.loadLocked()
	if err != nil {
		return 0, err
	}
	swept := 0
	for i := range jobs {
		if (jobs[i].State == StateSent || jobs[i].State == StateFailed) && jobs[i].Config.Password != "" {
			jobs[i].Config.Password = ""
			swept++
		}
	}
	if swept == 0 {
		return 0, nil
	}
	if err := s.saveLocked(jobs); err != nil {
		return 0, err
	}
	return swept, nil
}

// lock takes the cross-process mutation lock on <store>.lock for the
// span of one read-modify-write. The daemon is the writer in the common
// case, but scheduling clients also persist directly when the daemon is
// unreachable (persist-only fallback), and a lost update would mean a
// scheduled certified job silently vanishing — so writers serialize.
// Readers never lock: the atomic rename below already guarantees they
// observe either the old or the new file, never a torn one. Advisory
// flock — the deployment target is Linux/systemd.
func (s *Store) lock() (*os.File, error) {
	f, err := os.OpenFile(s.path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("jobber: opening lock file for %s: %w", s.path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("jobber: locking store %s: %w", s.path, err)
	}
	return f, nil
}

// unlockStore releases the mutation lock (nil-safe).
func unlockStore(f *os.File) {
	if f == nil {
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}

// saveLocked atomically persists the full job set as the slim index.
// Completed jobs are kept: the file doubles as history and verification
// log (their payload bytes are the digest-matching evidence — TODO.md
// item 3; the blobs ARE the archive). Caller must hold s.mu and the
// flock.
func (s *Store) saveLocked(jobs []Job) error {
	ordered := slices.Clone(jobs)
	slices.SortFunc(ordered, jobLess)
	// The never-drop invariant: a row that has not yet fired cannot be
	// persisted without a blob reference — extractLocked writes one for
	// every unsplit pending/inflight row here, so no write path can drop
	// the payload of a job that can still fire. Terminal rows are the
	// one deliberate exception: extract declines them (releasePayload),
	// and their payload is released by policy at the terminal persist
	// (Update) — never dropped implicitly, always released explicitly.
	for i := range ordered {
		if err := s.extractLocked(&ordered[i]); err != nil {
			return err
		}
	}
	rows := make([]storeRow, len(ordered))
	for i, j := range ordered {
		rows[i] = rowOf(j)
	}

	buf, err := json.MarshalIndent(storeFile{Version: storeVersion, Jobs: rows}, "", "  ")
	if err != nil {
		return fmt.Errorf("jobber: encoding store: %w", err)
	}
	buf = append(buf, '\n')

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("jobber: creating temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename

	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		return fmt.Errorf("jobber: writing temp file: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("jobber: chmod temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("jobber: syncing temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("jobber: closing temp file: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("jobber: renaming store: %w", err)
	}
	// Best-effort durability of the rename itself.
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
