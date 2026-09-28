package jobber

// Tests for the split store (store.go): the slim index + one write-once
// blob per job, the version-1 boot migration, the sha256 integrity
// anchor (a certified sender never puts unverified bytes on the wire),
// the never-drop invariant (no write path can slim a row into evidence
// loss), the orphan sweep, blob-name safety for caller-choosable IDs,
// the per-store blob directory (named after the store file, so stores
// sharing a state directory cannot destroy each other's evidence),
// and adoption of the first builds' shared blob directory.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gomailer/internal/mailer"
)

// splitTestJob builds a minimal pending job for the given id and body.
func splitTestJob(id, body string) Job {
	return Job{
		ID:        id,
		MessageID: "<" + id + "@pec.test>",
		FireAt:    time.Now().Add(time.Hour),
		CreatedAt: time.Now(),
		Content:   testContent(body),
		Config:    testConfig(),
		State:     StatePending,
	}
}

// fatContent builds a content with a recognizable fat payload.
func fatContent(body string, att []byte) mailer.MailContent {
	c := testContent(body)
	if att != nil {
		c.Attachments = []mailer.Attachment{{
			Filename:    "contratto.pdf",
			ContentType: "application/pdf",
			Data:        att,
		}}
	}
	return c
}

// TestStoreSplitsPayloadFromIndex: scheduling a fat job leaves a slim
// index (recipients and subject inline for the human audit, no payload
// bytes), one blob holding body + attachments, and a row that
// reassembles to the exact bytes via ContentOf.
func TestStoreSplitsPayloadFromIndex(t *testing.T) {
	store := testStore(t)
	s := NewScheduler(store)
	att := []byte("%PDF-1.4 the exact contract bytes")
	content := fatContent("vedi allegato", att)
	if _, err := s.ScheduleWithID("fat-split", testConfig(), &content, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	row, ok, err := store.GetByID("fat-split")
	if err != nil || !ok {
		t.Fatalf("GetByID: ok=%v err=%v", ok, err)
	}
	if row.ContentFile == "" || row.ContentSHA256 == "" {
		t.Fatalf("row must reference its blob and digest: %+v", row)
	}
	if want := int64(len("vedi allegato")) + int64(len(att)); row.PayloadBytes != want {
		t.Fatalf("PayloadBytes = %d, want %d", row.PayloadBytes, want)
	}
	// The slim half stays inline (the audit-readable fields).
	if row.Content.To[0] != "dest@pec.test" || row.Content.Subject != "test" {
		t.Fatalf("slim half lost its inline fields: %+v", row.Content)
	}
	// The fat half is NOT in the row.
	if row.Content.Body != "" || len(row.Content.Attachments) != 0 {
		t.Fatalf("row carries payload inline: body=%q atts=%d", row.Content.Body, len(row.Content.Attachments))
	}

	// The index is slim: the base64 of the bytes appears nowhere in it.
	raw, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), base64.StdEncoding.EncodeToString(att)) {
		t.Fatal("attachment bytes are inline in the index — the split must keep them in the blob")
	}
	// The blob exists, is owner-only, and reassembles to the exact bytes.
	blobFi, err := os.Stat(store.blobPath(row.ContentFile))
	if err != nil {
		t.Fatalf("blob missing: %v", err)
	}
	if blobFi.Mode().Perm() != 0o600 {
		t.Fatalf("blob mode = %o, want 600 (payload is evidence, owner-only)", blobFi.Mode().Perm())
	}
	full, err := store.ContentOf(row)
	if err != nil {
		t.Fatalf("ContentOf: %v", err)
	}
	if full.Body != "vedi allegato" || len(full.Attachments) != 1 || string(full.Attachments[0].Data) != string(att) {
		t.Fatalf("reassembled payload = %+v, want the exact original", full)
	}
}

// TestHydrateFailureIsTerminal: a blob that is missing or fails its
// sha256 must never reach the wire — the job dies terminal with the
// manual rule, exactly like any other pre-send failure.
func TestHydrateFailureIsTerminal(t *testing.T) {
	for _, tamper := range []struct {
		name string
		do   func(t *testing.T, store *Store, row Job)
		want string
	}{
		{"corrupt", func(t *testing.T, store *Store, row Job) {
			blob, err := os.ReadFile(store.blobPath(row.ContentFile))
			if err != nil {
				t.Fatal(err)
			}
			blob[0] ^= 0xff // flips a bit: the sha256 no longer matches
			if err := os.WriteFile(store.blobPath(row.ContentFile), blob, 0o600); err != nil {
				t.Fatal(err)
			}
		}, "integrity check"},
		{"missing", func(t *testing.T, store *Store, row Job) {
			if err := os.Remove(store.blobPath(row.ContentFile)); err != nil {
				t.Fatal(err)
			}
		}, "reading content blob"},
	} {
		t.Run(tamper.name, func(t *testing.T) {
			store := testStore(t)
			s := NewScheduler(store)
			c := coldCourier(s)
			content := fatContent("unverifiable", []byte("bytes that will not survive"))
			if _, err := s.ScheduleWithID("tampered", testConfig(), &content, time.Now().Add(100*time.Millisecond)); err != nil {
				t.Fatalf("schedule: %v", err)
			}
			row, ok, _ := store.GetByID("tampered")
			if !ok {
				t.Fatal("job was not persisted")
			}
			tamper.do(t, store, row)

			if err := s.Run(); err != nil {
				t.Fatalf("Run: %v", err)
			}
			c.mu.Lock()
			sends := len(c.sends)
			c.mu.Unlock()
			if sends != 0 {
				t.Fatalf("send calls = %d, want 0 (unverified bytes must never reach the wire)", sends)
			}
			jobs, _ := store.Load()
			if jobs[0].State != StateFailed || jobs[0].Result == nil {
				t.Fatalf("state = %q result = %+v, want terminal failed with a result", jobs[0].State, jobs[0].Result)
			}
			if !strings.Contains(jobs[0].Result.Err, tamper.want) || !strings.Contains(jobs[0].Result.Err, "NEW job") {
				t.Fatalf("failure must name the blob problem and the reschedule rule: %q", jobs[0].Result.Err)
			}
		})
	}
}

// writeV1Store hand-writes a version-1 file: the whole payload inline in
// every row, exactly as every gomailer before the split persisted it —
// envelope keys lowercase too ("version"/"jobs"), the shape the v1
// storeFile's json tags produced.
func writeV1Store(t *testing.T, path, id, body string, att []byte) {
	t.Helper()
	row := map[string]any{
		"ID":        id,
		"MessageID": "<" + id + "@pec.test>",
		"FireAt":    time.Now().Add(time.Hour).Format(time.RFC3339),
		"CreatedAt": time.Now().Add(-time.Minute).Format(time.RFC3339),
		"Content": map[string]any{
			"From":         "sender@pec.test",
			"To":           []string{"dest@pec.test"},
			"Subject":      "legacy",
			"Body":         body,
			"MessageID":    "<" + id + "@pec.test>",
			"TipoRicevuta": "completa",
			"Attachments": []map[string]any{{
				"Filename":    "old.pdf",
				"ContentType": "application/pdf",
				"Data":        base64.StdEncoding.EncodeToString(att),
			}},
		},
		"Config": map[string]any{
			"Hostname": "smtp.pec.test",
			"Port":     "465",
			"Username": "sender@pec.test",
			"Password": "secret",
		},
		"State": "pending",
	}
	buf, err := json.MarshalIndent(map[string]any{"version": 1, "jobs": []any{row}}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(buf, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestMigrateFromV1InlineStore: a pre-split file (payload inline in every
// row) migrates at boot — one blob write per row, ONE slim index
// rewrite — and the payload round-trips to the exact bytes. Idempotent on
// a second pass, and the migrated row still fires with its content.
func TestMigrateFromV1InlineStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.json")
	att := []byte("the old contract bytes")
	writeV1Store(t, path, "legacy", "the old body", att)

	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	// Pre-migration: the row loads hydrated (its bytes are still inline).
	jobs, err := store.Load()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("Load: err=%v jobs=%d", err, len(jobs))
	}
	if jobs[0].ContentFile != "" || jobs[0].Content.Body != "the old body" {
		t.Fatalf("legacy row must load hydrated until migrated: %+v", jobs[0])
	}

	migrated, n, err := store.Migrate()
	if err != nil || n != 1 {
		t.Fatalf("Migrate: n=%d err=%v, want 1, nil", n, err)
	}
	if migrated[0].ContentFile == "" {
		t.Fatal("the migrated row must reference its blob")
	}
	// The index is slim now: the base64 payload is gone from the file.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), base64.StdEncoding.EncodeToString(att)) {
		t.Fatal("migration left the payload inline in the index")
	}
	if !strings.Contains(string(raw), `"version": 2`) {
		t.Fatalf("migration must stamp the file version 2:\n%s", raw)
	}
	// The evidence round-trips to the exact bytes.
	row, ok, _ := store.GetByID("legacy")
	if !ok {
		t.Fatal("job vanished in migration")
	}
	full, err := store.ContentOf(row)
	if err != nil {
		t.Fatalf("ContentOf after migration: %v", err)
	}
	if full.Body != "the old body" || len(full.Attachments) != 1 || string(full.Attachments[0].Data) != string(att) {
		t.Fatalf("migrated payload = %+v, want the exact original bytes", full)
	}
	if row.PayloadBytes != int64(len("the old body"))+int64(len(att)) {
		t.Fatalf("PayloadBytes = %d, want the payload snapshot", row.PayloadBytes)
	}

	// Idempotent: nothing left to convert, nothing rewritten.
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, n, err := store.Migrate(); err != nil || n != 0 {
		t.Fatalf("second Migrate: n=%d err=%v, want 0, nil", n, err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("an empty migration rewrote the store")
	}

	// End to end: the migrated job fires with its legacy content intact.
	s := NewScheduler(store)
	c := coldCourier(s)
	if err := store.Update(func() Job { j := row; j.FireAt = time.Now().Add(50 * time.Millisecond); return j }()); err != nil {
		t.Fatalf("retime: %v", err)
	}
	if err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) != 1 || string(c.sends[0].msg) != "the old body" {
		t.Fatalf("sends = %v, want exactly one send of the legacy body", c.sends)
	}
}

// TestSaveNeverDropsPayload: a write into a NOT-yet-migrated version-1
// file (the CLI's persist-only fallback writing while the daemon is
// down) must extract EVERY loaded row, not just its own — slimming a row
// without writing its blob would be silent evidence loss.
func TestSaveNeverDropsPayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.json")
	att := []byte("legacy evidence bytes")
	writeV1Store(t, path, "legacy", "the old body", att)

	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	// The fallback seam: AddNew directly (what ScheduleWithID calls when
	// no daemon answers).
	fresh := fatContent("the new body", []byte("the new bytes"))
	job := Job{
		ID:        "fresh",
		MessageID: "<fresh@pec.test>",
		FireAt:    time.Now().Add(time.Hour),
		CreatedAt: time.Now(),
		Content:   fresh,
		Config:    testConfig(),
		State:     StatePending,
	}
	if _, created, err := store.AddNew(job); err != nil || !created {
		t.Fatalf("AddNew: created=%v err=%v", created, err)
	}

	jobs, err := store.Load()
	if err != nil || len(jobs) != 2 {
		t.Fatalf("Load: err=%v jobs=%d", err, len(jobs))
	}
	for _, j := range jobs {
		if j.ContentFile == "" {
			t.Fatalf("job %s has no blob reference — the write dropped its payload to disk-loss", j.ID)
		}
		full, err := store.ContentOf(j)
		if err != nil {
			t.Fatalf("job %s: ContentOf: %v", j.ID, err)
		}
		switch j.ID {
		case "legacy":
			if full.Body != "the old body" || string(full.Attachments[0].Data) != string(att) {
				t.Fatalf("legacy payload = %+v, want the exact original", full)
			}
		case "fresh":
			if full.Body != "the new body" {
				t.Fatalf("fresh payload = %+v, want the exact original", full)
			}
		}
	}
}

// TestSweepOrphanBlobs: blobs no row references (crash leftovers between
// the blob write and the row save) are swept at boot; referenced blobs
// survive.
func TestSweepOrphanBlobs(t *testing.T) {
	store := testStore(t)
	content := fatContent("keep me", nil)
	job := Job{
		ID:        "kept",
		MessageID: "<kept@pec.test>",
		FireAt:    time.Now().Add(time.Hour),
		CreatedAt: time.Now(),
		Content:   content,
		Config:    testConfig(),
		State:     StatePending,
	}
	if err := store.Add(job); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Crash residue: an orphan blob and a stranded temp file.
	if err := os.MkdirAll(store.blobDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.blobPath("orphan.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.blobPath("kept.json.tmp-123"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	swept, err := store.SweepOrphanBlobs()
	if err != nil || swept != 2 {
		t.Fatalf("sweep = %d err=%v, want 2 (orphan + temp)", swept, err)
	}
	entries, err := os.ReadDir(store.blobDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("blobs after sweep = %d, want exactly the referenced one", len(entries))
	}
	jobs, _ := store.Load()
	if _, err := store.ContentOf(jobs[0]); err != nil {
		t.Fatalf("the referenced blob must survive the sweep: %v", err)
	}
	// Idempotent.
	if swept, err := store.SweepOrphanBlobs(); err != nil || swept != 0 {
		t.Fatalf("second sweep = %d err=%v, want 0, nil", swept, err)
	}
}

// TestBlobDirNamespacedPerStore: the blob directory is named after the
// store file, so two stores sharing one state directory — an operator
// running a scratch or archive store beside the live one — own disjoint
// blob sets, and one store's orphan sweep can never delete the other's
// referenced evidence. (The first split-store builds shared one "blobs"
// sibling; the sweep of either store deleted the other's blobs.)
func TestBlobDirNamespacedPerStore(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenStore(filepath.Join(dir, "jobs.json"))
	if err != nil {
		t.Fatalf("OpenStore(a): %v", err)
	}
	b, err := OpenStore(filepath.Join(dir, "other.json"))
	if err != nil {
		t.Fatalf("OpenStore(b): %v", err)
	}
	if a.blobDir() == b.blobDir() {
		t.Fatalf("stores in one directory share the blob dir %q", a.blobDir())
	}
	if err := a.Add(splitTestJob("a1", "body a")); err != nil {
		t.Fatalf("Add(a1): %v", err)
	}
	if err := b.Add(splitTestJob("b1", "body b")); err != nil {
		t.Fatalf("Add(b1): %v", err)
	}
	// Crash residue in A's own directory: only it may be swept.
	if err := os.MkdirAll(a.blobDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(a.blobPath("a1.json.tmp-9"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if swept, err := a.SweepOrphanBlobs(); err != nil || swept != 1 {
		t.Fatalf("sweep(a) = %d err=%v, want 1 (its own orphan only)", swept, err)
	}
	// A's sweep must not have touched B's blobs, and vice versa.
	for _, s := range []*Store{a, b} {
		jobs, err := s.Load()
		if err != nil || len(jobs) != 1 {
			t.Fatalf("load %s: err=%v jobs=%d", s.path, err, len(jobs))
		}
		if _, err := s.ContentOf(jobs[0]); err != nil {
			t.Fatalf("store %s: blob did not survive the other store's sweep: %v", s.path, err)
		}
	}
}

// TestAdoptLegacyBlobs: blobs written by the first split-store builds
// lived in one shared "blobs" directory beside the index. Adoption
// moves (only) the blobs this store's rows reference into the
// store-named directory — byte-identical, digest still verified — and
// leaves bytes it does not own where they are. Idempotent.
func TestAdoptLegacyBlobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.json")
	att := []byte("legacy layout evidence")

	// The blob bytes exactly as the first builds wrote them (the digest
	// is computed over these bytes; ContentOf re-verifies it on read).
	blob, err := json.MarshalIndent(contentBlob{
		Body: "the split body",
		Attachments: []mailer.Attachment{{
			Filename:    "old.pdf",
			ContentType: "application/pdf",
			Data:        att,
		}},
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	blob = append(blob, '\n')
	sum := sha256.Sum256(blob)

	// The shared legacy directory, holding this store's blob and a
	// second store's blob side by side.
	legacyDir := filepath.Join(dir, "blobs")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "legacy.json"), blob, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "other.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	// A slim version-2 index whose row references the legacy-layout
	// blob, exactly as the first builds persisted it.
	row := storeRow{
		ID:        "legacy",
		MessageID: "<legacy@pec.test>",
		FireAt:    time.Now().Add(time.Hour),
		CreatedAt: time.Now(),
		Config:    testConfig(),
		State:     StatePending,
		Content: &mailer.MailContent{
			From:      "sender@pec.test",
			To:        []string{"dest@pec.test"},
			Subject:   "split",
			MessageID: "<legacy@pec.test>",
		},
		ContentFile:   "legacy.json",
		ContentSHA256: hex.EncodeToString(sum[:]),
		PayloadBytes:  int64(len("the split body")) + int64(len(att)),
	}
	idx, err := json.MarshalIndent(storeFile{Version: storeVersion, Jobs: []storeRow{row}}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(idx, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	jobs, err := store.Load()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("Load: err=%v jobs=%d", err, len(jobs))
	}

	adopted, err := store.AdoptLegacyBlobs()
	if err != nil || adopted != 1 {
		t.Fatalf("AdoptLegacyBlobs = %d err=%v, want 1, nil", adopted, err)
	}
	// The blob moved: gone from the shared directory, present under the
	// store's own — and the OTHER store's blob stayed where it was.
	if _, err := os.Stat(filepath.Join(legacyDir, "legacy.json")); !os.IsNotExist(err) {
		t.Fatalf("legacy blob still in the shared directory: %v", err)
	}
	if _, err := os.Stat(store.blobPath("legacy.json")); err != nil {
		t.Fatalf("adopted blob missing from the store's own directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(legacyDir, "other.json")); err != nil {
		t.Fatalf("adoption moved (or removed) a blob this store does not own: %v", err)
	}
	// The adopted bytes round-trip, digest-verified.
	full, err := store.ContentOf(jobs[0])
	if err != nil {
		t.Fatalf("ContentOf after adoption: %v", err)
	}
	if full.Body != "the split body" || len(full.Attachments) != 1 || string(full.Attachments[0].Data) != string(att) {
		t.Fatalf("adopted payload = %+v, want the exact original bytes", full)
	}
	// Idempotent, and the sweep never looks at the shared directory.
	if adopted, err := store.AdoptLegacyBlobs(); err != nil || adopted != 0 {
		t.Fatalf("second adoption = %d err=%v, want 0, nil", adopted, err)
	}
	if swept, err := store.SweepOrphanBlobs(); err != nil || swept != 0 {
		t.Fatalf("sweep = %d err=%v, want 0 (it must not touch the shared directory)", swept, err)
	}
	if _, err := os.Stat(filepath.Join(legacyDir, "other.json")); err != nil {
		t.Fatalf("sweep deleted from the shared directory: %v", err)
	}
}

// TestBlobNameSafety: job IDs are caller-choosable (the idempotency key
// rides the socket protocol), so a hostile or careless ID must never
// escape the blobs directory — it maps to a deterministic sha256 name,
// and the payload still round-trips.
func TestBlobNameSafety(t *testing.T) {
	store := testStore(t)
	for _, id := range []string{"../escape", "with space", strings.Repeat("x", 200)} {
		job := Job{
			ID:        id,
			MessageID: "<weird@pec.test>",
			FireAt:    time.Now().Add(time.Hour),
			CreatedAt: time.Now(),
			Content:   testContent("payload for " + id),
			Config:    testConfig(),
			State:     StatePending,
		}
		if err := store.Add(job); err != nil {
			t.Fatalf("Add(%q): %v", id, err)
		}
		jobs, err := store.Load()
		if err != nil {
			t.Fatal(err)
		}
		var row Job
		for _, j := range jobs {
			if j.ID == id {
				row = j
			}
		}
		if row.ContentFile == "" {
			t.Fatalf("job %q: no blob reference", id)
		}
		if strings.Contains(row.ContentFile, "/") || strings.Contains(row.ContentFile, "..") {
			t.Fatalf("blob name %q escapes the blobs directory", row.ContentFile)
		}
		// The traversal target of an id like "../escape" is the store
		// directory itself (blobs/../escape.json): nothing may exist there.
		if _, err := os.Stat(filepath.Join(filepath.Dir(store.path), "escape.json")); err == nil {
			t.Fatal("a blob was written OUTSIDE the blobs directory (path traversal)")
		}
		full, err := store.ContentOf(row)
		if err != nil {
			t.Fatalf("ContentOf(%q): %v", id, err)
		}
		if full.Body != "payload for "+id {
			t.Fatalf("payload for %q did not round-trip", id)
		}
	}
}

// TestStagedLeadBudgetsHydration: a split row's window covers the cost of
// reassembling its payload from the blob (read + sha256 + unmarshal of
// ~1.4× the payload) — the term a runner dispatched at its deadline would
// otherwise pay as lateness — while a job that already holds its content
// in RAM pays nothing.
func TestStagedLeadBudgetsHydration(t *testing.T) {
	s := NewScheduler(testStore(t))

	slim := splitTestJob("slim", "the body")
	slim.Content.Body = "" // as loaded from the index: the fat half is in the blob
	slim.ContentFile = "slim.json"
	slim.PayloadBytes = 20 * 1000 * 1000

	ram := slim
	ram.Content.Body = "the body" // a fresh schedule: full content in RAM

	hydrateTerm := time.Duration(slim.PayloadBytes*14/10) * time.Second / time.Duration(defaultHydrateRate)
	if hydrateTerm <= 0 {
		t.Fatalf("hydrate term = %s, want > 0", hydrateTerm)
	}
	if diff := s.stagedLead(slim) - s.stagedLead(ram); diff != hydrateTerm {
		t.Fatalf("stagedLead(slim) - stagedLead(ram) = %s, want the hydrate term %s", diff, hydrateTerm)
	}

	// The trigger mirrors Scheduler.hydrate exactly: a pre-migration
	// legacy row (inline content, no blob reference) also pays nothing.
	legacy := splitTestJob("legacy", "the body")
	legacy.PayloadBytes = slim.PayloadBytes
	if s.stagedLead(legacy) != s.stagedLead(ram) {
		t.Fatalf("legacy row sized differently from a RAM job: %s vs %s", s.stagedLead(legacy), s.stagedLead(ram))
	}

	// A tiny payload: the term is rounding noise (µs), never a window
	// opener on its own.
	small := splitTestJob("small", "b")
	small.Content.Body = ""
	small.ContentFile = "small.json"
	small.PayloadBytes = 100
	smallRam := small
	smallRam.Content.Body = "b"
	if d := s.stagedLead(small) - s.stagedLead(smallRam); d > time.Millisecond {
		t.Fatalf("tiny payload costs a hydrate term of %s, want sub-millisecond noise", d)
	}
}

// TestContentOfIsLockFreeUnderWriters: reassembly takes no store lock
// (see ContentOf), so runners hydrate while writers persist — and every
// reader still gets its exact, digest-verified bytes.
func TestContentOfIsLockFreeUnderWriters(t *testing.T) {
	store := testStore(t)
	const readers = 8
	for i := 0; i < readers; i++ {
		if err := store.Add(splitTestJob(fmt.Sprintf("r%d", i), fmt.Sprintf("fat body %d", i))); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	// A writer churning updates while every reader reassembles.
	stop := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		sentinel := splitTestJob("sentinel", "sentinel body")
		for n := 0; ; n++ {
			select {
			case <-stop:
				writerDone <- nil
				return
			default:
			}
			sentinel.FireAt = time.Now().Add(time.Duration(n+1) * time.Minute)
			if err := store.Update(sentinel); err != nil {
				writerDone <- err
				return
			}
		}
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("r%d", i)
			want := fmt.Sprintf("fat body %d", i)
			j, ok, err := store.GetByID(id)
			if err != nil || !ok {
				errCh <- fmt.Errorf("get %s: %v (ok=%v)", id, err, ok)
				return
			}
			for n := 0; n < 50; n++ {
				full, err := store.ContentOf(j)
				if err != nil {
					errCh <- fmt.Errorf("ContentOf %s: %w", id, err)
					return
				}
				if full.Body != want {
					errCh <- fmt.Errorf("ContentOf %s = %q, want %q", id, full.Body, want)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(stop)
	if err := <-writerDone; err != nil {
		t.Fatalf("writer: %v", err)
	}
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}
