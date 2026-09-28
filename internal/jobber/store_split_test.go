package jobber

// Tests for the split store (store.go): the slim index + one write-once
// blob per job, the version-1 boot migration, the sha256 integrity
// anchor (a certified sender never puts unverified bytes on the wire),
// the never-drop invariant (no write path can slim a row into evidence
// loss), the orphan sweep, blob-name safety for caller-choosable IDs,
// the per-store blob directory (named after the store file, so stores
// sharing a state directory cannot destroy each other's evidence),
// adoption of the first builds' shared blob directory, and the terminal
// release policy (payload bytes live until the job settles).

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
// writeV1Store hand-writes a version-1 file: the whole payload inline in
// every row, exactly as every gomailer before the split persisted it —
// envelope keys lowercase too ("version"/"jobs"), the shape the v1
// storeFile's json tags produced. state lets a row arrive settled
// (pre-split stores kept terminal rows fat too).
func writeV1Store(t *testing.T, path, id, body string, att []byte, state JobState) {
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
		"State": string(state),
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
	writeV1Store(t, path, "legacy", "the old body", att, StatePending)

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
	writeV1Store(t, path, "legacy", "the old body", att, StatePending)

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

// TestTerminalRelease: the payload blob lives exactly as long as the job
// can still fire. A settled row persists WITHOUT its reference — keeping
// the sha256 fingerprint and PayloadBytes — and the file is deleted;
// ContentOf on the released row refuses loudly instead of returning
// empty bytes. Both terminal states release.
func TestTerminalRelease(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state JobState
	}{
		{"sent", StateSent},
		{"failed", StateFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := testStore(t)
			j := splitTestJob("settle", "the body")
			j.Content = fatContent("the body", []byte("fat attachment bytes"))
			if _, created, err := store.AddNew(j); err != nil || !created {
				t.Fatalf("AddNew: created=%v err=%v", created, err)
			}
			row, ok, err := store.GetByID(j.ID)
			if err != nil || !ok || row.ContentFile == "" {
				t.Fatalf("pending row must reference its blob: ok=%v err=%v row=%+v", ok, err, row)
			}
			blob := row.ContentFile

			// Settle through the Update choke point — what fire, failNow
			// and boot recovery all call.
			row.State = tc.state
			row.Result = &JobResult{FiredAt: time.Now(), Err: "settled in test"}
			if err := store.Update(row); err != nil {
				t.Fatalf("Update: %v", err)
			}

			got, ok, err := store.GetByID(j.ID)
			if err != nil || !ok {
				t.Fatalf("GetByID: ok=%v err=%v", ok, err)
			}
			if got.ContentFile != "" {
				t.Fatalf("settled row still references its blob: %q", got.ContentFile)
			}
			if got.ContentSHA256 == "" || got.PayloadBytes == 0 {
				t.Fatalf("settled row must keep the fingerprint: sha=%q bytes=%d", got.ContentSHA256, got.PayloadBytes)
			}
			if got.Config.Password != "" {
				t.Fatal("settled row must be stripped of its credential (the pre-existing choke-point rule)")
			}
			if _, err := os.Stat(store.blobPath(blob)); !os.IsNotExist(err) {
				t.Fatalf("released file must be deleted: %v", err)
			}
			if _, err := store.ContentOf(got); err == nil || !strings.Contains(err.Error(), "released") {
				t.Fatalf("ContentOf on a released row must refuse loudly, got %v", err)
			}
			// The release deleted its own file: nothing was orphaned.
			if swept, err := store.SweepOrphanBlobs(); err != nil || swept != 0 {
				t.Fatalf("sweep = %d err=%v, want 0", swept, err)
			}
		})
	}
}

// TestReleaseNeverTouchesUnfiredPayloads: pending and inflight rows keep
// their payloads through arbitrary rewrites — the never-drop invariant
// now reads "no write path may drop the payload of a job that has not
// yet fired".
func TestReleaseNeverTouchesUnfiredPayloads(t *testing.T) {
	store := testStore(t)
	pending := splitTestJob("pending-keep", "pending body")
	pending.Content = fatContent("pending body", []byte("pending bytes"))
	if _, created, err := store.AddNew(pending); err != nil || !created {
		t.Fatalf("AddNew: %v", err)
	}
	inflight := splitTestJob("inflight-keep", "inflight body")
	inflight.Content = fatContent("inflight body", []byte("inflight bytes"))
	inflight.State = StateInflight
	if err := store.Add(inflight); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// An unrelated write rewrites the whole set: unfired rows must come
	// back with their payloads intact.
	if err := store.Add(splitTestJob("unrelated", "unrelated")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	for _, id := range []string{"pending-keep", "inflight-keep"} {
		j, ok, err := store.GetByID(id)
		if err != nil || !ok || j.ContentFile == "" {
			t.Fatalf("unfired row %s lost its blob reference: ok=%v err=%v row=%+v", id, ok, err, j)
		}
		full, err := store.ContentOf(j)
		if err != nil || !strings.Contains(full.Body, " body") {
			t.Fatalf("unfired row %s payload = %+v err=%v, want intact", id, full, err)
		}
	}
}

// TestKeepPayloadsEscapeHatch: with KeepPayloads set, settled rows keep
// their blobs (the pre-policy behavior) — the conservative operator's
// opt-out, wired to the -keep-payloads flag.
func TestKeepPayloadsEscapeHatch(t *testing.T) {
	store := testStore(t)
	store.KeepPayloads = true
	j := splitTestJob("kept", "kept body")
	j.Content = fatContent("kept body", []byte("kept bytes"))
	if _, created, err := store.AddNew(j); err != nil || !created {
		t.Fatalf("AddNew: %v", err)
	}
	row, _, _ := store.GetByID(j.ID)
	row.State = StateSent
	if err := store.Update(row); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _, _ := store.GetByID(j.ID)
	if got.ContentFile == "" {
		t.Fatal("KeepPayloads: settled row must keep its blob reference")
	}
	full, err := store.ContentOf(got)
	if err != nil || full.Body != "kept body" {
		t.Fatalf("KeepPayloads: payload must round-trip: %+v %v", full, err)
	}
	if _, err := os.Stat(store.blobPath(got.ContentFile)); err != nil {
		t.Fatalf("KeepPayloads: blob file must exist: %v", err)
	}
}

// TestReleaseTerminalPayloadsBootEviction: rows settled by older
// gomailers still reference their blobs; the boot step brings them under
// the policy — reference cleared (fingerprint kept), file deleted,
// unfired rows untouched, idempotent, and gated by KeepPayloads.
func TestReleaseTerminalPayloadsBootEviction(t *testing.T) {
	store := testStore(t)
	// A row settled by an OLDER gomailer: scheduled (blob written), then
	// settled under keep-forever behavior (KeepPayloads), which is what
	// the pre-policy releases wrote — terminal WITH its reference.
	sent := splitTestJob("old-sent", "old body")
	sent.Content = fatContent("old body", []byte("old bytes"))
	if _, created, err := store.AddNew(sent); err != nil || !created {
		t.Fatalf("AddNew: %v", err)
	}
	pending := splitTestJob("old-pending", "still fires")
	pending.Content = fatContent("still fires", []byte("live bytes"))
	if err := store.Add(pending); err != nil {
		t.Fatalf("Add: %v", err)
	}
	store.KeepPayloads = true
	sentRow, ok, err := store.GetByID("old-sent")
	if err != nil || !ok {
		t.Fatalf("GetByID: %v", err)
	}
	sentRow.State = StateSent
	if err := store.Update(sentRow); err != nil {
		t.Fatalf("Update: %v", err)
	}
	store.KeepPayloads = false // boot of the release-aware gomailer
	sentRow, _, _ = store.GetByID("old-sent")
	blob := sentRow.ContentFile
	if blob == "" {
		t.Fatal("pre-policy settled row must carry its reference")
	}

	released, err := store.ReleaseTerminalPayloads()
	if err != nil || released != 1 {
		t.Fatalf("ReleaseTerminalPayloads = %d err=%v, want 1", released, err)
	}
	got, _, _ := store.GetByID("old-sent")
	if got.ContentFile != "" || got.ContentSHA256 == "" {
		t.Fatalf("legacy settled row must be released with its fingerprint: %+v", got)
	}
	if _, err := os.Stat(store.blobPath(blob)); !os.IsNotExist(err) {
		t.Fatalf("released legacy file must be deleted: %v", err)
	}
	live, _, _ := store.GetByID("old-pending")
	if live.ContentFile == "" {
		t.Fatal("pending row must be untouched by the boot eviction")
	}
	if full, err := store.ContentOf(live); err != nil || full.Body != "still fires" {
		t.Fatalf("pending row payload = %+v err=%v, want intact", full, err)
	}

	// Idempotent: nothing left to release.
	if released, err := store.ReleaseTerminalPayloads(); err != nil || released != 0 {
		t.Fatalf("second ReleaseTerminalPayloads = %d err=%v, want 0", released, err)
	}

	// The escape hatch gates the boot step too: with KeepPayloads, a
	// row settled through the keep-forever behavior keeps its blob and
	// the step is a no-op.
	store.KeepPayloads = true
	late := splitTestJob("late-settled", "late body")
	late.Content = fatContent("late body", []byte("late bytes"))
	if _, created, err := store.AddNew(late); err != nil || !created {
		t.Fatalf("AddNew: %v", err)
	}
	lateRow, _, _ := store.GetByID("late-settled")
	lateRow.State = StateFailed
	if err := store.Update(lateRow); err != nil {
		t.Fatalf("Update: %v", err)
	}
	lateRow, _, _ = store.GetByID("late-settled")
	if lateRow.ContentFile == "" {
		t.Fatal("KeepPayloads: settled row must keep its blob")
	}
	if released, err := store.ReleaseTerminalPayloads(); err != nil || released != 0 {
		t.Fatalf("gated ReleaseTerminalPayloads = %d err=%v, want 0", released, err)
	}
	lateRow, _, _ = store.GetByID("late-settled")
	if lateRow.ContentFile == "" {
		t.Fatal("KeepPayloads: the boot step must not release")
	}
}

// TestMigrateSlimsSettledInlineRows: a pre-split store with a SETTLED row
// still carrying its payload inline: boot migration slims it — the
// retention policy for settled jobs, not a loss — while pending rows
// still split and round-trip. Idempotent.
func TestMigrateSlimsSettledInlineRows(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.json")
	att := []byte("settled inline bytes")
	writeV1Store(t, path, "old-settled", "the settled body", att, StateSent)

	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	jobs, err := store.Load()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("Load: err=%v jobs=%d", err, len(jobs))
	}
	if jobs[0].Content.Body != "the settled body" {
		t.Fatalf("pre-migration settled row must load hydrated: %+v", jobs[0].Content)
	}

	migrated, converted, err := store.Migrate()
	if err != nil || converted != 0 {
		t.Fatalf("Migrate: converted=%d err=%v, want 0 splits (nothing left to fire)", converted, err)
	}
	if migrated[0].Content.Body != "" || len(migrated[0].Content.Attachments) != 0 {
		t.Fatalf("settled row must be slimmed by the policy: %+v", migrated[0].Content)
	}
	// The index lost the inline bytes: the base64 payload is gone.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), base64.StdEncoding.EncodeToString(att)) {
		t.Fatal("settled row's inline payload survived migration")
	}
	// No blob was written for it (nothing to fire, nothing to keep).
	entries, err := os.ReadDir(store.blobDir())
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("reading blob dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("blobs after migration = %d, want 0", len(entries))
	}

	// Idempotent: an already-slim settled row has nothing to drop.
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, converted, err := store.Migrate(); err != nil || converted != 0 {
		t.Fatalf("second Migrate: converted=%d err=%v, want 0", converted, err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("an empty migration rewrote the store")
	}
}
