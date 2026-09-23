package jobber

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// storeVersion is the on-disk schema version, leaving room for migrations
// when this grows into a service backed by a SQL store.
const storeVersion = 1

// storeFile is the on-disk envelope.
type storeFile struct {
	Version int   `json:"version"`
	Jobs    []Job `json:"jobs"`
}

// Store persists jobs as a single human-readable JSON file.
//
// Every mutation rewrites the file atomically: content is written to a
// temp file, fsynced, then renamed over the old file (and the directory
// entry is fsynced too), so a reboot mid-write can never expose a torn
// file: a reader sees either the old or the new state, never a half one.
//
// SECURITY: Job carries the mailbox credentials, so the file is created
// with 0600 permissions. Do not loosen this; the future service should
// move credentials out of the job record entirely (vault/env).
type Store struct {
	path string
	mu   sync.Mutex
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

// Load returns all jobs currently in the store.
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
	if sf.Version != storeVersion {
		return nil, fmt.Errorf("jobber: store version %d not supported (want %d)", sf.Version, storeVersion)
	}
	return sf.Jobs, nil
}

// Add appends a new job. The whole read-modify-write is serialized under
// the store mutex, so concurrent Adds cannot lose jobs.
func (s *Store) Add(j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobs, err := s.loadLocked()
	if err != nil {
		return err
	}
	return s.saveLocked(append(jobs, j))
}

// Update replaces the job with the same ID, or appends it when absent.
func (s *Store) Update(j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobs, err := s.loadLocked()
	if err != nil {
		return err
	}
	for i := range jobs {
		if jobs[i].ID == j.ID {
			jobs[i] = j
			return s.saveLocked(jobs)
		}
	}
	return s.saveLocked(append(jobs, j))
}

// saveLocked atomically persists the full job set. Completed jobs are
// kept: the file doubles as history and verification log. Caller must
// hold s.mu.
func (s *Store) saveLocked(jobs []Job) error {
	ordered := slices.Clone(jobs)
	slices.SortFunc(ordered, jobLess)

	buf, err := json.MarshalIndent(storeFile{Version: storeVersion, Jobs: ordered}, "", "  ")
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
