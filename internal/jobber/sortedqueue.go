package jobber

import (
	"slices"
)

// SortedQueue is an in-memory queue of pending jobs kept ordered by the
// caller's key. The scheduler keeps exactly one of these, ordered by
// WHEN EACH JOB'S WARM WINDOW OPENS (see Scheduler.windowLess): the head
// is then by definition the next job to warm (or, cold, the next to
// fire), which is what the dispatch loop consumes — O(1) per dispatch
// decision, no queue-wide scans. The store is the source of truth across
// reboots; this is just the live firing order, and it deliberately does
// NOT share the store's on-disk fire-time order (jobLess, used by
// saveLocked to keep the human-audited index sorted by FireAt).
//
// COST MODEL (2026-09-28, decision: leave as is — see TODO item 6
// "Queue scaling"): the dispatch pop (a front-delete in the tender) and
// mid-queue inserts are O(N) memmoves of ~275B structs — payload bytes
// ride behind slice headers and never move. Sub-millisecond at ≤10⁴
// pending, spent inside the warm window, and unreachable from the
// runner-owned dot. The documented endgame (~10⁵+ pending) is a
// head-index now and "lock-and-load" buckets then, NOT a heap.
type SortedQueue []Job

// jobLess orders jobs by FireAt, breaking ties with CreatedAt (FIFO for
// jobs scheduled for the same instant). This is the STORE's on-disk order
// — the index is a human-audited fire-time log — NOT the scheduler's
// queue order (see Scheduler.windowLess for that).
func jobLess(a, b Job) int {
	if cmp := a.FireAt.Compare(b.FireAt); cmp != 0 {
		return cmp
	}
	return a.CreatedAt.Compare(b.CreatedAt)
}

// InsertFunc adds a job keeping the queue ordered by less
// (binary-search insertion).
func (sq *SortedQueue) InsertFunc(j Job, less func(a, b Job) int) {
	i, _ := slices.BinarySearchFunc(*sq, j, less)
	*sq = slices.Insert(*sq, i, j)
}
