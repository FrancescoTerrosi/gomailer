package jobber

import (
	"slices"
)

// SortedQueue is an in-memory queue of pending jobs kept ordered by fire
// time. The scheduler keeps exactly one of these; the store is the source
// of truth across reboots, this is just the live firing order.
type SortedQueue []Job

// jobLess orders jobs by FireAt, breaking ties with CreatedAt (FIFO for
// jobs scheduled for the same instant).
func jobLess(a, b Job) int {
	if cmp := a.FireAt.Compare(b.FireAt); cmp != 0 {
		return cmp
	}
	return a.CreatedAt.Compare(b.CreatedAt)
}

// Sort orders the queue in place.
func (sq SortedQueue) Sort() {
	slices.SortFunc(sq, jobLess)
}

// Insert adds a job keeping the queue ordered (binary-search insertion).
func (sq *SortedQueue) Insert(j Job) {
	i, _ := slices.BinarySearchFunc(*sq, j, jobLess)
	*sq = slices.Insert(*sq, i, j)
}
