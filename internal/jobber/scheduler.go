package jobber

import (
	"time"
)

type Scheduler struct {
	CurrentBatch SortedQueue
}

func (s *Scheduler) ExecuteBatch() {
	for _, job := range s.CurrentBatch {
		now := time.Now()

		if job.FireAt.After(now) {
			time.Sleep(job.FireAt.Sub(now))
		}

		job.Config.Send(&job.Content)
	}

	s.CurrentBatch = nil

}
