package jobber

import (
	"gomailer/internal/mailer"
	"slices"
	"time"
)

type Job struct {
	ID        string
	FireAt    time.Time
	CreatedAt time.Time
	Content   mailer.MailContent
	Config    mailer.MailConfig
}

type SortedQueue []Job

func (sq SortedQueue) Sort() {
	slices.SortFunc(sq, func(a Job, b Job) int {
		if cmp := a.FireAt.Compare(b.FireAt); cmp != 0 {
			return cmp
		}
		return a.CreatedAt.Compare(b.CreatedAt)
	})
}
