package jobber

// import (
// "gomailer/internal/mailer"
// "time"
// )
//
// type Job struct {
// ID      string
// FireAt  time.Time
// Content mailer.MailContent
// Index   int
// }
//
// type PriorityQueue []*Job
//
// func (pq PriorityQueue) Len() int { return len(pq) }
//
// func (pq PriorityQueue) Less(i int, j int) bool {
// return pq[i].FireAt.Before(pq[j].FireAt)
// }
//
// func (pq PriorityQueue) Swap(i int, j int) {
// pq[i], pq[j] = pq[j], pq[i]
// pq[i].Index = i
// pq[j].Index = j
// }
//
// func (pq *PriorityQueue) Push(x any) {
// n := len(*pq)
// job := x.(*Job)
// job.Index = n
// *pq = append(*pq, job)
// }
//
// func (pq *PriorityQueue) Pop() any {
// old := *pq
// n := len(old)
// item := old[n-1]
// old[n-1] = nil
// item.Index = -1
// *pq = old[0 : n-1]
// return item
// }
//
