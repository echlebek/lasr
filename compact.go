package lasr

// Compact performs compaction on the queue. While compaction is ongoing, the
// queue is locked for sends and receives. After compaction, the queue database
// file size will be reduced.
func (q *Q) Compact() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, err := q.db.Exec("VACUUM;")
	return err
}
