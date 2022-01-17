package lasr

import (
	"sync/atomic"
)

func (q *Q) ack(id ID) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	_, err := q.db.Exec(ackSQL, id)
	if err != nil {
		return err
	}

	q.inFlight.Done()

	if !q.isClosed() {
		q.waker.Wake()
	}

	return nil
}

func (q *Q) nack(id ID, retry bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if retry {
		_, err := q.db.Exec(nackRetrySQL, id)
		if err == nil {
			q.inFlight.Done()
		}
		q.waker.Wake()
		return err
	}
	return q.ack(id)
}

// Ack acknowledges successful receipt and processing of the Message.
func (m *Message) Ack() (err error) {
	if !atomic.CompareAndSwapInt32(&m.once, 0, 1) {
		return ErrAckNack
	}
	return m.q.ack(m.ID)
}

// Nack negatively acknowledges successful receipt and processing of the
// Message. If Nack is called with retry True, then the Message will be
// placed back in the queue in its original position.
func (m *Message) Nack(retry bool) (err error) {
	if !atomic.CompareAndSwapInt32(&m.once, 0, 1) {
		return ErrAckNack
	}
	return m.q.nack(m.ID, retry)
}
