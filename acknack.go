package lasr

import (
	"sync/atomic"
)

func (q *Q) ack(id ID) error {
	if _, err := q.queries.ack.Exec(id); err != nil {
		return err
	}

	q.inFlight.Done()

	if !q.isClosed() {
		q.waker.Wake()
	}

	return nil
}

func (q *Q) nack(id ID, retry bool) error {
	if retry {
		defer q.inFlight.Done()
		if _, err := q.queries.nackRetry.Exec(id); err != nil {
			return err
		}
		q.waker.Wake()
		return nil
	}
	return q.ack(id)
}

// Ack acknowledges successful receipt and processing of the Message.
func (m *Message) Ack() (err error) {
	m.q.mu.Lock()
	defer m.q.mu.Unlock()
	if !atomic.CompareAndSwapInt32(&m.once, 0, 1) {
		return ErrAckNack
	}
	return m.q.ack(m.ID)
}

// Nack negatively acknowledges successful receipt and processing of the
// Message. If Nack is called with retry True, then the Message will be
// placed back in the queue in its original position.
func (m *Message) Nack(retry bool) (err error) {
	m.q.mu.Lock()
	defer m.q.mu.Unlock()
	if !atomic.CompareAndSwapInt32(&m.once, 0, 1) {
		return ErrAckNack
	}
	return m.q.nack(m.ID, retry)
}
