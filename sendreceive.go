package lasr

import (
	"context"
)

// Send sends a message to Q. When send completes with nil error, the message
// sent to Q will be in the Ready state.
func (q *Q) Send(message []byte) (ID, error) {
	if q.isClosed() {
		return 0, ErrQClosed
	}
	var id ID
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.queries.send.QueryRow(q.name, message).Scan(&id); err != nil {
		return id, err
	}
	q.waker.Wake()
	return id, nil
}

// Receive receives a message from the queue. If no messages are available by
// the time the context is done, then the function will return a nil Message
// and the result of ctx.Err().
func (q *Q) Receive(ctx context.Context) (*Message, error) {
	if q.isClosed() {
		return nil, ErrQClosed
	}
	q.messages.Lock()
	defer q.messages.Unlock()
START:
	if q.messages.Len() > 0 {
		msg := q.messages.Pop()
		err := msg.err
		if err != nil {
			msg = nil
		} else {
			q.inFlight.Add(1)
		}
		return msg, err
	}
	select {
	case <-q.waker.C:
		if err := q.processReceives(ctx); err != nil {
			return nil, err
		}
		goto START
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.closed:
		return nil, ErrQClosed
	}
}

func (q *Q) processReceives(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	limit := q.messages.Cap() - q.messages.Len()
	rows, err := q.queries.recv.QueryContext(ctx, limit)
	if err != nil {
		return err
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
		var id ID
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return err
		}
		q.messages.Push(&Message{
			Body: data,
			ID:   id,
			q:    q,
		})
	}
	if count > 0 {
		q.waker.Wake()
	}
	return nil
}
