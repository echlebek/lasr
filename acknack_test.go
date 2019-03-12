package lasr

import (
	"context"
	"sync"
	"testing"

	"github.com/boltdb/bolt"
)

func TestAckConcurrent(t *testing.T) {
	q, cleanup := newQ(t, WithDeadLetters())
	defer cleanup()

	msg := []byte("foo")
	if _, err := q.Send(msg); err != nil {
		t.Fatal(err)
	}

	m, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	errors := make([]error, 100000)
	var wg sync.WaitGroup
	wg.Add(len(errors))

	for i := range errors {
		go func(i int) {
			errors[i] = m.Ack()
			wg.Done()
		}(i)
	}

	wg.Wait()

	ackCount, ackErrCount := 0, 0

	for _, err := range errors {
		switch err {
		case nil:
			ackCount += 1
		case ErrAckNack:
			ackErrCount += 1
		default:
			t.Fatal(err)
		}
	}

	if got, want := ackCount, 1; got != want {
		t.Errorf("wrong number of acks: got %d, want %d", got, want)
	}

	if got, want := ackErrCount, 99999; got != want {
		t.Errorf("wrong number of errors: got %d, want %d", got, want)
	}
}

func TestNackConcurrent(t *testing.T) {
	q, cleanup := newQ(t, WithDeadLetters())
	defer cleanup()

	msg := []byte("foo")
	if _, err := q.Send(msg); err != nil {
		t.Fatal(err)
	}

	m, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	errors := make([]error, 100000)
	var wg sync.WaitGroup
	wg.Add(len(errors))

	for i := range errors {
		go func(i int) {
			errors[i] = m.Ack()
			wg.Done()
		}(i)
	}

	wg.Wait()

	nackCount, nackErrCount := 0, 0

	for _, err := range errors {
		switch err {
		case nil:
			nackCount += 1
		case ErrAckNack:
			nackErrCount += 1
		default:
			t.Fatal(err)
		}
	}

	if got, want := nackCount, 1; got != want {
		t.Errorf("wrong number of nacks: got %d, want %d", got, want)
	}

	if got, want := nackErrCount, 99999; got != want {
		t.Errorf("wrong number of errors: got %d, want %d", got, want)
	}
}

func TestNackDeletesMessage_GH5(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	_, err := q.Send([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}

	msg, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if err := msg.Nack(false); err != nil {
		t.Fatal(err)
	}

	q.mu.RLock()
	err = q.db.Update(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.keys.unacked)
		if err != nil {
			return err
		}
		k, _ := bucket.Cursor().First()
		if k != nil {
			t.Error("message remains unacked")
		}
		return nil
	})
	q.mu.RUnlock()

	if err != nil {
		t.Fatal(err)
	}
}
