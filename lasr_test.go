package lasr

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/boltdb/bolt"
)

type fataler interface {
	Fatal(...interface{})
}

func newQ(t fataler, options ...Option) (*Q, func()) {
	td, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	fp := filepath.Join(td, "lasr.db")
	db, err := bolt.Open(fp, 0600, nil)
	if err != nil {
		defer os.RemoveAll(td)
		t.Fatal(err)
	}
	cleanup := func() {
		defer os.RemoveAll(td)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	q, err := NewQ(db, "testing", options...)
	if err != nil {
		defer cleanup()
		t.Fatal(err)
	}
	return q, cleanup
}

func TestNewQ(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	if q.db == nil {
		t.Error("nil db")
	}

	if got, want := string(q.name), "testing"; got != want {
		t.Errorf("bad queue name: got %q, want %q", got, want)
	}
}

func TestSendReceive(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	msg := []byte("foobar")
	if err := q.Send(msg); err != nil {
		t.Fatal(err)
	}
	message, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.Body, msg) {
		t.Error("messages do not match")
	}
	if err := message.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestSendReceiveNackNoRetry(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	msg := []byte("foobar")
	if err := q.Send(msg); err != nil {
		t.Fatal(err)
	}
	message, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := message.Nack(false); err != nil {
		t.Fatal(err)
	}
	if err := message.Ack(); err != ErrAckNack {
		t.Error("expected ErrAckNack")
	}
}

func TestSendReceiveNackWithRetry(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	msg := []byte("foobar")
	if err := q.Send(msg); err != nil {
		t.Fatal(err)
	}
	message, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := message.Nack(true); err != nil {
		t.Fatal(err)
	}
	message, err = q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.Body, msg) {
		t.Errorf("bad body: %q", string(message.Body))
	}
	if err := message.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestReceiveContextCancelled(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		q.Receive(ctx)
	}()
	cancel()
	<-done // blocks forever if the Receive never gets cancelled
}

func TestUnacked(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	msg := []byte("foo")
	if err := q.Send(msg); err != nil {
		t.Fatal(err)
	}

	if _, err := q.Receive(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Make sure the unacked message is in the unacked queue
	err := q.db.Update(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.unackedKey)
		if err != nil {
			return err
		}
		if bucket == nil {
			t.Error("nil bucket")
			return nil
		}
		stats := bucket.Stats()
		if got, want := stats.KeyN, 1; got != want {
			t.Errorf("got %d unacked messages, want %d", got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Assume at this point the program crashed and we loaded the db again.
	q, err = NewQ(q.db, "testing")
	if err != nil {
		t.Fatal(err)
	}

	// Make sure all the messages are moved out of the unacked bucket
	err = q.db.Update(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.unackedKey)
		if err != nil {
			return err
		}
		k, v := bucket.Cursor().First()
		if k != nil {
			t.Errorf("expected empty bucket, found key %x", k)
			fmt.Println(string(v))
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	m, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := m.Ack(); err != nil {
			t.Fatal(err)
		}
	}()

	if got, want := string(m.Body), "foo"; got != want {
		t.Errorf("bad body: got %q, want %q", got, want)
	}
}

type mockSeq struct {
	id uint64
}

func (m *mockSeq) NextSequence() (ID, error) {
	defer func() { m.id++ }()
	return Uint64ID(m.id), nil
}

func TestSequencer(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	q, err := NewQ(q.db, "testing", WithSequencer(&mockSeq{}))
	if err != nil {
		t.Fatal(err)
	}

	if err := q.Send([]byte("foo")); err != nil {
		t.Fatal(err)
	}

	msg, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	want, err := Uint64ID(0).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	if got := msg.ID; !bytes.Equal(got, want) {
		t.Errorf("bad ID: got %v, want %v", got, want)
	}
}

func TestDeadLetters(t *testing.T) {
	q, cleanup := newQ(t, WithDeadLetters())
	defer cleanup()

	msg := []byte("foo")
	if err := q.Send(msg); err != nil {
		t.Fatal(err)
	}

	m, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Nack(false); err != nil {
		t.Fatal(err)
	}

	d, err := DeadLetters(q)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m, err = d.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(m.Body, msg) {
		t.Errorf("bad body: %q", string(m.Body))
	}

}

func TestAckConcurrent(t *testing.T) {
	q, cleanup := newQ(t, WithDeadLetters())
	defer cleanup()

	msg := []byte("foo")
	if err := q.Send(msg); err != nil {
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
	if err := q.Send(msg); err != nil {
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

func benchSend(b *testing.B, msgSize int) {
	q, cleanup := newQ(b)
	defer cleanup()
	msg := make([]byte, msgSize)
	for i := 0; i < len(msg); i++ {
		msg[i] = byte(i % 256)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := q.Send(msg); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSend_64(b *testing.B) {
	benchSend(b, 64)
}

func benchRoundtrip(b *testing.B, msgSize int) {
	q, cleanup := newQ(b, WithMessageBufferSize(10))
	defer cleanup()
	msg := make([]byte, msgSize)
	for i := 0; i < len(msg); i++ {
		msg[i] = byte(i % 256)
	}
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := q.Send(msg); err != nil {
				b.Fatal(err)
			}
			msg, err := q.Receive(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if err := msg.Ack(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRoundtrip_64(b *testing.B) {
	benchRoundtrip(b, 64)
}
