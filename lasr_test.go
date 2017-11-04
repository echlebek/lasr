package lasr

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/boltdb/bolt"
)

func TestFifo(t *testing.T) {
	f := newFifo(5)
	for i := 0; i < 5; i++ {
		if got, want := f.Len(), i; got != want {
			t.Errorf("bad count: got %d, want %d", got, want)
		}
		f.Push(&Message{Body: []byte{byte(i)}})
		if got, want := f.Len(), i+1; got != want {
			t.Errorf("bad count: got %d, want %d", got, want)
		}
	}
	for i := 0; i < 5; i++ {
		if got, want := f.Len(), 5-i; got != want {
			t.Errorf("bad count: got %d, want %d", got, want)
		}
		m := f.Pop()
		if got, want := m.Body[0], byte(i); got != want {
			t.Errorf("bad Pop: got %d, want %d", got, want)
		}
		if got, want := f.Len(), 5-i-1; got != want {
			t.Errorf("bad count: got %d, want %d", got, want)
		}
	}
}

func TestFifoPanicOnFull(t *testing.T) {
	f := newFifo(5)
	for i := 0; i < 5; i++ {
		f.Push(nil)
	}
	defer func() {
		if r := recover(); r == nil {
			t.Error("no panic detected")
		}
	}()
	f.Push(nil)
}

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
	q, err := NewQ(db, "testing", options...)
	cleanup := func() {
		defer os.RemoveAll(td)
		if q != nil {
			q.Close()
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
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

	// deadlock unless we clear the wg here
	q.inFlight = sync.WaitGroup{}

	// Make sure the unacked message is in the unacked queue
	err := q.db.Update(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.keys.unacked)
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
		bucket, err := q.bucket(tx, q.keys.unacked)
		if err != nil {
			return err
		}
		k, _ := bucket.Cursor().First()
		if k != nil {
			t.Errorf("expected empty bucket, found key %x", k)
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

func TestClose(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := q.Send(nil), ErrQClosed; got != want {
		t.Errorf("bad error message: got %q, want %q", got, want)
	}
	_, err := q.Receive(nil)
	if got, want := err, ErrQClosed; got != want {
		t.Errorf("bad error message: got %q, want %q", got, want)
	}
	if got, want := q.Close(), ErrQClosed; got != want {
		t.Errorf("bad error message: got %q, want %q", got, want)
	}
	q, cleanup = newQ(t, WithMessageBufferSize(5))
	defer cleanup()
	for i := 0; i < 5; i++ {
		if err := q.Send([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	m, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := q.messages.Len(), 0; got != want {
		t.Errorf("buffer not drained")
	}
	err = q.db.Update(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.keys.unacked)
		if err != nil {
			return err
		}
		if got, want := bucket.Stats().KeyN, 0; got != want {
			t.Errorf("%d unacked messages", got)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDelayed(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()
	delayUntil := time.Now().Add(time.Millisecond * 100)
	if err := q.Delay(nil, delayUntil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	msg, err := q.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := msg.Ack(); err != nil {
			t.Fatal(err)
		}
	}()
	if time.Now().UnixNano() < delayUntil.UnixNano() {
		t.Errorf("delayed message arrived too early")
	}
}

func BenchmarkSend_64(b *testing.B) {
	benchSend(b, 64)
}

func benchRoundtrip(b *testing.B, msgSize int) {
	q, cleanup := newQ(b)
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
