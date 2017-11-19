package lasr

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/boltdb/bolt"
)

func TestSendReceive(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	msg := []byte("foobar")
	id, err := q.Send(msg)
	if err != nil {
		t.Fatal(err)
	}
	idb, err := id.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	message, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.ID, idb) {
		t.Error("messages do not match")
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
	if _, err := q.Send(msg); err != nil {
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
	id, err := q.Send(msg)
	if err != nil {
		t.Fatal(err)
	}
	idb, err := id.MarshalBinary()
	if err != nil {
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
	if !bytes.Equal(message.ID, idb) {
		t.Errorf("bad id: %v", message.ID)
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
	id, err := q.Send(msg)
	if err != nil {
		t.Fatal(err)
	}
	idb, err := id.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := q.Receive(context.Background()); err != nil {
		t.Fatal(err)
	}

	// deadlock unless we clear the wg here
	q.inFlight = sync.WaitGroup{}

	// Make sure the unacked message is in the unacked queue
	err = q.db.Update(func(tx *bolt.Tx) error {
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

	if got, want := m.ID, idb; !bytes.Equal(got, want) {
		t.Errorf("bad id: got %v, want %v", got, want)
	}
}

func BenchmarkSend_4K(b *testing.B) {
	benchSend(b, 4096)
}

func BenchmarkSend_1M(b *testing.B) {
	benchSend(b, 1<<20)
}

func BenchmarkSend_16M(b *testing.B) {
	benchSend(b, 1<<24)
}

func benchSend(b *testing.B, msgSize int) {
	q, cleanup := newQ(b)
	defer cleanup()
	msg := make([]byte, msgSize)
	for i := 0; i < len(msg); i++ {
		msg[i] = byte(i % 256)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := q.Send(msg); err != nil {
			b.Fatal(err)
		}
	}
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
	for i := 0; i < b.N; i++ {
		if _, err := q.Send(msg); err != nil {
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
}

func BenchmarkRoundtrip_4K(b *testing.B) {
	benchRoundtrip(b, 4096)
}

func BenchmarkRoundTrip_1M(b *testing.B) {
	benchRoundtrip(b, 1<<20)
}

func BenchmarkRoundTrip_16M(b *testing.B) {
	benchRoundtrip(b, 1<<24)
}
