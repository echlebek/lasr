package lasr

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/boltdb/bolt"
)

type fataler interface {
	Fatal(...interface{})
}

func newQ(t fataler) (*Q, func()) {
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
	q, err := NewQ(db, "testing")
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

func TestSendReeiveNackWithRetry(t *testing.T) {
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

func benchReceive(b *testing.B, msgSize int) {
	q, cleanup := newQ(b)
	defer cleanup()
	msg := make([]byte, msgSize)
	for i := 0; i < len(msg); i++ {
		msg[i] = byte(i % 256)
	}
	// make a huge queue
	for i := 0; i < 4000; i++ {
		if err := q.Send(msg); err != nil {
			b.Fatal(err)
		}
	}
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := q.Receive(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkReceive_64(b *testing.B) {
	benchReceive(b, 64)
}
