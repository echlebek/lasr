package lasr

import (
	"bytes"
	"context"
	"sync"
	"testing"
)

func TestSendReceive(t *testing.T) {
	q := newQ(t)

	msg := []byte("foobar")
	id, err := q.Send(msg)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := q.db.Query("SELECT id FROM queue")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
	}
	message, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if message.ID != id {
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
	q := newQ(t)

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
	q := newQ(t)

	msg := []byte("foobar")
	_, err := q.Send(msg)
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
	if !bytes.Equal(message.Body, msg) {
		t.Errorf("bad body: %q", string(message.Body))
	}
	if err := message.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestReceiveContextCancelled(t *testing.T) {
	q := newQ(t)

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
	q := newQ(t)

	msg := []byte("foo")
	id, err := q.Send(msg)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := q.Receive(context.Background()); err != nil {
		t.Fatal(err)
	}

	// deadlock unless we clear the wg here
	q.inFlight = sync.WaitGroup{}

	// Make sure the unacked message is in the unacked queue
	row := q.db.QueryRow("SELECT count(*) FROM queue WHERE queue.state = 1;")
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if got, want := count, 1; got != want {
		t.Errorf("bad count: got %d, want %d", got, want)
	}

	// Assume at this point the program crashed and we loaded the db again.
	q, err = NewQ(q.db, "testing")
	if err != nil {
		t.Fatal(err)
	}

	// Make sure all the messages are moved out of the unacked bucket
	row = q.db.QueryRow("SELECT count(id) FROM queue WHERE state = 1;")
	if err := row.Scan(&count); err != nil {
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

	if got, want := m.ID, id; got != want {
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
	q := newQ(b)
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
	q := newQ(b)
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
