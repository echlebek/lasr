package lasr

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestDeadLetters(t *testing.T) {
	q, cleanup := newQ(t, WithDeadLetters())
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

	if got, want := m.ID, idb; !bytes.Equal(got, want) {
		t.Errorf("bad id: got %v, want %v", got, want)
	}
}
