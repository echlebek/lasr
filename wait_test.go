package lasr

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestWait(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()
	var ids []ID
	msg := []byte("wayne brady")
	for i := 0; i < 10; i++ {
		id, err := q.Send(msg)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	waiting, err := q.Wait(msg, ids...)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 11; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		msg, err := q.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := msg.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	doneWaiting, err := q.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := doneWaiting.Ack(); err != nil {
			t.Fatal(err)
		}
	}()
	want, err := waiting.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if got := doneWaiting.ID; !bytes.Equal(got, want) {
		t.Errorf("wanted ID %x last, but got %x", got, want)
	}
}
