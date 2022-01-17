package lasr

import (
	"context"
	"testing"
	"time"
)

func TestWait(t *testing.T) {
	q := newQ(t)
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
	row := q.db.QueryRow("SELECT count(*) FROM wait")
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("bad count: got %d, want 0", count)
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
	if got, want := doneWaiting.ID, waiting; got != want {
		t.Errorf("wanted ID %x last, but got %x", got, want)
	}
}
