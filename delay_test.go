package lasr

import (
	"context"
	"testing"
	"time"
)

func TestDelayed(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()
	delayUntil := time.Now().Add(time.Millisecond * 100)
	id, err := q.Delay(nil, delayUntil)
	if err != nil {
		t.Fatal(err)
	}
	tm := int64(id.(Uint64ID))
	if tm, now := time.Unix(0, tm).Round(time.Hour), time.Now().Round(time.Hour); !tm.Equal(now) {
		t.Errorf("bad id: %v", tm)
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

func TestWakeAtOnReloadWithDelayed_GH6(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	nextWake := time.Now().Add(time.Hour)

	_, err := q.Delay([]byte("foo"), nextWake)
	if err != nil {
		t.Fatal(err)
	}

	_, err = q.Delay([]byte("bar"), time.Now().Add(time.Hour*2))
	if err != nil {
		t.Fatal(err)
	}

	q, err = NewQ(q.db, "testing")
	if err != nil {
		t.Fatal(err)
	}

	q.waker.Lock()
	if got, want := q.waker.nextWake, nextWake; !got.Equal(want) {
		t.Errorf("bad nextWake: got %v, want %v", got, want)
	}
	q.waker.Unlock()
}
