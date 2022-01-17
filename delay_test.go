package lasr

import (
	"context"
	"testing"
	"time"
)

func TestDelayed(t *testing.T) {
	q := newQ(t)
	delayUntil := time.Now().Add(time.Second)
	_, err := q.Delay([]byte("delayed"), delayUntil)
	if err != nil {
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
	if time.Now().Unix() < delayUntil.Unix() {
		t.Errorf("delayed message arrived too early")
	}
}

func TestWakeAtOnReloadWithDelayed_GH6(t *testing.T) {
	q := newQ(t)

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
	if got, want := q.waker.nextWake.Unix(), nextWake.Unix(); got != want {
		t.Errorf("bad nextWake: got %v, want %v", got, want)
	}
	q.waker.Unlock()
}
