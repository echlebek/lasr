package lasr

import (
	"testing"
	"time"
)

func TestWake(t *testing.T) {
	done := make(chan struct{})
	defer func() {
		close(done)
	}()
	w := newWaker(done)
	go func() {
		w.Wake()
	}()
	<-w.C
}

func TestWakeAt(t *testing.T) {
	done := make(chan struct{})
	defer func() {
		close(done)
	}()
	w := newWaker(done)
	now := time.Now()
	w.WakeAt(now.Add(time.Second * 5)) // will never happen
	w.WakeAt(now.Add(100 * time.Millisecond))
	<-w.C
	if got, want := time.Since(now), 100*time.Millisecond; got < want {
		t.Errorf("didn't wait long enough: %d < %d", got, want)
	} else if got > time.Second {
		t.Errorf("waited too long: %d > %d", got, want)
	}
}
