package lasr

import "testing"

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
