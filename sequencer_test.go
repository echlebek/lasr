package lasr

import (
	"bytes"
	"context"
	"testing"
)

type mockSeq struct {
	id uint64
}

func (m *mockSeq) NextSequence() (ID, error) {
	defer func() { m.id++ }()
	return Uint64ID(m.id), nil
}

func TestSequencer(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	q, err := NewQ(q.db, "testing", WithSequencer(&mockSeq{}))
	if err != nil {
		t.Fatal(err)
	}

	id, err := q.Send([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	idb, err := id.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	msg, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	want, err := Uint64ID(0).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	if got := msg.ID; !bytes.Equal(got, want) {
		t.Errorf("bad ID: got %v, want %v", got, want)
	}
	if got, want := msg.ID, idb; !bytes.Equal(got, want) {
		t.Errorf("bad ID: got %v, want %v", got, want)
	}
}
