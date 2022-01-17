package lasr

import (
	"context"
	"database/sql"
	"testing"
)

type fataler interface {
	Fatal(...interface{})
}

func newQ(t fataler, options ...Option) *Q {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewQ(db, "testing", options...)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestNewQ(t *testing.T) {
	q := newQ(t)

	if q.db == nil {
		t.Error("nil db")
	}

	if got, want := string(q.name), "testing"; got != want {
		t.Errorf("bad queue name: got %q, want %q", got, want)
	}
}

func TestClose(t *testing.T) {
	q := newQ(t)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := q.Send(nil)
	if got, want := err, ErrQClosed; got != want {
		t.Errorf("bad error message: got %q, want %q", got, want)
	}
	_, err = q.Receive(nil)
	if got, want := err, ErrQClosed; got != want {
		t.Errorf("bad error message: got %q, want %q", got, want)
	}
	if got, want := q.Close(), ErrQClosed; got != want {
		t.Errorf("bad error message: got %q, want %q", got, want)
	}
	q = newQ(t, WithMessageBufferSize(5))
	for i := 0; i < 5; i++ {
		if _, err := q.Send([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	m, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := q.messages.Len(), 0; got != want {
		t.Errorf("buffer not drained")
	}
	row := q.db.QueryRow("SELECT count(*) FROM queue WHERE state = 1;")
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count > 0 {
		t.Errorf("bad count: got %d, want 0", count)
	}
}
