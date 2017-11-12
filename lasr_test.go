package lasr

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/boltdb/bolt"
)

type fataler interface {
	Fatal(...interface{})
}

func newQ(t fataler, options ...Option) (*Q, func()) {
	td, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	fp := filepath.Join(td, "lasr.db")
	db, err := bolt.Open(fp, 0600, nil)
	if err != nil {
		defer os.RemoveAll(td)
		t.Fatal(err)
	}
	q, err := NewQ(db, "testing", options...)
	cleanup := func() {
		defer os.RemoveAll(td)
		if q != nil {
			q.Close()
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err != nil {
		defer cleanup()
		t.Fatal(err)
	}
	return q, cleanup
}

func TestNewQ(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()

	if q.db == nil {
		t.Error("nil db")
	}

	if got, want := string(q.name), "testing"; got != want {
		t.Errorf("bad queue name: got %q, want %q", got, want)
	}
}

func TestClose(t *testing.T) {
	q, cleanup := newQ(t)
	defer cleanup()
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
	q, cleanup = newQ(t, WithMessageBufferSize(5))
	defer cleanup()
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
	err = q.db.Update(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.keys.unacked)
		if err != nil {
			return err
		}
		if got, want := bucket.Stats().KeyN, 0; got != want {
			t.Errorf("%d unacked messages", got)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
