package lasr

import (
	"context"
	"fmt"
	"testing"
)

func TestCompactDB(t *testing.T) {
	q, _ := newQ(t)
	// defer cleanup()
	// TODO(eric): make cleanup work with Compact
	for i := 0; i < 1000; i++ {
		if _, err := q.Send([]byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 600; i++ {
		if _, err := q.Receive(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	oldStats := q.db.Stats()
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	if msg, err := q.Receive(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(msg.Body) == 0 {
		t.Fatal("message length zero")
	}
	newStats := q.db.Stats()
	oldPages := oldStats.TxStats.PageCount
	newPages := newStats.TxStats.PageCount
	if oldPages <= newPages {
		t.Fatalf("expected fewer pages in new db (%d old, %d new)", oldPages, newPages)
	}
}
