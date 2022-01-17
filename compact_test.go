package lasr

import (
	"context"
	"fmt"
	"testing"
)

func TestCompactDB(t *testing.T) {
	q := newQ(t)
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
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
}
