package lasr

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestInt64Fifo(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	inbox := filepath.Join(dir, "inbox")
	outbox := filepath.Join(dir, "outbox")
	fifo, err := newInt64Fifo(inbox, outbox)
	if err != nil {
		t.Fatal(err)
	}
	var result []int64
	for i := 0; i < 10; i++ {
		if err := fifo.push(int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		j, err := fifo.pop()
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, j)
		if err := fifo.push(int64(i) + 10); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		j, err := fifo.pop()
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, j)
	}
	for i := 0; i < 20; i++ {
		if result[i] != int64(i) {
			t.Errorf("bad result: got %d, want %d", result[i], i)
		}
	}
}

func BenchmarkInt64FifoPush(b *testing.B) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	inbox := filepath.Join(dir, "inbox")
	outbox := filepath.Join(dir, "outbox")
	fifo, err := newInt64Fifo(inbox, outbox)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fifo.push(int64(i))
	}
}

func BenchmarkInt64FifoPushPop(b *testing.B) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	inbox := filepath.Join(dir, "inbox")
	outbox := filepath.Join(dir, "outbox")
	fifo, err := newInt64Fifo(inbox, outbox)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fifo.push(int64(i))
		if i > 100000 {
			_, _ = fifo.pop()
		}
	}
}
