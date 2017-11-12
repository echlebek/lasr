package lasr

import (
	"testing"
	"time"
)

func TestUint64ID(t *testing.T) {
	want := Uint64ID(time.Now().Unix())
	b, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var got Uint64ID
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("bad Uint64ID: got %d, want %d", got, want)
	}
}
