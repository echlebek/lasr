package lasr

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

// int64Fifo is an on-disk fifo.
//
// The fifo is implemented as two stacks, so there are two files for each fifo.
type int64Fifo struct {
	inbox      *os.File
	inboxMu    sync.Mutex
	outbox     *os.File
	outboxMu   sync.Mutex
	outboxSize int64
}

func newInt64Fifo(inbox, outbox string) (*int64Fifo, error) {
	in, err := os.OpenFile(inbox, os.O_CREATE|os.O_RDWR, 0700)
	if err != nil {
		return nil, err
	}
	out, err := os.OpenFile(outbox, os.O_CREATE|os.O_RDWR, 0700)
	if err != nil {
		return nil, err
	}
	return &int64Fifo{
		inbox:  in,
		outbox: out,
	}, nil
}

func (f *int64Fifo) fillOutbox() (err error) {
	// n.b. fillOutbox assumes that the outbox mutex is locked elsewhere.
	f.inboxMu.Lock()
	defer f.inboxMu.Unlock()
	defer func() {
		inSyncErr := f.inbox.Sync()
		if err == nil {
			err = inSyncErr
		}
		outSyncErr := f.outbox.Sync()
		if err == nil {
			err = outSyncErr
		}
	}()
	if _, err := f.inbox.Seek(0, 0); err != nil {
		return err
	}
	in, err := ioutil.ReadAll(f.inbox)
	if len(in)%8 != 0 {
		return fmt.Errorf("input is not a multiple of 8: %d bytes read", len(in))
	}
	reverseInt64s(in)
	if _, err := f.outbox.Seek(0, 0); err != nil {
		return err
	}
	n, err := f.outbox.Write(in)
	if err != nil {
		if err == io.ErrShortWrite {
			_, _ = f.outbox.Seek(-int64(n), 1)
		}
		return err
	}
	f.outboxSize = int64(n)
	if err := f.inbox.Truncate(0); err != nil {
		return err
	}
	_, err = f.inbox.Seek(0, 0)
	return err
}

func (f *int64Fifo) push(i int64) (err error) {
	f.inboxMu.Lock()
	defer f.inboxMu.Unlock()
	defer func() {
		syncErr := f.inbox.Sync()
		if err == nil {
			err = syncErr
		}
	}()
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	if err := binary.Write(buf, binary.LittleEndian, i); err != nil {
		return err
	}
	n, err := f.inbox.Write(buf.Bytes())
	if err != nil {
		if n > 0 {
			_, _ = f.inbox.Seek(-int64(n), 1)
		}
	}
	return err
}

func (f *int64Fifo) pop() (result int64, err error) {
	f.outboxMu.Lock()
	defer f.outboxMu.Unlock()
	defer func() {
		syncErr := f.outbox.Sync()
		if err == nil {
			err = syncErr
		}
	}()
	buf := make([]byte, 8)
	if f.outboxSize == 0 {
		err := f.fillOutbox()
		if err != nil {
			return result, err
		}
	}
	if _, err := f.outbox.Seek(-8, 2); err != nil {
		return 0, err
	}
	if _, err := f.outbox.Read(buf); err != nil {
		return 0, err
	}
	if err = f.outbox.Truncate(f.outboxSize - 8); err != nil {
		return 0, err
	}
	f.outboxSize -= 8
	err = binary.Read(bytes.NewReader(buf), binary.LittleEndian, &result)
	return result, err
}

func reverseInt64s(b []byte) {
	if len(b)%8 != 0 {
		panic("corrupted file")
	}
	tmp := make([]byte, 8)
	for i := 0; i < len(b)/2; i += 8 {
		copy(tmp, b[i:i+8])
		copy(b[i:i+8], b[len(b)-i-8:len(b)-i])
		copy(b[len(b)-i-8:len(b)-i], tmp)
	}
}
