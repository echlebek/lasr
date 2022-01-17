package lasr

import (
	"fmt"
)

// Options can be passed to NewQ.
type Option func(q *Q) error

// WithMessageBufferSize sets the message buffer size. By default, the message
// buffer size is 0. Values less than 0 are not allowed.
//
// The buffer is used by Receive to efficiently ready messages for consumption.
// If the buffer is greater than 0, then multiple messages can retrieved in a
// single transaction.
//
// Buffered messages come with a caveat: messages will move into the "unacked"
// state before Receive is called.
//
// Buffered messages come at the cost of increased memory use. If messages are
// large in size, use this cautiously.
func WithMessageBufferSize(size int) Option {
	return func(q *Q) error {
		if q.optsApplied {
			return ErrOptionsApplied
		}
		if size < 0 {
			return fmt.Errorf("lasr: invalid message buffer size: %d", size)
		}
		q.messages = newFifo(size + 1)
		return nil
	}
}
