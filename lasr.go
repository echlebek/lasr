package lasr

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"sync"
)

// BoltError wraps bolt errors.
type BoltError error

var (
	emptyQ     = errors.New("empty queue")
	ErrAckNack = errors.New("Ack or Nack already called")
)

// ID is used for bolt keys. Every message will be assigned an ID.
type ID interface {
	encoding.BinaryMarshaler
}

// Uint64ID is the default ID used by lasr.
type Uint64ID uint64

func (id Uint64ID) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 8))
	err := binary.Write(buf, binary.BigEndian, id)
	return buf.Bytes(), err
}

type Status string

const (
	Ready    Status = "Ready"
	Unacked  Status = "Unacked"
	Returned Status = "Returned"
)

type Message struct {
	Body    []byte
	ID      []byte
	status  Status
	q       *Q
	ackNack sync.Once
}

// Sequencer returns an ID with each call to NextSequence and any error
// that occurred.
//
// A Sequencer should obey the following invariants:
//
// * NextSequence is goroutine-safe.
//
// * NextSequence will never generate the same ID.
//
// * NextSequence will return IDs whose big-endian binary representation is incrementing.
//
// Q is not guaranteed to use all of the IDs generated by its Sequencer.
type Sequencer interface {
	NextSequence() (ID, error)
}

// Options can be passed to NewQ.
type Option func(q *Q) error

// WithSequencer will cause a Q to use a user-provided Sequencer.
func WithSequencer(seq Sequencer) Option {
	return func(q *Q) error {
		q.seq = seq
		return nil
	}
}

// Ack acknowledges successful receipt and processing of the Message.
func (m *Message) Ack() (err error) {
	err = ErrAckNack
	m.ackNack.Do(func() {
		err = m.q.ack(m.ID)
	})
	return
}

// Nack negatively acknowledges successful receipt and processing of the
// Message. If Nack is called with retry True, then the Message will be
// placed back in the queue in its original position.
func (m *Message) Nack(retry bool) (err error) {
	err = ErrAckNack
	m.ackNack.Do(func() {
		err = m.q.nack(m.ID, retry)
	})
	return
}
