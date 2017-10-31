package lasr

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/boltdb/bolt"
)

// Q is a persistent message queue. Its methods are goroutine-safe.
// Q retains the data that is sent to it until messages are acked (or nacked
// without retry)
type Q struct {
	db          *bolt.DB
	name        []byte
	seq         Sequencer
	keys        bucketKeys
	messages    *fifo
	sync        synch
	optsApplied bool
}

type bucketKeys struct {
	ready    []byte
	returned []byte
	unacked  []byte
}

type synch struct {
	wakeup   chan struct{}
	closed   chan struct{}
	inFlight sync.WaitGroup
}

// Close closes q. When q is closed, Send, Receive, and Close will return
// ErrQClosed. Close blocks until all messages in the "unacked" state are Acked
// or Nacked.
func (q *Q) Close() error {
	q.messages.Lock()
	defer q.messages.Unlock()
	select {
	case <-q.sync.closed:
		return ErrQClosed
	default:
		close(q.sync.closed)
	}
	q.sync.inFlight.Wait()
	return q.equilibrate()
}

func (q *Q) isClosed() bool {
	select {
	case <-q.sync.closed:
		return true
	default:
		return false
	}
}

// wake wakes up q for I/O
func (q *Q) wake() {
	select {
	case q.sync.wakeup <- struct{}{}:
	default:
	}
}

func (q *Q) String() string {
	return fmt.Sprintf("Q{Name: %q}", string(q.name))
}

func (q *Q) nextSequence(tx *bolt.Tx) (ID, error) {
	if q.seq != nil {
		return q.seq.NextSequence()
	}
	return q.nextUint64ID(tx)
}

// NewQ creates a new Q.
func NewQ(db *bolt.DB, name string, options ...Option) (*Q, error) {
	bName := []byte(name)
	q := &Q{
		db:   db,
		name: bName,
		keys: bucketKeys{
			ready:   []byte("ready"),
			unacked: []byte("unacked"),
		}, sync: synch{
			wakeup: make(chan struct{}, 1),
			closed: make(chan struct{}),
		},
	}
	for _, o := range options {
		if err := o(q); err != nil {
			return nil, fmt.Errorf("lasr: couldn't create Q: %s", err)
		}
	}
	q.optsApplied = true
	if err := q.init(); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *Q) init() error {
	if q.messages == nil {
		q.messages = newFifo(1)
	}
	return q.equilibrate()
}

func (q *Q) equilibrate() error {
	return q.db.Update(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.keys.ready)
		if err != nil {
			return err
		}
		readyKeys := bucket.Stats().KeyN
		unacked, err := q.bucket(tx, q.keys.unacked)
		if err != nil {
			return err
		}
		cursor := unacked.Cursor()
		// put unacked messages from previous session back in the queue
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if err := bucket.Put(k, v); err != nil {
				return err
			}
			readyKeys++
		}
		if readyKeys > 0 {
			q.wake()
		}
		q.messages.Drain()
		root, err := tx.CreateBucketIfNotExists(q.name)
		if err != nil {
			return err
		}
		// Delete the unacked bucket now that the unacked messages have been
		// returned to the ready bucket.
		return root.DeleteBucket(q.keys.unacked)
	})
}

// If dead-lettering is enabled on q, DeadLetters will return a dead-letter
// queue that is named the same as q, but will emit dead-letters on Receive.
// The dead-letter queue itself does not support dead-lettering; nacked
// messages that are not retried will be deleted.
//
// If dead-lettering is not enabled on q, an error will be returned.
func DeadLetters(q *Q) (*Q, error) {
	if len(q.keys.returned) == 0 {
		return nil, errors.New("lasr: dead-letters not available")
	}
	d := &Q{
		db:   q.db,
		name: q.name,
		seq:  q.seq,
		keys: bucketKeys{
			ready:   q.keys.returned,
			unacked: []byte("deadletters-unacked"),
		},
		sync: synch{
			wakeup: make(chan struct{}, 1),
			closed: make(chan struct{}),
		},
	}
	if err := d.init(); err != nil {
		return nil, err
	}
	return d, nil
}

type bucketer interface {
	CreateBucketIfNotExists([]byte) (*bolt.Bucket, error)
	Bucket([]byte) *bolt.Bucket
}

func (q *Q) bucket(tx *bolt.Tx, key []byte) (*bolt.Bucket, error) {
	bucket, err := tx.CreateBucketIfNotExists(q.name)
	if err != nil {
		return nil, err
	}
	bucket, err = bucket.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func (q *Q) ack(id []byte) error {
	err := q.db.Batch(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.keys.unacked)
		if err != nil {
			return err
		}
		return bucket.Delete(id)
	})
	if err == nil {
		q.sync.inFlight.Done()
	}
	return err
}

func (q *Q) nack(id []byte, retry bool) error {
	err := q.db.Update(func(tx *bolt.Tx) (rerr error) {
		bucket, err := q.bucket(tx, q.keys.unacked)
		if err != nil {
			return err
		}
		val := bucket.Get(id)
		if retry {
			ready, err := q.bucket(tx, q.keys.ready)
			if err != nil {
				return err
			}
			return ready.Put(id, val)
		}
		if len(q.keys.returned) > 0 {
			returned, err := q.bucket(tx, q.keys.returned)
			if err != nil {
				return err
			}
			return returned.Put(id, val)
		}
		return nil
	})
	if err != nil {
		return err
	}
	q.sync.inFlight.Done()
	if retry && !q.isClosed() {
		q.wake()
	}
	return nil
}

func (q *Q) nextUint64ID(tx *bolt.Tx) (Uint64ID, error) {
	bucket := tx.Bucket(q.name)
	seq, err := bucket.NextSequence()

	if err != nil {
		return Uint64ID(0), err
	}

	return Uint64ID(seq), nil
}

// Send sends a message to Q. When send completes with nil error, the message
// sent to Q will be in the Ready state.
func (q *Q) Send(message []byte) error {
	if q.isClosed() {
		return ErrQClosed
	}
	err := q.db.Update(func(tx *bolt.Tx) (err error) {
		id, err := q.nextSequence(tx)
		if err != nil {
			return err
		}
		return q.send(id, message, tx)
	})
	if err == nil {
		q.wake()
	}
	return err
}

func (q *Q) send(id ID, body []byte, tx *bolt.Tx) error {
	key, err := id.MarshalBinary()
	if err != nil {
		return err
	}

	bucket, err := q.bucket(tx, q.keys.ready)
	if err != nil {
		return err
	}

	return bucket.Put(key, body)
}

// Receive receives a message from the queue. If no messages are available by
// the time the context is done, then the function will return a nil Message
// and the result of ctx.Err().
func (q *Q) Receive(ctx context.Context) (*Message, error) {
	if q.isClosed() {
		return nil, ErrQClosed
	}
	q.messages.Lock()
	defer q.messages.Unlock()
START:
	if q.messages.Len() > 0 {
		msg := q.messages.Pop()
		err := msg.err
		if err != nil {
			msg = nil
		} else {
			q.sync.inFlight.Add(1)
		}
		return msg, err
	}
	select {
	case <-q.sync.wakeup:
		q.processReceives()
		goto START
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.sync.closed:
		return nil, ErrQClosed
	}
}

func (q *Q) getMessages(tx *bolt.Tx, key []byte) error {
	bucket, err := q.bucket(tx, key)
	if err != nil {
		return err
	}
	cur := bucket.Cursor()
	i := q.messages.Len()
	for k, v := cur.First(); k != nil && i < q.messages.Cap(); k, v = cur.Next() {
		id := cloneBytes(k)
		body := cloneBytes(v)
		unacked, err := q.bucket(tx, q.keys.unacked)
		if err != nil {
			return err
		}
		if err := unacked.Put(k, v); err != nil {
			return err
		}
		if err := bucket.Delete(k); err != nil {
			return err
		}
		q.messages.Push(&Message{
			Body: body,
			ID:   id,
			q:    q,
		})
		i++
	}
	if i >= q.messages.Cap() {
		// More work could be available
		q.wake()
	}
	return nil
}

func (q *Q) processReceives() {
	q.messages.SetError(q.db.Update(func(tx *bolt.Tx) error {
		return q.getMessages(tx, q.keys.ready)
	}))
}

func cloneBytes(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}
