package lasr

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/boltdb/bolt"
)

// Q is a first-in, first-out queue. Its methods are goroutine-safe.
type Q struct {
	db              *bolt.DB
	name            []byte
	root            [][]byte
	seq             Sequencer
	readyKey        []byte
	unackedKey      []byte
	returnedKey     []byte
	messages        *fifo
	messagesBufSize int
	wakeup          chan struct{}
	closed          chan struct{}
	inFlight        sync.WaitGroup
}

// Close closes q. When q is closed, Send, Receive, and Close will return
// ErrQClosed. Close blocks until all messages in the "unacked" state are Acked
// or Nacked.
func (q *Q) Close() error {
	q.messages.Lock()
	defer q.messages.Unlock()
	select {
	case <-q.closed:
		return ErrQClosed
	default:
		close(q.closed)
	}
	q.inFlight.Wait()
	return q.init()
}

func (q *Q) isClosed() bool {
	select {
	case <-q.closed:
		return true
	default:
		return false
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
		db:         db,
		name:       bName,
		readyKey:   []byte("ready"),
		unackedKey: []byte("unacked"),
		wakeup:     make(chan struct{}, 1),
		closed:     make(chan struct{}),
	}
	for _, o := range options {
		if err := o(q); err != nil {
			return nil, fmt.Errorf("lasr: couldn't create Q: %s", err)
		}
	}
	if err := q.init(); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *Q) init() error {
	// for messages to be buffered, need at least size 2. 0 is invalid.
	q.messagesBufSize++
	q.messages = newFifo(q.messagesBufSize)
	return q.db.Update(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.readyKey)
		if err != nil {
			return err
		}
		readyKeys := bucket.Stats().KeyN
		unacked, err := q.bucket(tx, q.unackedKey)
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
			select {
			case q.wakeup <- struct{}{}:
			default:
			}
		}
		root, err := q.rootBucket(tx)
		if err != nil {
			return err
		}
		// Delete the unacked bucket now that the unacked messages have been
		// returned to the ready bucket.
		return root.DeleteBucket(q.unackedKey)
	})
}

// If dead-lettering is enabled on q, DeadLetters will return a dead-letter
// queue that is named the same as q, but will emit dead-letters on Receive.
// The dead-letter queue itself does not support dead-lettering; nacked
// messages that are not retried will be deleted.
//
// If dead-lettering is not enabled on q, an error will be returned.
func DeadLetters(q *Q) (*Q, error) {
	if len(q.returnedKey) == 0 {
		return nil, errors.New("lasr: dead-letters not available")
	}
	d := &Q{
		db:              q.db,
		name:            q.name,
		root:            q.root,
		seq:             q.seq,
		readyKey:        q.returnedKey,
		unackedKey:      []byte("deadletters-unacked"),
		messagesBufSize: q.messagesBufSize,
		wakeup:          make(chan struct{}, 1),
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

func (q *Q) rootBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	var bucket bucketer = tx
	if len(q.root) > 0 {
		var err error
		for _, k := range q.root {
			bucket, err = bucket.CreateBucketIfNotExists(k)
			if err != nil {
				return nil, err
			}
		}
	}
	result, err := bucket.CreateBucketIfNotExists(q.name)

	return result, err
}

func (q *Q) bucket(tx *bolt.Tx, key []byte) (*bolt.Bucket, error) {
	bucket, err := q.rootBucket(tx)
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
		bucket, err := q.bucket(tx, q.unackedKey)
		if err != nil {
			return err
		}
		return bucket.Delete(id)
	})
	if err == nil {
		q.inFlight.Done()
	}
	return err
}

func (q *Q) nack(id []byte, retry bool) error {
	err := q.db.Update(func(tx *bolt.Tx) (rerr error) {
		bucket, err := q.bucket(tx, q.unackedKey)
		if err != nil {
			return err
		}
		val := bucket.Get(id)
		if retry {
			ready, err := q.bucket(tx, q.readyKey)
			if err != nil {
				return err
			}
			return ready.Put(id, val)
		}
		if len(q.returnedKey) > 0 {
			returned, err := q.bucket(tx, q.returnedKey)
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
	q.inFlight.Done()
	if retry && !q.isClosed() {
		select {
		case q.wakeup <- struct{}{}:
		default:
		}
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

// Send sends a message to Q.
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
		select {
		case q.wakeup <- struct{}{}:
		default:
		}
	}
	return err
}

func (q *Q) send(id ID, body []byte, tx *bolt.Tx) error {
	key, err := id.MarshalBinary()
	if err != nil {
		return err
	}

	bucket, err := q.bucket(tx, q.readyKey)
	if err != nil {
		return err
	}

	if err := bucket.Put(key, body); err != nil {
		return err
	}
	return nil
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
			q.inFlight.Add(1)
		}
		return msg, nil
	}
	select {
	case <-q.wakeup:
		q.processReceives()
		goto START
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.closed:
		return nil, ErrQClosed
	}
}

func (q *Q) processReceives() {
	var body, id []byte
	messages := make([]*Message, 0, q.messagesBufSize)
	err := q.db.Update(func(tx *bolt.Tx) error {
		ready, err := q.bucket(tx, q.readyKey)
		if err != nil {
			return err
		}
		cur := ready.Cursor()
		i := 0
		for k, v := cur.First(); k != nil && i < q.messagesBufSize; k, v = cur.Next() {
			id = cloneBytes(k)
			body = cloneBytes(v)
			unacked, err := q.bucket(tx, q.unackedKey)
			if err != nil {
				return err
			}
			if err := unacked.Put(k, v); err != nil {
				return err
			}
			if err := ready.Delete(k); err != nil {
				return err
			}
			messages = append(messages, &Message{
				Body: body,
				ID:   id,
				q:    q,
			})
			i++
		}
		if i >= q.messagesBufSize {
			// More work could be available
			select {
			case q.wakeup <- struct{}{}:
			default:
			}
		}
		return nil
	})
	for _, m := range messages {
		m.err = err
		q.messages.Push(m)
	}
}

func cloneBytes(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}
