package lasr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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
	closed      chan struct{}
	inFlight    sync.WaitGroup
	waker       *waker
	optsApplied bool
}

type bucketKeys struct {
	ready     []byte
	returned  []byte
	unacked   []byte
	delayed   []byte
	waiting   []byte
	blockedOn []byte
	blocking  []byte
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
	return q.equilibrate()
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
	closed := make(chan struct{})
	q := &Q{
		db:   db,
		name: bName,
		keys: bucketKeys{
			ready:     []byte("ready"),
			unacked:   []byte("unacked"),
			delayed:   []byte("delayed"),
			waiting:   []byte("waiting"),
			blockedOn: []byte("blockedOn"),
			blocking:  []byte("blocking"),
		},
		waker:  newWaker(closed),
		closed: closed,
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
		if readyKeys > 0 && !q.isClosed() {
			q.waker.Wake()
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
	closed := make(chan struct{})
	d := &Q{
		db:   q.db,
		name: q.name,
		seq:  q.seq,
		keys: bucketKeys{
			ready:   q.keys.returned,
			unacked: []byte("deadletters-unacked"),
		},
		waker:  newWaker(closed),
		closed: closed,
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

// stopWaitingOn causes all messages waiting on id to not wait on id.
// If stopWaitingOn finds any messages that were waiting on id that are not
// waiting on any other messages, it will move them to the Ready state.
func (q *Q) stopWaitingOn(tx *bolt.Tx, id []byte) (bool, error) {
	// blocking -> x blocking y
	// blockedOn -> x blocked on y
	wake := false
	blocking, err := q.bucket(tx, q.keys.blocking)
	if err != nil {
		return wake, err
	}
	blocker := blocking.Bucket(id)
	if blocker == nil {
		return wake, nil
	}
	blockedOn, err := q.bucket(tx, q.keys.blockedOn)
	if err != nil {
		return wake, err
	}
	c := blocker.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		blockedMsg := blockedOn.Bucket(k)
		if blockedMsg == nil {
			continue
		}
		if err := blockedMsg.Delete(id); err != nil {
			return wake, err
		}
		c = blockedMsg.Cursor()
		if k, _ := c.First(); k != nil {
			// There are still messages blocking the release of blockedMsg
			continue
		}
		// at this point, nothing is causing 'blockedMsg' to wait anymore.
		// a message will be placed into the ready state
		if err := blockedOn.DeleteBucket(k); err != nil {
			return wake, err
		}
		waiting, err := q.bucket(tx, q.keys.waiting)
		if err != nil {
			return wake, err
		}
		v := waiting.Get(k)
		ready, err := q.bucket(tx, q.keys.ready)
		if err != nil {
			return wake, err
		}
		if err := ready.Put(k, v); err != nil {
			return wake, err
		}
		wake = true
		if err := waiting.Delete(k); err != nil {
			return wake, err
		}
	}
	return wake, blocking.DeleteBucket(id)
}

func (q *Q) ack(id []byte) error {
	var wake bool
	err := q.db.Update(func(tx *bolt.Tx) error {
		var err error
		wake, err = q.stopWaitingOn(tx, id)
		if err != nil {
			return err
		}
		bucket, err := q.bucket(tx, q.keys.unacked)
		if err != nil {
			return err
		}
		return bucket.Delete(id)
	})
	if err == nil {
		q.inFlight.Done()
	}
	if wake && !q.isClosed() {
		q.waker.Wake()
	}
	return err
}

func (q *Q) nack(id []byte, retry bool) error {
	var wake bool
	err := q.db.Update(func(tx *bolt.Tx) (rerr error) {
		bucket, err := q.bucket(tx, q.keys.unacked)
		if err != nil {
			return err
		}
		if retry {
			wake = true
			val := bucket.Get(id)
			ready, err := q.bucket(tx, q.keys.ready)
			if err != nil {
				return err
			}
			return ready.Put(id, val)
		}
		wake, err = q.stopWaitingOn(tx, id)
		if err != nil {
			return err
		}
		if len(q.keys.returned) > 0 {
			val := bucket.Get(id)
			returned, err := q.bucket(tx, q.keys.returned)
			if err != nil {
				return err
			}
			return returned.Put(id, val)
		}
		return bucket.Delete(id)
	})
	if err != nil {
		return err
	}
	q.inFlight.Done()
	if wake && !q.isClosed() {
		q.waker.Wake()
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
func (q *Q) Send(message []byte) (ID, error) {
	if q.isClosed() {
		return nil, ErrQClosed
	}
	var id ID
	err := q.db.Update(func(tx *bolt.Tx) (err error) {
		id, err = q.nextSequence(tx)
		if err != nil {
			return err
		}
		return q.send(id, message, tx)
	})
	if err == nil {
		q.waker.Wake()
	}
	return id, err
}

// Delay is like Send, but the message will not enter the Ready state until
// after "when" has occurred.
//
// If "when" has already occurred, then it will be set to time.Now().
func (q *Q) Delay(message []byte, when time.Time) (ID, error) {
	if when.After(MaxDelayTime) {
		return nil, fmt.Errorf("time out of range: %s", when.Format(time.RFC3339))
	}
	if when.Before(time.Now()) {
		when = time.Now()
	}
	id := Uint64ID(when.UnixNano())
	key, err := id.MarshalBinary()
	if err != nil {
		return nil, err
	}
	err = q.db.Update(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.keys.delayed)
		if err != nil {
			return err
		}
		// Reserve a spot for the message. If its exact time in unix
		// nanoseconds has already been reserved, pick the next spot,
		// ad-infinitum.
		for {
			k, _ := bucket.Cursor().Seek(key)
			if !bytes.Equal(k, key) {
				break
			}
			id++
			key, err = id.MarshalBinary()
			if err != nil {
				return err
			}
		}
		return bucket.Put(key, message)
	})
	if err == nil {
		q.waker.WakeAt(time.Unix(0, int64(id)))
	}
	return id, err
}

// Wait causes a message to wait for other messages to Ack, before entering the
// Ready state.
//
// When all of the messages Wait is waiting on have been Acked, then the message
// will enter the Ready state.
//
// When there are no messages to wait on, Wait behaves the same as Send.
func (q *Q) Wait(msg []byte, on ...ID) (ID, error) {
	if len(on) < 1 {
		return q.Send(msg)
	}
	var id ID
	return id, q.db.Update(func(tx *bolt.Tx) error {
		var err error
		id, err = q.nextSequence(tx)
		if err != nil {
			return err
		}
		idb, err := id.MarshalBinary()
		if err != nil {
			return err
		}
		blockedOn, err := q.bucket(tx, q.keys.blockedOn)
		if err != nil {
			return err
		}
		blockedMsg, err := blockedOn.CreateBucketIfNotExists(idb)
		if err != nil {
			return err
		}
		blocking, err := q.bucket(tx, q.keys.blocking)
		if err != nil {
			return err
		}
		for _, id := range on {
			idc, err := id.MarshalBinary()
			if err != nil {
				return err
			}
			if err := blockedMsg.Put(idc, nil); err != nil {
				return err
			}
			blockerMsg, err := blocking.CreateBucketIfNotExists(idc)
			if err != nil {
				return err
			}
			if err := blockerMsg.Put(idb, nil); err != nil {
				return err
			}
		}
		waiting, err := q.bucket(tx, q.keys.waiting)
		if err != nil {
			return err
		}
		return waiting.Put(idb, msg)
	})
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
			q.inFlight.Add(1)
		}
		return msg, err
	}
	select {
	case <-q.waker.C:
		q.processReceives()
		goto START
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.closed:
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
	var currentTime []byte
	if bytes.Equal(key, q.keys.delayed) {
		// special case for processing delays
		t := Uint64ID(time.Now().UnixNano())
		currentTime, err = t.MarshalBinary()
		if err != nil {
			return err
		}
	}
	for k, v := cur.First(); k != nil && i < q.messages.Cap(); k, v = cur.Next() {
		if currentTime != nil && bytes.Compare(k, currentTime) > 0 {
			return nil
		}
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
		q.waker.Wake()
	}
	return nil
}

func (q *Q) processReceives() {
	q.messages.SetError(q.db.Update(func(tx *bolt.Tx) error {
		// Prioritize delayed messages first. Not all instances of Q will
		// have delayed messages.
		if len(q.keys.delayed) > 0 {
			if err := q.getMessages(tx, q.keys.delayed); err != nil {
				return err
			}
			if q.messages.Len() == q.messages.Cap() {
				return nil
			}
		}
		return q.getMessages(tx, q.keys.ready)
	}))
}

func cloneBytes(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}
