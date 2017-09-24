package lasr

import (
	"context"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
)

// Q is a first-in, first-out queue. Its methods are goroutine-safe.
type Q struct {
	db          *bolt.DB
	name        []byte
	root        [][]byte
	seq         Sequencer
	tokens      chan struct{}
	readyKey    []byte
	unackedKey  []byte
	returnedKey []byte
}

func (q *Q) String() string {
	return fmt.Sprintf("Q{Name: %q, Messages: %d}", string(q.name), len(q.tokens))
}

func (q *Q) nextSequence() (ID, error) {
	if q.seq != nil {
		return q.seq.NextSequence()
	}
	return q.nextUint64ID()
}

func newTokens() chan struct{} {
	return make(chan struct{}, int(^uint(0)>>1)) // max int
}

// NewQ creates a new Q.
func NewQ(db *bolt.DB, name string, options ...Option) (*Q, error) {
	bName := []byte(name)
	q := &Q{
		db:         db,
		name:       bName,
		tokens:     newTokens(),
		readyKey:   []byte("ready"),
		unackedKey: []byte("unacked"),
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
		for i := 0; i < readyKeys; i++ {
			q.tokens <- struct{}{}
		}
		return nil
	})
}

// If Q has dead-lettering enabled, DeadLetters will return a dead letters
// queue that is the same as Q, but will emit dead letters on Receive.
// The dead letter queue itself does not support dead lettering; nacked
// messages that are not retried will be deleted.
//
// If dead-lettering  is not enabled on q, an error will be returned.
func DeadLetters(q *Q) (*Q, error) {
	if len(q.returnedKey) == 0 {
		return nil, errors.New("dead letters not available")
	}
	d := &Q{
		db:         q.db,
		name:       q.name,
		root:       q.root,
		seq:        q.seq,
		tokens:     newTokens(),
		readyKey:   q.returnedKey,
		unackedKey: []byte("deadletters-unacked"),
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

	return result, BoltError(err)
}

func (q *Q) bucket(tx *bolt.Tx, key []byte) (*bolt.Bucket, error) {
	bucket, err := q.rootBucket(tx)
	if err != nil {
		return nil, BoltError(err)
	}
	bucket, err = bucket.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, BoltError(err)
	}
	return bucket, nil
}

func (q *Q) ack(id []byte) error {
	return q.db.Batch(func(tx *bolt.Tx) error {
		bucket, err := q.bucket(tx, q.unackedKey)
		if err != nil {
			return err
		}
		return bucket.Delete(id)
	})
}

func (q *Q) nack(id []byte, retry bool) error {
	return q.db.Update(func(tx *bolt.Tx) (rerr error) {
		bucket, err := q.bucket(tx, q.unackedKey)
		if err != nil {
			return err
		}
		val := bucket.Get(id)
		if retry {
			defer func() {
				if rerr == nil {
					q.tokens <- struct{}{}
				}
			}()
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
}

func (q *Q) nextUint64ID() (Uint64ID, error) {
	var seq uint64
	var err error

	err = q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(q.name)
		seq, err = bucket.NextSequence()
		return err
	})

	if err != nil {
		return Uint64ID(0), BoltError(err)
	}

	return Uint64ID(seq), nil
}

// Send sends a message to Q.
func (q *Q) Send(message []byte) error {
	id, err := q.nextSequence()
	if err != nil {
		return err
	}
	return q.db.Update(func(tx *bolt.Tx) (err error) {
		defer func() {
			if err == nil {
				q.tokens <- struct{}{}
			}
		}()
		return q.send(id, message, tx)
	})
}

func (q *Q) send(id ID, body []byte, tx *bolt.Tx) error {
	key, err := id.MarshalBinary()
	if err != nil {
		return err
	}

	bucket, err := q.bucket(tx, q.readyKey)
	if err != nil {
		return BoltError(err)
	}

	if err := bucket.Put(key, body); err != nil {
		return BoltError(err)
	}
	return nil
}

// Receive receives a message from the queue. If no messages are available by
// the time the context is done, then the function will return a nil Message
// and the result of ctx.Err().
func (q *Q) Receive(ctx context.Context) (*Message, error) {
	select {
	case _, ok := <-q.tokens:
		if !ok {
			panic("tokens channel closed")
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var (
		body, id []byte
		err      error
	)
	err = q.db.Update(func(tx *bolt.Tx) (err error) {
		defer func() {
			if err != nil {
				// return the token
				q.tokens <- struct{}{}
			}
		}()

		ready, err := q.bucket(tx, q.readyKey)
		if err != nil {
			return err
		}
		cur := ready.Cursor()
		k, v := cur.First()
		if k == nil {
			return emptyQ
		}
		id = make([]byte, len(k))
		copy(id, k)
		body = make([]byte, len(v))
		copy(body, v)
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
		return nil
	})
	if err == emptyQ {
		panic("queue is empty and out of sync")
	}
	if err != nil {
		return nil, err
	}
	return &Message{Body: body, ID: id, status: Ready, q: q}, nil
}
