package lasr

import (
	"fmt"
	"sync"
	"time"

	"database/sql"

	_ "modernc.org/sqlite"
)

// Q is a persistent message queue. Its methods are goroutine-safe.
// Q retains the data that is sent to it until messages are acked (or nacked
// without retry)
type Q struct {
	db          *sql.DB
	name        string
	messages    *fifo
	closed      chan struct{}
	inFlight    sync.WaitGroup
	waker       *waker
	mu          sync.Mutex
	optsApplied bool
	deadLetters bool
}

type State int8

const (
	Ready    State = 0
	Unacked  State = 1
	Returned State = 2
)

// Close closes q. When q is closed, Send, Receive, and Close will return
// ErrQClosed. Close blocks until all messages in the "unacked" state are Acked
// or Nacked.
func (q *Q) Close() error {
	q.messages.Lock()
	defer q.messages.Unlock()
	err := q.equilibrate()
	select {
	case <-q.closed:
		return ErrQClosed
	default:
		close(q.closed)
	}
	q.inFlight.Wait()
	return err
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
	return fmt.Sprintf("Q{Name: %q}", q.name)
}

// NewQ creates a new Q. Multiple named queues can be created in a single
// sqlite database, but they will share tables.
func NewQ(db *sql.DB, name string, options ...Option) (*Q, error) {
	closed := make(chan struct{})
	q := &Q{
		db:     db,
		name:   name,
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
	_, err := q.db.Exec(schemaSQL)
	if err != nil {
		return err
	}
	return q.equilibrate()
}

func (q *Q) equilibrate() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	row := q.db.QueryRow(equilibrateSQL)
	var count int
	if err := row.Scan(&count); err != nil && err != sql.ErrNoRows {
		return err
	}
	if count > 0 {
		q.waker.Wake()
	}
	q.messages.Drain()
	rows, err := q.db.Query(getDelayedSQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	var ts int64
	for rows.Next() {
		if err := rows.Scan(&ts); err != nil {
			return err
		}
		if !q.isClosed() {
			q.waker.WakeAt(time.Unix(ts, 0))
		}
	}
	return nil
}
