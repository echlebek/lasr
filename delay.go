package lasr

import (
	"fmt"
	"time"
)

var (
	// MaxDelayTime is the maximum time that can be passed to Q.Delay().
	MaxDelayTime = time.Unix(0, (1<<63)-1)
)

// Delay is like Send, but the message will not enter the Ready state until
// after "when" has occurred.
//
// If "when" has already occurred, then it will be set to time.Now().
func (q *Q) Delay(message []byte, when time.Time) (ID, error) {
	q.waker.WakeAt(when)
	if when.After(MaxDelayTime) {
		return 0, fmt.Errorf("time out of range: %s", when.Format(time.RFC3339))
	}
	if when.Before(time.Now()) {
		when = time.Now()
	}
	state := when.Unix()
	row := q.db.QueryRow(delaySQL, q.name, message, state)
	var id ID
	err := row.Scan(&id)
	return id, err
}
