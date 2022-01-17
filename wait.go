package lasr

// Wait causes a message to wait for other messages to Ack, before entering the
// Ready state.
//
// When all of the messages Wait is waiting on have been Acked, then the message
// will enter the Ready state.
//
// When there are no messages to wait on, Wait behaves the same as Send.
func (q *Q) Wait(msg []byte, on ...ID) (id ID, err error) {
	if len(on) < 1 {
		return q.Send(msg)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	tx, terr := q.db.Begin()
	if terr != nil {
		return 0, terr
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()
	row := tx.QueryRow(sendSQL, q.name, msg)
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	for _, waitee := range on {
		if _, err := tx.Exec(waitOnSQL, id, waitee); err != nil {
			return 0, err
		}
	}
	return id, nil
}
