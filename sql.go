package lasr

const schemaSQL = `
-- lol that you have to turn foreign keys _on_
PRAGMA foreign_keys = ON;

-- we'd like WAL enabled for faster writes
PRAGMA journal_mode='wal';

PRAGMA busy_timeout = 10000;

-- queue is the primary storage for queue messages
CREATE TABLE IF NOT EXISTS queue (
	id    INTEGER PRIMARY KEY,        -- pk
	name  TEXT NOT NULL,              -- queue name
	state INTEGER NOT NULL DEFAULT 0, -- message state / hold timestamp
	data  BLOB NOT NULL               -- message value
);

-- wait tracks messages that are waiting on other messages' completion
CREATE TABLE IF NOT EXISTS wait (
	waiter INTEGER REFERENCES queue ( id ) ON DELETE CASCADE, -- message that is waiting
	waitee INTEGER REFERENCES queue ( id ) ON DELETE CASCADE  -- message being waited on
);

-- stats tracks statistics about the queue's operation
CREATE TABLE IF NOT EXISTS stats (
	queue       TEXT NOT NULL,
	acked       INTEGER NOT NULL DEFAULT 0,
	nacked      INTEGER NOT NULL DEFAULT 0,
	returned    INTEGER NOT NULL DEFAULT 0,
	total_bytes REAL NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS name_idx ON queue ( name );
CREATE INDEX IF NOT EXISTS state_idx ON queue ( state );
CREATE INDEX IF NOT EXISTS waiter_idx ON wait ( waiter );
CREATE INDEX IF NOT EXISTS waitee_idx ON wait ( waitee );
`

const sendSQL = `
INSERT INTO queue ( name, data, state )
VALUES ( ?, ?, 0 )
RETURNING id;
`

const delaySQL = `
INSERT INTO queue ( name, data, state )
VALUES ( ?, ?, ? )
RETURNING queue.id;
`

const recvSQL = `
WITH selected AS (
	SELECT queue.id AS id FROM queue
	WHERE id NOT IN ( SELECT wait.waiter AS id FROM wait )
	AND (
		state = 0 OR ( state > 16 AND state <= CAST(strftime('%s', 'now') AS INT))
	)
	ORDER BY id
	LIMIT ?
)
UPDATE queue
SET state = 1
FROM selected
WHERE queue.id = selected.id
RETURNING queue.id, queue.data;
`

const equilibrateSQL = `
UPDATE queue
SET state = 0
WHERE state = 1
RETURNING changes();
`

const getDelayedSQL = `
SELECT DISTINCT state
FROM queue
WHERE state > 16
ORDER BY state;
`

const ackSQL = `
DELETE FROM queue
WHERE id = ?;
`

const nackRetrySQL = `
INSERT INTO queue (name, data, state)
SELECT 
	queue.name AS name,
	queue.data AS data,
	0 AS state
FROM queue
WHERE id = ?1
LIMIT 1;
DELETE FROM queue
WHERE id = ?1;
`

const waitOnSQL = `
INSERT INTO wait (waiter, waitee)
VALUES (?, ?)
ON CONFLICT DO NOTHING;
`
