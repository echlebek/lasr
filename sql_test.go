package lasr

import (
	"database/sql"
	"testing"
)

func TestSQLSchema(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("ATTACH ':memory:' AS lasr;"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		t.Fatal(err)
	}
}

func TestSQLQueries(t *testing.T) {
	tests := map[string]string{
		"sendSQL":        sendSQL,
		"delaySQL":       delaySQL,
		"recvSQL":        recvSQL,
		"equilibrateSQL": equilibrateSQL,
		"getDelayedSQL":  getDelayedSQL,
		"ackSQL":         ackSQL,
		"nackRetrySQL":   nackRetrySQL,
		"waitOnSQL":      waitOnSQL,
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("ATTACH ':memory:' AS lasr;"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		t.Fatal(err)
	}
	for testname, testSQL := range tests {
		t.Run(testname, func(t *testing.T) {
			if _, err := db.Prepare(testSQL); err != nil {
				t.Fatal(err)
			}
		})
	}
}
