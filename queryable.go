package rosmar

import "database/sql"

// common interface of sql.DB, sql.Tx
type queryable interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// Implements `queryable` but its methods just return ErrBucketClosed.
type closedDB struct{}

func (db closedDB) Exec(query string, args ...any) (sql.Result, error) {
	return nil, ErrBucketClosed
}

func (db closedDB) Query(query string, args ...any) (*sql.Rows, error) {
	return nil, ErrBucketClosed
}

func (db closedDB) QueryRow(query string, args ...any) *sql.Row {
	// FIX! I can't return a sql.Row whose Scan() method will return ErrBucketClosed, because
	// sql.Row is a struct with private fields and there's no way to create one.
	panic("Attempt to use a closed Rosmar bucket")
}

var (
	// Enforce interface conformance:
	_ queryable = closedDB{}
)
