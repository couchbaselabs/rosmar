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
	return nil
}

// Wrapper around sql.Row.Scan() that handles a nil Row as returned by closedDB.
func scan(row *sql.Row, vals ...any) error {
	if row != nil {
		return row.Scan(vals...)
	} else {
		return ErrBucketClosed
	}
}

var (
	// Enforce interface conformance:
	_ queryable = closedDB{}
)
