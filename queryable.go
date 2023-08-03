// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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
