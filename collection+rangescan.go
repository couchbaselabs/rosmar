// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"database/sql"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
)

// Ensure Collection implements RangeScanStore.
var _ sgbucket.RangeScanStore = &Collection{}

func (c *Collection) Scan(scanType sgbucket.ScanType, opts sgbucket.ScanOptions) (sgbucket.ScanResultIterator, error) {
	rs, ok := scanType.(sgbucket.RangeScan)
	if !ok {
		return nil, fmt.Errorf("unsupported scan type: %T", scanType)
	}
	return c.rangeScan(rs, opts)
}

func (c *Collection) rangeScan(rs sgbucket.RangeScan, opts sgbucket.ScanOptions) (sgbucket.ScanResultIterator, error) {
	var query string
	var args []any

	if opts.IDsOnly {
		query = "SELECT key, cas FROM documents WHERE collection=? AND value IS NOT NULL"
	} else {
		query = "SELECT key, value, cas FROM documents WHERE collection=? AND value IS NOT NULL"
	}
	args = append(args, c.id)

	if rs.From != nil {
		if rs.From.Exclusive {
			query += " AND key > ?"
		} else {
			query += " AND key >= ?"
		}
		args = append(args, rs.From.Term)
	}
	if rs.To != nil {
		if rs.To.Exclusive {
			query += " AND key < ?"
		} else {
			query += " AND key <= ?"
		}
		args = append(args, rs.To.Term)
	}
	query += " ORDER BY key"

	return c.execScan(query, args, opts.IDsOnly)
}

func (c *Collection) execScan(query string, args []any, idsOnly bool) (sgbucket.ScanResultIterator, error) {
	rows, err := c.db().Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("range scan query failed: %w", err)
	}

	iter := &scanIterator{rows: rows, idsOnly: idsOnly}
	if c.bucket.inMemory {
		// An in-memory database has only a single connection, so leaving sql.Rows open
		// can cause deadlock. Read all rows eagerly and close immediately.
		return preRecordScan(iter), nil
	}
	return iter, nil
}

// scanIterator wraps sql.Rows for streaming scan results.
type scanIterator struct {
	rows    *sql.Rows
	idsOnly bool
}

func (it *scanIterator) Next() *sgbucket.ScanResultItem {
	if !it.rows.Next() {
		return nil
	}
	item := &sgbucket.ScanResultItem{IDOnly: it.idsOnly}
	var err error
	if it.idsOnly {
		err = it.rows.Scan(&item.ID, &item.Cas)
	} else {
		err = it.rows.Scan(&item.ID, &item.Body, &item.Cas)
	}
	if err != nil {
		return nil
	}
	return item
}

func (it *scanIterator) Close() error {
	return it.rows.Close()
}

// preRecordedScanIterator holds all scan results in memory.
type preRecordedScanIterator struct {
	items []*sgbucket.ScanResultItem
	err   error
}

func preRecordScan(iter *scanIterator) *preRecordedScanIterator {
	var items []*sgbucket.ScanResultItem
	for {
		item := iter.Next()
		if item == nil {
			break
		}
		items = append(items, item)
	}
	return &preRecordedScanIterator{
		items: items,
		err:   iter.Close(),
	}
}

func (it *preRecordedScanIterator) Next() *sgbucket.ScanResultItem {
	if len(it.items) == 0 || it.err != nil {
		return nil
	}
	item := it.items[0]
	it.items = it.items[1:]
	return item
}

func (it *preRecordedScanIterator) Close() error {
	return it.err
}
