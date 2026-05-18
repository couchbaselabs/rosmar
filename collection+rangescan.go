// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"context"
	"database/sql"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
)

var _ sgbucket.RangeScanStore = &Collection{}

func (c *Collection) Scan(ctx context.Context, scanType sgbucket.ScanType, opts sgbucket.ScanOptions) (sgbucket.ScanResultIterator, error) {
	rs, ok := scanType.(sgbucket.RangeScan)
	if !ok {
		return nil, fmt.Errorf("unsupported scan type: %T", scanType)
	}
	return c.rangeScan(ctx, rs, opts)
}

func (c *Collection) rangeScan(ctx context.Context, rs sgbucket.RangeScan, opts sgbucket.ScanOptions) (sgbucket.ScanResultIterator, error) {
	var query string
	var args []any

	if opts.IDsOnly {
		query = "SELECT key, cas FROM documents WHERE collection=? AND tombstone=0"
	} else {
		query = "SELECT key, value, cas FROM documents WHERE collection=? AND tombstone=0"
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

	rows, err := c.db().Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("range scan query failed: %w", err)
	}

	iter := &scanIterator{rows: rows, idsOnly: opts.IDsOnly}
	if c.bucket.inMemory {
		// An in-memory database has only a single connection, so leaving sql.Rows open
		// can cause deadlock. Read all rows eagerly and close immediately.
		return preRecordScan(ctx, iter), nil
	}
	return iter, nil
}

// scanIterator wraps sql.Rows for streaming scan results.
type scanIterator struct {
	rows    *sql.Rows
	idsOnly bool
	err     error
}

func (it *scanIterator) Next(_ context.Context) *sgbucket.ScanResultItem {
	if it.err != nil || !it.rows.Next() {
		return nil
	}
	item := &sgbucket.ScanResultItem{}
	if it.idsOnly {
		it.err = it.rows.Scan(&item.ID, &item.Cas)
	} else {
		it.err = it.rows.Scan(&item.ID, &item.Body, &item.Cas)
	}
	if it.err != nil {
		return nil
	}
	return item
}

func (it *scanIterator) Close(_ context.Context) error {
	closeErr := it.rows.Close()
	if it.err == nil {
		it.err = closeErr
	}
	if it.err == nil {
		it.err = it.rows.Err()
	}
	return it.err
}

// preRecordedScanIterator holds all scan results in memory.
type preRecordedScanIterator struct {
	items []*sgbucket.ScanResultItem
	err   error
}

func preRecordScan(ctx context.Context, iter *scanIterator) *preRecordedScanIterator {
	var items []*sgbucket.ScanResultItem
	for {
		item := iter.Next(ctx)
		if item == nil {
			break
		}
		items = append(items, item)
	}
	return &preRecordedScanIterator{
		items: items,
		err:   iter.Close(ctx),
	}
}

func (it *preRecordedScanIterator) Next(_ context.Context) *sgbucket.ScanResultItem {
	if len(it.items) == 0 || it.err != nil {
		return nil
	}
	item := it.items[0]
	it.items = it.items[1:]
	return item
}

func (it *preRecordedScanIterator) Close(_ context.Context) error {
	return it.err
}

var (
	_ sgbucket.ScanResultIterator = (*scanIterator)(nil)
	_ sgbucket.ScanResultIterator = (*preRecordedScanIterator)(nil)
)
