// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
)

//////// Interface QueryableStore

func (c *Collection) CanQueryIn(language sgbucket.QueryLanguage) bool {
	return language == sgbucket.SQLiteLanguage
}

// Implementation of Query. Supports `SQLiteLanguage` only, of course.
// Use `$_keyspace` to refer to the collection in the `FROM` clause.
// The table columns are `id` (document ID or key), `body`, `xattrs`.
// You can use SQLite's `->` and `->>` syntax to refer to JSON properties in body and xattrs.
// The `consistency` and `adhoc` parameters are currently ignored.
func (c *Collection) Query(
	language sgbucket.QueryLanguage,
	statement string,
	args map[string]any,
	consistency sgbucket.ConsistencyMode,
	adhoc bool,
) (iter sgbucket.QueryResultIterator, err error) {
	traceEnter("Query", "%q, query: %s)", language, statement)
	defer func() { traceExit("Query", err, "iterator") }()

	if language != sgbucket.SQLiteLanguage {
		err = fmt.Errorf("unsupported query language")
		return
	}

	// Replace `$_keyspace` with a sub-query matching documents in this collection:
	statement, sqlArgs := c.prepareQuery(statement, args)
	// TODO: Save a sql.Stmt if adhoc==false

	rows, err := c.db().Query(statement, sqlArgs...)
	if err != nil {
		err = fmt.Errorf("SQLite query failed: %w", err)
		return
	}

	it := &queryIterator{rows: rows}
	if c.bucket.inMemory {
		// An in-memory database has only a single connection, so it's dangerous to leave the
		// sql.Rows object open -- any other database call will block until it's closed. This
		// can easily lead to deadlock. As a workaround, read all the rows now, close the
		// Rows object, and return an iterator over the in-memory rows.
		iter = preRecord(it)
	} else {
		iter = it
	}
	return
}

// Implementation of CreateIndex.
// The table columns are `id` (document ID or key), `body`, `xattrs`.
// You can use SQLite's `->` and `->>` syntax to refer to JSON properties in body and xattrs.
func (c *Collection) CreateIndex(indexName string, expression string, filterExpression string) (err error) {
	traceEnter("CreateIndex", "%q, %q, %q)", indexName, expression, filterExpression)
	defer func() { traceExit("CreateIndex", err, "ok") }()

	// Rename the `id` and `body` columns to their actual names in the schema:
	re := regexp.MustCompile(`\bid\b`)
	expression = re.ReplaceAllString(expression, `key`)
	filterExpression = re.ReplaceAllString(filterExpression, `key`)
	re = regexp.MustCompile(`\bbody\b`)
	expression = re.ReplaceAllString(expression, `value`)
	filterExpression = re.ReplaceAllString(filterExpression, `value`)

	// Note that we prepend `id` to the index columns, since the Query interface always
	// evaluates a SELECT statement that matches the `id` with the specified Collection.
	stmt := fmt.Sprintf(`CREATE INDEX %s ON documents (id, %s) WHERE value NOT NULL`,
		indexName, expression)
	if filterExpression != "" {
		stmt += ` AND ` + filterExpression
	}
	_, err = c.db().Exec(stmt)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		err = sgbucket.ErrIndexExists
	}
	return err
}

// Returns a description of the query plan, created by SQLITE's `EXPLAIN QUERY PLAN`.
// The format of the result is: a single key "plan" whose value is an array of steps.
// Each step is an array with two integers (node id and parent id) and a string description.
// Example: `{"plan":[[3,0,"SEARCH documents USING INDEX docs_cas (collection=?)"]]}`
// For details, see https://sqlite.org/eqp.html
func (c *Collection) ExplainQuery(statement string, args map[string]any) (plan map[string]any, err error) {
	statement, sqlArgs := c.prepareQuery(statement, args)
	rows, err := c.db().Query(`EXPLAIN QUERY PLAN `+statement, sqlArgs...)
	if err != nil {
		return
	}
	result := []any{}
	for rows.Next() {
		var node, parent, unused int
		var description string
		if err = rows.Scan(&node, &parent, &unused, &description); err != nil {
			return
		}
		result = append(result, []any{node, parent, description})
	}
	if err = rows.Close(); err != nil {
		return
	}
	return map[string]any{"plan": result}, nil
}

func (c *Collection) prepareQuery(statement string, args map[string]any) (string, []any) {
	// Replace `$_keyspace` with a sub-query matching documents in this collection:
	statement = strings.Replace(statement, sgbucket.KeyspaceQueryToken, "_keyspace", -1)
	statement = fmt.Sprintf(`WITH _keyspace as (SELECT key as id, value as body, xattrs
							 FROM documents WHERE collection=%d AND value NOT NULL) %s`,
		c.id, statement)
	// Convert the args to an array of sql.NamedArg values:
	sqlArgs := make([]any, 0, len(args))
	for key, arg := range args {
		sqlArgs = append(sqlArgs, sql.Named(key, arg))
	}
	return statement, sqlArgs
}

//////// Interface QueryResultIterator:

type queryIterator struct {
	rows          *sql.Rows
	columnNames   [][]byte
	columnVals    []sql.NullString
	columnValPtrs []any
	err           error
}

// returns the next row as a JSON string.
func (iter *queryIterator) NextBytes() []byte {
	if iter.err != nil {
		return nil
	}
	if !iter.rows.Next() {
		return nil
	}

	// Initialize column arrays:
	if iter.columnVals == nil {
		var colNames []string
		if colNames, iter.err = iter.rows.Columns(); iter.err != nil {
			return nil
		}
		// Convert each column name to a JSON string, including quotes:
		nCols := len(colNames)
		iter.columnNames = make([][]byte, nCols)
		for i, name := range colNames {
			iter.columnNames[i], _ = json.Marshal(name)
		}
		// Create array of column values, and the pointers thereto:
		iter.columnVals = make([]sql.NullString, nCols)
		iter.columnValPtrs = make([]any, nCols)
		for i := range iter.columnNames {
			iter.columnValPtrs[i] = &iter.columnVals[i]
		}
	}

	// Read the row's columns into i.columnVals:
	iter.err = iter.rows.Scan(iter.columnValPtrs...)
	if iter.err != nil {
		return nil
	}

	// Generate JSON. Each column value must be a JSON string.
	row := bytes.Buffer{}
	row.WriteByte('{')
	first := true
	for i, val := range iter.columnVals {
		if val.Valid {
			if first {
				first = false
			} else {
				row.WriteByte(',')
			}
			row.Write(iter.columnNames[i])
			row.WriteByte(':')
			row.WriteString(val.String)
		}
	}
	row.WriteByte('}')
	return row.Bytes()
}

// unmarshals the query row into the pointed-to struct.
func (iter *queryIterator) Next(_ context.Context, valuePtr any) bool {
	bytes := iter.NextBytes()
	if bytes == nil {
		return false
	}
	if err := json.Unmarshal(bytes, valuePtr); err != nil {
		iter.err = fmt.Errorf("failed to unmarshal query result: %w; raw result is: %s", err, bytes)
		return false
	}
	return true
}

func (iter *queryIterator) One(ctx context.Context, valuePtr any) error {
	if !iter.Next(ctx, valuePtr) {
		iter.err = sgbucket.ErrNoRows
	}
	return iter.Close()
}

func (iter *queryIterator) Close() error {
	if iter.rows != nil {
		if closeErr := iter.rows.Close(); closeErr != nil && iter.err == nil {
			iter.err = closeErr
		}
		iter.rows = nil
	}
	return iter.err
}

//////// Pre-recorded query iterator:

type preRecordedQueryIterator struct {
	rows [][]byte
	err  error
}

func preRecord(iter *queryIterator) *preRecordedQueryIterator {
	var rows [][]byte
	for {
		if row := iter.NextBytes(); row != nil {
			rows = append(rows, row)
		} else {
			break
		}
	}
	return &preRecordedQueryIterator{
		rows: rows,
		err:  iter.Close(),
	}
}

func (iter *preRecordedQueryIterator) NextBytes() []byte {
	if len(iter.rows) == 0 || iter.err != nil {
		return nil
	}
	result := iter.rows[0]
	iter.rows = iter.rows[1:]
	return result
}

func (iter *preRecordedQueryIterator) Next(_ context.Context, valuePtr any) bool {
	bytes := iter.NextBytes()
	if bytes == nil {
		return false
	}
	if err := json.Unmarshal(bytes, valuePtr); err != nil {
		iter.err = fmt.Errorf("failed to unmarshal query result: %w; raw result is: %s", err, bytes)
		return false
	}
	return true
}

func (iter *preRecordedQueryIterator) One(ctx context.Context, valuePtr any) error {
	if !iter.Next(ctx, valuePtr) {
		iter.err = sgbucket.ErrNoRows
	}
	return iter.Close()
}

func (iter *preRecordedQueryIterator) Close() error {
	iter.rows = nil
	return iter.err
}

var (
	// Enforce interface conformance:
	_ sgbucket.QueryableStore      = &Collection{}
	_ sgbucket.QueryResultIterator = &queryIterator{}
	_ sgbucket.QueryResultIterator = &preRecordedQueryIterator{}
)
