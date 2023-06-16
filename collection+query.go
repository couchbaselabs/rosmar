package rosmar

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
)

//////// Interface Queryable

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
	if err == nil {
		iter = &queryIterator{rows: rows}
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
	columnNames   []string
	columnVals    []any
	columnValPtrs []any
	row           map[string]any
	err           error
}

// Returns the next query row as a map, or nil at the end or on error.
func (iter *queryIterator) next() map[string]any {
	if !iter.rows.Next() {
		return nil
	}

	// Initialize column arrays:
	if iter.columnVals == nil {
		if iter.columnNames, iter.err = iter.rows.Columns(); iter.err != nil {
			return nil
		}
		nCols := len(iter.columnNames)
		iter.columnVals = make([]any, nCols)
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

	// Convert to map:
	if iter.row == nil {
		iter.row = make(map[string]any, len(iter.columnNames))
	}
	for i, val := range iter.columnVals {
		iter.row[iter.columnNames[i]] = val
	}
	return iter.row
}

func (iter *queryIterator) NextBytes() []byte {
	if result := iter.next(); result != nil {
		bytes, _ := json.Marshal(result)
		return bytes
	} else {
		return nil
	}
}

func (iter *queryIterator) Next(valuePtr any) bool {
	switch val := valuePtr.(type) {
	case *map[string]any:
		// If caller wants a map, I can give that directly:
		result := iter.next()
		if result == nil {
			return false
		}
		*val = result
		iter.row = nil // so next() will not reuse the map I'm giving out
		return true
	default:
		// Otherwise marshal the map to JSON, then unmarshal into their result:
		bytes := iter.NextBytes()
		if bytes == nil {
			return false
		}
		iter.err = json.Unmarshal(bytes, valuePtr)
		return iter.err != nil
	}
}

func (iter *queryIterator) One(valuePtr any) error {
	if !iter.Next(valuePtr) {
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

var (
	// Enforce interface conformance:
	_ sgbucket.QueryableStore      = &Collection{}
	_ sgbucket.QueryResultIterator = &queryIterator{}
)
