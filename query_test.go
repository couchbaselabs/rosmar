// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"encoding/json"
	"log"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	ctx := testCtx(t)
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore().(*Collection)
	require.NoError(t, setJSON(coll, "doc1", `{"key": "k1", "value": "v1"}`))
	require.NoError(t, setJSON(coll, "doc2", `{"key": "k2", "value": "v2"}`))
	require.NoError(t, setJSON(coll, "doc3", `{"key": 17, "value": ["v3"]}`))
	require.NoError(t, setJSON(coll, "doc4", `{"key": [17, false], "value": null}`))
	require.NoError(t, setJSON(coll, "doc5", `{"key": [17, true], "value": null}`))

	// Add a matching doc to a different collection, to make sure the query won't return it:
	coll2, err := coll.bucket.NamedDataStore(sgbucket.DataStoreNameImpl{Scope: "foo", Collection: "bar"})
	require.NoError(t, err)
	require.NoError(t, setJSON(coll2, "doc1", `{"key": "k1", "value": "v1"}`))

	require.True(t, coll.CanQueryIn(sgbucket.SQLiteLanguage))
	require.False(t, coll.CanQueryIn(sgbucket.SQLppLanguage))

	rows, err := coll.Query(
		sgbucket.SQLiteLanguage,
		`SELECT json_quote(id) as id, json_quote(body->'key') as key FROM $_keyspace
		 WHERE body->>'value' IS NOT NULL ORDER BY id`,
		nil,
		sgbucket.RequestPlus,
		false)
	require.NoError(t, err)
	require.NotNil(t, rows, "rows")

	expectedDocs := []string{"doc1", "doc2", "doc3"}
	expectedKeys := []any{"k1", "k2", 17}

	n := 0
	var row map[string]any
	for rows.Next(ctx, &row) {
		log.Printf("Row = %+v", row)
		require.Less(t, n, 3)
		assert.EqualValues(t, expectedDocs[n], row["id"])
		assert.EqualValues(t, expectedKeys[n], row["key"])
		assert.Equal(t, 2, len(row))
		n++
	}
	assert.NoError(t, rows.Close(), "rows.Close")
	assert.Equal(t, 3, n)
}

func TestCreateIndex(t *testing.T) {
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore().(*Collection)

	err := coll.CreateIndex("myIndex", "body->>'location'", "body->>'location' NOT NULL")
	require.NoError(t, err)
	err = coll.CreateIndex("myIndex", "body->>'location'", "body->>'location' NOT NULL")
	assert.Equal(t, err, sgbucket.ErrIndexExists)

	exp, err := coll.ExplainQuery(`SELECT id, body->>'location' FROM $_keyspace WHERE body->>'location' > 100`, nil)
	require.NoError(t, err)
	require.NotNil(t, exp)

	j, _ := json.Marshal(exp)
	log.Printf("Plan: %s", j)

	plan, ok := exp["plan"].([]any)
	require.True(t, ok, "getting 'plan' key")
	for _, step := range plan {
		items, ok := step.([]any)
		require.True(t, ok, "getting step items")
		log.Printf("%2d  %2d  %s", items[0], items[1], items[2])
	}
}
