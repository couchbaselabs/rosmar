// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"errors"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDDocs(t *testing.T) {
	ensureNoLeakedFeeds(t)

	ddoc := sgbucket.DesignDoc{Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.key) emit(doc.key,doc.value)}`}}}
	coll := makeTestBucket(t).DefaultDataStore().(*Collection)
	err := coll.PutDDoc("docname", &ddoc)
	assert.NoError(t, err, "PutDDoc docnamefailed")
	err = coll.PutDDoc("docname2", &ddoc)
	assert.NoError(t, err, "PutDDoc docname2failed")

	ddocs, getErr := coll.GetDDocs()
	assert.NoError(t, getErr, "GetDDocs failed")
	assert.Equal(t, len(ddocs), 2)
}

// Create a simple view and run it on some documents
func TestView(t *testing.T) {
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore().(*Collection)

	ddoc := sgbucket.DesignDoc{Language: "javascript", Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.key) emit(doc.key,doc.value)}`}}}
	err := coll.PutDDoc("docname", &ddoc)
	assert.NoError(t, err, "PutDDoc failed")

	var echo sgbucket.DesignDoc
	echo, err = coll.GetDDoc("docname")
	require.NoError(t, err, "GetDDoc failed")
	assert.Equal(t, ddoc, echo)

	require.NoError(t, setJSON(coll, "doc1", `{"key": "k1", "value": "v1"}`))
	require.NoError(t, setJSON(coll, "doc2", `{"key": "k2", "value": "v2"}`))
	require.NoError(t, setJSON(coll, "doc3", `{"key": 17, "value": ["v3"]}`))
	require.NoError(t, setJSON(coll, "doc4", `{"key": [17, false], "value": null}`))
	require.NoError(t, setJSON(coll, "doc5", `{"key": [17, true], "value": null}`))

	// raw docs and counters should not be indexed by views
	_, err = coll.AddRaw("rawdoc", 0, []byte("this is raw data"))
	require.NoError(t, err)
	_, err = coll.Incr("counter", 1, 0, 0)
	require.NoError(t, err)

	options := map[string]interface{}{"stale": false}
	result, err := coll.View("docname", "view1", options)
	assert.NoError(t, err, "View call failed")
	require.Equal(t, 5, result.TotalRows)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc3", Key: 17.0, Value: []interface{}{"v3"}}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "k1", Value: "v1"}, result.Rows[1])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2"}, result.Rows[2])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc4", Key: []interface{}{17.0, false}}, result.Rows[3])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc5", Key: []interface{}{17.0, true}}, result.Rows[4])

	// Try a startkey:
	options["startkey"] = "k2"
	options["include_docs"] = true
	result, err = coll.View("docname", "view1", options)
	assert.NoError(t, err, "View call failed")
	assert.Equal(t, 3, result.TotalRows)
	var expectedDoc interface{} = map[string]interface{}{"key": "k2", "value": "v2"}
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc}, result.Rows[0])

	// Try descending:
	options["descending"] = true
	result, err = coll.View("docname", "view1", options)
	assert.NoError(t, err, "View call failed")
	assert.Equal(t, 3, result.TotalRows)
	assert.Equal(t, "doc2", result.Rows[0].ID)
	assert.Equal(t, "doc1", result.Rows[1].ID)
	assert.Equal(t, "doc3", result.Rows[2].ID)
	delete(options, "descending")

	// Try an endkey:
	options["endkey"] = "k2"
	result, err = coll.View("docname", "view1", options)
	assert.NoError(t, err, "View call failed")
	assert.Equal(t, 1, result.TotalRows)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc}, result.Rows[0])

	// Try an endkey out of range:
	options["endkey"] = "k999"
	result, err = coll.View("docname", "view1", options)
	assert.NoError(t, err, "View call failed")
	assert.Equal(t, 1, result.TotalRows)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc}, result.Rows[0])

	// Try without inclusive_end:
	options["endkey"] = "k2"
	options["inclusive_end"] = false
	result, err = coll.View("docname", "view1", options)
	assert.NoError(t, err, "View call failed")
	assert.Equal(t, 0, result.TotalRows)

	// Try a single key:
	options = map[string]interface{}{"stale": false, "key": "k2", "include_docs": true}
	result, err = coll.View("docname", "view1", options)
	assert.NoError(t, err, "View call failed")
	assert.Equal(t, 1, result.TotalRows)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc}, result.Rows[0])

	// Delete the design doc:
	assert.NoError(t, coll.DeleteDDoc("docname"), "DeleteDDoc")
	_, getErr := coll.GetDDoc("docname")
	assert.True(t, errors.Is(getErr, sgbucket.MissingError{Key: "docname"}))
}
