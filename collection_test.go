//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rosmar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const syncXattrName = "_sync" // name of xattr used for sync gateway metadata

func TestDeleteThenAdd(t *testing.T) {
	ensureNoLeaks(t)
	coll := makeTestBucket(t).DefaultDataStore()

	var value interface{}
	_, err := coll.Get("key", &value)
	assert.Equal(t, sgbucket.MissingError{Key: "key"}, err)
	addToCollection(t, coll, "key", 0, "value")
	_, err = coll.Get("key", &value)
	assert.NoError(t, err, "Get")
	assert.Equal(t, "value", value)
	assert.NoError(t, coll.Delete("key"), "Delete")
	_, err = coll.Get("key", &value)
	assert.Equal(t, sgbucket.MissingError{Key: "key"}, err)
	addToCollection(t, coll, "key", 0, "value")
}

func TestIncr(t *testing.T) {
	ensureNoLeaks(t)
	coll := makeTestBucket(t).DefaultDataStore()
	count, err := coll.Incr("count1", 1, 100, 0)
	assert.NoError(t, err, "Incr")
	assert.Equal(t, uint64(100), count)

	count, err = coll.Incr("count1", 0, 0, 0)
	assert.NoError(t, err, "Incr")
	assert.Equal(t, uint64(100), count)

	count, err = coll.Incr("count1", 10, 100, 0)
	assert.NoError(t, err, "Incr")
	assert.Equal(t, uint64(110), count)

	count, err = coll.Incr("count1", 0, 0, 0)
	assert.NoError(t, err, "Incr")
	assert.Equal(t, uint64(110), count)
}

// Spawns 1000 goroutines that 'simultaneously' use Incr to increment the same counter by 1.
func TestIncrAtomic(t *testing.T) {
	ensureNoLeaks(t)
	coll := makeTestBucket(t).DefaultDataStore()
	var waiters sync.WaitGroup
	numIncrements := 5
	waiters.Add(numIncrements)
	for i := uint64(1); i <= uint64(numIncrements); i++ {
		numToAdd := i // lock down the value for the goroutine
		go func() {
			_, err := coll.Incr("key", numToAdd, numToAdd, 0)
			assert.NoError(t, err, "Incr")
			waiters.Add(-1)
		}()
	}
	waiters.Wait()
	value, err := coll.Incr("key", 0, 0, 0)
	assert.NoError(t, err, "Incr")
	assert.Equal(t, numIncrements*(numIncrements+1)/2, int(value))
}

func TestAppend(t *testing.T) {
	ensureNoLeaks(t)
	coll := makeTestBucket(t).DefaultDataStore()

	exists, err := coll.Exists("key")
	assert.NoError(t, err)
	assert.False(t, exists)

	_, err = coll.WriteCas("key", 0, 0, []byte(" World"), sgbucket.Append)
	assert.Equal(t, sgbucket.MissingError{Key: "key"}, err)

	err = coll.SetRaw("key", 0, nil, []byte("Hello"))
	assert.NoError(t, err, "SetRaw")
	_, cas, err := coll.GetRaw("key")
	assert.NoError(t, err, "GetRaw")

	_, err = coll.WriteCas("key", 0, cas, []byte(" World"), sgbucket.Append)
	assert.NoError(t, err, "Append")
	value, _, err := coll.GetRaw("key")
	assert.NoError(t, err, "GetRaw")
	assert.Equal(t, []byte("Hello World"), value)
}

func TestGets(t *testing.T) {
	ensureNoLeaks(t)

	coll := makeTestBucket(t).DefaultDataStore()

	// Gets (JSON)
	addToCollection(t, coll, "key", 0, "value")

	var value interface{}
	cas, err := coll.Get("key", &value)
	assert.NoError(t, err, "Gets")
	assert.True(t, cas > 0)
	assert.Equal(t, "value", value)

	// GetsRaw
	err = coll.SetRaw("keyraw", 0, nil, []byte("Hello"))
	assert.NoError(t, err, "SetRaw")

	value, cas, err = coll.GetRaw("keyraw")
	assert.NoError(t, err, "GetsRaw")
	assert.True(t, cas > 0)
	assert.Equal(t, []byte("Hello"), value)
}

func TestParseSubdocPaths(t *testing.T) {
	_, err := parseSubdocPath("")
	assert.Error(t, err)

	path, err := parseSubdocPath("foo")
	assert.NoError(t, err)
	assert.Equal(t, []string{"foo"}, path)

	path, err = parseSubdocPath("foo.bar")
	assert.NoError(t, err)
	assert.Equal(t, []string{"foo", "bar"}, path)

	_, err = parseSubdocPath("foo[5]")
	assert.Error(t, err)
	_, err = parseSubdocPath(`foo\"quoted`)
	assert.Error(t, err)
}

func TestEvalSubdocPaths(t *testing.T) {
	rawJson := `{"one":1, "two":{"etc":2}, "array":[3,4]}`
	var doc map[string]any
	_ = json.Unmarshal([]byte(rawJson), &doc)

	// Valid 1-level paths:
	val, err := evalSubdocPath(doc, []string{"one"})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, val)
	val, err = evalSubdocPath(doc, []string{"two"})
	assert.NoError(t, err)
	assert.EqualValues(t, map[string]any{"etc": 2.0}, val)
	val, err = evalSubdocPath(doc, []string{"array"})
	assert.NoError(t, err)
	assert.EqualValues(t, []any{3.0, 4.0}, val)

	// Valid 2-level path:
	val, err = evalSubdocPath(doc, []string{"two", "etc"})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, val)

	// Missing paths:
	_, err = evalSubdocPath(doc, []string{"xxx"})
	assert.Error(t, err)
	_, err = evalSubdocPath(doc, []string{"two", "xxx", "yyy"})
	assert.Error(t, err)

	// Type mismatches:
	_, err = evalSubdocPath(doc, []string{"one", "xxx"})
	assert.Error(t, err)
	_, err = evalSubdocPath(doc, []string{"array", "xxx"})
	assert.Error(t, err)
}

func initSubDocTest(t *testing.T) (CAS, sgbucket.DataStore) {
	ensureNoLeaks(t)

	coll := makeTestBucket(t).DefaultDataStore()
	require.True(t, coll.IsSupported(sgbucket.BucketStoreFeatureSubdocOperations))

	rawJson := []byte(`{
        "rosmar":{
            "foo":"lol",
            "bar":"baz"}
        }`)

	addToCollection(t, coll, "key", 0, rawJson)

	var fullDoc map[string]any
	cas, err := coll.Get("key", &fullDoc)
	assert.NoError(t, err)
	assert.Greater(t, cas, CAS(0))

	return cas, coll
}

func TestWriteSubDoc(t *testing.T) {
	ctx := testCtx(t)
	initialCas, coll := initSubDocTest(t)

	// update json
	rawJson := []byte(`"was here"`)
	// test update using incorrect cas value
	cas1, err := coll.WriteSubDoc(ctx, "key", "rosmar", 10, rawJson)
	assert.Error(t, err)
	assert.Equal(t, CAS(0), cas1)

	// test update using correct cas value
	cas2, err := coll.WriteSubDoc(ctx, "key", "rosmar", initialCas, rawJson)
	assert.NoError(t, err)
	assert.Greater(t, cas2, initialCas)

	var fullDoc map[string]any
	cas2Get, err := coll.Get("key", &fullDoc)
	assert.NoError(t, err)
	assert.Equal(t, cas2, cas2Get)
	assert.EqualValues(t, map[string]any{"rosmar": "was here"}, fullDoc)

	// test update using 0 cas value
	cas3, err := coll.WriteSubDoc(ctx, "key", "rosmar", 0, rawJson)
	assert.NoError(t, err)
	assert.Greater(t, cas3, cas2)
}

func TestInsertSubDoc(t *testing.T) {
	ctx := testCtx(t)
	initialCas, coll := initSubDocTest(t)

	rosmarMap := map[string]any{"foo": "lol", "bar": "baz"}
	expectedDoc := map[string]any{"rosmar": rosmarMap}

	// test incorrect cas value
	err := coll.SubdocInsert(ctx, "key", "rosmar.kilroy", 10, "was here")
	assert.Error(t, err)

	// test update
	err = coll.SubdocInsert(ctx, "key", "rosmar.kilroy", initialCas, "was here")
	assert.NoError(t, err)

	var fullDoc map[string]any
	cas, err := coll.Get("key", &fullDoc)
	assert.NoError(t, err)
	assert.Greater(t, cas, initialCas)

	rosmarMap["kilroy"] = "was here"
	assert.EqualValues(t, expectedDoc, fullDoc)

	// test failed update:
	err = coll.SubdocInsert(ctx, "key", "rosmar", cas, "wrong")
	assert.Error(t, err)
	err = coll.SubdocInsert(ctx, "key", "rosmar.foo.xxx.yyy", cas, "wrong")
	assert.Error(t, err)
}

func TestWriteCas(t *testing.T) {
	ensureNoLeaks(t)

	coll := makeTestBucket(t).DefaultDataStore()

	// Add with WriteCas - JSON docs
	// Insert
	var obj interface{}
	mustUnmarshal(t, `{"value":"value1"}`, &obj)
	cas, err := coll.WriteCas("key1", 0, 0, obj, 0)
	assert.NoError(t, err, "WriteCas")
	assert.True(t, cas > 0, "Cas value should be greater than zero")

	// Update document with wrong (zero) cas value
	mustUnmarshal(t, `{"value":"value2"}`, &obj)
	newCas, err := coll.WriteCas("key1", 0, 0, obj, 0)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Update document with correct cas value
	mustUnmarshal(t, `{"value":"value2"}`, &obj)
	newCas, err = coll.WriteCas("key1", 0, cas, obj, 0)
	assert.True(t, err == nil, "Valid cas should not have returned error.")
	assert.True(t, cas > 0, "Cas value should be greater than zero")
	assert.True(t, cas != newCas, "Cas value should change on successful update")
	var result interface{}
	getCas, err := coll.Get("key1", &result)
	assert.NoError(t, err, "Get")
	assert.Equal(t, obj, result)
	assert.Equal(t, newCas, getCas)

	// Update document with obsolete case value
	mustUnmarshal(t, `{"value":"value3"}`, &obj)
	newCas, err = coll.WriteCas("key1", 0, cas, obj, 0)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Add with WriteCas - raw docs
	// Insert
	cas, err = coll.WriteCas("keyraw1", 0, 0, []byte("value1"), sgbucket.Raw)
	assert.NoError(t, err, "WriteCas")
	assert.True(t, cas > 0, "Cas value should be greater than zero")

	// Update document with wrong (zero) cas value
	newCas, err = coll.WriteCas("keyraw1", 0, 0, []byte("value2"), sgbucket.Raw)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Update document with correct cas value
	newCas, err = coll.WriteCas("keyraw1", 0, cas, []byte("value2"), sgbucket.Raw)
	assert.True(t, err == nil, "Valid cas should not have returned error.")
	assert.True(t, cas > 0, "Cas value should be greater than zero")
	assert.True(t, cas != newCas, "Cas value should change on successful update")
	value, getCas, err := coll.GetRaw("keyraw1")
	assert.NoError(t, err, "GetRaw")
	assert.Equal(t, []byte("value2"), value)
	assert.Equal(t, newCas, getCas)

	// Update document with obsolete cas value
	newCas, err = coll.WriteCas("keyraw1", 0, cas, []byte("value3"), sgbucket.Raw)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Delete document, attempt to recreate w/ cas set to 0
	err = coll.Delete("keyraw1")
	assert.True(t, err == nil, "Delete failed")
	newCas, err = coll.WriteCas("keyraw1", 0, 0, []byte("resurrectValue"), sgbucket.Raw)
	require.NoError(t, err, "Recreate with cas=0 should succeed.")
	assert.True(t, cas > 0, "Cas value should be greater than zero")
	value, getCas, err = coll.GetRaw("keyraw1")
	assert.NoError(t, err, "GetRaw")
	assert.Equal(t, []byte("resurrectValue"), value)
	assert.Equal(t, newCas, getCas)

}

func TestRemove(t *testing.T) {
	ensureNoLeaks(t)

	coll := makeTestBucket(t).DefaultDataStore()

	// Add with WriteCas - JSON docs
	// Insert
	var obj interface{}
	mustUnmarshal(t, `{"value":"value1"}`, &obj)
	cas, err := coll.WriteCas("key1", 0, 0, obj, 0)
	assert.NoError(t, err, "WriteCas")
	assert.True(t, cas > 0, "Cas value should be greater than zero")

	// Update document with correct cas value
	mustUnmarshal(t, `{"value":"value2"}`, &obj)
	newCas, err := coll.WriteCas("key1", 0, cas, obj, 0)
	assert.True(t, err == nil, "Valid cas should not have returned error.")
	assert.True(t, cas > 0, "Cas value should be greater than zero")
	assert.True(t, cas != newCas, "Cas value should change on successful update")
	var result interface{}
	getCas, err := coll.Get("key1", &result)
	assert.NoError(t, err, "Get")
	assert.Equal(t, obj, result)
	assert.Equal(t, newCas, getCas)

	// Remove document with incorrect cas value
	newCas, err = coll.Remove("key1", cas)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Remove document with correct cas value
	newCas, err = coll.Remove("key1", getCas)
	assert.True(t, err == nil, "Valid cas should not have returned error on remove.")
	assert.True(t, newCas != uint64(0), "Remove should return non-zero cas")
}

// Test read and write of json as []byte
func TestNonRawBytes(t *testing.T) {
	ctx := testCtx(t)
	ensureNoLeakedFeeds(t)

	coll := makeTestBucket(t).DefaultDataStore()

	byteBody := []byte(`{"value":"value1"}`)

	// Add with WriteCas - JSON doc as []byte and *[]byte
	_, err := coll.WriteCas("writeCas1", 0, 0, byteBody, 0)
	assert.NoError(t, err, "WriteCas []byte")
	_, err = coll.WriteCas("writeCas2", 0, 0, &byteBody, 0)
	assert.NoError(t, err, "WriteCas *[]byte")

	// Add with Add - JSON doc as []byte and *[]byte
	addToCollection(t, coll, "add1", 0, byteBody)
	addToCollection(t, coll, "add2", 0, &byteBody)

	// Set - JSON doc as []byte
	// Set - JSON doc as *[]byte
	// Add with Add - JSON doc as []byte and *[]byte
	err = coll.Set("set1", 0, nil, byteBody)
	assert.NoError(t, err, "Set []byte")
	err = coll.Set("set2", 0, nil, &byteBody)
	assert.NoError(t, err, "Set *[]byte")

	keySet := []string{"writeCas1", "writeCas2", "add1", "add2", "set1", "set2"}
	for _, key := range keySet {
		// Verify retrieval as map[string]interface{}
		var result map[string]interface{}
		cas, err := coll.Get(key, &result)
		assert.NoError(t, err, fmt.Sprintf("Error for Get %s", key))
		assert.True(t, cas > 0, fmt.Sprintf("CAS is zero for key: %s", key))
		assert.True(t, result != nil, fmt.Sprintf("result is nil for key: %s", key))
		if result != nil {
			assert.Equal(t, "value1", result["value"])
		}

		// Verify retrieval as *[]byte
		var rawResult []byte
		cas, err = coll.Get(key, &rawResult)
		assert.NoError(t, err, fmt.Sprintf("Error for Get %s", key))
		assert.True(t, cas > 0, fmt.Sprintf("CAS is zero for key: %s", key))
		assert.True(t, result != nil, fmt.Sprintf("result is nil for key: %s", key))
		if result != nil {
			matching := bytes.Compare(rawResult, byteBody)
			assert.Equal(t, 0, matching)
		}
	}

	// Verify values are stored as JSON and can be retrieved via view
	ddoc := sgbucket.DesignDoc{Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.value) emit(doc.key,doc.value)}`}}}
	err = coll.(*Collection).PutDDoc(ctx, "docname", &ddoc)
	assert.NoError(t, err, "PutDDoc failed")

	options := map[string]interface{}{"stale": false}
	result, err := coll.(*Collection).View(ctx, "docname", "view1", options)
	assert.NoError(t, err, "View call failed")
	assert.Equal(t, len(keySet), result.TotalRows)
}

//////// HELPERS:

func mustUnmarshal(t *testing.T, j string, obj any) {
	require.NoError(t, json.Unmarshal([]byte(j), &obj))
}

func setJSON(coll sgbucket.DataStore, docid string, jsonDoc string) error {
	var obj interface{}
	err := json.Unmarshal([]byte(jsonDoc), &obj)
	if err != nil {
		return err
	}
	return coll.Set(docid, 0, nil, obj)
}

func addToCollection(t *testing.T, coll sgbucket.DataStore, key string, exp uint32, value interface{}) {
	added, err := coll.Add(key, exp, value)
	require.NoError(t, err)
	require.True(t, added, "Expected doc to be added")
}

func ensureNoLeaks(t *testing.T) {
	t.Cleanup(func() { assert.Len(t, GetBucketNames(), 0) })
	ensureNoLeakedFeeds(t)
}

func ensureNoLeakedFeeds(t *testing.T) {
	if !assert.Equal(t, int32(0), atomic.LoadInt32(&activeFeedCount), "Previous test left unclosed Tap/DCP feeds") {
		return
	}

	t.Cleanup(func() {
		var count int32
		for i := 0; i < 100; i++ {
			count = atomic.LoadInt32(&activeFeedCount)
			if count == 0 {
				break
			}
			//log.Printf("Still %d feeds active; waiting...", count)
			time.Sleep(10 * time.Millisecond)
		}
		assert.Equal(t, int32(0), count, "Not all feed goroutines finished")
	})
}

func TestNoCasOnResurrection(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore()
	const docID = "doc1"
	const exp = 0
	casOut, err := col.WriteCas(docID, exp, 0, []byte("{}"), sgbucket.Raw)
	require.NoError(t, err)
	require.NotEqual(t, 0, casOut)
	require.NoError(t, col.Delete(docID))

	ressurectedCasOut, err := col.WriteCas(docID, exp, casOut, []byte("{}"), sgbucket.AddOnly)
	require.NoError(t, err)
	require.NotEqual(t, 0, ressurectedCasOut)
}

func TestWriteCasWithXattrExistingXattr(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore()

	const docID = "DocExistsXattrExists"

	val := make(map[string]interface{})
	val["type"] = docID

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 123
	xattrVal["rev"] = "1-1234"

	var exp uint32
	xattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, xattrVal)}
	ctx := testCtx(t)
	cas := uint64(0)
	cas, err := col.WriteWithXattrs(ctx, docID, exp, cas, mustMarshalJSON(t, val), xattrs, nil)
	require.NoError(t, err)

	updatedXattrVal := make(map[string]interface{})
	updatedXattrVal["seq"] = 123
	updatedXattrVal["rev"] = "2-1234"
	newXattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, updatedXattrVal)}

	const deleteBody = true
	// First attempt to update with a bad cas value, and ensure we're getting the expected error
	_, err = col.WriteTombstoneWithXattrs(ctx, docID, exp, uint64(1234), newXattrs, deleteBody, nil)

	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	_, err = col.WriteTombstoneWithXattrs(ctx, docID, exp, cas, newXattrs, deleteBody, nil)
	require.NoError(t, err)

	verifyEmptyBodyAndSyncXattr(t, col.(*Collection), docID)

}

func TestWriteCasWithXattrNoXattr(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore().(*Collection)
	const docID = "DocExistsNoXattr"
	val := make(map[string]interface{})
	val["type"] = docID
	cas, err := col.WriteCas(docID, 0, 0, val, 0)
	require.NoError(t, err)

	updatedXattrVal := make(map[string]interface{})
	updatedXattrVal["seq"] = 123
	updatedXattrVal["rev"] = "2-1234"
	xattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, updatedXattrVal)}
	const deleteBody = true
	ctx := testCtx(t)
	_, err = col.WriteTombstoneWithXattrs(ctx, docID, 0, uint64(1234), xattrs, deleteBody, nil)

	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	_, err = col.WriteTombstoneWithXattrs(ctx, docID, 0, cas, xattrs, deleteBody, nil)
	require.NoError(t, err)
	verifyEmptyBodyAndSyncXattr(t, col, docID)
}

func TestWriteCasWithXattrXattrExistsNoDoc(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore().(*Collection)
	const docID = "XattrExistsNoDoc"

	val := make(map[string]interface{})
	val["type"] = docID

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 456
	xattrVal["rev"] = "1-1234"

	xattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, xattrVal)}
	ctx := testCtx(t)
	// Create w/ XATTR
	cas := uint64(0)
	cas, err := col.WriteWithXattrs(ctx, docID, 0, cas, mustMarshalJSON(t, val), xattrs, nil)
	require.NoError(t, err)

	// Delete the doc body
	cas, err = col.Remove(docID, cas)
	require.NoError(t, err)

	updatedXattrVal := make(map[string]interface{})
	updatedXattrVal["seq"] = 123
	updatedXattrVal["rev"] = "2-1234"
	xattrValBytes, err := json.Marshal(updatedXattrVal)
	require.NoError(t, err)

	updatedXattrs := map[string][]byte{syncXattrName: xattrValBytes}
	// First attempt to update with a bad cas value, and ensure we're getting the expected error
	const deleteBody = false
	_, err = col.WriteTombstoneWithXattrs(ctx, docID, 0, uint64(1234), updatedXattrs, deleteBody, nil)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	_, err = col.WriteTombstoneWithXattrs(ctx, docID, 0, cas, updatedXattrs, deleteBody, nil)
	require.NoError(t, err)
	verifyEmptyBodyAndSyncXattr(t, col, docID)
}

func TestWriteCasWithXattrOnTombstone(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore().(*Collection)
	const docID = "XattrExistsNoDoc"

	val := make(map[string]interface{})
	val["type"] = docID

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 456
	xattrVal["rev"] = "1-1234"

	xattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, xattrVal)}
	ctx := testCtx(t)
	cas, err := col.WriteWithXattrs(ctx, docID, 0, 0, mustMarshalJSON(t, val), xattrs, nil)
	require.NoError(t, err)

	deleteCas, err := col.Remove(docID, cas)
	require.NoError(t, err)
	require.NotEqual(t, cas, deleteCas)

	postDeleteCas, err := col.WriteWithXattrs(ctx, docID, 0, 0, mustMarshalJSON(t, val), xattrs, nil)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})
	require.NotEqual(t, cas, postDeleteCas)
}

func verifyEmptyBodyAndSyncXattr(t *testing.T, store sgbucket.DataStore, key string) {
	xattrKeys := []string{syncXattrName}
	retrievedVal, retrievedXattrs, _, err := store.GetWithXattrs(testCtx(t), key, xattrKeys)

	require.NoError(t, err)
	require.Nil(t, retrievedVal) // require that the doc body is empty
	syncXattrRaw, ok := retrievedXattrs[syncXattrName]
	require.True(t, ok)
	require.Greater(t, len(syncXattrRaw), 0)
}

func TestSetWithMetaNoDocument(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore()
	const docID = "TestSetWithMeta"
	cas2 := CAS(1)
	body := []byte(`{"foo":"bar"}`)
	err := col.(*Collection).setWithMeta(docID, 0, cas2, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.NoError(t, err)

	val, cas, err := col.GetRaw(docID)
	require.NoError(t, err)
	require.Equal(t, cas2, cas)
	require.JSONEq(t, string(body), string(val))
}

func TestSetWithMetaOverwriteJSON(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore()
	docID := t.Name()
	cas1, err := col.WriteCas(docID, 0, 0, []byte("{}"), sgbucket.Raw)
	require.NoError(t, err)
	require.Greater(t, cas1, CAS(0))

	cas2 := CAS(1)
	body := []byte(`{"foo":"bar"}`)
	err = col.(*Collection).setWithMeta(docID, cas1, cas2, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.NoError(t, err)

	val, cas, err := col.GetRaw(docID)
	require.NoError(t, err)
	require.Equal(t, cas2, cas)
	require.JSONEq(t, string(body), string(val))
}

func TestSetWithMetaOverwriteNotJSON(t *testing.T) {
	bucket := makeTestBucket(t)
	col := bucket.DefaultDataStore()
	docID := t.Name()

	events, _ := startFeed(t, bucket)
	cas1, err := col.WriteCas(docID, 0, 0, []byte("{}"), 0)
	require.NoError(t, err)
	require.Greater(t, cas1, CAS(0))

	event1 := <-events
	require.Equal(t, docID, string(event1.Key))
	require.Equal(t, sgbucket.FeedOpMutation, event1.Opcode)
	require.Equal(t, sgbucket.FeedDataTypeJSON, event1.DataType)

	cas2 := CAS(1)
	body := []byte(`ABC`)
	err = col.(*Collection).setWithMeta(docID, cas1, cas2, 0, nil, body, sgbucket.FeedDataTypeRaw)
	require.NoError(t, err)

	val, cas, err := col.GetRaw(docID)
	require.NoError(t, err)
	require.Equal(t, cas2, cas)
	require.Equal(t, body, val)

	event2 := <-events
	require.Equal(t, docID, string(event2.Key))
	require.Equal(t, sgbucket.FeedOpMutation, event2.Opcode)
	require.Equal(t, sgbucket.FeedDataTypeRaw, event2.DataType)
}

func TestSetWithMetaOverwriteTombstone(t *testing.T) {
	bucket := makeTestBucket(t)
	col := bucket.DefaultDataStore()
	docID := t.Name()
	cas1, err := col.WriteCas(docID, 0, 0, []byte("{}"), sgbucket.Raw)
	require.NoError(t, err)
	require.Greater(t, cas1, CAS(0))
	deletedCas, err := col.Remove(docID, cas1)
	require.NoError(t, err)

	cas2 := CAS(1)
	body := []byte(`ABC`)

	// make sure there is a cas check even for tombstone
	err = col.(*Collection).setWithMeta(docID, CAS(0), cas2, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	events, _ := startFeed(t, bucket)

	// cas check even on tombstone
	err = col.(*Collection).setWithMeta(docID, deletedCas, cas2, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.NoError(t, err)

	event := <-events
	require.Equal(t, docID, string(event.Key))
	require.Equal(t, sgbucket.FeedOpMutation, event.Opcode)

	val, cas, err := col.GetRaw(docID)
	require.NoError(t, err)
	require.Equal(t, cas2, cas)
	require.Equal(t, body, val)
}

func TestSetWithMetaCas(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore()
	docID := t.Name()

	body := []byte(`{"foo":"bar"}`)

	badStartingCas := CAS(1234)
	specifiedCas := CAS(1)

	// document doesn't exist, so cas mismatch will occur if CAS != 0
	err := col.(*Collection).setWithMeta(docID, badStartingCas, specifiedCas, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	// document doesn't exist, but CAS 0 will allow writing
	err = col.(*Collection).setWithMeta(docID, CAS(0), specifiedCas, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.NoError(t, err)

	val, cas, err := col.GetRaw(docID)
	require.NoError(t, err)
	require.Equal(t, specifiedCas, cas)
	require.JSONEq(t, string(body), string(val))
}

func TestDeleteWithMeta(t *testing.T) {
	testCases := []struct {
		name     string
		dataType sgbucket.WriteOptions
	}{
		{
			name:     "JSON",
			dataType: 0, // automatically determined
		},
		{
			name:     "Raw",
			dataType: sgbucket.Raw,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			bucket := makeTestBucket(t)
			col := bucket.DefaultDataStore()
			docID := t.Name()

			startingCas, err := col.WriteCas(docID, 0, 0, []byte(`{"foo": "bar"}`), testCase.dataType)
			require.NoError(t, err)
			specifiedCas := CAS(1)

			events, _ := startFeed(t, bucket)

			// pass a bad CAS and document will not delete
			badStartingCas := CAS(1234)
			// document doesn't exist, but CAS 0 will allow writing
			err = col.(*Collection).deleteWithMeta(docID, badStartingCas, specifiedCas, 0, nil)
			require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

			// tombstone with a good cas
			err = col.(*Collection).deleteWithMeta(docID, startingCas, specifiedCas, 0, nil)
			require.NoError(t, err)

			event := <-events
			require.Equal(t, docID, string(event.Key))
			require.Equal(t, sgbucket.FeedOpDeletion, event.Opcode)
			require.Equal(t, sgbucket.FeedDataTypeRaw, event.DataType)

			_, err = col.Get(docID, nil)
			require.ErrorAs(t, err, &sgbucket.MissingError{})
		})
	}
}

func TestDeleteWithMetaXattr(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore().(*Collection)
	docID := t.Name()

	val := make(map[string]interface{})
	val["type"] = docID

	xattrVal := make(map[string][]byte)
	const (
		userXattr      = "userXattr"
		systemXattr    = "_systemXattr"
		systemXattrVal = "bar"
	)
	xattrVal[userXattr] = mustMarshalJSON(t, "foo")
	xattrVal[systemXattr] = mustMarshalJSON(t, systemXattrVal)

	ctx := testCtx(t)
	startingCas, err := col.WriteWithXattrs(ctx, docID, 0, 0, mustMarshalJSON(t, val), xattrVal, nil)
	require.NoError(t, err)

	specifiedCas := CAS(1)
	// pass a bad CAS and document will not delete
	badStartingCas := CAS(1234)
	// document doesn't exist, but CAS 0 will allow writing
	err = col.deleteWithMeta(docID, badStartingCas, specifiedCas, 0, nil)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	// tombstone with a good cas
	err = col.deleteWithMeta(docID, startingCas, specifiedCas, 0, []byte(fmt.Sprintf(fmt.Sprintf(`{"%s": "%s"}`, systemXattr, systemXattrVal))))
	require.NoError(t, err)

	_, err = col.Get(docID, nil)
	require.ErrorAs(t, err, &sgbucket.MissingError{})

	xattrKeys := []string{syncXattrName, userXattr, systemXattr}

	xattrs, tombstoneCas, err := col.GetXattrs(ctx, docID, xattrKeys)
	require.NoError(t, err)
	require.Equal(t, specifiedCas, tombstoneCas)

	require.Contains(t, xattrs, systemXattr)
	require.NotContains(t, xattrs, userXattr)
}

func mustMarshalJSON(t *testing.T, obj any) []byte {
	bytes, err := json.Marshal(obj)
	require.NoError(t, err)
	return bytes
}
