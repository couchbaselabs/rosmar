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

func TestDeleteThenAdd(t *testing.T) {
	ensureNoLeakedFeeds(t)
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
	ensureNoLeakedFeeds(t)
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
	ensureNoLeakedFeeds(t)
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
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore()

	exists, err := coll.Exists("key")
	assert.NoError(t, err)
	assert.False(t, exists)

	_, err = coll.WriteCas("key", 0, 0, 0, []byte(" World"), sgbucket.Append)
	assert.Equal(t, sgbucket.MissingError{Key: "key"}, err)

	err = coll.SetRaw("key", 0, nil, []byte("Hello"))
	assert.NoError(t, err, "SetRaw")
	_, cas, err := coll.GetRaw("key")
	assert.NoError(t, err, "GetRaw")

	_, err = coll.WriteCas("key", 0, 0, cas, []byte(" World"), sgbucket.Append)
	assert.NoError(t, err, "Append")
	value, _, err := coll.GetRaw("key")
	assert.NoError(t, err, "GetRaw")
	assert.Equal(t, []byte("Hello World"), value)
}

/*

// Create a simple view and run it on some documents
func TestView(t *testing.T) {
	ensureNoLeakedFeeds(t)

	ddoc := sgbucket.DesignDoc{Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.key) emit(doc.key,doc.value)}`}}}
	coll := makeTestBucket(t).DefaultDataStore()
	err := coll.PutDDoc("docname", &ddoc)
	assert.NoError(t, err, "PutDDoc failed")

	var echo sgbucket.DesignDoc
	echo, err = coll.GetDDoc("docname")
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
	assert.Equal(t, 5, result.TotalRows)
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

func TestGetDDocs(t *testing.T) {
	ensureNoLeakedFeeds(t)

	ddoc := sgbucket.DesignDoc{Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.key) emit(doc.key,doc.value)}`}}}
	coll := makeTestBucket(t).DefaultDataStore()
	err := coll.PutDDoc("docname", &ddoc)
	assert.NoError(t, err, "PutDDoc docnamefailed")
	err = coll.PutDDoc("docname2", &ddoc)
	assert.NoError(t, err, "PutDDoc docname2failed")

	ddocs, getErr := coll.GetDDocs()
	assert.NoError(t, getErr, "GetDDocs failed")
	assert.Equal(t, len(ddocs), 2)
}

*/

func TestGets(t *testing.T) {
	ensureNoLeakedFeeds(t)

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

func TestWriteSubDoc(t *testing.T) {
	ensureNoLeakedFeeds(t)

	coll := makeTestBucket(t).DefaultDataStore()
	require.True(t, coll.IsSupported(sgbucket.BucketStoreFeatureSubdocOperations))

	rawJson := []byte(`{
        "walrus":{
            "foo":"lol",
            "bar":"baz"}
        }`)

	addToCollection(t, coll, "key", 0, rawJson)

	var fullDoc map[string]interface{}
	cas, err := coll.Get("key", &fullDoc)
	assert.NoError(t, err)
	assert.Equal(t, CAS(1), cas)

	// update json
	rawJson = []byte(`"was here"`)
	// test update using incorrect cas value
	cas, err = coll.WriteSubDoc("key", "walrus", 10, rawJson)
	assert.Error(t, err)

	// test update using correct cas value
	cas, err = coll.WriteSubDoc("key", "walrus", cas, rawJson)
	assert.NoError(t, err)
	assert.Equal(t, CAS(2), cas)

	cas, err = coll.Get("key", &fullDoc)
	assert.NoError(t, err)
	assert.Equal(t, CAS(2), cas)
	assert.EqualValues(t, map[string]any{"walrus": "was here"}, fullDoc)

	// test update using 0 cas value
	cas, err = coll.WriteSubDoc("key", "walrus", 0, rawJson)
	assert.NoError(t, err)
	assert.Equal(t, CAS(3), cas)
}

func TestWriteCas(t *testing.T) {
	ensureNoLeakedFeeds(t)

	coll := makeTestBucket(t).DefaultDataStore()

	// Add with WriteCas - JSON docs
	// Insert
	var obj interface{}
	mustUnmarshal(t, `{"value":"value1"}`, &obj)
	cas, err := coll.WriteCas("key1", 0, 0, 0, obj, 0)
	assert.NoError(t, err, "WriteCas")
	assert.True(t, cas > 0, "Cas value should be greater than zero")

	// Update document with wrong (zero) cas value
	mustUnmarshal(t, `{"value":"value2"}`, &obj)
	newCas, err := coll.WriteCas("key1", 0, 0, 0, obj, 0)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Update document with correct cas value
	mustUnmarshal(t, `{"value":"value2"}`, &obj)
	newCas, err = coll.WriteCas("key1", 0, 0, cas, obj, 0)
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
	newCas, err = coll.WriteCas("key1", 0, 0, cas, obj, 0)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Add with WriteCas - raw docs
	// Insert
	cas, err = coll.WriteCas("keyraw1", 0, 0, 0, []byte("value1"), sgbucket.Raw)
	assert.NoError(t, err, "WriteCas")
	assert.True(t, cas > 0, "Cas value should be greater than zero")

	// Update document with wrong (zero) cas value
	newCas, err = coll.WriteCas("keyraw1", 0, 0, 0, []byte("value2"), sgbucket.Raw)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Update document with correct cas value
	newCas, err = coll.WriteCas("keyraw1", 0, 0, cas, []byte("value2"), sgbucket.Raw)
	assert.True(t, err == nil, "Valid cas should not have returned error.")
	assert.True(t, cas > 0, "Cas value should be greater than zero")
	assert.True(t, cas != newCas, "Cas value should change on successful update")
	value, getCas, err := coll.GetRaw("keyraw1")
	assert.NoError(t, err, "GetRaw")
	assert.Equal(t, []byte("value2"), value)
	assert.Equal(t, newCas, getCas)

	// Update document with obsolete cas value
	newCas, err = coll.WriteCas("keyraw1", 0, 0, cas, []byte("value3"), sgbucket.Raw)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Delete document, attempt to recreate w/ cas set to 0
	err = coll.Delete("keyraw1")
	assert.True(t, err == nil, "Delete failed")
	newCas, err = coll.WriteCas("keyraw1", 0, 0, 0, []byte("resurrectValue"), sgbucket.Raw)
	assert.NoError(t, err, "Recreate with cas=0 should succeed.")
	assert.True(t, cas > 0, "Cas value should be greater than zero")
	value, getCas, err = coll.GetRaw("keyraw1")
	assert.NoError(t, err, "GetRaw")
	assert.Equal(t, []byte("resurrectValue"), value)
	assert.Equal(t, newCas, getCas)

}

func TestRemove(t *testing.T) {
	ensureNoLeakedFeeds(t)

	coll := makeTestBucket(t).DefaultDataStore()

	// Add with WriteCas - JSON docs
	// Insert
	var obj interface{}
	mustUnmarshal(t, `{"value":"value1"}`, &obj)
	cas, err := coll.WriteCas("key1", 0, 0, 0, obj, 0)
	assert.NoError(t, err, "WriteCas")
	assert.True(t, cas > 0, "Cas value should be greater than zero")

	// Update document with correct cas value
	mustUnmarshal(t, `{"value":"value2"}`, &obj)
	newCas, err := coll.WriteCas("key1", 0, 0, cas, obj, 0)
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
	ensureNoLeakedFeeds(t)

	coll := makeTestBucket(t).DefaultDataStore()

	byteBody := []byte(`{"value":"value1"}`)

	// Add with WriteCas - JSON doc as []byte and *[]byte
	_, err := coll.WriteCas("writeCas1", 0, 0, 0, byteBody, 0)
	assert.NoError(t, err, "WriteCas []byte")
	_, err = coll.WriteCas("writeCas2", 0, 0, 0, &byteBody, 0)
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

	/*
		// Verify values are stored as JSON and can be retrieved via view
		ddoc := sgbucket.DesignDoc{Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.value) emit(doc.key,doc.value)}`}}}
		err = coll.PutDDoc("docname", &ddoc)
		assert.NoError(t, err, "PutDDoc failed")

		options := map[string]interface{}{"stale": false}
		result, err := coll.View("docname", "view1", options)
		assert.NoError(t, err, "View call failed")
		assert.Equal(t, len(keySet), result.TotalRows)
	*/
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
