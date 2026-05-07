// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackfill(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore(t.Context())

	addToCollection(t, c, "able", 0, "A")
	addToCollection(t, c, "baker", 0, "B")
	addToCollection(t, c, "charlie", 0, "C")

	args := sgbucket.FeedArguments{
		Backfill: 0,
		Dump:     true,
	}
	events, doneChan := startFeedWithArgs(t, bucket, args)

	event := <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)

	readExpectedEventsABC(t, events)

	event = <-events
	assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

	_, ok := <-doneChan
	assert.False(t, ok)
}

func TestMutations(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore(ctx)

	addToCollection(t, c, "able", 0, "A")
	addToCollection(t, c, "baker", 0, "B")
	addToCollection(t, c, "charlie", 0, "C")

	events, doneChan := startFeed(t, bucket)

	addToCollection(t, c, "delta", 0, "D")
	addToCollection(t, c, "eskimo", 0, "E")

	go func() {
		addToCollection(t, c, "fahrvergnügen", 0, "F")
		err := c.Delete(ctx, "eskimo")
		require.NoError(t, err)
	}()

	readExpectedEventsDEF(t, events)

	// Read the mutation of "eskimo":
	e := <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpDeletion, Key: []byte("eskimo"), DataType: sgbucket.FeedDataTypeRaw, RevNo: 2}, e)

	require.NoError(t, bucket.CloseAndDelete(ctx))

	_, ok := <-doneChan
	assert.False(t, ok)
}

func TestCheckpoint(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore(t.Context())

	addToCollection(t, c, "able", 0, "A")
	addToCollection(t, c, "baker", 0, "B")
	addToCollection(t, c, "charlie", 0, "C")

	// Run the feed:
	args := sgbucket.FeedArguments{
		ID:               "myID",
		Backfill:         sgbucket.FeedResume,
		Dump:             true,
		CheckpointPrefix: "Checkpoint",
	}
	events, doneChan := startFeedWithArgs(t, bucket, args)

	event := <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)
	readExpectedEventsABC(t, events)
	event = <-events
	assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

	_, ok := <-doneChan
	assert.False(t, ok)

	// Create new docs:
	addToCollection(t, c, "delta", 0, "D")
	addToCollection(t, c, "eskimo", 0, "E")
	addToCollection(t, c, "fahrvergnügen", 0, "F")

	// Resume the feed:
	t.Logf("---- Resuming feed from checkpoint ---")
	args = sgbucket.FeedArguments{
		ID:               "myID",
		Backfill:         sgbucket.FeedResume,
		Dump:             true,
		CheckpointPrefix: "Checkpoint",
	}
	events, doneChan = startFeedWithArgs(t, bucket, args)

	event = <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)

	// The first event will be the writing of the checkpoint itself:
	e := <-events
	assert.Equal(t, "Checkpoint", string(e.Key))

	readExpectedEventsDEF(t, events)

	event = <-events
	assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

	_, ok = <-doneChan
	assert.False(t, ok)
}

func TestResumeFromCheckpoint(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore()

	addToCollection(t, c, "doc1", 0, "V1")
	addToCollection(t, c, "doc2", 0, "V2")

	prefix := "ResumeCheckpoint"

	// Run feed to checkpoint
	args := sgbucket.FeedArguments{
		ID:               "myID",
		Backfill:         sgbucket.FeedResume,
		Dump:             true,
		CheckpointPrefix: prefix,
	}
	events, doneChan := startFeedWithArgs(t, bucket, args)

	event := <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)

	e := <-events
	assert.Equal(t, "doc1", string(e.Key))
	e = <-events
	assert.Equal(t, "doc2", string(e.Key))

	event = <-events
	assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

	<-doneChan

	// Add new docs while feed is off
	addToCollection(t, c, "doc3", 0, "V3")
	addToCollection(t, c, "doc4", 0, "V4")

	// Resume feed
	events, doneChan = startFeedWithArgs(t, bucket, args)

	event = <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)

	// First event might be the checkpoint doc itself depending on timing/CAS, but we definitely shouldn't see doc1/doc2 again
	var seenDocs []string
	for e := range events {
		if e.Opcode == sgbucket.FeedOpEndBackfill {
			break
		}
		seenDocs = append(seenDocs, string(e.Key))
	}

	assert.NotContains(t, seenDocs, "doc1")
	assert.NotContains(t, seenDocs, "doc2")
	assert.Contains(t, seenDocs, "doc3")
	assert.Contains(t, seenDocs, "doc4")
	assert.Contains(t, seenDocs, prefix) // The checkpoint doc update

	<-doneChan
}

func TestSharedCheckpoint(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c1 := bucket.DefaultDataStore().(*Collection)
	c2, err := bucket.getOrCreateCollection(sgbucket.DataStoreNameImpl{Scope: "S", Collection: "C"}, true)
	require.NoError(t, err)

	addToCollection(t, c1, "c1-doc", 0, "V1")
	addToCollection(t, c2, "c2-doc", 0, "V2")

	prefix := "SharedCheckpoint"

	// Run feed for c1
	args1 := sgbucket.FeedArguments{
		ID:               "id1",
		Backfill:         sgbucket.FeedResume,
		Dump:             true,
		CheckpointPrefix: prefix,
	}
	events1, done1 := startFeedWithArgs(t, bucket, args1)
	for e := range events1 {
		if e.Opcode == sgbucket.FeedOpEndBackfill {
			break
		}
	}
	<-done1

	// Run feed for c2
	args2 := sgbucket.FeedArguments{
		ID:               "id2",
		Backfill:         sgbucket.FeedResume,
		Dump:             true,
		CheckpointPrefix: prefix,
		Scopes:           map[string][]string{"S": {"C"}},
	}
	events2, done2 := startFeedWithArgs(t, bucket, args2)
	for e := range events2 {
		if e.Opcode == sgbucket.FeedOpEndBackfill {
			break
		}
	}
	<-done2

	// Verify checkpoint document in DefaultDataStore (c1)
	var checkpt checkpoint
	_, err = c1.Get(prefix, &checkpt)
	require.NoError(t, err)
	assert.Len(t, checkpt.LastCas, 2)
	assert.Contains(t, checkpt.LastCas, c1.GetCollectionID())
	assert.Contains(t, checkpt.LastCas, c2.GetCollectionID())

	// Resume c1 and c2, verify they pick up from where they left off
	addToCollection(t, c1, "c1-doc2", 0, "V1-2")
	addToCollection(t, c2, "c2-doc2", 0, "V2-2")

	events1, done1 = startFeedWithArgs(t, bucket, args1)
	foundC1Doc2 := false
	for e := range events1 {
		if string(e.Key) == "c1-doc2" {
			foundC1Doc2 = true
		}
		if e.Opcode == sgbucket.FeedOpEndBackfill {
			break
		}
	}
	assert.True(t, foundC1Doc2)
	<-done1

	events2, done2 = startFeedWithArgs(t, bucket, args2)
	foundC2Doc2 := false
	for e := range events2 {
		if string(e.Key) == "c2-doc2" {
			foundC2Doc2 = true
		}
		if e.Opcode == sgbucket.FeedOpEndBackfill {
			break
		}
	}
	assert.True(t, foundC2Doc2)
	<-done2
}

func startFeed(t *testing.T, bucket *Bucket) (events chan sgbucket.FeedEvent, doneChan chan struct{}) {
	return startFeedWithArgs(t, bucket, sgbucket.FeedArguments{Backfill: sgbucket.FeedNoBackfill})
}

func startFeedWithArgs(t *testing.T, bucket *Bucket, args sgbucket.FeedArguments) (events chan sgbucket.FeedEvent, doneChan chan struct{}) {
	events = make(chan sgbucket.FeedEvent, 10)
	callback := func(_ context.Context, event sgbucket.FeedEvent) bool {
		events <- event
		return true
	}
	if args.DoneChan == nil {
		args.DoneChan = make(chan struct{})
	}
	err := bucket.StartDCPFeed(context.TODO(), args, callback, nil)
	require.NoError(t, err, "StartDCPFeed failed")
	return events, args.DoneChan
}

func assertEventEquals(t *testing.T, expected sgbucket.FeedEvent, actual sgbucket.FeedEvent) {
	assert.Equal(t, expected.Opcode, actual.Opcode)
	assert.Equal(t, expected.Key, actual.Key)
	assert.Equal(t, expected.Value, actual.Value)
	assert.Equal(t, expected.DataType, actual.DataType)
	assert.NotZero(t, actual.RevNo)
	assert.Equal(t, expected.RevNo, actual.RevNo)
}

func readExpectedEventsABC(t *testing.T, events chan sgbucket.FeedEvent) {
	e := <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("able"), Value: []byte(`"A"`), DataType: sgbucket.FeedDataTypeJSON, RevNo: 1}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("baker"), Value: []byte(`"B"`), DataType: sgbucket.FeedDataTypeJSON, RevNo: 1}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("charlie"), Value: []byte(`"C"`), DataType: sgbucket.FeedDataTypeJSON, RevNo: 1}, e)
}

func readExpectedEventsDEF(t *testing.T, events chan sgbucket.FeedEvent) {
	e := <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("delta"), Value: []byte(`"D"`), DataType: sgbucket.FeedDataTypeJSON, RevNo: 1}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("eskimo"), Value: []byte(`"E"`), DataType: sgbucket.FeedDataTypeJSON, RevNo: 1}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("fahrvergnügen"), Value: []byte(`"F"`), DataType: sgbucket.FeedDataTypeJSON, RevNo: 1}, e)
}

func TestCrossBucketEvents(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore(ctx)

	addToCollection(t, c, "able", 0, "A")
	addToCollection(t, c, "baker", 0, "B")
	addToCollection(t, c, "charlie", 0, "C")

	// Open a 2nd bucket on the same file, to receive events:
	bucket2, err := OpenBucket(bucket.url, strings.ToLower(t.Name()), ReOpenExisting)
	require.NoError(t, err)
	t.Cleanup(func() {
		bucket2.Close(ctx)
	})

	events, doneChan := startFeed(t, bucket)
	events2, doneChan2 := startFeed(t, bucket2)

	addToCollection(t, c, "delta", 0, "D")
	addToCollection(t, c, "eskimo", 0, "E")

	go func() {
		addToCollection(t, c, "fahrvergnügen", 0, "F")
		err = c.Delete(ctx, "eskimo")
		require.NoError(t, err)
	}()

	readExpectedEventsDEF(t, events)
	readExpectedEventsDEF(t, events2)

	bucket.Close(ctx)
	require.NoError(t, bucket2.CloseAndDelete(ctx))

	_, ok := <-doneChan
	assert.False(t, ok)

	_, ok = <-doneChan2
	assert.False(t, ok)
}

func TestCollectionMutations(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)

	huddle := makeTestBucket(t)
	defer huddle.Close(ctx)

	collection1, err := huddle.NamedDataStore(ctx, dsName("scope1", "collection1"))
	require.NoError(t, err)
	collection2, err := huddle.NamedDataStore(ctx, dsName("scope1", "collection2"))
	require.NoError(t, err)
	numDocs := 50

	collectionID_1 := collection1.GetCollectionID()
	collectionID_2 := collection2.GetCollectionID()

	// Add n docs to two collections
	for i := 1; i <= numDocs; i++ {
		ok, err := collection1.Add(ctx, fmt.Sprintf("doc%d", i), 0, fmt.Sprintf("value%d", i))
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = collection2.Add(ctx, fmt.Sprintf("doc%d", i), 0, fmt.Sprintf("value%d", i))
		require.NoError(t, err)
		require.True(t, ok)
	}

	var callbackMutex sync.Mutex
	var c1Count, c2Count int
	c1Keys := make(map[string]struct{})
	c2Keys := make(map[string]struct{})

	callback := func(_ context.Context, event sgbucket.FeedEvent) bool {
		if event.Opcode != sgbucket.FeedOpMutation {
			return false
		}
		callbackMutex.Lock()
		defer callbackMutex.Unlock()
		if event.CollectionID == collectionID_1 {
			c1Count++
			key := string(event.Key)
			_, ok := c1Keys[key]
			assert.False(t, ok)
			c1Keys[key] = struct{}{}
		} else if event.CollectionID == collectionID_2 {
			c2Count++
			key := string(event.Key)
			_, ok := c2Keys[key]
			assert.False(t, ok)
			c2Keys[key] = struct{}{}
		}
		return true
	}

	args := sgbucket.FeedArguments{
		Scopes: map[string][]string{
			"scope1": {"collection1", "collection2"},
		},
		Terminator: make(chan bool),
	}
	defer close(args.Terminator)
	err = huddle.StartDCPFeed(context.TODO(), args, callback, nil)
	require.NoError(t, err, "StartTapFeed failed")

	// wait for mutation counts to reach expected
	expectedCountReached := false
	for i := 0; i < 100; i++ {
		callbackMutex.Lock()
		if c1Count == numDocs && c2Count == numDocs {
			callbackMutex.Unlock()
			expectedCountReached = true
			break
		}
		callbackMutex.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	assert.True(t, expectedCountReached)
	assert.Equal(t, len(c1Keys), numDocs)
	assert.Equal(t, len(c2Keys), numDocs)
}

func TestSetRawAutodetectJSON(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	defer bucket.Close(context.Background())
	c := bucket.DefaultDataStore()

	testCases := []struct {
		name       string
		key        string
		data       []byte
		method     string
		isJSONType bool
	}{
		// Object
		{"SetRaw_Object", "set_obj", []byte(`{"foo":"bar"}`), "SetRaw", true},
		{"AddRaw_Object", "add_obj", []byte(`{"foo":"bar"}`), "AddRaw", true},
		{"WriteCas_Object", "cas_obj", []byte(`{"foo":"bar"}`), "WriteCas", true},

		// Array
		{"SetRaw_Array", "set_arr", []byte(`[1,2,3]`), "SetRaw", true},
		{"AddRaw_Array", "add_arr", []byte(`[1,2,3]`), "AddRaw", true},
		{"WriteCas_Array", "cas_arr", []byte(`[1,2,3]`), "WriteCas", true},

		// String
		{"SetRaw_String", "set_str", []byte(`"hello"`), "SetRaw", true},
		{"AddRaw_String", "add_str", []byte(`"hello"`), "AddRaw", true},
		{"WriteCas_String", "cas_str", []byte(`"hello"`), "WriteCas", true},

		// Integer
		{"SetRaw_Integer", "set_int", []byte(`12345`), "SetRaw", true},
		{"AddRaw_Integer", "add_int", []byte(`12345`), "AddRaw", true},
		{"WriteCas_Integer", "cas_int", []byte(`12345`), "WriteCas", true},

		// Boolean
		{"SetRaw_Boolean", "set_bool", []byte(`true`), "SetRaw", true},
		{"AddRaw_Boolean", "add_bool", []byte(`true`), "AddRaw", true},
		{"WriteCas_Boolean", "cas_bool", []byte(`true`), "WriteCas", true},

		// Null
		{"SetRaw_Null", "set_null", []byte(`null`), "SetRaw", true},
		{"AddRaw_Null", "add_null", []byte(`null`), "AddRaw", true},
		{"WriteCas_Null", "cas_null", []byte(`null`), "WriteCas", true},

		// Binary
		{"SetRaw_Binary", "set_bin", []byte{0, 1, 2, 3}, "SetRaw", false},
		{"AddRaw_Binary", "add_bin", []byte{0, 1, 2, 3}, "AddRaw", false},
		{"WriteCas_Binary", "cas_bin", []byte{0, 1, 2, 3}, "WriteCas", false},
	}

	// Write data using specified method
	for _, tc := range testCases {
		switch tc.method {
		case "SetRaw":
			err := c.SetRaw(tc.key, 0, nil, tc.data)
			require.NoError(t, err, "Failed for %s", tc.name)
		case "AddRaw":
			added, err := c.AddRaw(tc.key, 0, tc.data)
			require.NoError(t, err, "Failed for %s", tc.name)
			require.True(t, added, "Failed for %s", tc.name)
		case "WriteCas":
			_, err := c.WriteCas(tc.key, 0, 0, tc.data, sgbucket.Raw|sgbucket.AddOnly)
			require.NoError(t, err, "Failed for %s", tc.name)
		}
	}

	// Start feed to check DataType
	args := sgbucket.FeedArguments{
		Backfill: 0,
		Dump:     true,
	}
	events, doneChan := startFeedWithArgs(t, bucket, args)

	event := <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)

	// Collect all mutations
	mutations := make(map[string]sgbucket.FeedEvent)
	for event := range events {
		if event.Opcode == sgbucket.FeedOpMutation {
			mutations[string(event.Key)] = event
		} else if event.Opcode == sgbucket.FeedOpEndBackfill {
			break
		}
	}

	// Verify each test case
	for _, tc := range testCases {
		event, ok := mutations[tc.key]
		require.True(t, ok, "Missing mutation for %s", tc.key)
		expectedDataType := sgbucket.FeedDataTypeRaw
		if tc.isJSONType {
			expectedDataType = sgbucket.FeedDataTypeJSON
		}
		assert.Equal(t, expectedDataType, event.DataType, "Wrong DataType for %s", tc.name)
	}

	// The feed should stop — DoneChan closes
	select {
	case _, ok := <-doneChan:
		assert.False(t, ok, "DoneChan should be closed")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for feed to stop")
	}
}

// TestFeedEventIsolation verifies that multiple feeds on the same collection receive independent
// copies of events, so mutating one feed's event does not affect another's.
func TestFeedEventIsolation(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore(ctx)

	// Start two feeds on the same collection with different FeedContent modes.
	// Feed 1 wants full content (default), feed 2 wants keys only.
	terminator1 := make(chan bool)
	args1 := sgbucket.FeedArguments{
		Backfill:    sgbucket.FeedNoBackfill,
		FeedContent: sgbucket.FeedContentDefault,
		Terminator:  terminator1,
	}
	events1, _ := startFeedWithArgs(t, bucket, args1)

	terminator2 := make(chan bool)
	args2 := sgbucket.FeedArguments{
		Backfill:    sgbucket.FeedNoBackfill,
		FeedContent: sgbucket.FeedContentKeysOnly,
		Terminator:  terminator2,
	}
	events2, _ := startFeedWithArgs(t, bucket, args2)

	// Write a doc with body and xattrs
	_, err := c.WriteWithXattrs(ctx, "doc1", 0, 0,
		[]byte(`{"body":true}`),
		map[string][]byte{"_xattr": []byte(`{"x":1}`)},
		nil, nil)
	require.NoError(t, err)

	e1 := <-events1
	e2 := <-events2

	// Feed 1 (default) should have value with body+xattrs
	assert.Equal(t, sgbucket.FeedOpMutation, e1.Opcode)
	require.NotNil(t, e1.Value)
	assert.Contains(t, string(e1.Value), `"body":true`)

	// Feed 2 (keys only) should have nil value
	assert.Equal(t, sgbucket.FeedOpMutation, e2.Opcode)
	assert.Nil(t, e2.Value)

	// Both should have the same key
	assert.Equal(t, "doc1", string(e1.Key))
	assert.Equal(t, "doc1", string(e2.Key))

	// Mutate the event from feed 1 — feed 2's event must not be affected
	e1.Key = []byte("MUTATED")
	e1.Value = []byte("MUTATED")
	assert.Equal(t, "doc1", string(e2.Key), "mutating feed 1 event should not affect feed 2")
	assert.Nil(t, e2.Value, "mutating feed 1 event should not affect feed 2")

	close(terminator1)
	close(terminator2)
}

// TestAsFeedEventErrorOnCorruptXattrs verifies that asFeedEvent returns an error when xattrs
// contain invalid JSON, and that feeds which don't need xattrs are unaffected.
func TestAsFeedEventErrorOnCorruptXattrs(t *testing.T) {
	e := &event{
		opcode:   sgbucket.FeedOpMutation,
		key:      "doc1",
		value:    []byte(`"value1"`),
		xattrs:   []byte(`not valid json`),
		cas:      1,
		exp:      0,
		revSeqNo: 1,
	}

	// FeedContentDefault parses xattrs — should fail
	_, err := e.asFeedEvent(0, sgbucket.FeedContentDefault)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `couldn't parse xattrs for key "doc1"`)

	// FeedContentXattrOnly also parses xattrs — should fail
	_, err = e.asFeedEvent(0, sgbucket.FeedContentXattrOnly)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `couldn't parse xattrs for key "doc1"`)

	// FeedContentKeysOnly doesn't touch xattrs — should succeed
	feedEvent, err := e.asFeedEvent(0, sgbucket.FeedContentKeysOnly)
	require.NoError(t, err)
	assert.Nil(t, feedEvent.Value)

	// FeedContentBodyOnly doesn't touch xattrs — should succeed
	feedEvent, err = e.asFeedEvent(0, sgbucket.FeedContentBodyOnly)
	require.NoError(t, err)
	assert.Equal(t, []byte(`"value1"`), feedEvent.Value)
}

// TestFeedStopsOnCorruptEvent verifies that a feed stops (closes DoneChan) when it encounters
// a corrupt event that cannot be converted to a FeedEvent.
func TestFeedStopsOnCorruptEvent(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore(t.Context())

	// Write a doc then corrupt its xattrs directly in SQLite
	addToCollection(t, c, "doc1", 0, "value1")
	col := c.(*Collection)
	_, err := col.db().Exec(
		`UPDATE documents SET xattrs=? WHERE collection=? AND key=?`,
		[]byte(`not valid json`), col.id, "doc1")
	require.NoError(t, err)

	args := sgbucket.FeedArguments{
		Backfill: 0,
		Dump:     true,
	}
	events, doneChan := startFeedWithArgs(t, bucket, args)

	event := <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)

	// The feed should stop without delivering doc1 — DoneChan closes
	select {
	case _, ok := <-doneChan:
		assert.False(t, ok, "DoneChan should be closed after corrupt event")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for feed to stop after corrupt event")
	}
}

func TestFeedContent(t *testing.T) {
	tests := []struct {
		name        string
		feedContent sgbucket.FeedContent
	}{
		{"Default", sgbucket.FeedContentDefault},
		{"KeysOnly", sgbucket.FeedContentKeysOnly},
		{"BodyOnly", sgbucket.FeedContentBodyOnly},
		{"XattrOnly", sgbucket.FeedContentXattrOnly},
	}

	assertFeedContentEvent := func(t *testing.T, feedContent sgbucket.FeedContent, hasXattrs bool, event sgbucket.FeedEvent) {
		t.Helper()
		assert.Equal(t, sgbucket.FeedOpMutation, event.Opcode)
		t.Logf("event value: %s", event.Value)
		switch feedContent {
		case sgbucket.FeedContentKeysOnly:
			assert.Nil(t, event.Value)
		case sgbucket.FeedContentDefault:
			require.NotNil(t, event.Value)
			assert.Contains(t, string(event.Value), `"doc_body_value":true`)
			if hasXattrs {
				assert.Contains(t, string(event.Value), `"xattr_value":12345`)
				assert.Contains(t, string(event.Value), `xattr_name`)
			}
		case sgbucket.FeedContentBodyOnly:
			require.NotNil(t, event.Value)
			assert.Contains(t, string(event.Value), `"doc_body_value":true`)
			assert.NotContains(t, string(event.Value), `"xattr_value":12345`)
			assert.NotContains(t, string(event.Value), `xattr_name`)
		case sgbucket.FeedContentXattrOnly:
			if hasXattrs {
				require.NotNil(t, event.Value)
				assert.NotContains(t, string(event.Value), `"doc_body_value":true`)
				assert.Contains(t, string(event.Value), `"xattr_value":12345`)
				assert.Contains(t, string(event.Value), `xattr_name`)
			} else {
				assert.Nil(t, event.Value)
			}
		}
	}

	for _, test := range tests {
		t.Run(test.name+"/Backfill", func(t *testing.T) {
			ctx := t.Context()
			ensureNoLeakedFeeds(t)
			bucket := makeTestBucket(t)
			c := bucket.DefaultDataStore(ctx)

			// write a doc with a body and xattrs before starting the feed
			_, err := c.WriteWithXattrs(ctx, "able", 0, 0,
				[]byte(`{"doc_body_value":true}`),
				map[string][]byte{"xattr_name": []byte(`{"xattr_value":12345}`)},
				nil, nil)
			require.NoError(t, err)

			args := sgbucket.FeedArguments{
				Backfill:    0,
				Dump:        true,
				FeedContent: test.feedContent,
			}
			events, doneChan := startFeedWithArgs(t, bucket, args)

			event := <-events
			assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)

			assertFeedContentEvent(t, test.feedContent, true, <-events)

			event = <-events
			assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

			_, ok := <-doneChan
			assert.False(t, ok)
		})

		t.Run(test.name+"/Live", func(t *testing.T) {
			ctx := t.Context()
			ensureNoLeakedFeeds(t)
			bucket := makeTestBucket(t)
			c := bucket.DefaultDataStore(ctx)

			// start feed before writing the doc (no backfill)
			terminator := make(chan bool)
			args := sgbucket.FeedArguments{
				Backfill:    sgbucket.FeedNoBackfill,
				FeedContent: test.feedContent,
				Terminator:  terminator,
			}
			events, _ := startFeedWithArgs(t, bucket, args)

			// write a doc with a body and xattrs after feed is started
			_, err := c.WriteWithXattrs(ctx, "able", 0, 0,
				[]byte(`{"doc_body_value":true}`),
				map[string][]byte{"xattr_name": []byte(`{"xattr_value":12345}`)},
				nil, nil)
			require.NoError(t, err)

			assertFeedContentEvent(t, test.feedContent, true, <-events)

			close(terminator)
		})

		t.Run(test.name+"/Backfill/NoXattrs", func(t *testing.T) {
			ctx := t.Context()
			ensureNoLeakedFeeds(t)
			bucket := makeTestBucket(t)
			c := bucket.DefaultDataStore(ctx)

			// write a doc with body only (no xattrs)
			ok, err := c.Add(ctx, "able", 0, []byte(`{"doc_body_value":true}`))
			require.NoError(t, err)
			require.True(t, ok)

			args := sgbucket.FeedArguments{
				Backfill:    0,
				Dump:        true,
				FeedContent: test.feedContent,
			}
			events, doneChan := startFeedWithArgs(t, bucket, args)

			event := <-events
			assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)

			assertFeedContentEvent(t, test.feedContent, false, <-events)

			event = <-events
			assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

			_, ok = <-doneChan
			assert.False(t, ok)
		})

		t.Run(test.name+"/Live/NoXattrs", func(t *testing.T) {
			ctx := t.Context()
			ensureNoLeakedFeeds(t)
			bucket := makeTestBucket(t)
			c := bucket.DefaultDataStore(ctx)

			// start feed before writing the doc (no backfill)
			terminator := make(chan bool)
			args := sgbucket.FeedArguments{
				Backfill:    sgbucket.FeedNoBackfill,
				FeedContent: test.feedContent,
				Terminator:  terminator,
			}
			events, _ := startFeedWithArgs(t, bucket, args)

			// write a doc with body only (no xattrs) after feed is started
			ok, err := c.Add(ctx, "able", 0, []byte(`{"doc_body_value":true}`))
			require.NoError(t, err)
			require.True(t, ok)

			assertFeedContentEvent(t, test.feedContent, false, <-events)

			close(terminator)
		})
	}
}
