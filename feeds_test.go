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
	c := bucket.DefaultDataStore()

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
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore()

	addToCollection(t, c, "able", 0, "A")
	addToCollection(t, c, "baker", 0, "B")
	addToCollection(t, c, "charlie", 0, "C")

	events, doneChan := startFeed(t, bucket)

	addToCollection(t, c, "delta", 0, "D")
	addToCollection(t, c, "eskimo", 0, "E")

	go func() {
		addToCollection(t, c, "fahrvergnügen", 0, "F")
		err := c.Delete("eskimo")
		require.NoError(t, err)
	}()

	readExpectedEventsDEF(t, events)

	// Read the mutation of "eskimo":
	e := <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpDeletion, Key: []byte("eskimo"), DataType: sgbucket.FeedDataTypeRaw}, e)

	require.NoError(t, bucket.CloseAndDelete(testCtx(t)))

	_, ok := <-doneChan
	assert.False(t, ok)
}

func TestCheckpoint(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore()

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
	assert.Equal(t, "Checkpoint:myID", string(e.Key))

	readExpectedEventsDEF(t, events)

	event = <-events
	assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

	_, ok = <-doneChan
	assert.False(t, ok)
}

func startFeed(t *testing.T, bucket *Bucket) (events chan sgbucket.FeedEvent, doneChan chan struct{}) {
	return startFeedWithArgs(t, bucket, sgbucket.FeedArguments{Backfill: sgbucket.FeedNoBackfill})
}

func startFeedWithArgs(t *testing.T, bucket *Bucket, args sgbucket.FeedArguments) (events chan sgbucket.FeedEvent, doneChan chan struct{}) {
	events = make(chan sgbucket.FeedEvent, 10)
	callback := func(event sgbucket.FeedEvent) bool {
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
}

func readExpectedEventsABC(t *testing.T, events chan sgbucket.FeedEvent) {
	e := <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("able"), Value: []byte(`"A"`), DataType: sgbucket.FeedDataTypeJSON}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("baker"), Value: []byte(`"B"`), DataType: sgbucket.FeedDataTypeJSON}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("charlie"), Value: []byte(`"C"`), DataType: sgbucket.FeedDataTypeJSON}, e)
}

func readExpectedEventsDEF(t *testing.T, events chan sgbucket.FeedEvent) {
	e := <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("delta"), Value: []byte(`"D"`), DataType: sgbucket.FeedDataTypeJSON}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("eskimo"), Value: []byte(`"E"`), DataType: sgbucket.FeedDataTypeJSON}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assertEventEquals(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("fahrvergnügen"), Value: []byte(`"F"`), DataType: sgbucket.FeedDataTypeJSON}, e)
}

func TestCrossBucketEvents(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucketName := strings.ToLower(t.Name())
	bucket := makeTestBucketWithName(t, bucketName)
	c := bucket.DefaultDataStore()

	addToCollection(t, c, "able", 0, "A")
	addToCollection(t, c, "baker", 0, "B")
	addToCollection(t, c, "charlie", 0, "C")

	// Open a 2nd bucket on the same file, to receive events:
	bucket2, err := OpenBucket(bucket.url, bucketName, ReOpenExisting)
	require.NoError(t, err)
	t.Cleanup(func() {
		bucket2.Close(testCtx(t))
	})

	events, doneChan := startFeed(t, bucket)
	events2, doneChan2 := startFeed(t, bucket2)

	addToCollection(t, c, "delta", 0, "D")
	addToCollection(t, c, "eskimo", 0, "E")

	go func() {
		addToCollection(t, c, "fahrvergnügen", 0, "F")
		err = c.Delete("eskimo")
		require.NoError(t, err)
	}()

	readExpectedEventsDEF(t, events)
	readExpectedEventsDEF(t, events2)

	bucket.Close(testCtx(t))
	require.NoError(t, bucket2.CloseAndDelete(testCtx(t)))

	_, ok := <-doneChan
	assert.False(t, ok)

	_, ok = <-doneChan2
	assert.False(t, ok)
}

func TestCollectionMutations(t *testing.T) {
	ensureNoLeakedFeeds(t)

	huddle := makeTestBucket(t)
	defer huddle.Close(testCtx(t))

	collection1, err := huddle.NamedDataStore(dsName("scope1", "collection1"))
	require.NoError(t, err)
	collection2, err := huddle.NamedDataStore(dsName("scope1", "collection2"))
	require.NoError(t, err)
	numDocs := 50

	collectionID_1 := collection1.GetCollectionID()
	collectionID_2 := collection2.GetCollectionID()

	// Add n docs to two collections
	for i := 1; i <= numDocs; i++ {
		ok, err := collection1.Add(fmt.Sprintf("doc%d", i), 0, fmt.Sprintf("value%d", i))
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = collection2.Add(fmt.Sprintf("doc%d", i), 0, fmt.Sprintf("value%d", i))
		require.NoError(t, err)
		require.True(t, ok)
	}

	var callbackMutex sync.Mutex
	var c1Count, c2Count int
	c1Keys := make(map[string]struct{})
	c2Keys := make(map[string]struct{})

	callback := func(event sgbucket.FeedEvent) bool {
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
