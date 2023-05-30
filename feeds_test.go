package rosmar

import (
	"fmt"
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

	events := make(chan sgbucket.FeedEvent, 10)
	callback := func(event sgbucket.FeedEvent) bool {
		events <- event
		return true
	}

	args := sgbucket.FeedArguments{
		Backfill: 0,
		Dump:     true,
		DoneChan: make(chan struct{}),
	}
	err := bucket.StartDCPFeed(args, callback, nil)
	assert.NoError(t, err, "StartDCPFeed failed")

	event := <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)
	results := map[string]string{}
	for i := 0; i < 3; i++ {
		event := <-events
		assert.Equal(t, sgbucket.FeedOpMutation, event.Opcode)
		results[string(event.Key)] = string(event.Value)
	}
	assert.Equal(t, map[string]string{
		"able": `"A"`, "baker": `"B"`, "charlie": `"C"`}, results)

	event = <-events
	assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

	_, ok := <-args.DoneChan
	assert.False(t, ok)
}

func TestMutations(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore()

	addToCollection(t, c, "able", 0, "A")
	addToCollection(t, c, "baker", 0, "B")
	addToCollection(t, c, "charlie", 0, "C")

	events := make(chan sgbucket.FeedEvent, 10)
	callback := func(event sgbucket.FeedEvent) bool {
		events <- event
		return true
	}

	args := sgbucket.FeedArguments{
		Backfill: sgbucket.FeedNoBackfill,
		DoneChan: make(chan struct{}),
	}
	err := bucket.StartDCPFeed(args, callback, nil)
	assert.NoError(t, err, "StartTapFeed failed")

	addToCollection(t, c, "delta", 0, "D")
	addToCollection(t, c, "eskimo", 0, "E")

	go func() {
		addToCollection(t, c, "fahrvergnügen", 0, "F")
		err = c.Delete("eskimo")
		require.NoError(t, err)
	}()

	e := <-events
	e.TimeReceived = time.Time{}
	assert.Equal(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("delta"), Value: []byte(`"D"`), Cas: 4, DataType: sgbucket.FeedDataTypeJSON}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assert.Equal(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("eskimo"), Value: []byte(`"E"`), Cas: 5, DataType: sgbucket.FeedDataTypeJSON}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assert.Equal(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("fahrvergnügen"), Value: []byte(`"F"`), Cas: 6, DataType: sgbucket.FeedDataTypeJSON}, e)
	e = <-events
	e.TimeReceived = time.Time{}
	assert.Equal(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpDeletion, Key: []byte("eskimo"), Cas: 7, DataType: sgbucket.FeedDataTypeRaw}, e)

	bucket.Close()

	_, ok := <-args.DoneChan
	assert.False(t, ok)
}

func TestCollectionMutations(t *testing.T) {
	ensureNoLeakedFeeds(t)

	huddle := makeTestBucket(t)
	defer huddle.Close()

	collection1, err := huddle.NamedDataStore(dsName("scope1", "collection1"))
	require.NoError(t, err)
	collection2, err := huddle.NamedDataStore(dsName("scope1", "collection2"))
	require.NoError(t, err)
	numDocs := 50

	collectionID_1 := collection1.(sgbucket.Collection).GetCollectionID()
	collectionID_2 := collection2.(sgbucket.Collection).GetCollectionID()

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
	err = huddle.StartDCPFeed(args, callback, nil)
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
