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
	"sync/atomic"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackfill verifies that a dump feed replays the documents already in the collection, bracketed
// by BeginBackfill and EndBackfill, and then closes DoneChan.
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

// TestMutations verifies that a feed with no backfill delivers only mutations made after it starts,
// including deletions, and closes DoneChan when the bucket is closed.
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

// TestCheckpoint verifies that a feed writes a checkpoint when it is terminated, and that a later
// feed with the same CheckpointPrefix resumes from it rather than replaying earlier documents.
func TestCheckpoint(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore(t.Context())

	addToCollection(t, c, "able", 0, "A")
	addToCollection(t, c, "baker", 0, "B")
	addToCollection(t, c, "charlie", 0, "C")

	// Run the feed:
	args := sgbucket.FeedArguments{
		Scopes: map[string][]string{
			"_default": {"_default"},
		},
		ID:               "myID",
		Backfill:         sgbucket.FeedResume,
		Dump:             false,
		Terminator:       make(chan bool),
		CheckpointPrefix: "Checkpoint",
	}

	events, doneChan := startFeedWithArgs(t, bucket, args)

	event := <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)
	readExpectedEventsABC(t, events)
	event = <-events
	assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

	close(args.Terminator)
	_, ok := <-doneChan
	assert.False(t, ok)

	// Create new docs:
	addToCollection(t, c, "delta", 0, "D")
	addToCollection(t, c, "eskimo", 0, "E")
	addToCollection(t, c, "fahrvergnügen", 0, "F")

	// Resume the feed:
	t.Logf("---- Resuming feed from checkpoint ---")
	args = sgbucket.FeedArguments{
		Scopes: map[string][]string{
			"_default": {"_default"},
		},
		ID:               "myID",
		Backfill:         sgbucket.FeedResume,
		Dump:             false,
		Terminator:       make(chan bool),
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

	close(args.Terminator)
	_, ok = <-doneChan
	assert.False(t, ok)
}

// TestDumpDeletesCheckpoint verifies that a dump feed deletes its checkpoint once it has streamed
// everything, so a second dump feed with the same CheckpointPrefix replays the collection from the
// start rather than resuming.
func TestDumpDeletesCheckpoint(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	ctx := t.Context()
	c := bucket.DefaultDataStore(ctx)

	addToCollection(t, c, "able", 0, "A")
	addToCollection(t, c, "baker", 0, "B")
	addToCollection(t, c, "charlie", 0, "C")

	const prefix = "DumpCheckpoint"
	args := sgbucket.FeedArguments{
		Scopes: map[string][]string{
			"_default": {"_default"},
		},
		ID:               "myID",
		Backfill:         sgbucket.FeedResume,
		Dump:             true,
		CheckpointPrefix: prefix,
	}

	// A dump feed ends on its own once it has streamed everything.
	events, doneChan := startFeedWithArgs(t, bucket, args)

	event := <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)
	readExpectedEventsABC(t, events)
	event = <-events
	assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

	_, ok := <-doneChan
	assert.False(t, ok)

	// The checkpoint is spent, so the feed removed it.
	var checkpt checkpoint
	_, err := c.Get(ctx, prefix, &checkpt)
	require.Error(t, err)
	require.IsType(t, sgbucket.MissingError{}, err)

	// With no checkpoint to resume from, a second dump feed replays the same documents.
	t.Logf("---- Second dump feed, no checkpoint to resume from ---")
	events, doneChan = startFeedWithArgs(t, bucket, args)

	event = <-events
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)
	readExpectedEventsABC(t, events)

	// the first feed wrote a checkpoint before the feed deleted it, so its tombstone is in the backfill
	e := <-events
	assert.Equal(t, sgbucket.FeedOpDeletion, e.Opcode)
	assert.Equal(t, prefix, string(e.Key))

	event = <-events
	assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

	_, ok = <-doneChan
	assert.False(t, ok)
}

// TestDumpWithoutMetadataStore verifies that a dump feed given a checkpoint prefix but no metadata
// store finishes without panicking. Such a feed never wrote a checkpoint, so there is none to delete.
func TestDumpWithoutMetadataStore(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	ctx := t.Context()

	doneChan := make(chan struct{})
	args := sgbucket.FeedArguments{
		ID:               "myID",
		Backfill:         0,
		Dump:             true,
		CheckpointPrefix: "NoMetadataStoreCheckpoint",
		DoneChan:         doneChan,
	}
	require.NoError(t, bucket.StartDCPFeed(ctx, args, func(sgbucket.FeedEvent) bool { return true }, nil))
	<-doneChan
}

// TestDumpAcrossCollectionsDeletesCheckpointOnlyWhenAllComplete verifies that the checkpoint shared by
// a multi-collection feed survives while any collection is still using it. The feed runs one goroutine
// per collection, so one collection finishing must not drop the document the others depend on.
func TestDumpAcrossCollectionsDeletesCheckpointOnlyWhenAllComplete(t *testing.T) {
	const prefix = "MultiCollectionDumpCheckpoint"
	scopes := map[string][]string{"scope1": {"collection1", "collection2"}}

	newArgs := func(terminator chan bool) sgbucket.FeedArguments {
		return sgbucket.FeedArguments{
			ID:               "myID",
			Backfill:         sgbucket.FeedResume,
			Dump:             true,
			Terminator:       terminator,
			CheckpointPrefix: prefix,
			Scopes:           scopes,
		}
	}

	seed := func(t *testing.T, bucket *Bucket) (*Collection, uint32) {
		ctx := t.Context()
		c1, err := bucket.NamedDataStore(ctx, dsName("scope1", "collection1"))
		require.NoError(t, err)
		c2, err := bucket.NamedDataStore(ctx, dsName("scope1", "collection2"))
		require.NoError(t, err)
		for i := range 500 {
			_, err := c1.Add(ctx, fmt.Sprintf("c1-doc%d", i), 0, "V")
			require.NoError(t, err)
			_, err = c2.Add(ctx, fmt.Sprintf("c2-doc%d", i), 0, "V")
			require.NoError(t, err)
		}
		return bucket.DefaultDataStore(ctx).(*Collection), c2.GetCollectionID()
	}

	t.Run("all collections complete", func(t *testing.T) {
		ensureNoLeakedFeeds(t)
		bucket := makeTestBucket(t)
		ctx := t.Context()
		metadataStore, _ := seed(t, bucket)

		args := newArgs(nil)
		args.MetadataStore = metadataStore
		doneChan := make(chan struct{})
		args.DoneChan = doneChan
		require.NoError(t, bucket.StartDCPFeed(ctx, args, func(sgbucket.FeedEvent) bool { return true }, nil))
		<-doneChan

		var checkpt checkpoint
		_, err := metadataStore.Get(ctx, prefix, &checkpt)
		require.Error(t, err)
		require.IsType(t, sgbucket.MissingError{}, err)
	})

	t.Run("one collection terminated", func(t *testing.T) {
		ensureNoLeakedFeeds(t)
		bucket := makeTestBucket(t)
		ctx := t.Context()
		metadataStore, c2ID := seed(t, bucket)

		terminator := make(chan bool)
		doneChan := make(chan struct{})
		args := newArgs(terminator)
		args.MetadataStore = metadataStore
		args.DoneChan = doneChan

		// step collection2 one event at a time, so it is still running when the feed is stopped
		step := make(chan struct{})
		callback := func(event sgbucket.FeedEvent) bool {
			if event.CollectionID == c2ID && event.Opcode == sgbucket.FeedOpMutation {
				<-step
			}
			return true
		}
		require.NoError(t, bucket.StartDCPFeed(ctx, args, callback, nil))

		for range 3 {
			step <- struct{}{}
		}
		close(terminator)
		for done := false; !done; {
			select {
			case <-doneChan:
				done = true
			case step <- struct{}{}:
			}
		}

		// collection2 was cut short, so the shared checkpoint is still needed
		var checkpt checkpoint
		_, err := metadataStore.Get(ctx, prefix, &checkpt)
		require.NoError(t, err)
		assert.NotEmpty(t, checkpt.LastCas)
	})
}

// TestDumpKeepsCheckpointWhenTerminated verifies that a dump feed stopped before it streams
// everything keeps its checkpoint, so the next feed can resume from where it stopped.
func TestDumpKeepsCheckpointWhenTerminated(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	ctx := t.Context()
	c := bucket.DefaultDataStore(ctx)

	const numDocs = 1000
	for i := range numDocs {
		addToCollection(t, c, fmt.Sprintf("doc%d", i), 0, "V")
	}

	const prefix = "TerminatedDumpCheckpoint"
	terminator := make(chan bool)
	doneChan := make(chan struct{})

	// Advance the feed one event at a time. Blocking between events gives the terminator a scheduling
	// point to close the queue, which a free-running feed can otherwise outrun.
	step := make(chan struct{})
	var processed atomic.Int64
	callback := func(event sgbucket.FeedEvent) bool {
		if event.Opcode == sgbucket.FeedOpMutation {
			processed.Add(1)
			<-step
		}
		return true
	}

	args := sgbucket.FeedArguments{
		ID:               "myID",
		Backfill:         sgbucket.FeedResume,
		Dump:             true,
		Terminator:       terminator,
		DoneChan:         doneChan,
		CheckpointPrefix: prefix,
		MetadataStore:    c,
	}
	require.NoError(t, bucket.StartDCPFeed(ctx, args, callback, nil))

	// let a few documents through, so the feed has progress worth checkpointing
	for range 3 {
		step <- struct{}{}
	}

	close(terminator)
	for done := false; !done; {
		select {
		case <-doneChan:
			done = true
		case step <- struct{}{}:
		}
	}

	// the feed was cut short, so it never reached its end-of-feed marker
	assert.Less(t, processed.Load(), int64(numDocs))

	// a terminated feed keeps its checkpoint, unlike one that streamed everything
	var checkpt checkpoint
	_, err := c.Get(ctx, prefix, &checkpt)
	require.NoError(t, err)
	assert.NotEmpty(t, checkpt.LastCas)
}

// TestResumeFromCheckpoint verifies that a resumed feed delivers only the documents written while it
// was stopped, and that it sees the checkpoint document itself as a mutation.
func TestResumeFromCheckpoint(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	ctx := t.Context()
	c := bucket.DefaultDataStore(ctx)

	addToCollection(t, c, "doc1", 0, "V1")
	addToCollection(t, c, "doc2", 0, "V2")

	prefix := "ResumeCheckpoint"

	// Run feed to checkpoint
	args := sgbucket.FeedArguments{
		ID:               "myID",
		Backfill:         sgbucket.FeedResume,
		Dump:             false,
		Terminator:       make(chan bool),
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

	close(args.Terminator)
	<-doneChan

	// Add new docs while feed is off
	addToCollection(t, c, "doc3", 0, "V3")
	addToCollection(t, c, "doc4", 0, "V4")

	// Resume feed
	args.Terminator = make(chan bool)
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

	close(args.Terminator)
	<-doneChan
}

// TestSharedCheckpoint verifies that feeds on different collections share one checkpoint document,
// storing a last CAS per collection, and that each resumes from its own entry.
func TestSharedCheckpoint(t *testing.T) {
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	ctx := t.Context()
	c1 := bucket.DefaultDataStore(ctx).(*Collection)
	c2, err := bucket.getOrCreateCollection(sgbucket.DataStoreNameImpl{Scope: "S", Collection: "C"}, true)
	require.NoError(t, err)

	addToCollection(t, c1, "c1-doc", 0, "V1")
	addToCollection(t, c2, "c2-doc", 0, "V2")

	prefix := "SharedCheckpoint"

	// Run feed for c1
	args1 := sgbucket.FeedArguments{
		ID:               "id1",
		Backfill:         sgbucket.FeedResume,
		Dump:             false,
		Terminator:       make(chan bool),
		CheckpointPrefix: prefix,
	}
	events1, done1 := startFeedWithArgs(t, bucket, args1)
	for e := range events1 {
		if e.Opcode == sgbucket.FeedOpEndBackfill {
			break
		}
	}
	close(args1.Terminator)
	<-done1

	// Run feed for c2
	args2 := sgbucket.FeedArguments{
		ID:               "id2",
		Backfill:         sgbucket.FeedResume,
		Dump:             false,
		Terminator:       make(chan bool),
		CheckpointPrefix: prefix,
		Scopes:           map[string][]string{"S": {"C"}},
	}
	events2, done2 := startFeedWithArgs(t, bucket, args2)
	for e := range events2 {
		if e.Opcode == sgbucket.FeedOpEndBackfill {
			break
		}
	}
	close(args2.Terminator)
	<-done2

	// Verify checkpoint document in DefaultDataStore (c1)
	var checkpt checkpoint
	_, err = c1.Get(ctx, prefix, &checkpt)
	require.NoError(t, err)
	assert.Len(t, checkpt.LastCas, 2)
	assert.Contains(t, checkpt.LastCas, c1.GetCollectionID())
	assert.Contains(t, checkpt.LastCas, c2.GetCollectionID())

	// Resume c1 and c2, verify they pick up from where they left off
	addToCollection(t, c1, "c1-doc2", 0, "V1-2")
	addToCollection(t, c2, "c2-doc2", 0, "V2-2")

	args1.Terminator = make(chan bool)
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
	close(args1.Terminator)
	<-done1

	args2.Terminator = make(chan bool)
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
	close(args2.Terminator)
	<-done2
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
	if args.CheckpointPrefix != "" && args.MetadataStore == nil {
		args.MetadataStore = bucket.DefaultDataStore(context.TODO())
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

// TestCrossBucketEvents verifies that a feed on a second bucket handle opened over the same file
// receives the mutations written through the first.
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

// TestCollectionMutations verifies that a feed scoped to two named collections reports each event
// against the right collection ID, and delivers every document from both.
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
		Terminator:    make(chan bool),
		MetadataStore: huddle.MobileSystemDataStore(context.TODO()),
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

// TestSetRawAutodetectJSON verifies that the raw write methods detect JSON content and set the feed
// event's DataType accordingly.
func TestSetRawAutodetectJSON(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	defer bucket.Close(ctx)
	c := bucket.DefaultDataStore(ctx)

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
			err := c.SetRaw(ctx, tc.key, 0, nil, tc.data)
			require.NoError(t, err, "Failed for %s", tc.name)
		case "AddRaw":
			added, err := c.AddRaw(ctx, tc.key, 0, tc.data)
			require.NoError(t, err, "Failed for %s", tc.name)
			require.True(t, added, "Failed for %s", tc.name)
		case "WriteCas":
			_, err := c.WriteCas(ctx, tc.key, 0, 0, tc.data, sgbucket.Raw|sgbucket.AddOnly)
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

// TestFeedContent verifies that each FeedContent option controls what a feed event carries: the body,
// the xattrs, both, or neither.
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

// TestStartFeedOnClosedBucket verifies that a feed cannot be started on a closed copy of a bucket.  Such a feed
// would be registered in the shared feed list with nothing left to stop it: Bucket.Close only stops the feeds
// the copy had when it closed, so it would run until the database itself closes -- never, for an in-memory
// bucket that is closed rather than deleted.
func TestStartFeedOnClosedBucket(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore(ctx).(*Collection)

	// A 2nd copy keeps the database open, so only this copy of the bucket is closed:
	bucket2, err := OpenBucket(bucket.url, strings.ToLower(t.Name()), ReOpenExisting)
	require.NoError(t, err)
	t.Cleanup(func() { bucket2.Close(ctx) })

	bucket.Close(ctx)

	callback := func(sgbucket.FeedEvent) bool { return true }
	args := sgbucket.FeedArguments{Backfill: sgbucket.FeedNoBackfill, DoneChan: make(chan struct{})}

	require.ErrorIs(t, bucket.StartDCPFeed(ctx, args, callback, nil), ErrBucketClosed)
	_, err = c.startDCPFeed(ctx, args, callback, nil)
	require.ErrorIs(t, err, ErrBucketClosed)

	requireNoRegisteredFeeds(t, bucket)

	// The still-open copy can start a feed on the same collection:
	c2 := bucket2.DefaultDataStore(ctx).(*Collection)
	events2, _ := startFeed(t, bucket2)
	addToCollection(t, c2, "able", 0, "A")
	e := <-events2
	require.Equal(t, "able", string(e.Key))
}

// TestStartDCPFeedUnknownCollection verifies that a bucket-level feed on a collection that does not exist
// fails without creating the collection and without starting a feed on the collections that do exist.
func TestStartDCPFeedUnknownCollection(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)

	_, err := bucket.NamedDataStore(ctx, dsName("scope1", "collection1"))
	require.NoError(t, err)

	callback := func(sgbucket.FeedEvent) bool { return true }
	args := sgbucket.FeedArguments{
		Backfill: sgbucket.FeedNoBackfill,
		Scopes:   map[string][]string{"scope1": {"collection1", "collection2"}},
		DoneChan: make(chan struct{}),
	}
	err = bucket.StartDCPFeed(ctx, args, callback, nil)
	require.ErrorContains(t, err, "scope1:collection2")
	var missing sgbucket.MissingError
	require.ErrorAs(t, err, &missing)

	stores, err := bucket.ListDataStores(ctx)
	require.NoError(t, err)
	require.NotContains(t, stores, dsName("scope1", "collection2"), "the feed created the collection")

	requireNoRegisteredFeeds(t, bucket)
}

// TestStartDCPFeedMissingDefaultCollection verifies that a bucket-level feed with no scopes fails when the
// default collection does not exist, instead of creating it.
func TestStartDCPFeedMissingDefaultCollection(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)

	_, err := bucket.db().Exec(`DELETE FROM collections WHERE scope=?1 AND name=?2`,
		defaultDataStoreName.Scope, defaultDataStoreName.Collection)
	require.NoError(t, err)

	callback := func(sgbucket.FeedEvent) bool { return true }
	args := sgbucket.FeedArguments{Backfill: sgbucket.FeedNoBackfill, DoneChan: make(chan struct{})}
	require.Error(t, bucket.StartDCPFeed(ctx, args, callback, nil))

	stores, err := bucket.ListDataStores(ctx)
	require.NoError(t, err)
	require.Empty(t, stores, "the feed created the default collection")
}

// TestStartDCPFeedCollectionFailure verifies that a bucket-level feed reports a collection feed that cannot
// start.  Ignoring the failure returns success for a feed that never runs, and leaves the caller's DoneChan
// waiting on a collection feed that will never close it.
func TestStartDCPFeedCollectionFailure(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)

	for _, name := range []string{"collection1", "collection2"} {
		_, err := bucket.NamedDataStore(ctx, dsName("scope1", name))
		require.NoError(t, err)
	}

	callback := func(sgbucket.FeedEvent) bool { return true }
	// FeedResume without a CheckpointPrefix cannot start:
	args := sgbucket.FeedArguments{
		Backfill: sgbucket.FeedResume,
		Scopes:   map[string][]string{"scope1": {"collection1", "collection2"}},
		DoneChan: make(chan struct{}),
	}
	require.ErrorContains(t, bucket.StartDCPFeed(ctx, args, callback, nil), "CheckpointPrefix")
	requireNoRegisteredFeeds(t, bucket)
}

// requireNoRegisteredFeeds asserts that none of the bucket's collections has a registered feed.
func requireNoRegisteredFeeds(t *testing.T, bucket *Bucket) {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()
	require.Empty(t, bucket.collectionFeeds, "a feed is registered on the bucket")
}

// TestDropCollectionStopsOnlyItsFeeds verifies that dropping a collection leaves the feeds on the other
// collections registered and running.  Clearing every collection's feeds would leave those feeds
// unreachable: nothing posts events to them, and nothing is left to stop them.
func TestDropCollectionStopsOnlyItsFeeds(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)

	collection1, err := bucket.NamedDataStore(ctx, dsName("scope1", "collection1"))
	require.NoError(t, err)
	_, err = bucket.NamedDataStore(ctx, dsName("scope1", "collection2"))
	require.NoError(t, err)

	args := sgbucket.FeedArguments{
		Backfill:   sgbucket.FeedNoBackfill,
		Scopes:     map[string][]string{"scope1": {"collection1"}},
		Terminator: make(chan bool),
	}
	events, _ := startFeedWithArgs(t, bucket, args)
	defer close(args.Terminator)

	require.NoError(t, bucket.DropDataStore(ctx, dsName("scope1", "collection2")))

	addToCollection(t, collection1, "able", 0, "A")
	select {
	case e := <-events:
		require.Equal(t, "able", string(e.Key))
	case <-time.After(5 * time.Second):
		require.Fail(t, "collection1's feed got no event after collection2 was dropped")
	}
}

// TestStartDCPFeedCollectionFailureDeregisters verifies that a bucket-level feed that fails partway through
// leaves no feed registered.  A stopped feed left in the collection's feed list keeps getting events pushed
// to it for the life of the bucket, and every retry adds another one.
func TestStartDCPFeedCollectionFailureDeregisters(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)

	for _, name := range []string{"collection1", "collection2"} {
		_, err := bucket.NamedDataStore(ctx, dsName("scope1", name))
		require.NoError(t, err)
	}

	// Fail the 2nd collection's checkpoint read, so the 1st collection's feed has already started:
	metadataStore := &failingGetDataStore{DataStore: bucket.DefaultDataStore(ctx)}
	metadataStore.succeedingGets.Store(1)

	callback := func(sgbucket.FeedEvent) bool { return true }
	args := sgbucket.FeedArguments{
		Backfill:         sgbucket.FeedResume,
		CheckpointPrefix: "Checkpoint",
		MetadataStore:    metadataStore,
		Scopes:           map[string][]string{"scope1": {"collection1", "collection2"}},
		DoneChan:         make(chan struct{}),
	}
	require.ErrorIs(t, bucket.StartDCPFeed(ctx, args, callback, nil), errInjectedGet)
	requireNoRegisteredFeeds(t, bucket)
}

// TestStartDCPFeedClosedBucketError verifies that a bucket-level feed on a closed bucket reports the closed
// bucket, rather than blaming the collection the caller asked for.
func TestStartDCPFeedClosedBucketError(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)

	// A 2nd copy creates the collection and keeps the database open, so this copy has never opened it:
	bucket2, err := OpenBucket(bucket.url, strings.ToLower(t.Name()), ReOpenExisting)
	require.NoError(t, err)
	t.Cleanup(func() { bucket2.Close(ctx) })
	_, err = bucket2.NamedDataStore(ctx, dsName("scope1", "collection1"))
	require.NoError(t, err)

	bucket.Close(ctx)

	callback := func(sgbucket.FeedEvent) bool { return true }
	args := sgbucket.FeedArguments{
		Backfill: sgbucket.FeedNoBackfill,
		Scopes:   map[string][]string{"scope1": {"collection1"}},
		DoneChan: make(chan struct{}),
	}
	err = bucket.StartDCPFeed(ctx, args, callback, nil)
	require.ErrorIs(t, err, ErrBucketClosed)
	require.NotContains(t, err.Error(), "unknown collection")
}

var errInjectedGet = fmt.Errorf("injected metadata store failure")

// failingGetDataStore is a metadata store whose Get fails once succeedingGets of them have succeeded.
type failingGetDataStore struct {
	sgbucket.DataStore
	succeedingGets atomic.Int32
}

func (ds *failingGetDataStore) Get(ctx context.Context, key string, rv any) (uint64, error) {
	if ds.succeedingGets.Add(-1) < 0 {
		return 0, errInjectedGet
	}
	return ds.DataStore.Get(ctx, key, rv)
}
