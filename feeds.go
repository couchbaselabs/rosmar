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
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"sync/atomic"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

var activeFeedCount int32 // for tests

//////// BUCKET API: (sgbucket.MutationFeedStore interface)

func (bucket *Bucket) StartDCPFeed(ctx context.Context, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	traceEnter("StartDCPFeed", "bucket=%s, args=%+v", bucket.GetName(), args)
	// Validate requested collections exist before starting feeds. No scopes means the default
	// collection, if it exists.
	requestedCollections := make([]*Collection, 0)
	if len(args.Scopes) == 0 {
		collection, err := bucket.getCollection(defaultDataStoreName)
		if err != nil {
			return err
		}
		requestedCollections = append(requestedCollections, collection)
	}
	for scopeName, collections := range args.Scopes {
		for _, collectionName := range collections {
			collection, err := bucket.getCollection(sgbucket.DataStoreNameImpl{Scope: scopeName, Collection: collectionName})
			if err != nil {
				return fmt.Errorf("couldn't open collection %s:%s for DCP feed: %w", scopeName, collectionName, err)
			}
			requestedCollections = append(requestedCollections, collection)
		}
	}

	doneChan := args.DoneChan
	doneChans := map[*Collection]chan struct{}{}
	startedFeeds := make([]*dcpFeed, 0, len(requestedCollections))
	for _, collection := range requestedCollections {
		// Not bothering to remove scopes from args for the single collection feeds
		// here because it's ignored by Collection.startDCPFeed
		collectionID := collection.GetCollectionID()
		collectionAwareCallback := func(event sgbucket.FeedEvent) bool {
			event.CollectionID = collectionID
			return callback(event)
		}

		// have each collection maintain its own doneChan
		doneChans[collection] = make(chan struct{})
		argsCopy := args
		argsCopy.DoneChan = doneChans[collection]

		feed, err := collection.startDCPFeed(ctx, argsCopy, collectionAwareCallback, dbStats)
		if err != nil {
			// Stop the feeds that did start: no doneChan of theirs is coalesced into the caller's now.
			for _, started := range startedFeeds {
				started.stop()
			}
			return fmt.Errorf("couldn't start DCP feed on %s: %w", collection.DataStoreNameImpl, err)
		}
		startedFeeds = append(startedFeeds, feed)
	}

	// coalesce doneChans
	go func() {
		for _, collection := range requestedCollections {
			<-doneChans[collection]
		}
		// The checkpoint is shared by every collection in the feed, so drop it only once they have all
		// streamed to the end. A single terminated collection means it is still needed to resume.
		if args.Dump && allFeedsCompleted(startedFeeds) {
			if err := startedFeeds[0].deleteCheckpoint(); err != nil {
				logError("Error deleting checkpoint %q: %v", args.CheckpointPrefix, err)
			}
		}
		if doneChan != nil {
			close(doneChan)
		}
	}()

	return nil
}

//////// COLLECTION API:

// startDCPFeed starts a feed and returns it, so that a caller starting feeds on several collections can stop
// the ones it already started.
func (c *Collection) startDCPFeed(ctx context.Context, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) (*dcpFeed, error) {
	traceEnter("startDCPFeed", "collection=%s, args=%+v", c, args)
	// Refuse up front on a closed bucket, so no backfill work is done for a feed that can't be registered:
	if c.bucket.isClosed() {
		return nil, ErrBucketClosed
	}
	feed := &dcpFeed{
		ctx:           ctx,
		collection:    c,
		metadataStore: args.MetadataStore,
		args:          args,
		callback:      callback,
	}
	feed.events.init()

	if args.Backfill != sgbucket.FeedNoBackfill {
		startCas := args.Backfill
		if args.Backfill == sgbucket.FeedResume {
			if args.CheckpointPrefix == "" {
				return nil, fmt.Errorf("feed's Backfill is FeedResume but no CheckpointPrefix given")
			}
			if err := feed.readCheckpoint(); err != nil {
				return nil, fmt.Errorf("couldn't read DCP feed checkpoint: %w", err)
			}
			startCas = feed.lastCas + 1
		}

		debug("%s starting backfill from CAS 0x%x", feed, startCas)
		feed.events.push(&event{opcode: sgbucket.FeedOpBeginBackfill})
		err := c.enqueueBackfillEvents(startCas, args.FeedContent, &feed.events)
		if err != nil {
			return nil, err
		}
		debug("%s ended backfill", feed)
		feed.events.push(&event{opcode: sgbucket.FeedOpEndBackfill})
	}

	if args.Dump {
		feed.events.push(nil) // push an eof
	} else {
		// Register the feed with the collection for future notifications.  Registering on a closed copy of the
		// bucket would leave a feed nothing can stop: Close only stops the feeds the copy holds when it closes.
		c.bucket.mutex.Lock()
		if c.bucket._closed {
			c.bucket.mutex.Unlock()
			return nil, ErrBucketClosed
		}
		c.bucket.collectionFeeds[c.DataStoreNameImpl] = append(c.bucket.collectionFeeds[c.DataStoreNameImpl], feed)
		c.bucket.mutex.Unlock()
	}
	go feed.run()
	return feed, nil
}

func (c *Collection) enqueueBackfillEvents(startCas uint64, feedContent sgbucket.FeedContent, q *eventQueue) error {
	needsValue := feedContent != sgbucket.FeedContentKeysOnly && feedContent != sgbucket.FeedContentXattrOnly
	needsXattrs := feedContent != sgbucket.FeedContentKeysOnly && feedContent != sgbucket.FeedContentBodyOnly
	sql := fmt.Sprintf(`SELECT key, %s, %s, isJSON, cas, tombstone, revSeqNo FROM documents
						WHERE collection=?1 AND cas >= ?2
						ORDER BY cas`,
		ifelse(needsValue, `value`, `null`),
		ifelse(needsXattrs, `xattrs`, `null`))
	rows, err := c.db().Query(sql, c.id, startCas)
	if err != nil {
		return err
	}
	for rows.Next() {
		var e event
		if err := rows.Scan(&e.key, &e.value, &e.xattrs, &e.isJSON, &e.cas, &e.isDeletion, &e.revSeqNo); err != nil {
			return err
		}
		e.opcode = ifelse(e.isDeletion, sgbucket.FeedOpDeletion, sgbucket.FeedOpMutation)
		q.push(&e)
	}
	return rows.Close()
}

// postNewEvent pushes a new live event to all registered feeds. The full event (body + xattrs) is
// always sent; FeedContent filtering is applied consumer-side in asFeedEvent. Only backfill events
// are filtered at the SQL level in enqueueBackfillEvents.
func (c *Collection) postNewEvent(e *event) {
	info("DCP: %s cas 0x%x: %q = %#.50q ---- xattrs %#q", c, e.cas, e.key, e.value, e.xattrs)
	e.opcode = ifelse(e.isDeletion, sgbucket.FeedOpDeletion, sgbucket.FeedOpMutation)
	c.postEvent(e)
	c.bucket.expManager.scheduleExpirationAtOrBefore(e.exp)
}

func (c *Collection) postEvent(e *event) {
	c.bucket.mutex.Lock()
	feeds := c.bucket.collectionFeeds[c.DataStoreNameImpl]
	c.bucket.mutex.Unlock()

	for _, feed := range feeds {
		if feed != nil {
			eCopy := *e // each feed gets its own copy to avoid cross-feed event mutations and data races
			feed.events.push(&eCopy)
		}
	}
}

// stops all feeds. Caller MUST hold the bucket's lock.
func (c *Collection) _stopFeeds() {
	for _, feed := range c.bucket.collectionFeeds[c.DataStoreNameImpl] {
		feed.close()
	}
	delete(c.bucket.collectionFeeds, c.DataStoreNameImpl)
}

//////// DCPFEED:

type eventQueue = queue[*event]

type checkpoint struct {
	LastCas map[uint32]uint64 `json:"last_cas"`
}

type dcpFeed struct {
	ctx            context.Context // TODO: Use this
	collection     *Collection
	metadataStore  sgbucket.DataStore
	args           sgbucket.FeedArguments
	callback       sgbucket.FeedEventCallbackFunc
	events         eventQueue
	lastCas        CAS
	lastCasChanged bool
	// completed reports that the feed drained to its end-of-feed marker, rather than being terminated.
	completed atomic.Bool
}

func (feed *dcpFeed) String() string {
	return fmt.Sprintf("Feed(%s %s)", feed.collection, feed.args.ID)
}

func (feed *dcpFeed) checkpointKey() string {
	return feed.args.CheckpointPrefix
}

// Reads the feed's lastCas from the checkpoint document, if there is one.
func (feed *dcpFeed) readCheckpoint() (err error) {
	if feed.args.CheckpointPrefix == "" {
		return
	}
	key := feed.checkpointKey()
	var checkpt checkpoint
	if _, err = feed.metadataStore.Get(feed.ctx, key, &checkpt); err != nil {
		if _, ok := err.(sgbucket.MissingError); ok {
			err = nil
			debug("%s checkpoint %q missing", feed, key)
		} else {
			logError("%s failed to read lastCas from %q: %v", feed, key, err)
		}
		return
	}
	if checkpt.LastCas != nil {
		colID := feed.collection.GetCollectionID()
		feed.lastCas = checkpt.LastCas[colID]
	}
	debug("%s read lastCas 0x%x from %q", feed, feed.lastCas, key)
	return
}

// Writes the feed's lastCas to the checkpoint document, if there is one.
func (feed *dcpFeed) writeCheckpoint() (err error) {
	if feed.args.CheckpointPrefix == "" || !feed.lastCasChanged {
		return
	}
	key := feed.checkpointKey()
	colID := feed.collection.GetCollectionID()

	_, err = feed.metadataStore.Update(feed.ctx, key, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
		var checkpt checkpoint
		if current != nil {
			if err := json.Unmarshal(current, &checkpt); err != nil {
				return nil, nil, false, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
			}
		}
		if checkpt.LastCas == nil {
			checkpt.LastCas = make(map[uint32]uint64)
		}
		checkpt.LastCas[colID] = feed.lastCas

		updated, err = json.Marshal(checkpt)
		if err != nil {
			return nil, nil, false, fmt.Errorf("failed to marshal checkpoint: %w", err)
		}
		return updated, nil, false, nil
	})

	if err == nil {
		debug("%s wrote lastCas 0x%x to %q", feed, feed.lastCas, key)
	} else {
		logError("%s failed to write checkpoint to %q: %v", feed, key, err)
	}
	return err
}

func (feed *dcpFeed) run() {
	atomic.AddInt32(&activeFeedCount, 1)
	defer atomic.AddInt32(&activeFeedCount, -1)

	if feed.args.Terminator != nil {
		go func() {
			<-feed.args.Terminator
			debug("%s terminator closed", feed)
			feed.events.close()
		}()
	}

	if feed.args.DoneChan != nil {
		defer close(feed.args.DoneChan)
	}

	collectionID := feed.collection.GetCollectionID()
	feedContent := feed.args.FeedContent
	for {
		if e := feed.events.pull(); e != nil {
			feedEvent, err := e.asFeedEvent(collectionID, feedContent)
			if err != nil {
				logError("Fatal error converting %s event to feed event: %v", feed, err)
				break
			}
			feed.callback(*feedEvent)
			if feedEvent.Cas > feed.lastCas {
				feed.lastCas = feedEvent.Cas
				feed.lastCasChanged = true
				debug("%s lastCas = 0x%x", feed, feed.lastCas)
				// TODO: Set a timer to write the checkpoint "soon"
			}
		} else {
			break
		}
	}
	debug("%s stopping", feed)

	// Recorded before the deferred close of DoneChan, so a waiter sees a settled value. The checkpoint
	// is shared by every collection in the feed, so only StartDCPFeed can decide to delete it.
	feed.completed.Store(!feed.events.closed())

	if feed.lastCasChanged {
		if err := feed.writeCheckpoint(); err != nil {
			logError("Error saving %s checkpoint: %v", feed, err)
		}
	}
}

// allFeedsCompleted reports whether every feed drained to its end-of-feed marker.
func allFeedsCompleted(feeds []*dcpFeed) bool {
	for _, feed := range feeds {
		if !feed.completed.Load() {
			return false
		}
	}
	return len(feeds) > 0
}

// Deletes the feed's checkpoint document, if there is one. A feed given no metadata store never wrote
// one, so there is nothing to delete.
func (feed *dcpFeed) deleteCheckpoint() error {
	if feed.args.CheckpointPrefix == "" || feed.metadataStore == nil {
		return nil
	}
	err := feed.metadataStore.Delete(feed.ctx, feed.checkpointKey())
	if _, ok := errors.AsType[sgbucket.MissingError](err); ok {
		return nil
	}
	return err
}

func (feed *dcpFeed) close() {
	feed.events.close()
}

// stop closes the feed and removes it from its collection's registered feeds, so that nothing keeps pushing
// events to it.  Must not be called with the bucket's lock held.
func (feed *dcpFeed) stop() {
	feed.close()

	c := feed.collection
	c.bucket.mutex.Lock()
	defer c.bucket.mutex.Unlock()
	feeds := c.bucket.collectionFeeds[c.DataStoreNameImpl]
	// A new slice, rather than deleting in place: postEvent iterates the old one without the lock.
	remaining := make([]*dcpFeed, 0, len(feeds))
	for _, f := range feeds {
		if f != feed {
			remaining = append(remaining, f)
		}
	}
	if len(remaining) == 0 {
		delete(c.bucket.collectionFeeds, c.DataStoreNameImpl)
	} else {
		c.bucket.collectionFeeds[c.DataStoreNameImpl] = remaining
	}
}

//////// EVENTS

type event struct {
	opcode     sgbucket.FeedOpcode // FeedOpMutation, FeedOpDeletion, or sentinel opcodes
	key        string              // Doc ID
	value      []byte              // Raw data content, or nil if deleted
	isDeletion bool                // True if it's a deletion event (used for SQL scan)
	isJSON     bool                // Is the data a JSON document?
	xattrs     []byte              // Extended attributes (JSON-encoded)
	cas        CAS                 // Sequence in collection
	exp        Exp                 // Expiration time
	revSeqNo   uint64              // Revision sequence number
}

func (e *event) asFeedEvent(collectionID uint32, feedContent sgbucket.FeedContent) (*sgbucket.FeedEvent, error) {
	// Sentinel events (backfill markers) have no document data
	if e.opcode == sgbucket.FeedOpBeginBackfill || e.opcode == sgbucket.FeedOpEndBackfill {
		return &sgbucket.FeedEvent{Opcode: e.opcode}, nil
	}

	if e.exp != absoluteExpiry(e.exp) {
		panic(fmt.Sprintf("expiry %d isn't absolute", e.exp)) // caller forgot absoluteExpiry()
	}
	if e.revSeqNo == 0 {
		panic("event missing revSeqNo")
	}

	feedEvent := sgbucket.FeedEvent{
		Opcode:       e.opcode,
		CollectionID: collectionID,
		Key:          []byte(e.key),
		Cas:          e.cas,
		Expiry:       e.exp,
		DataType:     ifelse(e.isJSON, sgbucket.FeedDataTypeJSON, sgbucket.FeedDataTypeRaw),
		RevNo:        e.revSeqNo,
		TimeReceived: time.Now(),
	}

	switch feedContent {
	case sgbucket.FeedContentKeysOnly:
		// No value or xattrs needed
	case sgbucket.FeedContentBodyOnly:
		feedEvent.Value = e.value
	case sgbucket.FeedContentXattrOnly:
		if len(e.xattrs) > 0 {
			xattrs, err := e.parseXattrs()
			if err != nil {
				return nil, fmt.Errorf("couldn't parse xattrs for key %q: %w", e.key, err)
			}
			feedEvent.Value = sgbucket.EncodeValueWithXattrs(nil, xattrs...)
			feedEvent.DataType |= sgbucket.FeedDataTypeXattr
		}
	default: // FeedContentDefault
		if len(e.xattrs) > 0 {
			xattrs, err := e.parseXattrs()
			if err != nil {
				return nil, fmt.Errorf("couldn't parse xattrs for key %q: %w", e.key, err)
			}
			feedEvent.Value = sgbucket.EncodeValueWithXattrs(e.value, xattrs...)
			feedEvent.DataType |= sgbucket.FeedDataTypeXattr
		} else {
			feedEvent.Value = e.value
		}
	}

	return &feedEvent, nil
}

// parseXattrs decodes the JSON-encoded xattrs into sgbucket.Xattr slice.
func (e *event) parseXattrs() ([]sgbucket.Xattr, error) {
	var xattrMap map[string]json.RawMessage
	if err := json.Unmarshal(e.xattrs, &xattrMap); err != nil {
		return nil, err
	}
	xattrs := make([]sgbucket.Xattr, 0, len(xattrMap))
	for k, v := range xattrMap {
		xattrs = append(xattrs, sgbucket.Xattr{Name: k, Value: v})
	}
	return xattrs, nil
}
