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
	"expvar"
	"fmt"
	"sync/atomic"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

var activeFeedCount int32 // for tests

//////// BUCKET API: (sgbucket.MutationFeedStore interface)

func (bucket *Bucket) StartDCPFeed(
	ctx context.Context,
	args sgbucket.FeedArguments,
	callback sgbucket.FeedEventCallbackFunc,
	dbStats *expvar.Map,
) error {
	traceEnter("StartDCPFeed", "bucket=%s, args=%+v", bucket.GetName(), args)
	// If no scopes are specified, return feed for the default collection, if it exists
	if len(args.Scopes) == 0 {
		return bucket.DefaultDataStore().(*Collection).StartDCPFeed(ctx, args, callback, dbStats)
	}

	// Validate requested collections exist before starting feeds
	requestedCollections := make([]*Collection, 0)
	for scopeName, collections := range args.Scopes {
		for _, collectionName := range collections {
			collection, err := bucket.getCollection(sgbucket.DataStoreNameImpl{Scope: scopeName, Collection: collectionName})
			if err != nil {
				return fmt.Errorf("DCPFeed args specified unknown collection: %s:%s", scopeName, collectionName)
			}
			requestedCollections = append(requestedCollections, collection)
		}
	}

	doneChan := args.DoneChan
	doneChans := map[*Collection]chan struct{}{}
	for _, collection := range requestedCollections {
		// Not bothering to remove scopes from args for the single collection feeds
		// here because it's ignored by Collection.StartDCPFeed
		collectionID := collection.GetCollectionID()
		collectionAwareCallback := func(event sgbucket.FeedEvent) bool {
			event.CollectionID = collectionID
			return callback(event)
		}

		// have each collection maintain its own doneChan
		doneChans[collection] = make(chan struct{})
		argsCopy := args
		argsCopy.DoneChan = doneChans[collection]

		// Ignoring error is safe because Collection doesn't have error scenarios for StartDCPFeed
		_ = collection.StartDCPFeed(ctx, argsCopy, collectionAwareCallback, dbStats)
	}

	// coalesce doneChans
	go func() {
		for _, collection := range requestedCollections {
			<-doneChans[collection]
		}
		if doneChan != nil {
			close(doneChan)
		}
	}()

	return nil
}

//////// COLLECTION API:

func (c *Collection) StartDCPFeed(
	ctx context.Context,
	args sgbucket.FeedArguments,
	callback sgbucket.FeedEventCallbackFunc,
	dbStats *expvar.Map,
) error {
	traceEnter("StartDCPFeed", "collection=%s, args=%+v", c, args)
	feed := &dcpFeed{
		ctx:        ctx,
		collection: c,
		args:       args,
		callback:   callback,
	}
	feed.events.init()

	if args.Backfill != sgbucket.FeedNoBackfill {
		startCas := args.Backfill
		if args.Backfill == sgbucket.FeedResume {
			if args.CheckpointPrefix == "" {
				return fmt.Errorf("feed's Backfill is FeedResume but no CheckpointPrefix given")
			}
			if err := feed.readCheckpoint(); err != nil {
				return fmt.Errorf("couldn't read DCP feed checkpoint: %w", err)
			}
			startCas = feed.lastCas + 1
		}

		debug("%s starting backfill from CAS 0x%x", feed, startCas)
		feed.events.push(&event{opcode: sgbucket.FeedOpBeginBackfill})
		err := c.enqueueBackfillEvents(startCas, args.FeedContent, &feed.events)
		if err != nil {
			return err
		}
		debug("%s ended backfill", feed)
		feed.events.push(&event{opcode: sgbucket.FeedOpEndBackfill})
	}

	if args.Dump {
		feed.events.push(nil) // push an eof
	} else {
		// Register the feed with the collection for future notifications:
		c.bucket.mutex.Lock()
		c.bucket.collectionFeeds[c.DataStoreNameImpl] = append(c.bucket.collectionFeeds[c.DataStoreNameImpl], feed)
		c.bucket.mutex.Unlock()
	}
	go feed.run()
	return nil
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
			eCopy := *e // each feed gets its own copy to avoid data races
			feed.events.push(&eCopy)
		}
	}
}

// stops all feeds. Caller MUST hold the bucket's lock.
func (c *Collection) _stopFeeds() {
	for _, feed := range c.bucket.collectionFeeds[c.DataStoreNameImpl] {
		feed.close()
	}
	c.bucket.collectionFeeds = nil
}

//////// DCPFEED:

type eventQueue = queue[*event]

type checkpoint struct {
	LastSeq uint64 `json:"last_seq"`
}

type dcpFeed struct {
	ctx            context.Context // TODO: Use this
	collection     *Collection
	args           sgbucket.FeedArguments
	callback       sgbucket.FeedEventCallbackFunc
	events         eventQueue
	lastCas        CAS
	lastCasChanged bool
}

func (feed *dcpFeed) String() string {
	return fmt.Sprintf("Feed(%s %s)", feed.collection, feed.args.ID)
}

func (feed *dcpFeed) checkpointKey() string {
	return feed.args.CheckpointPrefix + ":" + feed.args.ID
}

// Reads the feed's lastCas from the checkpoint document, if there is one.
func (feed *dcpFeed) readCheckpoint() (err error) {
	if feed.args.CheckpointPrefix == "" {
		return
	}
	key := feed.checkpointKey()
	var checkpt checkpoint
	if _, err = feed.collection.Get(key, &checkpt); err != nil {
		if _, ok := err.(sgbucket.MissingError); ok {
			err = nil
			debug("%s checkpoint %q missing", feed, key)
		} else {
			logError("%s failed to read lastCas from %q: %v", feed, key, err)
		}
		return
	}
	feed.lastCas = checkpt.LastSeq
	debug("%s read lastCas 0x%x from %q", feed, feed.lastCas, key)
	return
}

// Writes the feed's lastCas to the checkpoint document, if there is one.
func (feed *dcpFeed) writeCheckpoint() (err error) {
	if feed.args.CheckpointPrefix == "" || !feed.lastCasChanged {
		return
	}
	key := feed.checkpointKey()
	err = feed.collection.Set(key, 0, nil, checkpoint{LastSeq: feed.lastCas})
	if err == nil {
		debug("%s wrote lastCas 0x%x to %q", feed, feed.lastCas, key)
	} else {
		logError("%s failed to write lastCas to %q: %v", feed, key, err)
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

	if feed.lastCasChanged {
		if err := feed.writeCheckpoint(); err != nil {
			logError("Error saving %s checkpoint: %v", feed, err)
		}
	}
}

func (feed *dcpFeed) close() {
	feed.events.close()
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
