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

func (bucket *Bucket) GetFeedType() sgbucket.FeedType {
	return sgbucket.DcpFeedType
}

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

func (wh *Bucket) StartTapFeed(
	args sgbucket.FeedArguments,
	dbStats *expvar.Map,
) (sgbucket.MutationFeed, error) {
	return nil, &ErrUnimplemented{"rosmar bucket doesn't support tap feed, use DCP"}
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
		feed.events.push(&sgbucket.FeedEvent{Opcode: sgbucket.FeedOpBeginBackfill})
		err := c.enqueueBackfillEvents(startCas, args.KeysOnly, &feed.events)
		if err != nil {
			return err
		}
		debug("%s ended backfill", feed)
		feed.events.push(&sgbucket.FeedEvent{Opcode: sgbucket.FeedOpEndBackfill})
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

func (c *Collection) enqueueBackfillEvents(startCas uint64, keysOnly bool, q *eventQueue) error {
	sql := fmt.Sprintf(`SELECT key, %s, %s, isJSON, cas FROM documents
						WHERE collection=?1 AND cas >= ?2 AND value NOT NULL
						ORDER BY cas`,
		ifelse(keysOnly, `null`, `value`),
		ifelse(keysOnly, `null`, `xattrs`))
	rows, err := c.db().Query(sql, c.id, startCas)
	if err != nil {
		return err
	}
	e := event{collectionID: c.GetCollectionID()}
	for rows.Next() {
		if err := rows.Scan(&e.key, &e.value, &e.xattrs, &e.isJSON, &e.cas); err != nil {
			return err
		}
		q.push(e.asFeedEvent())
	}
	return rows.Close()
}

func (c *Collection) postNewEvent(e *event) {
	info("DCP: %s cas 0x%x: %q = %#.50q ---- xattrs %#q", c, e.cas, e.key, e.value, e.xattrs)
	e.collectionID = c.GetCollectionID()
	feedEvent := e.asFeedEvent()

	c.postEvent(feedEvent)
	c.bucket.scheduleExpirationAtOrBefore(e.exp)

	/*
		// Tell collections of other buckets on the same db file to post the event too:
		for _, otherBucket := range bucketsAtURL(c.bucket.url) {
			if otherBucket != c.bucket {
				if otherCollection := otherBucket.getOpenCollectionByID(c.id); otherCollection != nil {
					otherCollection.postEvent(feedEvent)
				}
			}
		}
	*/
}

func (c *Collection) postEvent(event *sgbucket.FeedEvent) {
	c.bucket.mutex.Lock()
	feeds := c.bucket.collectionFeeds[c.DataStoreNameImpl]
	c.bucket.mutex.Unlock()

	for _, feed := range feeds {
		if feed != nil {
			if feed.args.KeysOnly {
				var eventNoValue sgbucket.FeedEvent = *event // copies the struct
				eventNoValue.Value = nil
				feed.events.push(&eventNoValue)
			} else {
				feed.events.push(event)
			}
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

type eventQueue = queue[*sgbucket.FeedEvent]

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

	for {
		if event := feed.events.pull(); event != nil {
			feed.callback(*event)
			if event.Cas > feed.lastCas {
				feed.lastCas = event.Cas
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
	collectionID uint32 // Public collection ID (not the same as Collection.id)
	key          string // Doc ID
	value        []byte // Raw data content, or nil if deleted
	isDeletion   bool   // True if it's a deletion event
	isJSON       bool   // Is the data a JSON document?
	xattrs       []byte // Extended attributes
	cas          CAS    // Sequence in collection
	exp          Exp    // Expiration time
}

func (e *event) asFeedEvent() *sgbucket.FeedEvent {
	if e.exp != absoluteExpiry(e.exp) {
		panic(fmt.Sprintf("expiry %d isn't absolute", e.exp)) // caller forgot absoluteExpiry()
	}
	feedEvent := sgbucket.FeedEvent{
		Opcode:       ifelse(e.isDeletion, sgbucket.FeedOpDeletion, sgbucket.FeedOpMutation),
		CollectionID: e.collectionID,
		Key:          []byte(e.key),
		Value:        e.value,
		Cas:          e.cas,
		Expiry:       e.exp,
		DataType:     ifelse(e.isJSON, sgbucket.FeedDataTypeJSON, sgbucket.FeedDataTypeRaw),
		// VbNo:     uint16(sgbucket.VBHash(doc.key, kNumVbuckets)),
		TimeReceived: time.Now(),
	}
	if len(e.xattrs) > 0 {
		var xattrMap map[string]json.RawMessage
		_ = json.Unmarshal(e.xattrs, &xattrMap)
		var xattrs []sgbucket.Xattr
		for k, v := range xattrMap {
			xattrs = append(xattrs, sgbucket.Xattr{Name: k, Value: v})
		}
		feedEvent.Value = sgbucket.EncodeValueWithXattrs(e.value, xattrs...)
		feedEvent.DataType |= sgbucket.FeedDataTypeXattr
	}
	return &feedEvent
}

var (
	// Enforce interface conformance:
	_ sgbucket.MutationFeedStore2 = &Bucket{}
)
