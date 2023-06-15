package rosmar

import (
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
	args sgbucket.FeedArguments,
	callback sgbucket.FeedEventCallbackFunc,
	dbStats *expvar.Map,
) error {
	traceEnter("StartDCPFeed", "bucket=%s, args=%+v", bucket.GetName(), args)
	// If no scopes are specified, return feed for the default collection, if it exists
	if args.Scopes == nil || len(args.Scopes) == 0 {
		return bucket.DefaultDataStore().(*Collection).StartDCPFeed(args, callback, dbStats)
	}

	// Validate requested collections exist before starting feeds
	requestedCollections := make([]*Collection, 0)
	for scopeName, collections := range args.Scopes {
		for _, collectionName := range collections {
			collection, err := bucket.getCollection(sgbucket.DataStoreNameImpl{scopeName, collectionName})
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
		// here because it's ignored by RosmarBucket's StartDCPFeed
		collectionID := collection.GetCollectionID()
		collectionAwareCallback := func(event sgbucket.FeedEvent) bool {
			event.CollectionID = collectionID
			return callback(event)
		}

		// have each collection maintain its own doneChan
		doneChans[collection] = make(chan struct{})
		argsCopy := args
		argsCopy.DoneChan = doneChans[collection]

		// Ignoring error is safe because RosmarBucket doesn't have error scenarios for StartDCPFeed
		_ = collection.StartDCPFeed(argsCopy, collectionAwareCallback, dbStats)
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
	args sgbucket.FeedArguments,
	callback sgbucket.FeedEventCallbackFunc,
	dbStats *expvar.Map,
) error {
	traceEnter("StartDCPFeed", "collection=%s, args=%+v", c, args)
	feed := &dcpFeed{
		collection: c,
		args:       args,
		callback:   callback,
	}
	feed.events.init()

	if args.Backfill != sgbucket.FeedNoBackfill {
		feed.events.push(&sgbucket.FeedEvent{Opcode: sgbucket.FeedOpBeginBackfill})
		err := c.enqueueBackfillEvents(args.Backfill, args.KeysOnly, &feed.events)
		if err != nil {
			return err
		}
		feed.events.push(&sgbucket.FeedEvent{Opcode: sgbucket.FeedOpEndBackfill})
	}

	if args.Dump {
		feed.events.push(nil) // push an eof
	} else {
		// Register the feed with the collection for future notifications:
		c.mutex.Lock()
		c.feeds = append(c.feeds, feed)
		c.mutex.Unlock()
	}
	go feed.run()
	return nil
}

func (c *Collection) enqueueBackfillEvents(startCas uint64, keysOnly bool, q *eventQueue) error {
	sql := fmt.Sprintf(`SELECT key, %s, isJSON, cas FROM documents
						WHERE collection=?1 AND cas >= ?2 AND value NOT NULL
						ORDER BY cas`,
		ifelse(keysOnly, `null`, `value`))
	rows, err := c.db().Query(sql, c.id, startCas)
	if err != nil {
		return err
	}
	e := event{collectionID: c.GetCollectionID()}
	for rows.Next() {
		if err := rows.Scan(&e.key, &e.value, &e.isJSON, &e.cas); err != nil {
			return err
		}
		q.push(e.asFeedEvent())
	}
	return rows.Close()
}

func (c *Collection) postEvent(event *sgbucket.FeedEvent) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.feeds) > 0 {
		info("%s: postEvent(op=%v, %q, cas=0x%x, type=%d, flags=0x%x)", c.DataStoreNameImpl, event.Opcode, event.Key, event.Cas, event.DataType, event.Flags)
		var eventNoValue sgbucket.FeedEvent = *event // copies the struct
		eventNoValue.Value = nil

		for _, feed := range c.feeds {
			if feed != nil {
				if feed.args.KeysOnly {
					feed.events.push(&eventNoValue)
				} else {
					feed.events.push(event)
				}
			}
		}
	}
}

// stops all feeds. Caller MUST hold the bucket's lock.
func (c *Collection) stopFeeds() {
	for _, feed := range c.feeds {
		feed.close()
	}
	c.feeds = nil
}

//////// DCPFEED:

type eventQueue = queue[*sgbucket.FeedEvent]

type dcpFeed struct {
	collection *Collection
	args       sgbucket.FeedArguments
	callback   sgbucket.FeedEventCallbackFunc
	events     eventQueue
}

func (feed *dcpFeed) run() {
	atomic.AddInt32(&activeFeedCount, 1)
	defer atomic.AddInt32(&activeFeedCount, -1)

	if feed.args.Terminator != nil {
		go func() {
			_ = <-feed.args.Terminator
			debug("FEED %s terminator closed", feed.collection)
			feed.events.close()
		}()
	}

	if feed.args.DoneChan != nil {
		defer close(feed.args.DoneChan)
	}

	for {
		if event := feed.events.pull(); event != nil {
			feed.callback(*event)
		} else {
			break
		}
	}
	debug("FEED %s stopping", feed.collection)
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

func (c *Collection) postDocEvent(e *event) {
	e.collectionID = c.GetCollectionID()
	c.postEvent(e.asFeedEvent())
	c.bucket.scheduleExpirationAtOrBefore(e.exp)
}

var (
	// Enforce interface conformance:
	_ sgbucket.MutationFeedStore2 = &Bucket{}
)
