package rosmar

import (
	"expvar"
	"fmt"
	"sync/atomic"

	sgbucket "github.com/couchbase/sg-bucket"
)

var activeFeedCount int32 // for tests

//////// BUCKET API: (sgbucket.MutationFeedStore interface)

func (bucket *Bucket) StartDCPFeed(
	args sgbucket.FeedArguments,
	callback sgbucket.FeedEventCallbackFunc,
	dbStats *expvar.Map,
) error {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	// If no scopes are specified, return feed for the default collection, if it exists
	if args.Scopes == nil || len(args.Scopes) == 0 {
		return bucket.DefaultDataStore().(*Collection).StartDCPFeed(args, callback, dbStats)
	}

	// Validate requested collections exist before starting feeds
	requestedCollections := make([]*Collection, 0)
	for scopeName, collections := range args.Scopes {
		for _, collectionName := range collections {
			collectionID, err := bucket.getCollectionID(scopeName, collectionName)
			if err != nil {
				return fmt.Errorf("DCPFeed args specified unknown collection: %s:%s", scopeName, collectionName)
			}
			requestedCollections = append(requestedCollections, bucket.collections[collectionID])
		}
	}

	doneChan := args.DoneChan
	doneChans := map[*Collection]chan struct{}{}
	for _, collection := range requestedCollections {
		// Not bothering to remove scopes from args for the single collection feeds
		// here because it's ignored by WalrusBucket's StartDCPFeed
		collectionID := collection.id
		collectionAwareCallback := func(event sgbucket.FeedEvent) bool {
			event.CollectionID = uint32(collectionID)
			return callback(event)
		}

		// have each collection maintain its own doneChan
		doneChans[collection] = make(chan struct{})
		argsCopy := args
		argsCopy.DoneChan = doneChans[collection]

		// Ignoring error is safe because WalrusBucket doesn't have error scenarios for StartDCPFeed
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
	sql := fmt.Sprintf(`SELECT key, %s, cas FROM documents
						WHERE collection=$1 AND cas >= $2 AND value NOT NULL
						ORDER BY cas`,
		ifelse(keysOnly, `null`, `value`))
	rows, err := c.db.Query(sql, c.id, startCas)
	if err != nil {
		return err
	}
	for rows.Next() {
		var doc document
		if err := rows.Scan(&doc.key, &doc.value, &doc.cas); err != nil {
			return err
		}
		q.push(doc.makeEvent(sgbucket.FeedOpMutation))
	}
	return rows.Close()
}

func (c *Collection) postEvent(event *sgbucket.FeedEvent) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	info("%s: postEvent(%v, %q, type=%d, flags=0x%x)", c.DataStoreName, event.Opcode, event.Key, event.DataType, event.Flags)
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

func (c *Collection) postDocEvent(doc *document, op sgbucket.FeedOpcode) {
	c.postEvent(doc.makeEvent(op))
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
}

func (feed *dcpFeed) close() {
	feed.events.close()
}

func (doc *document) makeEvent(opcode sgbucket.FeedOpcode) *sgbucket.FeedEvent {
	event := sgbucket.FeedEvent{
		Opcode:   opcode,
		Key:      []byte(doc.key),
		Cas:      doc.cas,
		DataType: sgbucket.FeedDataTypeJSON,
	}
	if !doc.isJSON {
		event.DataType = sgbucket.FeedDataTypeRaw
	}
	if len(doc.xattrs) > 0 {
		var xattrList []sgbucket.Xattr
		for k, v := range doc.xattrs {
			xattrList = append(xattrList, sgbucket.Xattr{Name: k, Value: v})
		}
		event.Value = sgbucket.EncodeValueWithXattrs(doc.value, xattrList...)
		event.DataType |= sgbucket.FeedDataTypeXattr
	} else {
		event.Value = doc.value
	}
	return &event
}
