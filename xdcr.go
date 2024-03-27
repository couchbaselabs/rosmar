// Copyright 2024-Present Couchbase, Inc.
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
	"fmt"
	"strings"
	"sync/atomic"

	sgbucket "github.com/couchbase/sg-bucket"
)

// XDCR implements a XDCR bucket to bucket setup within rosmar.
type XDCR struct {
	terminator              chan bool
	fromBucketCollectionIDs map[uint32]sgbucket.DataStoreName
	fromBucket              *Bucket
	toBucket                *Bucket
	replicationID           string
	docsWritten             atomic.Uint64
	docsFiltered            atomic.Uint64
	errorCount              atomic.Uint64
}

// NewXDCR creates an instance of XDCR backed by rosmar. This is not started until Start is called.
func NewXDCR(_ context.Context, fromBucket, toBucket *Bucket, opts sgbucket.XDCROptions) (*XDCR, error) {

	return &XDCR{
		fromBucket:              fromBucket,
		toBucket:                toBucket,
		replicationID:           fmt.Sprintf("%s-%s", fromBucket.GetName(), toBucket.GetName()),
		fromBucketCollectionIDs: map[uint32]sgbucket.DataStoreName{},
		terminator:              make(chan bool),
	}, nil

}

// getFromBucketCollectionName returns the collection name for a given collection ID in the from bucket.
func (r *XDCR) getFromBucketCollectionName(collectionID uint32) (sgbucket.DataStoreName, error) {
	dsName, ok := r.fromBucketCollectionIDs[collectionID]
	if ok {
		return dsName, nil
	}

	dataStores, err := r.fromBucket.ListDataStores()

	if err != nil {
		return nil, fmt.Errorf("Could not list data stores: %w", err)

	}

	for _, dsName := range dataStores {
		dataStore, err := r.fromBucket.NamedDataStore(dsName)
		if err != nil {
			return nil, fmt.Errorf("Could not get data store %s: %w", dsName, err)
		}

		if dataStore.GetCollectionID() == collectionID {
			name := dataStore.DataStoreName()
			r.fromBucketCollectionIDs[collectionID] = name
			return name, nil

		}
	}
	return nil, fmt.Errorf("Could not find collection with ID %d", collectionID)
}

// Start starts the replication.
func (r *XDCR) Start(ctx context.Context) error {
	args := sgbucket.FeedArguments{
		ID:         "xdcr-" + r.replicationID,
		Backfill:   sgbucket.FeedNoBackfill,
		Terminator: r.terminator,
	}

	callback := func(event sgbucket.FeedEvent) bool {
		docID := string(event.Key)
		trace("Got event %s, opcode: %s", docID, event.Opcode)
		dsName, err := r.getFromBucketCollectionName(event.CollectionID)
		if err != nil {
			warn("Could not find collection with ID %d for docID %s: %s", event.CollectionID, docID, err)
			r.errorCount.Add(1)
			return false

		}

		switch event.Opcode {
		case sgbucket.FeedOpDeletion, sgbucket.FeedOpMutation:
			if strings.HasPrefix(docID, sgbucket.SyncDocPrefix) && !strings.HasPrefix(docID, sgbucket.Att2Prefix) {
				trace("Filtering doc %s", docID)
				r.docsFiltered.Add(1)
				return true

			}

			toDataStore, err := r.toBucket.NamedDataStore(dsName)
			if err != nil {
				warn("Replicating doc %s, could not find matching datastore for %s in target bucket", event.Key, dsName)
				r.errorCount.Add(1)
				return false
			}

			originalCas, err := toDataStore.Get(docID, nil)
			if err != nil && !toDataStore.IsError(err, sgbucket.KeyNotFoundError) {
				warn("Skipping replicating doc %s, could not perform a kv op get doc in toBucket: %s", event.Key, err)
				r.errorCount.Add(1)
				return false
			}

			/* full LWW conflict resolution is not implemented in rosmar yet

			CBS algorithm is:

			if (command.CAS > document.CAS)
			  command succeeds
			else if (command.CAS == document.CAS)
			  // Check the RevSeqno
			  if (command.RevSeqno > document.RevSeqno)
			    command succeeds
			  else if (command.RevSeqno == document.RevSeqno)
			    // Check the expiry time
			    if (command.Expiry > document.Expiry)
			      command succeeds
			    else if (command.Expiry == document.Expiry)
			      // Finally check flags
			      if (command.Flags < document.Flags)
			        command succeeds


			command fails

			In the current state of rosmar:

			1. all CAS values are unique.
			2. RevSeqno is not implemented
			3. Expiry is implemented and could be compared except all CAS values are unique.
			4. Flags are not implemented

			*/

			if event.Cas <= originalCas {
				trace("Skipping replicating doc %s, cas %d <= %d", docID, event.Cas, originalCas)
				return true
			}

			toCollection, ok := toDataStore.(*Collection)
			if !ok {
				warn("Datastore is not of type Collection, is of type %T", toDataStore)
				r.errorCount.Add(1)
			}

			err = writeDoc(ctx, toCollection, originalCas, event)
			if err != nil {
				warn("Replicating doc %s, could not write doc: %s", event.Key, err)
				r.errorCount.Add(1)
				return false

			}
			r.docsWritten.Add(1)
		}

		return true

	}
	return r.fromBucket.StartDCPFeed(ctx, args, callback, nil)
}

// Stop terminates the replication.
func (r *XDCR) Stop(_ context.Context) error {
	close(r.terminator)
	r.terminator = nil
	return nil
}

// writeDoc writes a document to the target datastore. This will not return an error on a CAS mismatch, but will return error on other types of write.
func writeDoc(ctx context.Context, collection *Collection, originalCas uint64, event sgbucket.FeedEvent) error {
	if event.Opcode == sgbucket.FeedOpDeletion {
		_, err := collection.Remove(string(event.Key), originalCas)
		if !errors.Is(err, sgbucket.CasMismatchErr{}) {
			return err
		}
		return nil
	}

	var xattrs []byte
	var body []byte
	if event.DataType&sgbucket.FeedDataTypeXattr != 0 {
		var err error
		var dcpXattrs []sgbucket.Xattr
		body, dcpXattrs, err = sgbucket.DecodeValueWithXattrs(event.Value)
		if err != nil {
			return err
		}

		xattrs, err = xattrToBytes(dcpXattrs)
		if err != nil {
			return err
		}

	} else {

		body = event.Value

	}

	err := collection.SetWithMeta(ctx, string(event.Key), originalCas, event.Cas, event.Expiry, xattrs, body, event.DataType)

	if !collection.IsError(err, sgbucket.KeyNotFoundError) {
		return err
	}

	return nil

}

// Stats returns the stats of the XDCR replication.

func (r *XDCR) Stats(context.Context) (*sgbucket.XDCRStats, error) {

	return &sgbucket.XDCRStats{
		DocsWritten:  r.docsWritten.Load(),
		DocsFiltered: r.docsFiltered.Load(),
		ErrorCount:   r.errorCount.Load(),
	}, nil
}

// xattrToBytes converts a slice of Xattrs to a byte slice of marshalled json.
func xattrToBytes(xattrs []sgbucket.Xattr) ([]byte, error) {
	xattrMap := make(map[string]json.RawMessage)
	for _, xattr := range xattrs {
		xattrMap[xattr.Name] = xattr.Value
	}
	return json.Marshal(xattrMap)
}
