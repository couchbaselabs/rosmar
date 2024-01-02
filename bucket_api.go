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
	"database/sql"
	"errors"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
)

func (bucket *Bucket) String() string {
	return fmt.Sprintf("B#%d", bucket.serial)
}

//////// Interface BucketStore

// The URL used to open the bucket.
func (bucket *Bucket) GetURL() string { return bucket.url }

// The bucket's name. This defaults to the last path component of the URL.
func (bucket *Bucket) GetName() string { return bucket.name }

// Renames the bucket. This doesn't affect its URL, only the value returned by GetName.
func (bucket *Bucket) setName(name string) error {
	info("Bucket %s is now named %q", bucket, name)
	_, err := bucket.db().Exec(`UPDATE bucket SET name=?1`, name)
	if err == nil {
		bucket.name = name
	}
	return err
}

// The universally unique ID given the bucket when it was created.
func (bucket *Bucket) UUID() (string, error) {
	var uuid string
	row := bucket.db().QueryRow(`SELECT uuid FROM bucket;`)
	err := scan(row, &uuid)
	return uuid, err
}

// Closes a bucket.
func (bucket *Bucket) Close(_ context.Context) {
	traceEnter("Bucket.Close", "%s", bucket)

	err := unregisterBucket(bucket)
	if err != nil {
		warn("Error unregistering bucket: %v", err)
	}
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	bucket.closed = true
}

// _closeSqliteDB closes the underlying sqlite database and shuts down dcpFeeds. Must have a lock to call this function.
func (bucket *Bucket) _closeSqliteDB() error {
	bucket.expManager.stop()
	for _, c := range bucket.collections {
		c.close()
	}
	if bucket.sqliteDB == nil {
		return nil
	}
	err := bucket.sqliteDB.Close()
	bucket.collections = nil
	return err
}

// Closes a bucket and deletes its directory and files (unless it's in-memory.)
func (bucket *Bucket) CloseAndDelete(ctx context.Context) error {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()
	err := bucket._closeSqliteDB()
	if err != nil {
		return err
	}
	return deleteBucket(ctx, bucket)
}

func (bucket *Bucket) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	switch feature {
	case sgbucket.BucketStoreFeatureCollections:
		return true
	case sgbucket.BucketStoreFeatureSubdocOperations:
		return true
	case sgbucket.BucketStoreFeatureXattrs:
		return true
	case sgbucket.BucketStoreFeatureCrc32cMacroExpansion:
		return true
	case sgbucket.BucketStoreFeaturePreserveExpiry:
		return true
	case sgbucket.BucketStoreFeatureN1ql:
		return false
	default:
		return false
	}
}

func (bucket *Bucket) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	if err == nil {
		return false
	}
	switch errorType {
	case sgbucket.KeyNotFoundError:
		_, ok := err.(sgbucket.MissingError)
		return ok
	default:
		return false
	}
}

func (bucket *Bucket) GetMaxVbno() (uint16, error) {
	return kNumVbuckets, nil
}

//////// DATA STORES:

var defaultDataStoreName = sgbucket.DataStoreNameImpl{
	Scope:      sgbucket.DefaultScope,
	Collection: sgbucket.DefaultCollection,
}

func validateName(name sgbucket.DataStoreName) (sgbucket.DataStoreNameImpl, error) {
	return sgbucket.NewValidDataStoreName(name.ScopeName(), name.CollectionName())
}

func (bucket *Bucket) DefaultDataStore() sgbucket.DataStore {
	traceEnter("DefaultDataStore", "%s", bucket)
	collection, err := bucket.getOrCreateCollection(defaultDataStoreName, true)
	if err != nil {
		warn("DefaultDataStore() ->  %v", err)
		return nil
	}
	return collection
}

func (bucket *Bucket) NamedDataStore(name sgbucket.DataStoreName) (sgbucket.DataStore, error) {
	traceEnter("NamedDataStore", "%s.%s", bucket, name)
	sc, err := validateName(name)
	if err != nil {
		warn("NamedDataStore(%q) -> %v", name, err)
		return nil, err
	}

	collection, err := bucket.getOrCreateCollection(sc, true)
	if err != nil {
		err = fmt.Errorf("unable to retrieve NamedDataStore for rosmar Bucket: %v", err)
		warn("NamedDataStore(%q) -> %v", name, err)
		return nil, err
	}
	return collection, nil
}

func (bucket *Bucket) CreateDataStore(_ context.Context, name sgbucket.DataStoreName) error {
	traceEnter("CreateDataStore", "%s.%s", bucket, name)
	sc, err := validateName(name)
	if err != nil {
		return err
	}
	_, err = bucket.createCollection(sc)
	return err
}

func (bucket *Bucket) DropDataStore(name sgbucket.DataStoreName) error {
	traceEnter("DropDataStore", "%s.%s", bucket, name)
	sc, err := validateName(name)
	if err != nil {
		return err
	}
	return bucket.dropCollection(sc)
}

// ListDataStores returns a list of the names of all data stores in the bucket.
func (bucket *Bucket) ListDataStores() (result []sgbucket.DataStoreName, err error) {
	traceEnter("ListDataStores", "%s", bucket)
	defer func() { traceExit("ListDataStores", err, "%v", result) }()
	rows, err := bucket.db().Query(`SELECT id, scope, name FROM collections ORDER BY id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id CollectionID
		var scope, name string
		if err := rows.Scan(&id, &scope, &name); err != nil {
			return nil, err
		}
		result = append(result, sgbucket.DataStoreNameImpl{Scope: scope, Collection: name})
	}
	err = rows.Close()
	return
}

//////// COLLECTION INTERNALS:

func (bucket *Bucket) _getCollectionID(scope, collection string) (id CollectionID, err error) {
	row := bucket._db().QueryRow(`SELECT id FROM collections WHERE scope=?1 AND name=?2`, scope, collection)
	err = scan(row, &id)
	return
}

func (bucket *Bucket) createCollection(name sgbucket.DataStoreNameImpl) (*Collection, error) {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	return bucket._createCollection(name)
}

// caller must hold bucket mutex
func (bucket *Bucket) _createCollection(name sgbucket.DataStoreNameImpl) (*Collection, error) {
	result, err := bucket._db().Exec(`INSERT INTO collections (scope, name) VALUES (?, ?)`, name.Scope, name.Collection)
	if err != nil {
		return nil, err
	}
	collectionID, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	return bucket._initCollection(name, CollectionID(collectionID)), nil
}

func (bucket *Bucket) _initCollection(name sgbucket.DataStoreNameImpl, id CollectionID) *Collection {
	collection := newCollection(bucket, name, id)
	bucket.collections[name] = collection
	return collection
}

func (bucket *Bucket) getCollection(name sgbucket.DataStoreNameImpl) (*Collection, error) {
	return bucket.getOrCreateCollection(name, false)
}

func (bucket *Bucket) getOrCreateCollection(name sgbucket.DataStoreNameImpl, orCreate bool) (*Collection, error) {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	if collection, ok := bucket.collections[name]; ok {
		return collection, nil
	}

	id, err := bucket._getCollectionID(name.Scope, name.Collection)
	if err == nil {
		return bucket._initCollection(name, id), nil
	} else if err == sql.ErrNoRows {
		if orCreate {
			return bucket._createCollection(name)
		} else {
			return nil, sgbucket.MissingError{Key: name.String()}
		}
	} else {
		return nil, err
	}
}

func (bucket *Bucket) dropCollection(name sgbucket.DataStoreNameImpl) error {
	if name.IsDefault() {
		return errors.New("default collection cannot be dropped")
	}

	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	if c := bucket.collections[name]; c != nil {
		c.close()
		delete(bucket.collections, name)
	}

	_, err := bucket._db().Exec(`DELETE FROM collections WHERE scope=? AND name=?`, name.ScopeName(), name.CollectionName())
	if err != nil {
		return err
	}
	return nil
}

//////// EXPIRATION (CUSTOM API):

// nextExpiration returns the earliest expiration time of any document, or 0 if none.
func (bucket *Bucket) nextExpiration() (exp Exp, err error) {
	var expVal sql.NullInt64
	row := bucket.db().QueryRow(`SELECT min(exp) FROM documents WHERE exp > 0`)
	err = scan(row, &expVal)
	if expVal.Valid {
		exp = Exp(expVal.Int64)
	}
	return
}

// expireDocuments immediately deletes all expired documents in this bucket.
func (bucket *Bucket) expireDocuments() (int64, error) {
	names, err := bucket.ListDataStores()
	if err != nil {
		return 0, err
	}
	var count int64
	for _, name := range names {
		if coll, err := bucket.getCollection(name.(sgbucket.DataStoreNameImpl)); err != nil {
			return 0, err
		} else if n, err := coll.expireDocuments(); err != nil {
			return 0, err
		} else {
			count += n
		}
	}
	return count, nil
}

// scheduleExpiration schedules the next expiration of documents to occur, from the minimum expiration value in the bucket. This requires locking expiration manager.
func (bucket *Bucket) _scheduleExpiration() {
	if nextExp, err := bucket.nextExpiration(); err == nil && nextExp > 0 {
		bucket.expManager._scheduleExpirationAtOrBefore(nextExp)
	}
}

func (bucket *Bucket) doExpiration() {
	bucket.expManager._clearNext()

	debug("EXP: Running scheduled expiration...")
	if n, err := bucket.expireDocuments(); err != nil {
		// If there's an error expiring docs, it means there is a programming error of a leaked expiration goroutine.
		panic("Error expiring docs: " + err.Error())
	} else if n > 0 {
		info("Bucket %s expired %d docs", bucket, n)
	}

	bucket._scheduleExpiration()
}

// Completely removes all deleted documents (tombstones).
func (bucket *Bucket) PurgeTombstones() (count int64, err error) {
	traceEnter("PurgeTombstones", "")
	err = bucket.inTransaction(func(txn *sql.Tx) error {
		result, err := txn.Exec(`DELETE FROM documents WHERE value IS NULL`)
		if err == nil {
			count, err = result.RowsAffected()
		}
		return err
	})
	traceExit("PurgeTombstones", err, "%d", count)
	return
}

var (
	// Enforce interface conformance:
	_ sgbucket.BucketStore            = &Bucket{}
	_ sgbucket.DynamicDataStoreBucket = &Bucket{}
	_ sgbucket.DeleteableStore        = &Bucket{}
)
