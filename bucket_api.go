package rosmar

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

//////// Interface BucketStore

// The URL used to open the bucket.
func (bucket *Bucket) GetURL() string { return bucket.url }

// The bucket's name. This defaults to the last path component of the URL.
func (bucket *Bucket) GetName() string { return bucket.name }

// Renames the bucket. This doesn't affect its URL, only the value returned by GetName.
func (bucket *Bucket) SetName(name string) error {
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
func (bucket *Bucket) Close() {
	traceEnter("Bucket.Close", "")
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	if bucket.expTimer != nil {
		bucket.expTimer.Stop()
	}
	for _, c := range bucket.collections {
		c.close()
	}
	if bucket._db != nil {
		bucket._db.Close()
		bucket._db = nil
		bucket.collections = nil
	}
}

// Closes a bucket and deletes its directory and files (unless it's in-memory.)
func (bucket *Bucket) CloseAndDelete() (err error) {
	bucket.Close()

	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()
	if bucket.url != "" {
		err = DeleteBucket(bucket.url)
		bucket.url = ""
	}
	return err
}

func (bucket *Bucket) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	switch feature {
	case sgbucket.BucketStoreFeatureCollections:
		return true
	case sgbucket.BucketStoreFeatureSubdocOperations:
		return false //true
	case sgbucket.BucketStoreFeatureXattrs:
		return true
	case sgbucket.BucketStoreFeatureCrc32cMacroExpansion:
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
	traceEnter("DefaultDataStore", "")
	collection, err := bucket.getOrCreateCollection(defaultDataStoreName, true)
	if err != nil {
		warn("DefaultDataStore() ->  %v", err)
		return nil
	}
	return collection
}

func (bucket *Bucket) NamedDataStore(name sgbucket.DataStoreName) (sgbucket.DataStore, error) {
	traceEnter("NamedDataStore", "%q", name)
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

func (bucket *Bucket) CreateDataStore(name sgbucket.DataStoreName) error {
	traceEnter("CreateDataStore", "%q", name)
	sc, err := validateName(name)
	if err != nil {
		return err
	}
	_, err = bucket.createCollection(sc)
	return err
}

func (bucket *Bucket) DropDataStore(name sgbucket.DataStoreName) error {
	traceEnter("DropDataStore", "%q", name)
	sc, err := validateName(name)
	if err != nil {
		return err
	}
	return bucket.dropCollection(sc)
}

func (bucket *Bucket) ListDataStores() (result []sgbucket.DataStoreName, err error) {
	traceEnter("ListDataStores", "")
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

func (bucket *Bucket) getCollectionID(scope, collection string) (id CollectionID, err error) {
	row := bucket.db().QueryRow(`SELECT id FROM collections WHERE scope=?1 AND name=?2`, scope, collection)
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
	result, err := bucket.db().Exec(`INSERT INTO collections (scope, name) VALUES (?, ?)`, name.Scope, name.Collection)
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

	id, err := bucket.getCollectionID(name.Scope, name.Collection)
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

func (bucket *Bucket) getCollectionByID(id CollectionID) (*Collection, error) {
	// unused (so far)
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	for _, coll := range bucket.collections {
		if coll.id == id {
			return coll, nil
		}
	}
	row := bucket.db().QueryRow(`SELECT scope,name FROM collections WHERE id=?1`, id)
	var scope, name string
	err := scan(row, &scope, &name)
	if err != nil {
		return nil, err
	}
	dsName := sgbucket.DataStoreNameImpl{Scope: scope, Collection: name}
	return bucket._initCollection(dsName, id), nil
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

	_, err := bucket.db().Exec(`DELETE FROM collections WHERE scope=? AND name=?`, name.ScopeName(), name.CollectionName())
	if err != nil {
		return err
	}
	return nil
}

//////// EXPIRATION (CUSTOM API):

// Returns the earliest expiration time of any document, or 0 if none.
func (bucket *Bucket) NextExpiration() (exp Exp, err error) {
	var expVal sql.NullInt64
	row := bucket.db().QueryRow(`SELECT min(exp) FROM documents WHERE exp > 0`)
	err = scan(row, &expVal)
	if expVal.Valid {
		exp = Exp(expVal.Int64)
	}
	return
}

// Immediately deletes all expired documents in this bucket.
func (bucket *Bucket) ExpireDocuments() (int64, error) {
	names, err := bucket.ListDataStores()
	if err != nil {
		return 0, err
	}
	var count int64
	for _, name := range names {
		if coll, err := bucket.getCollection(name.(sgbucket.DataStoreNameImpl)); err != nil {
			return 0, err
		} else if n, err := coll.ExpireDocuments(); err != nil {
			return 0, err
		} else {
			count += n
		}
	}
	return count, nil
}

func (bucket *Bucket) scheduleExpiration() {
	if nextExp, err := bucket.NextExpiration(); err == nil && nextExp > 0 {
		bucket.scheduleExpirationAtOrBefore(nextExp)
	}
}

func (bucket *Bucket) scheduleExpirationAtOrBefore(exp uint32) {
	if exp > 0 {
		bucket.mutex.Lock()
		defer bucket.mutex.Unlock()
		if exp < bucket.nextExp || bucket.nextExp == 0 {
			bucket.nextExp = exp
			dur := expDuration(exp)
			if dur < 0 {
				dur = 0
			}
			debug("EXP: Scheduling in %s", dur)
			if bucket.expTimer == nil {
				bucket.expTimer = time.AfterFunc(dur, bucket.doExpiration)
			} else {
				bucket.expTimer.Reset(dur)
			}
		}
	}
}

func (bucket *Bucket) doExpiration() {
	bucket.mutex.Lock()
	bucket.nextExp = 0
	bucket.mutex.Unlock()

	debug("EXP: Running scheduled expiration...")
	bucket.ExpireDocuments()

	bucket.scheduleExpiration()
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
