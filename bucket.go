package rosmar

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"sync"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/google/uuid"

	_ "modernc.org/sqlite"
)

// Rosmar implementation of a collection-aware bucket.
// Implements sgbucket interfaces BucketStore, DynamicDataStoreBucket, DeletableStore
type Bucket struct {
	url           string                                      // Filesystem path or other URL
	name          string                                      // bucket name
	collectionIDs map[sgbucket.DataStoreNameImpl]CollectionID // collectionID by scope and collection name
	collections   map[CollectionID]*Collection                // Collection by collectionID
	lock          sync.Mutex                                  // mutex for synchronized access to Bucket
	db            *sql.DB                                     // SQLite database handle
}

const kMaxOpenConnections = 8

const kNumVbuckets = 32

// URL to open, to create an in-memory database with no file
const InMemoryURL = ":memory:"

//go:embed schema.sql
var kSchema string

func encodeDBURL(urlStr string) (*url.URL, error) {
	if urlStr == InMemoryURL {
		return &url.URL{
			Scheme:   "file",
			Path:     "/",
			OmitHost: true,
			RawQuery: "mode=memory",
		}, nil
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "" && u.Scheme != "file" {
		return nil, fmt.Errorf("rosmar requires file: URLs")
	} else if u.User != nil || u.Host != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("rosmar URL may not have user, host or query")
	}
	u.Scheme = "file"
	u.OmitHost = true
	return u, nil
}

func deleteBucketPath(urlStr string) error {
	if urlStr == InMemoryURL {
		return nil
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return err
	}
	if u.Query().Get("mode") == "memory" {
		return nil
	}
	info("Deleting db at path %s", u.Path)
	err = os.Remove(u.Path)
	_ = os.Remove(urlStr + "_wal")
	_ = os.Remove(urlStr + "_shm")
	if errors.Is(err, fs.ErrNotExist) {
		err = nil
	}
	return err
}

// Value to use as the URL in NewBucket to create a new in-memory database.
//const MemoryBucketURL = ":memory:"

func NewBucket(urlStr, bucketName string) (*Bucket, error) {
	bucket, err := openBucket(urlStr, bucketName, false)
	if err != nil {
		return nil, err
	}

	uuid := uuid.New().String()
	_, err = bucket.db.Exec(kSchema, bucketName, uuid, sgbucket.DefaultScope, sgbucket.DefaultCollection)
	if err != nil {
		_ = bucket.CloseAndDelete()
		panic("Rosmar SQL schema is invalid: " + err.Error())
		//return nil, err
	}

	return bucket, nil
}

func GetBucket(urlStr, bucketName string) (bucket *Bucket, err error) {
	if bucket, err = openBucket(urlStr, bucketName, true); err == nil {
		// sql.Open() doesn't really open the db; run a query to test the connection:
		if _, err = bucket.getCollectionID(sgbucket.DefaultScope, sgbucket.DefaultCollection); err != nil {
			bucket = nil
		}
	}
	return
}

func openBucket(urlStr string, bucketName string, exists bool) (*Bucket, error) {
	u, err := encodeDBURL(urlStr)
	if err != nil {
		return nil, err
	}
	urlStr = u.String()

	query := u.Query()
	inMemory := query.Get("mode") == "memory"
	if inMemory {
		if exists {
			return nil, fmt.Errorf("in-memory db must be created as new")
		}
	} else {
		query.Set("mode", ifelse(exists, "rw", "rwc"))
	}
	query.Add("_pragma", "busy_timeout=10000")
	query.Add("_pragma", "journal_mode=WAL")
	u.RawQuery = query.Encode()
	debug("Opening %s", u)

	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	// An in-memory db cannot have multiple connections
	db.SetMaxOpenConns(ifelse(inMemory, 1, kMaxOpenConnections))

	bucket := &Bucket{
		url:           urlStr,
		name:          bucketName,
		db:            db,
		collections:   make(map[CollectionID]*Collection),
		collectionIDs: make(map[sgbucket.DataStoreNameImpl]CollectionID),
	}
	return bucket, nil
}

func (bucket *Bucket) GetName() string {
	return bucket.name
}

func (bucket *Bucket) GetURL() string {
	return bucket.url
}

func (bucket *Bucket) UUID() (string, error) {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()
	var uuid string
	row := bucket.db.QueryRow(`SELECT uuid FROM bucket;`)
	err := row.Scan(&uuid)
	return uuid, err
}

func (bucket *Bucket) Close() {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()
	for _, c := range bucket.collections {
		c.close()
	}
	if bucket.db != nil {
		bucket.db.Close()
		bucket.db = nil
		bucket.collections = nil
		bucket.collectionIDs = nil
	}
}

func (bucket *Bucket) CloseAndDelete() error {
	bucket.Close()

	bucket.lock.Lock()
	defer bucket.lock.Unlock()
	var err error
	if bucket.url != "" {
		err = deleteBucketPath(bucket.url)
		bucket.url = ""
	}
	return err
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
	collection, err := bucket.getOrCreateCollection(defaultDataStoreName, true)
	if err != nil {
		warn("Unable to retrieve DefaultDataStore for rosmar Bucket: %v", err)
		return nil
	}
	return collection
}

func (bucket *Bucket) NamedDataStore(name sgbucket.DataStoreName) (sgbucket.DataStore, error) {
	sc, err := validateName(name)
	if err != nil {
		return nil, fmt.Errorf("attempting to create/update database with a scope/collection that is %w", err)
	}

	collection, err := bucket.getOrCreateCollection(sc, true)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve NamedDataStore for rosmar Bucket: %v", err)
	}
	return collection, nil
}

func (bucket *Bucket) CreateDataStore(name sgbucket.DataStoreName) error {
	sc, err := validateName(name)
	if err != nil {
		return err
	}
	_, err = bucket.createCollection(sc)
	return err
}

func (bucket *Bucket) DropDataStore(name sgbucket.DataStoreName) error {
	sc, err := validateName(name)
	if err != nil {
		return err
	}
	return bucket.dropCollection(sc)
}

func (bucket *Bucket) ListDataStores() ([]sgbucket.DataStoreName, error) {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	rows, err := bucket.db.Query(`SELECT id, scope, name FROM collections ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var result []sgbucket.DataStoreName
	for rows.Next() {
		var id CollectionID
		var scope, name string
		if err := rows.Scan(&id, &scope, &name); err != nil {
			return nil, err
		}
		result = append(result, sgbucket.DataStoreNameImpl{Scope: scope, Collection: name})
	}
	return result, rows.Close()
}

//////// COLLECTIONS:

func (bucket *Bucket) getCollectionID(scope, collection string) (id CollectionID, err error) {
	row := bucket.db.QueryRow(`SELECT id FROM collections WHERE scope=$1 AND name=$2`, scope, collection)
	err = row.Scan(&id)
	return
}

func (bucket *Bucket) createCollection(name sgbucket.DataStoreNameImpl) (*Collection, error) {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	return bucket._createCollection(name)
}

func (bucket *Bucket) _createCollection(name sgbucket.DataStoreNameImpl) (*Collection, error) {
	result, err := bucket.db.Exec(`INSERT INTO collections (scope, name) VALUES (?, ?)`, name.Scope, name.Collection)
	if err != nil {
		return nil, err
	}
	collectionID, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	return bucket._initCollection(name, CollectionID(collectionID)), nil
}

func (bucket *Bucket) getCollection(name sgbucket.DataStoreNameImpl) (*Collection, error) {
	return bucket.getOrCreateCollection(name, false)
}

func (bucket *Bucket) getOrCreateCollection(name sgbucket.DataStoreNameImpl, orCreate bool) (*Collection, error) {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if collectionID, ok := bucket.collectionIDs[name]; ok {
		return bucket.collections[collectionID], nil
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

func (bucket *Bucket) dropCollection(name sgbucket.DataStoreNameImpl) error {
	if name.IsDefault() {
		return errors.New("default collection cannot be dropped")
	}

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	_, err := bucket.db.Exec(`DELETE FROM collections WHERE scope=? AND name=?`, name.ScopeName(), name.CollectionName())
	if err != nil {
		return err
	}

	if collectionID, ok := bucket.collectionIDs[name]; ok {
		delete(bucket.collections, collectionID)
		delete(bucket.collectionIDs, name)
	}
	return nil
}

func (bucket *Bucket) _initCollection(name sgbucket.DataStoreNameImpl, id CollectionID) *Collection {
	collection := newCollection(bucket, name, id, bucket.db)
	bucket.collections[id] = collection
	bucket.collectionIDs[name] = id
	return collection
}

var (
	// Enforce interface conformance:
	_ sgbucket.BucketStore            = &Bucket{}
	_ sgbucket.DynamicDataStoreBucket = &Bucket{}
	_ sgbucket.DeleteableStore        = &Bucket{}
)
