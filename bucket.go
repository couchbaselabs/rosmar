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
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/google/uuid"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// Rosmar implementation of a collection-aware bucket.
// Implements sgbucket interfaces BucketStore, DynamicDataStoreBucket, DeletableStore
type Bucket struct {
	url         string         // Filesystem path or other URL
	name        string         // Bucket name
	collections collectionsMap // Collections, indexed by DataStoreName
	mutex       sync.Mutex     // mutex for synchronized access to Bucket
	_db         *sql.DB        // SQLite database handle
}

type collectionsMap = map[sgbucket.DataStoreNameImpl]*Collection

// Scheme of Rosmar URLs
const URLScheme = "rosmar"

// URL to open, to create an in-memory database with no file
const InMemoryURL = "rosmar:/?mode=memory"

var ErrBucketClosed = fmt.Errorf("this Rosmar bucket has been closed")

// Filename used for persistent databases
const kDBFilename = "rosmar.sqlite3"

// Maximum number of SQLite connections to open.
const kMaxOpenConnections = 8

// Number of vbuckets I pretend to have.
const kNumVbuckets = 32

//go:embed schema.sql
var kSchema string

var setupOnce sync.Once

// Creates a new bucket.
func NewBucket(urlStr, bucketName string) (*Bucket, error) {
	bucket, _, err := openBucket(urlStr, false)
	if err != nil {
		return nil, err
	}
	if err = bucket.initializeSchema(bucketName); err != nil {
		bucket = nil
	}
	return bucket, err
}

// Opens an existing bucket.
func GetBucket(urlStr string, bucketName string) (bucket *Bucket, err error) {
	bucket, inMemory, err := openBucket(urlStr, true)
	if err != nil {
		return nil, err
	}
	if inMemory {
		err = bucket.initializeSchema(bucketName)
	} else {
		row := bucket.db().QueryRow(`SELECT name FROM bucket`)
		err = row.Scan(&bucket.name)
	}
	if err != nil {
		bucket = nil
	}
	return
}

func DeleteBucket(urlStr string) error {
	if urlStr == InMemoryURL {
		return nil
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return err
	} else if u.Scheme != "" && u.Scheme != "file" && u.Scheme != URLScheme {
		return fmt.Errorf("not a rosmar: or file: URL")
	} else if u.Query().Get("mode") == "memory" {
		return nil
	}
	u = u.JoinPath(kDBFilename)
	info("Deleting db at path %s", u.Path)
	err = os.Remove(u.Path)
	if errors.Is(err, fs.ErrNotExist) {
		err = nil
	}
	if err == nil {
		_ = os.Remove(urlStr + "_wal")
		_ = os.Remove(urlStr + "_shm")
	}
	return err
}

func openBucket(urlStr string, mustExist bool) (bucket *Bucket, inMemory bool, err error) {
	u, err := encodeDBURL(urlStr)
	if err != nil {
		return
	}
	urlStr = u.String()

	// See https://github.com/mattn/go-sqlite3#connection-string
	query := u.Query()
	inMemory = query.Get("mode") == "memory"
	if !inMemory {
		u = u.JoinPath(kDBFilename)
		query.Set("mode", ifelse(mustExist, "rw", "rwc"))
	}
	query.Add("_busy_timeout", "10000")
	query.Add("_journal_mode", "WAL")
	u.RawQuery = query.Encode()
	u.Scheme = "file"
	info("Opening Rosmar db %s", u)

	setupOnce.Do(func() {
		sql.Register("sqlite3_for_rosmar",
			&sqlite3.SQLiteDriver{
				ConnectHook: connectHook,
			})
	})

	db, err := sql.Open("sqlite3_for_rosmar", u.String())
	if err != nil {
		return
	}
	// An in-memory db cannot have multiple connections
	db.SetMaxOpenConns(ifelse(inMemory, 1, kMaxOpenConnections))

	// A simple query to see if the db is actually available:
	var vers int
	if err = db.QueryRow(`pragma user_version`).Scan(&vers); err != nil {
		err = remapError(err)
		return
	}

	bucket = &Bucket{
		url:         urlStr,
		_db:         db,
		collections: make(map[sgbucket.DataStoreNameImpl]*Collection),
	}
	return
}

func (bucket *Bucket) initializeSchema(bucketName string) (err error) {
	uuid := uuid.New().String()
	_, err = bucket.db().Exec(kSchema, bucketName, uuid, sgbucket.DefaultScope, sgbucket.DefaultCollection)
	if err != nil {
		_ = bucket.CloseAndDelete()
		panic("Rosmar SQL schema is invalid: " + err.Error())
	}
	bucket.name = bucketName
	return
}

func connectHook(conn *sqlite3.SQLiteConn) error {
	warn("CONNECT HOOK!")
	var collator sgbucket.JSONCollator
	return conn.RegisterCollation("JSON", func(s1, s2 string) int {
		cmp := collator.CollateRaw([]byte(s1), []byte(s2))
		warn("COLLATE: %s   <=>   %s   = %d", s1, s2, cmp)
		return cmp
	})
}

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
	if u.Scheme != "" && u.Scheme != "file" && u.Scheme != URLScheme {
		return nil, fmt.Errorf("rosmar requires rosmar: or file: URLs")
	} else if u.User != nil || u.Host != "" || u.Fragment != "" {
		return nil, fmt.Errorf("rosmar URL may not have user, host or fragment")
	} else if u.RawQuery != "" && u.RawQuery != "mode=memory" {
		return nil, fmt.Errorf("unsupported query in rosmar URL")
	}

	u.Scheme = "rosmar"
	u.OmitHost = true
	return u, nil
}

func (bucket *Bucket) db() queryable {
	if db := bucket._db; db != nil {
		return db
	} else {
		return closedDB{}
	}
}

// Runs a function within a SQLite transaction.
func (bucket *Bucket) inTransaction(fn func(txn *sql.Tx) error) error {
	// SQLite allows only a single writer, so use a mutex to avoid BUSY and LOCKED errors.
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	if bucket._db == nil {
		return ErrBucketClosed
	}

	var err error
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			warn("\tinTransaction: %s; retrying (#%d) ...", err, attempt+1)
			time.Sleep(time.Duration((10 * attempt * attempt) * int(time.Millisecond)))
		}

		var txn *sql.Tx
		txn, err = bucket._db.Begin()
		if err != nil {
			break
		}

		err = fn(txn)

		if err == nil {
			err = txn.Commit()
		}

		if err != nil {
			txn.Rollback()
			if sqliteErrCode(err) == sqlite3.ErrBusy {
				continue // retry
			} else {
				logError("\tinTransaction: ROLLBACK with error %T %v", err, err)
			}
		} else if attempt > 0 {
			warn("\tinTransaction: COMMIT successful on attempt #%d", attempt+1)
		}
		break
	}
	return remapError(err)
}

// Returns the earliest expiration time of any document, or 0 if none.
func (bucket *Bucket) NextExpiration() (exp Exp, err error) {
	var expVal sql.NullInt64
	err = bucket.db().QueryRow(`SELECT min(exp) FROM documents WHERE exp > 0`).Scan(&expVal)
	if expVal.Valid {
		exp = Exp(expVal.Int64)
	}
	return
}

// Deletes any expired documents.
func (bucket *Bucket) ExpireDocuments() (count int64, err error) {
	err = bucket.inTransaction(func(txn *sql.Tx) error {
		result, err := txn.Exec(`DELETE FROM documents WHERE exp > 0 AND exp < ?1`,
			time.Now().Unix())
		if err == nil {
			count, err = result.RowsAffected()
			debug("ExpireDocuments: purged %d docs", count)
		}
		return err
	})
	return
}

//////// Interface BucketStore

func (bucket *Bucket) GetURL() string { return bucket.url }

func (bucket *Bucket) GetName() string { return bucket.name }

func (bucket *Bucket) UUID() (string, error) {
	var uuid string
	row := bucket.db().QueryRow(`SELECT uuid FROM bucket;`)
	err := row.Scan(&uuid)
	return uuid, err
}

func (bucket *Bucket) Close() {
	debug("Close(bucket %s)", bucket.GetName())
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	for _, c := range bucket.collections {
		c.close()
	}
	if bucket._db != nil {
		bucket._db.Close()
		bucket._db = nil
		bucket.collections = nil
	}
}

func (bucket *Bucket) CloseAndDelete() error {
	bucket.Close()

	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()
	var err error
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
	debug("DefaultDataStore()")
	collection, err := bucket.getOrCreateCollection(defaultDataStoreName, true)
	if err != nil {
		warn("DefaultDataStore() ->  %v", err)
		return nil
	}
	return collection
}

func (bucket *Bucket) NamedDataStore(name sgbucket.DataStoreName) (sgbucket.DataStore, error) {
	debug("NamedDataStore(%q)", name)
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
	debug("CreateDataStore(%q)", name)
	sc, err := validateName(name)
	if err != nil {
		return err
	}
	_, err = bucket.createCollection(sc)
	return err
}

func (bucket *Bucket) DropDataStore(name sgbucket.DataStoreName) error {
	debug("DropDataStore(%q)", name)
	sc, err := validateName(name)
	if err != nil {
		return err
	}
	return bucket.dropCollection(sc)
}

func (bucket *Bucket) ListDataStores() ([]sgbucket.DataStoreName, error) {
	debug("ListDataStores()")
	rows, err := bucket.db().Query(`SELECT id, scope, name FROM collections ORDER BY id`)
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
	debug("ListDataStores() -> %v", result)
	return result, rows.Close()
}

//////// COLLECTIONS:

func (bucket *Bucket) getCollectionID(scope, collection string) (id CollectionID, err error) {
	row := bucket.db().QueryRow(`SELECT id FROM collections WHERE scope=?1 AND name=?2`, scope, collection)
	err = row.Scan(&id)
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

var (
	// Enforce interface conformance:
	_ sgbucket.BucketStore            = &Bucket{}
	_ sgbucket.DynamicDataStoreBucket = &Bucket{}
	_ sgbucket.DeleteableStore        = &Bucket{}
)
