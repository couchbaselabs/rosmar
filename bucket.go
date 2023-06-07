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
// Implements sgbucket interfaces BucketStore, DynamicDataStoreBucket, DeletableStore, MutationFeedStore2.
type Bucket struct {
	url         string         // Filesystem path or other URL
	name        string         // Bucket name
	collections collectionsMap // Collections, indexed by DataStoreName
	mutex       sync.Mutex     // mutex for synchronized access to Bucket
	_db         *sql.DB        // SQLite database handle (do not access; call db() instead)
}

type collectionsMap = map[sgbucket.DataStoreNameImpl]*Collection

// Scheme of Rosmar URLs
const URLScheme = "rosmar"

// URL representing an in-memory database with no file
const InMemoryURL = "rosmar:/?mode=memory"

// Filename used for persistent databases
const kDBFilename = "rosmar.sqlite3"

// Maximum number of SQLite connections to open.
const kMaxOpenConnections = 8

// Number of vbuckets I pretend to have.
const kNumVbuckets = 32

//go:embed schema.sql
var kSchema string

var setupOnce sync.Once

// Creates a new bucket, or opens an existing one.
// The URL should have the scheme 'rosmar' or 'file' and a filesystem path.
// The final component of the path is the directory name that will be created,
// containing the SQLite database files.
// Alternatively, the `InMemoryURL` can be given, to create an in-memory bucket with no file.
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

// Opens an existing bucket; fails if it doesn't exist. See `NewBucket` for details on the URL.
func GetBucket(urlStr string, bucketName string) (bucket *Bucket, err error) {
	bucket, inMemory, err := openBucket(urlStr, true)
	if err != nil {
		return nil, err
	}
	if inMemory {
		err = bucket.initializeSchema(bucketName)
	} else {
		row := bucket.db().QueryRow(`SELECT name FROM bucket`)
		err = scan(row, &bucket.name)
	}
	if err != nil {
		bucket = nil
	}
	return
}

// Deletes the bucket at the given URL. No-op if it's in-memory.
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
	query.Add("_auto_vacuum", "1")      // Full auto-vacuum
	query.Add("_busy_timeout", "10000") // 10-sec timeout for db-busy
	query.Add("_journal_mode", "WAL")   // Use write-ahead log (supports read during write)
	u.RawQuery = query.Encode()
	u.Scheme = "file"
	info("Opening Rosmar db %s", u)

	// One-time registration of the SQLite3 database driver.
	setupOnce.Do(func() {
		connectHook := func(conn *sqlite3.SQLiteConn) error {
			// This hook is called when a SQLite connection opens.
			// Register our custom JSON collator with it:
			var collator sgbucket.JSONCollator
			return conn.RegisterCollation("JSON", func(s1, s2 string) int {
				cmp := collator.CollateRaw([]byte(s1), []byte(s2))
				return cmp
			})
		}
		sql.Register("sqlite3_for_rosmar", &sqlite3.SQLiteDriver{ConnectHook: connectHook})
	})

	db, err := sql.Open("sqlite3_for_rosmar", u.String())
	if err != nil {
		return
	}
	// An in-memory db cannot have multiple connections
	db.SetMaxOpenConns(ifelse(inMemory, 1, kMaxOpenConnections))

	bucket = &Bucket{
		url:         urlStr,
		_db:         db,
		collections: make(map[sgbucket.DataStoreNameImpl]*Collection),
	}
	return
}

// Validates the URL string given to NewBucket or OpenBucket, and converts it to a URL object.
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

// Returns the database handle as a `queryable` interface value.
// If the bucket has been closed, it returns a special `closedDB` value that will return
// ErrBucketClosed from any call.
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
