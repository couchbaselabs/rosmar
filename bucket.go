package rosmar

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
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
	expTimer    *time.Timer    // Schedules expiration of docs
	nextExp     uint32         // Timestamp when expTimer will run (0 if never)
}

type collectionsMap = map[sgbucket.DataStoreNameImpl]*Collection

// Scheme of Rosmar database URLs
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

// Options for opening a bucket.
type OpenMode int

const (
	CreateOrOpen   = iota // Create a new bucket, or reopen an existing one.
	CreateNew             // Create a new bucket, or fail if the directory exists.
	ReOpenExisting        // Open an existing bucket, or fail if none exists.
)

// Creates a new bucket, or opens an existing one.
//
// The URL should have the scheme 'rosmar' or 'file' and a filesystem path.
// The path represents a directory; the SQLite database files will be created inside it.
// Alternatively, the `InMemoryURL` can be given, to create an in-memory bucket with no file.
func OpenBucket(urlStr string, mode OpenMode) (bucket *Bucket, err error) {
	traceEnter("OpenBucket", "%q, %d", urlStr, mode)
	defer func() { traceExit("OpenBucket", err, "ok") }()
	u, err := encodeDBURL(urlStr)
	if err != nil {
		return nil, err
	}
	urlStr = u.String()

	query := u.Query()
	inMemory := query.Get("mode") == "memory"
	var bucketName string
	if inMemory {
		if mode == ReOpenExisting {
			return nil, fs.ErrNotExist
		}
		bucketName = "memory"
	} else {
		dir := u.Path
		if mode != ReOpenExisting {
			if _, err = os.Stat(dir); err == nil {
				if mode == CreateNew {
					err = fs.ErrExist
				}
			} else if errors.Is(err, fs.ErrNotExist) {
				err = os.Mkdir(dir, 0700)
			}
			if err != nil {
				return nil, err
			}
		}

		query.Set("mode", ifelse(mode == ReOpenExisting, "rw", "rwc"))
		u = u.JoinPath(kDBFilename)
		bucketName = path.Base(dir)
	}

	// See https://github.com/mattn/go-sqlite3#connection-string
	query.Add("_auto_vacuum", "1")      // Full auto-vacuum
	query.Add("_busy_timeout", "10000") // 10-sec timeout for db-busy
	query.Add("_foreign_keys", "1")     // Enable foreign-key constraints
	query.Add("_journal_mode", "WAL")   // Use write-ahead log (supports read during write)
	u.RawQuery = query.Encode()
	u.Scheme = "file"

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

	info("Opening Rosmar db %s", u)
	db, err := sql.Open("sqlite3_for_rosmar", u.String())
	if err != nil {
		return nil, err
	}
	// An in-memory db cannot have multiple connections (or each would be a separate database!)
	db.SetMaxOpenConns(ifelse(inMemory, 1, kMaxOpenConnections))

	bucket = &Bucket{
		url:         urlStr,
		_db:         db,
		collections: make(map[sgbucket.DataStoreNameImpl]*Collection),
	}

	// Initialize the schema if necessary:
	var vers int
	err = db.QueryRow(`PRAGMA user_version`).Scan(&vers)
	if err != nil {
		return nil, err
	}
	if vers == 0 {
		if err = bucket.initializeSchema(bucketName); err != nil {
			return nil, err
		}
	} else {
		bucket.scheduleExpiration()
	}
	return bucket, err
}

// Creates or re-opens a bucket, like OpenBucket.
// The difference is that the input bucket URL is split into a parent directory URL and a
// bucket name. The bucket will be opened in a subdirectory of the directory URL.
func OpenBucketIn(dirUrlStr string, bucketName string, mode OpenMode) (*Bucket, error) {
	if isInMemoryURL(dirUrlStr) {
		bucket, err := OpenBucket(dirUrlStr, mode)
		if err == nil {
			bucket.SetName(bucketName)
		}
		return bucket, err
	}

	u, err := parseDBFileURL(dirUrlStr)
	if err != nil {
		return nil, err
	}
	return OpenBucket(u.JoinPath(bucketName).String(), mode)
}

// Deletes the bucket at the given URL, i.e. the filesystem directory at its path, if it exists.
// If given `InMemoryURL` it's a no-op.
// Warning: Never call this while there are any open Bucket instances on this URL!
func DeleteBucket(urlStr string) (err error) {
	traceEnter("DeleteBucket", "%q", urlStr)
	defer func() { traceExit("DeleteBucket", err, "ok") }()
	if urlStr == InMemoryURL {
		return nil
	}
	info("DeleteBucket(%q)", urlStr)
	u, err := parseDBFileURL(urlStr)
	if err != nil {
		return err
	} else if u.Query().Get("mode") == "memory" {
		return nil
	}
	// For safety's sake, don't delete just any directory. Ensure it contains a db file:
	err = os.Remove(u.JoinPath(kDBFilename).Path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err == nil {
		err = os.Remove(u.Path)
	}
	return err
}

func isInMemoryURL(urlStr string) bool {
	return urlStr == InMemoryURL || urlStr == "walrus:"
}

// Validates the URL string given to NewBucket or OpenBucket, and converts it to a URL object.
func encodeDBURL(urlStr string) (*url.URL, error) {
	if isInMemoryURL(urlStr) {
		return &url.URL{
			Scheme:   "file",
			Path:     "/",
			OmitHost: true,
			RawQuery: "mode=memory",
		}, nil
	}
	u, err := parseDBFileURL(urlStr)
	if err != nil {
		return nil, err
	}
	u.Scheme = "rosmar"
	u.OmitHost = true
	return u, nil
}

func parseDBFileURL(urlStr string) (*url.URL, error) {
	if u, err := url.Parse(urlStr); err != nil {
		return nil, err
	} else if u.Scheme != "" && u.Scheme != "file" && u.Scheme != URLScheme {
		return nil, fmt.Errorf("rosmar requires rosmar: or file: URLs")
	} else if u.User != nil || u.Host != "" || u.Fragment != "" {
		return nil, fmt.Errorf("rosmar URL may not have user, host or fragment")
	} else if u.RawQuery != "" && u.RawQuery != "mode=memory" {
		return nil, fmt.Errorf("unsupported query in rosmar URL")
	} else {
		return u, err
	}
}

func (bucket *Bucket) initializeSchema(bucketName string) (err error) {
	uuid := uuid.New().String()
	_, err = bucket.db().Exec(kSchema,
		sql.Named("NAME", bucketName),
		sql.Named("UUID", uuid),
		sql.Named("SCOPE", sgbucket.DefaultScope),
		sql.Named("COLL", sgbucket.DefaultCollection),
	)
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
	// However, BUSY errors can still occur (somehow?), so we retry if we get one.
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
