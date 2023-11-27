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
	_ "embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/google/uuid"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// Rosmar implementation of a collection-aware bucket.
// Implements sgbucket interfaces BucketStore, DynamicDataStoreBucket, DeletableStore, MutationFeedStore2.
type Bucket struct {
	url             string         // Filesystem path or other URL
	name            string         // Bucket name
	collections     collectionsMap // Collections, indexed by DataStoreName
	collectionFeeds map[sgbucket.DataStoreNameImpl][]*dcpFeed
	mutex           *sync.Mutex    // mutex for synchronized access to Bucket
	sqliteDB        *sql.DB        // SQLite database handle (do not access; call db() instead)
	expManager      *expiryManager // expiration manager for bucket
	serial          uint32         // Serial number for logging
	inMemory        bool           // True if it's an in-memory database
	closed          bool           // represents state when it is closed
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

var lastSerial uint32

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
func OpenBucket(urlStr string, bucketName string, mode OpenMode) (b *Bucket, err error) {
	traceEnter("OpenBucket", "%q, %d", urlStr, mode)
	defer func() { traceExit("OpenBucket", err, "ok") }()

	ctx := context.TODO()
	u, err := encodeDBURL(urlStr)
	if err != nil {
		return nil, err
	}
	urlStr = u.String()

	bucket, err := getCachedBucket(bucketName, urlStr, mode)
	if err != nil {
		return nil, err
	}
	if bucket != nil {
		return bucket, nil
	}

	query := u.Query()
	inMemory := query.Get("mode") == "memory"
	if inMemory {
		if mode == ReOpenExisting {
			return nil, fs.ErrNotExist
		}
		u = u.JoinPath(bucketName)
	} else {
		dir := u.Path
		if runtime.GOOS == "windows" {
			dir = strings.TrimPrefix(dir, "/")
		}

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
	}

	// See https://github.com/mattn/go-sqlite3#connection-string
	query.Add("_auto_vacuum", "1")      // Full auto-vacuum
	query.Add("_busy_timeout", "10000") // 10-sec timeout for db-busy
	query.Add("_foreign_keys", "1")     // Enable foreign-key constraints
	query.Add("_journal_mode", "WAL")   // Use write-ahead log (supports read during write)
	query.Add("_txlock", "immediate")   // Transaction grabs file lock ASAP when it begins
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

	serial := atomic.AddUint32(&lastSerial, 1)
	info("Opening Rosmar bucket B#%d on %s", serial, u)
	db, err := sql.Open("sqlite3_for_rosmar", u.String())
	if err != nil {
		return nil, err
	}
	// An in-memory db cannot have multiple connections (or each would be a separate database!)
	db.SetMaxOpenConns(ifelse(inMemory, 1, kMaxOpenConnections))

	bucket = &Bucket{
		url:             urlStr,
		sqliteDB:        db,
		collections:     make(map[sgbucket.DataStoreNameImpl]*Collection),
		collectionFeeds: make(map[sgbucket.DataStoreNameImpl][]*dcpFeed),
		mutex:           &sync.Mutex{},
		inMemory:        inMemory,
		serial:          serial,
	}
	bucket.expManager = newExpirationManager(bucket.doExpiration)
	defer func() {
		if err != nil {
			_ = bucket.CloseAndDelete(ctx)
		}
	}()

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
	}
	err = bucket.setName(bucketName)
	if err != nil {
		return nil, err
	}

	exists, bucketCopy := registerBucket(bucket)
	// someone else beat registered the bucket in the registry, that's OK we'll close ours
	if exists {
		bucket.Close(ctx)
	}
	// only schedule expiration if bucket is not new. This doesn't need to be locked because only one bucket will execute this code.
	if vers != 0 {
		bucket._scheduleExpiration()
	}

	return bucketCopy, err
}

// Creates or re-opens a bucket, like OpenBucket.
// The difference is that the input bucket URL is split into a parent directory URL and a
// bucket name. The bucket will be opened in a subdirectory of the directory URL.
func OpenBucketIn(dirUrlStr string, bucketName string, mode OpenMode) (*Bucket, error) {
	if isInMemoryURL(dirUrlStr) {
		return OpenBucket(dirUrlStr, bucketName, mode)
	}

	u, err := parseDBFileURL(dirUrlStr)
	if err != nil {
		return nil, err
	}
	return OpenBucket(u.JoinPath(bucketName).String(), bucketName, mode)
}

// Deletes the bucket at the given URL, i.e. the filesystem directory at its path, if it exists.
// If given `InMemoryURL` it's a no-op.
// Warning: Never call this while there are any open Bucket instances on this URL!
func DeleteBucketAt(urlStr string) (err error) {
	traceEnter("DeleteBucketAt", "%q", urlStr)
	defer func() { traceExit("DeleteBucket", err, "ok") }()
	if isInMemoryURL(urlStr) {
		return nil
	}
	info("DeleteBucketAt(%q)", urlStr)
	u, err := parseDBFileURL(urlStr)
	if err != nil {
		return err
	} else if u.Query().Get("mode") == "memory" {
		return nil
	}

	// For safety's sake, don't delete just any directory. Ensure it contains a db file:
	dir := u.Path
	if runtime.GOOS == "windows" {
		dir = strings.TrimPrefix(dir, "/")
	}

	err = os.Remove(filepath.Join(dir, kDBFilename))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	} else {
		return os.Remove(dir)
	}
}

func isInMemoryURL(urlStr string) bool {
	return urlStr == InMemoryURL || urlStr == "file:/?mode=memory" || urlStr == "walrus:"
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
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	} else if u.Scheme != "" && u.Scheme != "file" && u.Scheme != URLScheme {
		return nil, fmt.Errorf("rosmar requires rosmar: or file: URLs, not %+v", u)
	} else if u.User != nil || u.Host != "" || u.Fragment != "" {
		return nil, fmt.Errorf("rosmar URL may not have user, host or fragment %+v", u)
	} else if u.RawQuery != "" && u.RawQuery != "mode=memory" {
		return nil, fmt.Errorf("unsupported query in rosmar URL: %+v", u)
	}

	return u, err
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
		return fmt.Errorf("Rosmar SQL schema is invalid: %w", err)
	}
	bucket.name = bucketName
	return
}

// db returns the database handle as a `queryable` interface value.
// If the bucket has been closed, it returns a special `closedDB` value that will return
// ErrBucketClosed from any call.
func (bucket *Bucket) db() queryable {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()
	return bucket._db()
}

// db returns the database handle as a `queryable` interface value. This is the same as `db()` without locking. This is not safe to call without bucket.mutex being locked the caller.
func (bucket *Bucket) _db() queryable {
	if bucket.closed {
		return closedDB{}
	}
	return bucket.sqliteDB
}

// Runs a function within a SQLite transaction.
func (bucket *Bucket) inTransaction(fn func(txn *sql.Tx) error) error {
	// SQLite allows only a single writer, so use a mutex to avoid BUSY and LOCKED errors.
	// However, these errors can still occur (somehow?), so we retry if we get one.
	// --Update, 25 July 2023: After adding "_txlock=immediate" to the DB options when opening,
	// the busy/locked errors have gone away. But there's no harm leaving the retry code in place.
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	if bucket.closed {
		return ErrBucketClosed
	}

	var err error
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			warn("Transaction failed with: %s; retrying (#%d) ...", err, attempt+1)
			time.Sleep(time.Duration((10 * attempt * attempt) * int(time.Millisecond)))
		}

		var txn *sql.Tx
		txn, err = bucket.sqliteDB.Begin()
		if err != nil {
			break
		}

		err = fn(txn)

		if err == nil {
			err = txn.Commit()
		}

		if err != nil {
			_ = txn.Rollback()
			if sqliteErrCode(err) == sqlite3.ErrBusy || sqliteErrCode(err) == sqlite3.ErrLocked {
				continue // retry
			} else {
				debug("Transaction aborted with error %T %v", err, err)
			}
		} else if attempt > 0 {
			warn("Transaction: COMMIT successful on attempt #%d", attempt+1)
		}
		break
	}
	return remapError(err)
}

// Make a copy of the bucket, but it holds the same underlying structures.
func (b *Bucket) copy() *Bucket {
	r := &Bucket{
		url:             b.url,
		name:            b.name,
		collectionFeeds: b.collectionFeeds,
		collections:     make(collectionsMap),
		mutex:           b.mutex,
		sqliteDB:        b.sqliteDB,
		expManager:      b.expManager,
		serial:          b.serial,
		inMemory:        b.inMemory,
	}
	return r
}

// uriFromPath converts a file path to a rosmar URI. On windows, these need to have forward slashes and drive letters will have an extra /, such as romsar://c:/foo/bar.
func uriFromPath(path string) string {
	uri := "rosmar://"
	if runtime.GOOS != "windows" {
		return uri + path
	}
	path = filepath.ToSlash(path)
	if !filepath.IsAbs(path) {
		return uri + path
	}
	return uri + "/" + path
}
