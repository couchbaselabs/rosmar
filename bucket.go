// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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
	url         string         // Filesystem path or other URL
	name        string         // Bucket name
	collections collectionsMap // Collections, indexed by DataStoreName
	mutex       sync.Mutex     // mutex for synchronized access to Bucket
	_db         *sql.DB        // SQLite database handle (do not access; call db() instead)
	expTimer    *time.Timer    // Schedules expiration of docs
	nextExp     uint32         // Timestamp when expTimer will run (0 if never)
	serial      uint32         // Serial number for logging
	inMemory    bool           // True if it's an in-memory database
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

// OpenBucketFromPath opens a bucket from a filesystem path. See OpenBucket for details.
func OpenBucketFromPath(path string, mode OpenMode) (*Bucket, error) {
	return OpenBucket(uriFromPath(path), mode)
}

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
		bucketName = path.Base(dir)
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
		url:         urlStr,
		_db:         db,
		collections: make(map[sgbucket.DataStoreNameImpl]*Collection),
		inMemory:    inMemory,
		serial:      serial,
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

	registerBucket(bucket)
	return bucket, err
}

// Creates or re-opens a bucket, like OpenBucket.
// The difference is that the input bucket URL is split into a parent directory URL and a
// bucket name. The bucket will be opened in a subdirectory of the directory URL.
func OpenBucketIn(dirUrlStr string, bucketName string, mode OpenMode) (*Bucket, error) {
	if isInMemoryURL(dirUrlStr) {
		bucket, err := OpenBucket(dirUrlStr, mode)
		if err == nil {
			err = bucket.SetName(bucketName)
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

	if len(bucketsAtURL(u.String())) > 0 {
		return fmt.Errorf("there is a Bucket open at that URL")
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
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	if db := bucket._db; db != nil {
		return db
	} else {
		return closedDB{}
	}
}

// Runs a function within a SQLite transaction.
func (bucket *Bucket) inTransaction(fn func(txn *sql.Tx) error) error {
	// SQLite allows only a single writer, so use a mutex to avoid BUSY and LOCKED errors.
	// However, these errors can still occur (somehow?), so we retry if we get one.
	// --Update, 25 July 2023: After adding "_txlock=immediate" to the DB options when opening,
	// the busy/locked errors have gone away. But there's no harm leaving the retry code in place.
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	if bucket._db == nil {
		return ErrBucketClosed
	}

	var err error
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			warn("Transaction failed with: %s; retrying (#%d) ...", err, attempt+1)
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
