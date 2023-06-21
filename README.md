# Rosmar

**Rosmar** is an upgraded rewrite of **Walrus**, a lightweight database back-end for Couchbase Sync Gateway.

(The word _Rosmar_ is Norwegian for "walrus".)

Rosmar uses SQLite, so it is persistent by default, though it can use an in-memory database.

Rosmar supports:

* Collections
* Xattrs
* Subdocuments
* DCP feeds
* Expiration (automatic)
* Metadata purging (on demand)
* Map/reduce views
* Queries (in SQLite SQL, not N1QL!)

## Building It

Rosmar requires an updated version of sg-bucket -- this is on the `walrus-xattrs` branch. Rosmar's `go.mod` file points to the appropriate commit.

## API

Most of Rosmar's API is defined by the interfaces in sg-bucket, but (as with other storage implementations) it has its own concrete structs and custom functions for creating and managing buckets. These are different from Walrus's; they are:

```go
// Rosmar implementation of a collection-aware bucket.
type Bucket struct { ... }

// Options for opening a bucket.
type OpenMode int

const (
    CreateOrOpen   = iota // Create a new bucket, or reopen an existing one.
    CreateNew             // Create a new bucket, or fail if the directory exists.
    ReOpenExisting        // Open an existing bucket, or fail if none exists.
)

// Creates a new bucket, or opens an existing one.
func OpenBucket(urlStr string, mode OpenMode) (bucket *Bucket, err error)

// Creates or re-opens a bucket, like OpenBucket.
// The difference is that the input bucket URL is split into a parent directory URL and a
// bucket name. The bucket will be opened in a subdirectory of the directory URL.
func OpenBucketIn(dirUrlStr string, bucketName string, mode OpenMode) (*Bucket, error)

// Deletes the bucket at the given URL, i.e. the filesystem directory at its path, if it exists.
func DeleteBucketAt(urlStr string) (err error)
```

### Bucket URLs

A Bucket IS identified by a URL with the scheme `rosmar:` and a path. The path is interpreted as a filesystem path naming a directory. The `file:` URL scheme is accepted too.

The special URL `rosmar:/?mode=memory` opens an ephemeral in-memory database. Don't hardcode that URL; use the constant `InMemoryURL` instead.

> Note: The directory contains the SQLite database file `rosmar.sqlite` plus SQLite side files. But its contents should be considered opaque.
