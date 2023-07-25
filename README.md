# Rosmar

![Walrus god; image by staplesart on DeviantArt](walrus_god_by_staplesart.jpg)

**Rosmar** is an upgraded rewrite of **Walrus**, a lightweight database back-end for Couchbase Sync Gateway.

(The word _Rosmar_ is Norwegian for "walrus".)

Rosmar uses SQLite, so it is persistent by default, though it can use an in-memory database.

Rosmar supports:

* Collections
* Subdocuments
* DCP feeds
* Map/reduce views
* Xattrs 🆕
* Expiration (automatic) 🆕
* Metadata purging (on demand) 🆕
* Queries (in SQLite SQL, not N1QL/SQL++) 🆕

## 1. Building and Using It

Rosmar requires an updated version of sg-bucket -- this is on the `feature/walrus-xattrs` branch. Rosmar's `go.mod` file points to the appropriate commit.

To use Rosmar in Sync Gateway, check out the latter's `feature/walrus_xattrs` branch, in which Walrus has been replaced with Rosmar.

To run SG normally with a Rosmar bucket, use a non-persistent SG config file like this one:

```json
{
  "disable_persistent_config": true,

  "logging": {
    "console": { "log_level": "info", "log_keys": ["*"] }
  },

  "adminInterface": ":4985",

  "databases": {
    "travel-sample": {
      "bucket": "travel-sample",
      "server": "rosmar:///Users/snej/Couchbase/buckets/",
      "enable_shared_bucket_access": true,
      "use_views": true,
      "users": {
        "GUEST": {"disabled": false, "admin_channels": ["*"] }
      }
    }
  }
}
```

The directory given in the `server` property has to exist beforehand. SG will create the bucket in a subdirectory.

## 2. Architecture

### SQLite Bindings

Rosmar primarily uses Go's database-neutral `database/sql` package to issue SQL commands and queries.

The current database driver is [`github.com/mattn/go-sqlite3`](https://pkg.go.dev/github.com/mattn/go-sqlite3), which embeds the SQLite library and uses CGo to bridge to its C API.

An alternative that avoids CGo would be [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite). This package takes the unusual approach of using a custom transpiler to translate the SQLite source code from C to Go; it's thus a pure Go package. I used it initially, but switched because it didn't offer support for custom collations (q.v.)

The `database/sql` API is pretty straightforward, but took me some getting used to because it manages a _pool_ of database connections and transparencly dispatches every request to an available connection. This is great for concurrency, but has some odd (to me) side effects that are worth pointing out:

1. Opening a transaction produces a `Tx` object, which has the same `Exec` and `Query` methods as the regular `DB`. The code performing the transaction must use the `Tx` object instead of the `DB`, even for reads, or it won't be able to read its own writes. (That's because calls to the `DB` will be issued to a different connection that, due to isolation, can't see uncommitted changes in the transaction.) This is easy to overlook when a `Bucket` or `Collection` method running a transaction calls into some helper method that uses the receiver's `db` property to issue queries. The workaround is for such helpers to take a parameter that can be either a `DB` or `Txn`. Surprisingly, the `sql` package doesn't define such an interface, so I had to [define one](queryable.go).
2. When using an in-memory database, each SQLite connection opens up a new independent database. This causes chaos unless you call `DB.SetMaxOpenConns(1)` to limit to a single connection. (This reduces concurrency, but at least in-memory queries are pretty damn fast.)

### Schema

Rosmar implements a bucket as a SQLite database, which may be either in memory (ephemeral), or on disk. Each on-disk bucket is given its own directory, since SQLite creates side-files next to the main database file; treating the database as a directory ensures all the files can be moved or deleted as a group and don't get lost.

The [SQL schema](schema.sql) is pretty straightforward. The tables are:

* **bucket**: A singleton; its one row contains the bucket's name, UUID and the last CAS value it's generated.
* **collections**: Each row is a collection, with a scope and a name as well as its own last CAS value.
* **documents**: A document belongs to a collection and has a key, value, xattrs, CAS and expiration time. The value may or may not be JSON. The xattrs column is either null or a JSON object.
* **designDocs**: A design document belongs to a collection and has a name. It serves as a container for views.
* **views**: A view belongs to a design doc. It has a name, a JS map and/or reduce function, and remembers the latest CAS it's mapped.
* **mapped**: This table contains the individual key/value pairs emitted by map functions. Each row belongs to a view, references its source document, and has a JSON key and value.

There is a custom collation called `JSON`, implemented as a SQLite callback, that compares two JSON values of any type according to the standard ordering used in views. This is applied to the `mapped.key` column so that view queries will automatically return properly sorted results.

As in Walrus, CAS values are produced by a monotonically increasing counter starting from 1; they're not timestamps as in present-day Server. This could be changed pretty easily.

## 3. API

### Bucket Management

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
// If given `InMemoryURL` it's a no-op.
// Warning: Never call this while there are any open Bucket instances on this URL!
func DeleteBucketAt(urlStr string) (err error)
```

### Bucket URLs

A Bucket is identified by a URL with the scheme `rosmar:` and a path. The path is interpreted as a filesystem path naming a directory. The `file:` URL scheme is accepted too.

The special URL `rosmar:/?mode=memory` opens an ephemeral in-memory database. Don't hardcode that URL; use the constant `InMemoryURL` instead.

> Note: The directory contains the SQLite database file `rosmar.sqlite` plus SQLite side files. But its contents should be considered opaque.

### Metadata Purging

Rosmar does not purge tombstones (deleted documents) automatically; call the method `Bucket.PurgeTombstones()`.

### Logging

The variable `Logging` determines the level of logging, along the typical spectrum from `None` to `Trace`. The default is `None`. `Trace` logs the entry and exit of every API call, which can be very useful when debugging.

Log messages are written by calling the function pointer `LoggingCallback`. By default this calls `log.Printf`, but you can set it to point to your own function.

At initialization time, Rosmar checks the environment variable `SG_ROSMAR_LOGGING` and turns on logging if it's present. It recognizes the values `none`, `error`, `warn`, `info`, `debug`, or `trace`.

### SQL Queries

Rosmar doesn't support N1QL/SQL++, but I have implemented an experimental query interface `sgbucket.QueryableStore` that lets the client issue SQLite queries.

Queries should use the pseudo-variable `$_keyspace` in the `FROM` clause to refer to the collection being queried. Rosmar will replace it with the actual collection name. The accessible columns are `id`, `body` and `xattrs`. The query can use SQLite's `->` and `->>` operators to access JSON properties.

There is also a `CreateIndex` method to create indexes to optimize queries. (Each index is added to all collections.)

## 4. Limitations

### Nested Subdocument Properties

Like Walrus, Rosmar's Subdocument implementation only supports top-level document properties. (This would be pretty easy to fix, though.)

### N1QL / SQL++

Supporting N1QL/SQL++ would be a pretty massive task! Even Couchbase Lite's complex query translator only supports a subset of N1QL, and it's missing several features that Sync Gateway uses, like `UNNEST`.

### Performance

Currently, every bucket write call creates and commits its own SQLite transaction. This is a well-known performance anti-pattern in SQLite, because the overhead of committing a transaction is pretty high due to filesystem flush calls. Write-heavy operations can be sped up by orders of magnitude by grouping as many writes as possible in a single transaction.

Since the bucket interface has no notion of transactions, Rosmar would have to heuristically group consecutive writes, leaving a transaction opening between calls and committing it after some brief time interval.

However, while that transaction was open, _all_ operations would have to use it (i.e. call into the specific SQLite connection with the open transaction.) Otherwise reads wouldn't see yet-uncommitted writes, and writes would block (SQLite only allows a single transaction at a time per database file.) This would reduce parallelism of reads, which might be an issue.

## 5. Debugging Tips

Some advice for using Rosmar when debugging something in Sync Gateway:

### API Tracing

If you crank the log level for `KeyWalrus` up to `LevelTrace`, Rosmar will log on entry and exit of every API call. The messages will include the most important parameters and return values. Error returns will be logged at Error level.

### Inspecting a Bucket

If you have a persistent bucket you can use the `sqlite3` CLI tool, even while SG is running, to inspect it using `select` statements.

* The tool supports many different output modes, selectable with the `.mode` command; I find `box` easier to read than the default.
* If you forget the schema, the `.schema` command will dump it.
* The tool can't query the `mapped` table because it has a custom collation.

```
$ sqlite3 /path/to/bucketname/rosmar.sqlite3
sqlite> .mode box
sqlite> select key,value,cas from documents where isJSON order by cas desc limit 10;
┌───────────┬──────────────────────────────────────────────────────────────┬───────┐
│    key    │                            value                             │  cas  │
├───────────┼──────────────────────────────────────────────────────────────┼───────┤
│ doc-30801 │ {"id":9211,"type":"route","airline":"AF","airlineid":"airlin │ 34774 │
│           │ e_137","sourceairport":"BKK","stops":0,"equipment":"320","sc │       │
│           │ hedule":[{"day":0,"utc":"06:03:00","flight":"AF920"},{"day": │       │
│           │ 0,"utc":"15:05:00","flight":"AF040"},{"day":0,"utc":"06:14:0 │       │
│           │ 0","flight":"AF625"},{"day":0,"utc":"07:37:00","flight":"AF0 │       │
│           │ 53"},{"day":1,"utc":"01:57:00","flight":"AF870"},{"day":1,"u │       │
...
...
```
