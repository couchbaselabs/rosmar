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
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	sgbucket "github.com/couchbase/sg-bucket"
)

//TODO: Use prepared statements for performance (sql.Stmt)

var MaxDocSize int = 20 * 1024 * 1024

// A collection within a Bucket.
// Implements sgbucket interfaces DataStore, DataStoreName, ViewStore, QueryableStore
type Collection struct {
	sgbucket.DataStoreNameImpl // Fully qualified name (scope and collection)
	bucket                     *Bucket
	id                         CollectionID // Row ID in collections table; public ID + 1
	mutex                      sync.Mutex
	viewCache                  map[viewKey]*rosmarView
}

type CollectionID uint32
type CAS = uint64
type Exp = uint32

func newCollection(bucket *Bucket, name sgbucket.DataStoreNameImpl, id CollectionID) *Collection {
	c := &Collection{
		bucket:            bucket,
		DataStoreNameImpl: name,
		id:                id,
	}
	info("Opened collection %s on %s", c, c.GetName())
	return c
}

func (c *Collection) String() string {
	return fmt.Sprintf("B#%dc%d", c.bucket.serial, c.GetCollectionID())
}

func (c *Collection) close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c._stopFeeds()
}

func (c *Collection) db() queryable {
	return c.bucket.db()
}

//////// Interface DataStore

func (c *Collection) GetName() string {
	return c.bucket.GetName() + "." + c.DataStoreNameImpl.String()
}

func (c *Collection) GetCollectionID() uint32 {
	return uint32(c.id) - 1 // SG expects that the default collection has id 0, so subtract 1
}

//////// Interface KVStore

//// Raw:

func (c *Collection) Exists(key string) (exists bool, err error) {
	traceEnter("Exists", "%q", key)
	exists, err = c.exists(c.db(), key)
	traceExit("Exists", err, "%v", exists)
	return
}

func (c *Collection) exists(q queryable, key string) (exists bool, err error) {
	row := q.QueryRow(`SELECT 1 FROM documents
							WHERE collection=? AND key=? AND value NOT NULL`, c.id, key)
	var i int
	err = scan(row, &i)
	exists = (err == nil)
	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

func (c *Collection) GetRaw(key string) (val []byte, cas CAS, err error) {
	traceEnter("GetRaw", "%q", key)
	val, cas, err = c.getRaw(c.db(), key)
	traceExit("GetRaw", err, "cas=0x%x, val %s", cas, val)
	return
}

// isTombstone returns true if the document is a tombstone.
func (c *Collection) isTombstone(q queryable, key string) bool {
	row := q.QueryRow("SELECT 1 FROM documents WHERE collection=? AND key=? AND tombstone=1", c.id, key)
	var i int
	err := scan(row, &i)
	return err == nil
}

func (c *Collection) getRaw(q queryable, key string) (val []byte, cas CAS, err error) {
	row := q.QueryRow("SELECT value, cas FROM documents WHERE collection=? AND key=?", c.id, key)
	if err = scan(row, &val, &cas); err != nil {
		err = remapKeyError(err, key)
	} else if val == nil {
		err = sgbucket.MissingError{Key: key}
	}
	return
}

func (c *Collection) GetAndTouchRaw(key string, exp Exp) (val []byte, cas CAS, err error) {
	traceEnter("GetAndTouchRaw", "%q, %d", key, exp)
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (e *event, err error) {
		exp = absoluteExpiry(exp)
		val, cas, err = c.getRaw(txn, key)
		if err == nil {
			_, err = txn.Exec(`UPDATE documents SET exp=?1 WHERE key=?2`, exp, key)
		}
		return
	})
	traceExit("GetAndTouchRaw", err, "cas=0x%x, val %s", cas, val)
	return
}

func (c *Collection) AddRaw(key string, exp Exp, val []byte) (added bool, err error) {
	traceEnter("AddRaw", "%q, %d, ...", key, exp)
	added, err = c.add(key, exp, val, looksLikeJSON(val))
	traceExit("AddRaw", err, "%v", added)
	return
}

// Common implementation of Add and AddRaw.
func (c *Collection) add(key string, exp Exp, val []byte, isJSON bool) (added bool, err error) {
	if err = checkDocSize(len(val)); err != nil {
		return false, err
	}
	var casOut CAS
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (e *event, err error) {
		exp = absoluteExpiry(exp)
		result, err := txn.Exec(
			`INSERT INTO documents (collection,key,value,cas,exp,isJSON) VALUES (?1,?2,?3,?4,?5,?6)
				ON CONFLICT(collection,key) DO
					UPDATE SET value=?3, xattrs=null, cas=?4, exp=?5, isJSON=?6
					WHERE tombstone != 0`,
			c.id, key, val, newCas, exp, isJSON)
		if err != nil {
			return
		}
		casOut = newCas
		n, _ := result.RowsAffected()
		added = (n > 0)

		e = &event{
			key:    key,
			value:  val,
			cas:    casOut,
			exp:    exp,
			isJSON: isJSON,
		}
		e.xattrs, err = c.getRawXattrs(txn, key) // needed for the DCP event
		return
	})
	return
}

func (c *Collection) SetRaw(key string, exp Exp, opts *sgbucket.UpsertOptions, val []byte) (err error) {
	traceEnter("SetRaw", "%q, %d, ...", key, exp)
	err = c.set(key, exp, opts, val, false)
	traceExit("SetRaw", err, "ok")
	return
}

// Common implementation of Set and SetRaw.
func (c *Collection) set(key string, exp Exp, opts *sgbucket.UpsertOptions, val []byte, isJSON bool) (err error) {
	if err = checkDocSize(len(val)); err != nil {
		return err
	}
	return c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		exp = absoluteExpiry(exp)
		xattrs, err := c._set(txn, key, exp, opts, val, isJSON, newCas)
		if err != nil {
			return nil, err
		}
		return &event{
			key:    key,
			value:  val,
			cas:    newCas,
			exp:    exp,
			isJSON: isJSON,
			xattrs: xattrs,
		}, err
	})
}

// Core code of Set/SetRaw/Incr. Must be in a transaction when called.
func (c *Collection) _set(txn *sql.Tx, key string, exp Exp, opts *sgbucket.UpsertOptions, val []byte, isJSON bool, newCas CAS) (xattrs []byte, err error) {
	exp = absoluteExpiry(exp)

	// First get the existing xattrs and exp, and check whether the doc is a tombstone:
	exists := false
	hadValue := false
	var oldExp Exp = 0
	row := txn.QueryRow(`SELECT value NOT NULL, xattrs, exp FROM documents
						WHERE collection=? AND key=?`, c.id, key)
	if err = scan(row, &hadValue, &xattrs, &oldExp); err == nil {
		exists = true
		if !hadValue {
			xattrs = nil // xattrs are cleared whenever resurrecting a tombstone
		}
	} else if err != sql.ErrNoRows {
		err = remapKeyError(err, key)
		return
	}

	// Now write the new state:
	var stmt string
	if exists {
		if opts != nil && opts.PreserveExpiry {
			exp = oldExp
		}
		stmt = `UPDATE documents SET value=?3, xattrs=?4, cas=?5, exp=?6, isJSON=?7
				WHERE collection=?1 AND key=?2`
	} else {
		stmt = `INSERT INTO documents (collection,key,value,xattrs,cas,exp,isJSON)
				VALUES (?1,?2,?3,?4,?5,?6,?7)`
	}
	_, err = txn.Exec(stmt, c.id, key, val, xattrs, newCas, exp, isJSON)
	return
}

// Non-Raw:

func (c *Collection) Get(key string, outVal any) (cas CAS, err error) {
	traceEnter("Get", "%q", key)
	cas, err = c.get(c.db(), key, outVal)
	traceExit("Get", err, "cas=0x%x, val %v", cas, outVal)
	return
}

func (c *Collection) get(q queryable, key string, outVal any) (cas CAS, err error) {
	raw, cas, err := c.getRaw(q, key)
	if err == nil {
		err = decodeRaw(raw, outVal)
	}
	return
}

func (c *Collection) GetExpiry(_ context.Context, key string) (exp Exp, err error) {
	traceEnter("GetExpiry", "%q", key)
	row := c.db().QueryRow("SELECT exp FROM documents WHERE collection=? AND key=?", c.id, key)
	err = scan(row, &exp)
	err = remapKeyError(err, key)
	traceExit("GetExpiry", err, "%d", exp)
	return
}

func (c *Collection) Touch(key string, exp Exp) (cas CAS, err error) {
	_, cas, err = c.GetAndTouchRaw(key, exp)
	return
}

func (c *Collection) Add(key string, exp Exp, val any) (added bool, err error) {
	traceEnter("Add", "%q, %v", key, val)
	raw, err := encodeAsRaw(val, true)
	if err == nil {
		added, err = c.add(key, exp, raw, true)
	}
	traceExit("Add", err, "%v", added)
	return
}

func (c *Collection) Set(key string, exp Exp, opts *sgbucket.UpsertOptions, val any) (err error) {
	traceEnter("Set", "%q, %v", key, val)
	raw, err := encodeAsRaw(val, true)
	if err == nil {
		err = c.set(key, exp, opts, raw, true)
	}
	traceExit("Set", err, "ok")
	return
}

func (c *Collection) WriteCas(key string, flags int, exp Exp, cas CAS, val any, opt sgbucket.WriteOptions) (casOut CAS, err error) {
	// Marshal JSON if the value is not raw:
	isJSON := (opt&(sgbucket.Raw|sgbucket.Append) == 0)
	raw, err := encodeAsRaw(val, isJSON)
	traceEnter("WriteCas", "%q, exp=%d, cas=0x%x, opt=%v, val=%s", key, exp, cas, opt, raw)
	defer func() { traceExit("WriteCas", err, "0x%x", casOut) }()
	if err != nil {
		return 0, err
	}
	if err = checkDocSize(len(raw)); err != nil {
		return
	}
	if raw == nil {
		isJSON = false
	}

	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		wasTombstone := false
		if cas != 0 {
			wasTombstone = c.isTombstone(txn, key)
		}
		exp = absoluteExpiry(exp)
		var sql string
		if (opt & sgbucket.Append) != 0 {
			// Append:
			sql = `UPDATE documents SET value=value || ?1, cas=?2, exp=?6, isJSON=0,
						xattrs=iif(tombstone != 0, null, xattrs)
				   WHERE collection=?3 AND key=?4 AND cas=?5`
		} else if (opt&sgbucket.AddOnly) != 0 || cas == 0 {
			// Insert, but fall back to Update if the doc is a tombstone
			sql = `INSERT INTO documents (collection, key, value, cas, exp, isJSON) VALUES(?3,?4,?1,?2,?6,?7)
					ON CONFLICT(collection,key) DO
						UPDATE SET value=?1, xattrs=null, cas=?2, exp=?6, isJSON=?7, tombstone=0
						WHERE tombstone == 1`
			if !wasTombstone && cas != 0 {
				sql += ` AND cas=?5`
			}
		} else {
			// Regular write:
			sql = `UPDATE documents SET value=?1, cas=?2, exp=?6, isJSON=?7,
						xattrs=iif(tombstone != 0, null, xattrs)
				   WHERE collection=?3 AND key=?4 AND cas=?5`
		}
		result, err := txn.Exec(sql, raw, newCas, c.id, key, cas, exp, isJSON)
		if err != nil {
			return nil, err
		}
		if nRows, _ := result.RowsAffected(); nRows == 0 {
			// SQLite didn't insert/update anything. Why not?
			if _, existingCas, err2 := c.getRaw(txn, key); err2 == nil {
				if opt&sgbucket.AddOnly != 0 {
					err = sgbucket.ErrKeyExists
				} else {
					err = sgbucket.CasMismatchErr{Expected: cas, Actual: existingCas}
				}
			} else {
				err = err2
			}
			return nil, err
		}

		xattrs, err := c.getRawXattrs(txn, key) // needed for the DCP event
		if err != nil {
			return nil, err
		}
		casOut = newCas
		return &event{
			key:        key,
			value:      raw,
			isDeletion: (raw == nil),
			cas:        newCas,
			exp:        exp,
			isJSON:     isJSON,
			xattrs:     xattrs,
		}, nil
	})
	return
}

// Remove creates a document tombstone. It removes the document's value and user xattrs.
func (c *Collection) Remove(key string, cas CAS) (casOut CAS, err error) {
	traceEnter("Remove", "%q, 0x%x", key, cas)
	casOut, err = c.remove(key, &cas)
	traceExit("Remove", err, "0x%x", casOut)
	return
}

// Delete creates a document tombstone. It removes the document's value and user xattrs. Equivalent to Remove without a CAS check.
func (c *Collection) Delete(key string) (err error) {
	traceEnter("Delete", "%q", key)
	_, err = c.remove(key, nil)
	traceExit("Delete", err, "ok")
	return err
}

// remove creates a document tombstone. It removes the document's value and user xattrs. checkClosed will allow removing the document even the bucket instance is "closed".
func (c *Collection) remove(key string, ifCas *CAS) (casOut CAS, err error) {
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (e *event, err error) {
		// Get the doc, possibly checking cas:
		var cas CAS
		var rawXattrs []byte
		row := txn.QueryRow(
			`SELECT cas, xattrs FROM documents WHERE collection=?1 AND key=?2`,
			c.id, key)
		if err = scan(row, &cas, &rawXattrs); err != nil {
			return nil, remapKeyError(err, key)
		} else if ifCas != nil && cas != *ifCas {
			return nil, sgbucket.CasMismatchErr{Expected: *ifCas, Actual: cas}
		}

		// Deleting a doc removes user xattrs but not system ones:
		if len(rawXattrs) > 0 {
			var xattrs map[string]json.RawMessage
			_ = json.Unmarshal(rawXattrs, &xattrs)
			for k := range xattrs {
				if k == "" || k[0] != '_' {
					delete(xattrs, k)
				}
			}
			if len(xattrs) > 0 {
				rawXattrs, _ = json.Marshal(xattrs)
			} else {
				rawXattrs = nil
			}
		}
		// Now update, setting value=null, isJSON=false, and updating the xattrs:
		_, err = txn.Exec(
			`UPDATE documents SET value=null, cas=?1, exp=0, isJSON=0, xattrs=?2, tombstone=1
			 WHERE collection=?3 AND key=?4`,
			newCas, rawXattrs, c.id, key)
		if err == nil {
			e = &event{
				key:        key,
				cas:        newCas,
				isDeletion: true,
				xattrs:     rawXattrs,
			}
		}
		casOut = newCas
		return
	})
	return
}

func (c *Collection) Update(key string, exp Exp, callback sgbucket.UpdateFunc) (casOut CAS, err error) {
	traceEnter("Update", "%q, %d, ...", key, exp)
	defer func() { traceExit("Update", err, "0x%x", casOut) }()
	for {
		var raw []byte
		var cas CAS
		if raw, cas, err = c.getRaw(c.db(), key); err != nil && !c.IsError(err, sgbucket.KeyNotFoundError) {
			return
		}

		var newRaw []byte
		var newExp *uint32
		var delete bool
		newRaw, newExp, delete, err = callback(raw)
		trace("\t callback(%q) -> %q, exp=%v, delete=%v, err=%v", raw, newRaw, exp, delete, err)
		if err != nil {
			if err == sgbucket.ErrCasFailureShouldRetry {
				continue // Callback wants us to retry
			} else {
				return cas, err
			}
		}
		if newRaw == nil && newExp == nil && !delete {
			return 0, nil // Callback canceled
		}
		if newRaw != nil || delete {
			raw = newRaw
		}
		if newExp != nil {
			exp = *newExp
		}

		var opt sgbucket.WriteOptions = 0 // Hardcoded; callback cannot customize this :(
		casOut, err = c.WriteCas(key, 0, exp, cas, raw, opt)
		if err == nil {
			break
		} else if _, ok := err.(sgbucket.CasMismatchErr); !ok {
			return 0, err // fatal error
		}
	}
	return casOut, err
}

func (c *Collection) Incr(key string, amt, deflt uint64, exp Exp) (result uint64, err error) {
	traceEnter("Incr", "%q, %d, %d", key, amt, deflt)
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		exp = absoluteExpiry(exp)
		_, err = c.get(txn, key, &result)
		debug("INCR: result=%v, err=%v", result, err)
		if err == nil {
			result += amt
		} else if _, ok := err.(sgbucket.MissingError); ok {
			result = deflt
		} else {
			return nil, err
		}

		raw := []byte(strconv.FormatUint(result, 10))

		xattrs, err := c._set(txn, key, exp, nil, raw, true, newCas)
		if err != nil {
			return nil, err
		}
		return &event{
			key:    key,
			value:  raw,
			xattrs: xattrs,
			cas:    newCas,
			exp:    exp,
		}, nil
	})
	traceExit("Incr", err, "%d", result)
	return
}

//////// Interface TypedErrorStore

func (c *Collection) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
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

//////// Interface BucketStoreFeatureIsSupported

func (c *Collection) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	return c.bucket.IsSupported(feature)
}

//////// EXPIRATION

// _expireDocuments immediately deletes all expired documents in this collection.
func (c *Collection) expireDocuments() (count int64, err error) {
	traceEnter("_expireDocuments", "")
	defer func() { traceExit("_expireDocuments", err, "%d", count) }()

	// First find all the expired docs and collect their keys:
	exp := nowAsExpiry()
	rows, err := c.db().Query(`SELECT key FROM documents
								WHERE collection = ?1 AND exp > 0 AND exp <= ?2`, c.id, exp)
	if err != nil {
		return
	}
	var keys []string
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return
		}
		keys = append(keys, key)
	}
	if err = rows.Err(); err != nil {
		return
	}

	// Now delete each doc. (This has to be done after the above query finishes, because Delete()
	// will get its own db connection, and if the db only supports one connection (i.e. in-memory)
	// having both queries active would deadlock.)
	for _, key := range keys {
		if c.Delete(key) == nil {
			count++
		}
	}
	return
}

//////// Utilities:

// Returns the last CAS assigned to any doc in _any_ collection.
func (bucket *Bucket) getLastCas(txn *sql.Tx) (cas CAS, err error) {
	row := txn.QueryRow("SELECT lastCas FROM bucket")
	err = scan(row, &cas)
	return
}

// Returns the last CAS assigned to any doc in this collection.
func (c *Collection) getLastCas(q queryable) (cas CAS, err error) {
	row := q.QueryRow("SELECT lastCas FROM collections WHERE id=?1", c.id)
	err = scan(row, &cas)
	return
}

// Updates the collection's and the bucket's lastCas.
func (c *Collection) setLastCas(txn *sql.Tx, cas CAS) (err error) {
	_, err = txn.Exec(`UPDATE bucket SET lastCas=?1`, cas)
	if err == nil {
		_, err = txn.Exec(`UPDATE collections SET lastCas=?1 WHERE id=?2`, cas, c.id)
	}
	return
}

// Runs a function within a SQLite transaction, passing it a new CAS to assign to the
// document being modified. The function returns an event to be posted.
func (c *Collection) withNewCas(fn func(txn *sql.Tx, newCas CAS) (*event, error)) error {
	var e *event
	err := c.bucket.inTransaction(func(txn *sql.Tx) error {
		newCas, err := c.bucket.getLastCas(txn)
		if err == nil {
			newCas++
			e, err = fn(txn, newCas)
			if err == nil {
				err = c.setLastCas(txn, newCas)
			}
		}
		return err
	})
	if err == nil && e != nil {
		c.postNewEvent(e)
	}
	return err
}

var (
	// Enforce interface conformance:
	_ sgbucket.DataStore     = &Collection{}
	_ sgbucket.DataStoreName = &Collection{}
)
