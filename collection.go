package rosmar

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	sgbucket "github.com/couchbase/sg-bucket"
)

//TODO: Use prepared statements for performance (sql.Stmt)

var MaxDocSize int = 20 * 1024 * 1024

// A collection within a Bucket.
// Implements sgbucket interfaces DataStore, DataStoreName
type Collection struct {
	sgbucket.DataStoreNameImpl // Fully qualified name (scope and collection)
	bucket                     *Bucket
	id                         CollectionID // Row ID in collections table; public ID + 1
	mutex                      sync.Mutex
	feeds                      []*dcpFeed
	viewCache                  map[viewKey]*rosmarView
}

type CollectionID uint32
type CAS = uint64
type Exp = uint32

func newCollection(bucket *Bucket, name sgbucket.DataStoreNameImpl, id CollectionID) *Collection {
	return &Collection{
		bucket:            bucket,
		DataStoreNameImpl: name,
		id:                id,
	}
}

func (c *Collection) close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.stopFeeds()
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
	return c.exists(c.db(), key)
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
	trace("rosmar.GetRaw(%q)", key)
	return c.getRaw(c.db(), key)
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
	trace("rosmar.GetAndTouchRaw(%q)", key)
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (e *event, err error) {
		exp = absoluteExpiry(exp)
		val, cas, err = c.getRaw(txn, key)
		if err == nil {
			_, err = txn.Exec(`UPDATE documents SET exp=?1 WHERE key=?2`, exp, key)
		}
		return
	})
	return
}

func (c *Collection) AddRaw(key string, exp Exp, val []byte) (added bool, err error) {
	trace("rosmar.AddRaw(%q, %d, ...)", key, exp)
	return c.add(key, exp, val, looksLikeJSON(val))
}

func (c *Collection) add(key string, exp Exp, val []byte, isJSON bool) (added bool, err error) {
	if len(val) > MaxDocSize {
		return false, &sgbucket.DocTooBigErr{}
	}
	var casOut CAS
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (e *event, err error) {
		exp = absoluteExpiry(exp)
		result, err := txn.Exec(
			`INSERT INTO documents (collection,key,value,cas,exp,isJSON) VALUES (?1,?2,?3,?4,?5,?6)
				ON CONFLICT(collection,key) DO UPDATE SET value=?3, cas=?4, exp=?5, isJSON=?6
				WHERE value IS NULL`,
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
	trace("rosmar.SetRaw(%q, %d, ...)", key, exp)
	return c.set(key, exp, opts, val, false)
}

func (c *Collection) set(key string, exp Exp, opts *sgbucket.UpsertOptions, val []byte, isJSON bool) (err error) {
	if len(val) > MaxDocSize {
		err = &sgbucket.DocTooBigErr{}
		return
	}
	return c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		exp = absoluteExpiry(exp)
		err = c._set(txn, key, exp, opts, val, isJSON, newCas)
		if err != nil {
			return nil, err
		}
		xattrs, err := c.getRawXattrs(txn, key) // needed for the DCP event
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

func (c *Collection) _set(txn *sql.Tx, key string, exp Exp, opts *sgbucket.UpsertOptions, val []byte, isJSON bool, newCas CAS) error {
	exp = absoluteExpiry(exp)
	var stmt string
	if opts != nil && opts.PreserveExpiry {
		stmt = `INSERT INTO documents (collection,key,value,cas,isJSON) VALUES (?1,?2,?3,?4,?6)
			ON CONFLICT(collection,key) DO UPDATE SET value=?3, cas=?4, isJSON=?6`
	} else {
		stmt = `INSERT INTO documents (collection,key,value,cas,exp,isJSON) VALUES (?1,?2,?3,?4,?5,?6)
			ON CONFLICT(collection,key) DO UPDATE SET value=?3, cas=?4, exp=?5, isJSON=?6`
	}
	_, err := txn.Exec(stmt, c.id, key, val, newCas, exp, isJSON)
	return err
}

// Non-Raw:

func (c *Collection) Get(key string, outVal any) (cas CAS, err error) {
	cas, err = c.get(c.db(), key, outVal)
	trace("rosmar.Get(%q) -> %d, %v", key, cas, err)
	return
}

func (c *Collection) get(q queryable, key string, outVal any) (cas CAS, err error) {
	raw, cas, err := c.getRaw(q, key)
	if err == nil {
		err = decodeRaw(raw, outVal)
	}
	return
}

func (c *Collection) GetExpiry(key string) (exp Exp, err error) {
	row := c.db().QueryRow("SELECT exp FROM documents WHERE collection=? AND key=?", c.id, key)
	err = scan(row, &exp)
	err = remapKeyError(err, key)
	return
}

func (c *Collection) Touch(key string, exp Exp) (cas CAS, err error) {
	_, cas, err = c.GetAndTouchRaw(key, exp)
	return
}

func (c *Collection) Add(key string, exp Exp, val any) (added bool, err error) {
	trace("rosmar.Add(%q, %v)", key, val)
	raw, err := encodeAsRaw(val, true)
	if err == nil {
		added, err = c.add(key, exp, raw, true)
	}
	trace("\tadd -> %v, %v", added, err)
	return
}

func (c *Collection) Set(key string, exp Exp, opts *sgbucket.UpsertOptions, val any) (err error) {
	trace("rosmar.Set(%q, %v)", key, val)
	raw, err := encodeAsRaw(val, true)
	if err == nil {
		err = c.set(key, exp, opts, raw, true)
	}
	return
}

func (c *Collection) WriteCas(key string, flags int, exp Exp, cas CAS, val any, opt sgbucket.WriteOptions) (casOut CAS, err error) {
	// Marshal JSON if the value is not raw:
	isJSON := (opt&(sgbucket.Raw|sgbucket.Append) == 0)
	raw, err := encodeAsRaw(val, isJSON)
	if err != nil {
		return 0, err
	}
	trace("rosmar.WriteCas(%q, cas=%x, opt=%v, val=%s)", key, cas, opt, raw)
	if len(raw) > MaxDocSize {
		return 0, &sgbucket.DocTooBigErr{}
	}
	if raw == nil {
		isJSON = false
	}

	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		exp = absoluteExpiry(exp)
		var sql string
		if (opt & sgbucket.Append) != 0 {
			// Append:
			sql = `UPDATE documents SET value=value || ?1, cas=?2, exp=?6, isJSON=0
					 WHERE collection=?3 AND key=?4 AND cas=?5`
		} else if (opt&sgbucket.AddOnly) != 0 || cas == 0 {
			// Insert, but fall back to Update if the doc is a tombstone
			sql = `INSERT INTO documents (collection, key, value, cas, exp, isJSON) VALUES(?3,?4,?1,?2,?6,?7)
				ON CONFLICT(collection,key) DO UPDATE SET value=?1, cas=?2, exp=?6, isJSON=?7
											WHERE value IS NULL`
			if cas != 0 {
				sql += ` AND cas=?5`
			}
		} else {
			// Regular write:
			sql = `UPDATE documents SET value=?1, cas=?2, exp=?6, isJSON=?7
				   WHERE collection=?3 AND key=?4 AND cas=?5`
		}
		result, err := txn.Exec(sql, raw, newCas, c.id, key, cas, exp, isJSON)
		if err != nil {
			return nil, err
		}
		if nRows, _ := result.RowsAffected(); nRows == 0 {
			if exists, err2 := c.exists(txn, key); exists && err2 == nil {
				if opt&sgbucket.AddOnly != 0 {
					err = sgbucket.ErrKeyExists
				} else {
					err = sgbucket.CasMismatchErr{Expected: cas, Actual: 0}
				}
			} else if err2 == nil {
				err = sgbucket.MissingError{Key: key}
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

func (c *Collection) Remove(key string, cas CAS) (casOut CAS, err error) {
	trace("rosmar.Remove(%q, %x)", key, cas)
	return c.remove(key, &cas)
}

func (c *Collection) Delete(key string) (err error) {
	trace("rosmar.Delete(%q)", key)
	_, err = c.remove(key, nil)
	return err
}

func (c *Collection) remove(key string, ifCas *CAS) (casOut CAS, err error) {
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (e *event, err error) {
		// Get the doc, possibly checking cas:
		var cas CAS
		var rawXattrs []byte
		row := txn.QueryRow(
			`SELECT cas, xattrs FROM documents WHERE collection=?1 AND key=?2`,
			c.id, key)
		if err = scan(row, &cas, &rawXattrs); err != nil {
			return
		} else if ifCas != nil && cas != *ifCas {
			return nil, sgbucket.CasMismatchErr{Expected: *ifCas, Actual: cas}
		}

		// Deleting a doc removes user xattrs but not system ones:
		if len(rawXattrs) > 0 {
			var xattrs map[string]json.RawMessage
			_ = json.Unmarshal(rawXattrs, &xattrs)
			for k, _ := range xattrs {
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
			`UPDATE documents SET value=null, cas=?1, isJSON=0, xattrs=?2
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
	trace("rosmar.Update(%q, %d, ...)", key, exp)
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

		casOut, err = c.WriteCas(key, 0, exp, cas, raw, sgbucket.Raw)
		if err == nil {
			break
		} else if _, ok := err.(sgbucket.CasMismatchErr); !ok {
			return 0, err // fatal error
		}
	}
	return casOut, err
}

func (c *Collection) Incr(key string, amt, deflt uint64, exp Exp) (result uint64, err error) {
	trace("rosmar.Incr(%q, %d, %d)", key, amt, deflt)
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		exp = absoluteExpiry(exp)
		_, err = c.get(txn, key, &result)
		if err == nil {
			result += amt
		} else if _, ok := err.(sgbucket.MissingError); ok {
			result = deflt
		} else {
			return nil, err
		}

		raw := []byte(strconv.FormatUint(result, 10))

		err = c._set(txn, key, exp, nil, raw, true, newCas)
		if err != nil {
			return nil, err
		}
		return &event{
			key:   key,
			value: raw,
			cas:   newCas,
			exp:   exp,
		}, nil
	})
	trace("\tincr -> %d, %v", result, err)
	return
}

//////// Interface SubdocStore

func (c *Collection) SubdocInsert(key string, subdocKey string, cas CAS, value any) (err error) {
	trace("rosmar.SubdocInsert(%q, %q, %d)", key, subdocKey, cas)
	return &ErrUnimplemented{reason: "Rosmar does not implement SubdocInsert"}
}

func (c *Collection) GetSubDocRaw(key string, subdocKey string) (value []byte, casOut uint64, err error) {
	// TODO: Use SQLite JSON syntax to get the property
	trace("rosmar.SubdocGetRaw(%q, %q)", key, subdocKey)
	if subdocKeyNesting(subdocKey) {
		err = &ErrUnimplemented{reason: "Rosmar does not support subdoc nesting"}
		return
	}

	var fullDoc map[string]interface{}
	casOut, err = c.Get(key, &fullDoc)
	if err != nil {
		return
	}

	subdoc, ok := fullDoc[subdocKey]
	if !ok {
		err = fmt.Errorf("subdoc key %q not found in doc %q, %w", subdocKey, key, &sgbucket.MissingError{Key: key})
		return
	}

	value, err = json.Marshal(subdoc)
	return value, casOut, err
}

func (c *Collection) WriteSubDoc(key string, subdocKey string, cas CAS, value []byte) (casOut CAS, err error) {
	// TODO: Use SQLite JSON syntax to update the property
	trace("rosmar.WriteSubDoc(%q, %q, %d, %s)", key, subdocKey, cas, value)
	if subdocKeyNesting(subdocKey) {
		err = &ErrUnimplemented{reason: "Rosmar does not support subdoc nesting"}
		return
	}

	var subDocVal any
	if len(value) > 0 {
		if err = json.Unmarshal(value, &subDocVal); err != nil {
			return
		}
	}

	// Get doc (if it exists) to change sub doc value in
	var fullDoc map[string]any
	casOut, err = c.Get(key, &fullDoc)
	if err != nil && !c.IsError(err, sgbucket.KeyNotFoundError) {
		return 0, err
	}
	if cas != 0 && casOut != cas {
		err = sgbucket.CasMismatchErr{Expected: cas, Actual: casOut}
		return 0, err
	}

	// Set new subdoc value
	if fullDoc == nil {
		fullDoc = map[string]any{}
	}
	if subDocVal != nil {
		fullDoc[subdocKey] = subDocVal
	} else {
		delete(fullDoc, subdocKey)
	}

	// Write full doc back to collection
	casOut, err = c.WriteCas(key, 0, 0, casOut, fullDoc, 0)
	if err != nil {
		return 0, err
	}
	return casOut, nil
}

// Returns true if the subDocKey would be using nested sub docs
func subdocKeyNesting(subDocKey string) (nesting bool) {
	return strings.ContainsAny(subDocKey, ".|]")
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
		c.postDocEvent(e)
	}
	return err
}

var (
	// Enforce interface conformance:
	_ sgbucket.DataStore     = &Collection{}
	_ sgbucket.DataStoreName = &Collection{}
	_ sgbucket.ViewStore     = &Collection{}
)
