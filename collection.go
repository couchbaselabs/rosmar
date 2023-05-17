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

// A collection within a Bucket.
// Implements sgbucket interfaces DataStore, DataStoreName
type Collection struct {
	sgbucket.DataStoreNameImpl // Fully qualified name (scope and collection)
	bucket                     *Bucket
	db                         *sql.DB
	id                         CollectionID // Unique collectionID
	mutex                      sync.Mutex
	feeds                      []*dcpFeed
	viewCache                  map[viewKey]*rosmarView
}

type CollectionID uint32
type CAS = uint64

func newCollection(bucket *Bucket, name sgbucket.DataStoreNameImpl, id CollectionID, db *sql.DB) *Collection {
	return &Collection{
		bucket:            bucket,
		db:                db,
		DataStoreNameImpl: name,
		id:                id,
	}
}

func (c *Collection) close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.stopFeeds()
}

//////// Interface DataStore

func (c *Collection) GetName() string {
	return c.bucket.name + "." + c.DataStoreNameImpl.String()
}

func (c *Collection) GetCollectionID() uint32 {
	return uint32(c.id)
}

//////// Interface KVStore

//// Raw:

func (c *Collection) Exists(key string) (exists bool, err error) {
	row := c.db.QueryRow(`SELECT 1 FROM documents
							WHERE collection=? AND key=? AND value NOT NULL`, c.id, key)
	var i int
	err = row.Scan(&i)
	exists = (err == nil)
	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

func (c *Collection) GetRaw(key string) (val []byte, cas CAS, err error) {
	row := c.db.QueryRow("SELECT value, cas FROM documents WHERE collection=? AND key=?", c.id, key)
	if err = row.Scan(&val, &cas); err != nil {
		err = remapError(err, key)
	} else if val == nil {
		err = sgbucket.MissingError{Key: key}
	}
	return
}

func (c *Collection) GetAndTouchRaw(key string, exp uint32) (val []byte, cas CAS, err error) {
	// Until rosmar supports expiry, the exp value is ignored
	return c.GetRaw(key)
}

func (c *Collection) AddRaw(key string, exp uint32, val []byte) (added bool, err error) {
	return c.add(key, exp, val, false)
}

func (c *Collection) add(key string, exp uint32, val []byte, isJSON bool) (added bool, err error) {
	if len(val) > MaxDocSize {
		return false, &sgbucket.DocTooBigErr{}
	}
	var casOut CAS
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) error {
		result, err := txn.Exec(
			`INSERT INTO documents (collection,key,value,cas,exp,isJSON) VALUES (?,?,?,?,?,?)
				ON CONFLICT(collection,key) DO UPDATE SET value=$3, cas=$4, exp=$5, isJSON=$6
				WHERE value IS NULL`,
			c.id, key, val, newCas, exp, isJSON)
		if err != nil {
			return err
		}
		casOut = newCas
		n, _ := result.RowsAffected()
		added = (n > 0)
		return err
	})

	if added {
		c.postDocEvent(&document{
			key:    key,
			value:  val,
			cas:    casOut,
			isJSON: isJSON,
		}, sgbucket.FeedOpMutation)
	}
	return
}

func (c *Collection) SetRaw(key string, exp uint32, opts *sgbucket.UpsertOptions, val []byte) (err error) {
	return c.set(key, exp, opts, val, false)
}

func (c *Collection) set(key string, exp uint32, opts *sgbucket.UpsertOptions, val []byte, isJSON bool) (err error) {
	if len(val) > MaxDocSize {
		err = &sgbucket.DocTooBigErr{}
		return
	}
	var casOut CAS
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) error {
		_, err := txn.Exec(
			`INSERT INTO documents (collection,key,value,cas,exp,isJSON) VALUES ($1,$2,$3,$4,$5,$6)
				ON CONFLICT(collection,key) DO UPDATE SET value=$3, cas=$4, exp=$5, isJSON=$6`,
			c.id, key, val, newCas, exp, isJSON)
		casOut = newCas
		return err
	})
	if err == nil {
		c.postDocEvent(&document{
			key:    key,
			value:  val,
			cas:    casOut,
			isJSON: isJSON,
		}, sgbucket.FeedOpMutation)
	}
	return err
}

// Non-Raw:

func (c *Collection) Get(key string, outVal any) (cas CAS, err error) {
	raw, cas, err := c.GetRaw(key)
	if err == nil {
		err = decodeRaw(raw, outVal)
	}
	return
}

func (c *Collection) Touch(key string, exp uint32) (cas CAS, err error) {
	// Until rosmar supports expiry, the exp value is ignored
	_, cas, err = c.GetRaw(key)
	return
}

func (c *Collection) Add(key string, exp uint32, val any) (added bool, err error) {
	raw, err := encodeAsRaw(val, true)
	if err == nil {
		added, err = c.add(key, exp, raw, true)
	}
	return
}

func (c *Collection) Set(key string, exp uint32, opts *sgbucket.UpsertOptions, val any) (err error) {
	raw, err := encodeAsRaw(val, true)
	if err == nil {
		err = c.set(key, exp, opts, raw, true)
	}
	return
}

func (c *Collection) WriteCas(key string, flags int, exp uint32, cas CAS, val any, opt sgbucket.WriteOptions) (casOut CAS, err error) {
	// Marshal JSON if the value is not raw:
	isJSON := (opt&(sgbucket.Raw|sgbucket.Append) == 0)
	raw, err := encodeAsRaw(val, isJSON)
	if err != nil {
		return 0, err
	}
	if len(raw) > MaxDocSize {
		return 0, &sgbucket.DocTooBigErr{}
	}
	if raw == nil {
		isJSON = false
	}

	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) error {
		var sql string
		if (opt & sgbucket.Append) != 0 {
			sql = `UPDATE documents SET value=value || $1, cas=$2, exp=$6, isJSON=$7
					 WHERE collection=$3 AND key=$4 AND cas=$5`
		} else if (opt&sgbucket.AddOnly) != 0 || cas == 0 {
			// Insert but fall back to Update if the doc is a tombstone
			sql = `INSERT INTO documents (collection, key, value, cas, isJSON) VALUES($3,$4,$1,$2,$7)
				ON CONFLICT(collection,key) DO UPDATE SET value=$1, cas=$2, exp=$6, isJSON=$7
											WHERE value IS NULL`
			if cas != 0 {
				sql += ` AND cas=$5`
			}
		} else {
			sql = `UPDATE documents SET value=$1, cas=$2, exp=$6, isJSON=$7
					 WHERE collection=$3 AND key=$4 AND cas=$5`
		}
		result, err := txn.Exec(sql, raw, newCas, c.id, key, cas, exp, isJSON)
		if err == nil {
			if nRows, _ := result.RowsAffected(); nRows > 0 {
				casOut = newCas
			} else if exists, err2 := c.Exists(key); exists && err2 == nil {
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
		}
		return err
	})

	if err == nil {
		c.postDocEvent(&document{
			key:    key,
			value:  raw,
			cas:    casOut,
			isJSON: isJSON,
		}, ifelse(raw != nil, sgbucket.FeedOpMutation, sgbucket.FeedOpDeletion))
	}
	return
}

func (c *Collection) Delete(key string) (err error) {
	var casOut CAS
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) error {
		_, err := txn.Exec(
			`UPDATE documents SET value=null, cas=$1, isJSON=0 WHERE collection=$2 AND key=$3`,
			newCas, c.id, key)
		casOut = newCas
		return err
	})
	if err == nil {
		c.postDocEvent(&document{
			key: key,
			cas: casOut,
		}, sgbucket.FeedOpDeletion)
	}
	return
}

func (c *Collection) Remove(key string, cas CAS) (casOut CAS, err error) {
	return c.WriteCas(key, 0, 0, cas, nil, sgbucket.Raw)
}

func (c *Collection) Update(key string, exp uint32, callback sgbucket.UpdateFunc) (casOut CAS, err error) {
	for {
		var raw []byte
		var cas CAS
		if raw, cas, err = c.GetRaw(key); err != nil {
			return
		}

		var delete bool
		raw, _, delete, err = callback(raw)
		if err != nil {
			if err == sgbucket.ErrCasFailureShouldRetry {
				continue // Callback wants us to retry
			} else {
				return cas, err
			}
		}
		if raw == nil && !delete {
			return 0, nil // Callback canceled
		}

		casOut, err = c.WriteCas(key, 0, 0, cas, raw, sgbucket.Raw)
		if err == nil {
			break
		} else if _, ok := err.(sgbucket.CasMismatchErr); !ok {
			return 0, err // fatal error
		}
	}
	return casOut, err
}

func (c *Collection) Incr(key string, amt, def uint64, exp uint32) (result uint64, err error) {
	var casOut CAS
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) error {
		result, err := txn.Exec(
			`UPDATE documents SET value = ifnull(value,$1) + $2, cas=$3, isJSON=0
				WHERE collection=$4 AND key=$5`,
			def, amt, newCas, c.id, key)
		if err == nil {
			if n, _ := result.RowsAffected(); n == 0 {
				raw, _ := encodeAsRaw(def, true)
				_, err = txn.Exec(
					`INSERT INTO documents (collection,key,value,cas,exp,isJSON) VALUES ($1,$2,$3,$4,$5,0)
						ON CONFLICT(collection,key) DO UPDATE SET value=$3, cas=$4, exp=$5, isJSON=0`,
					c.id, key, raw, newCas, exp)
			}
		}
		casOut = newCas
		return err
	})
	if err == nil {
		_, err = c.Get(key, &result)
		c.postDocEvent(&document{
			key:   key,
			value: []byte(strconv.FormatUint(result, 10)),
			cas:   casOut,
		}, sgbucket.FeedOpDeletion)
	}
	return
}

func (c *Collection) GetExpiry(key string) (expiry uint32, err error) {
	return 0, nil
}

//////// Interface XattrStore

func (c *Collection) WriteCasWithXattr(key string, xattrKey string, exp uint32, cas CAS, opts *sgbucket.MutateInOptions, outVal any, xv any) (casOut CAS, err error) {
	err = ErrUNIMPLEMENTED
	return
}
func (c *Collection) WriteWithXattr(key string, xattrKey string, exp uint32, cas CAS, opts *sgbucket.MutateInOptions, value []byte, xattrValue []byte, isDelete bool, deleteBody bool) (casOut CAS, err error) {
	err = ErrUNIMPLEMENTED
	return
}
func (c *Collection) SetXattr(key string, xattrKey string, xv []byte) (casOut CAS, err error) {
	err = ErrUNIMPLEMENTED
	return
}
func (c *Collection) RemoveXattr(key string, xattrKey string, cas CAS) (err error) {
	err = ErrUNIMPLEMENTED
	return
}
func (c *Collection) DeleteXattrs(key string, xattrKeys ...string) (err error) {
	err = ErrUNIMPLEMENTED
	return
}
func (c *Collection) GetXattr(key string, xattrKey string, xv any) (casOut CAS, err error) {
	err = ErrUNIMPLEMENTED
	return
}
func (c *Collection) GetWithXattr(key string, xattrKey string, userXattrKey string, outVal any, xv any, uxv any) (cas CAS, err error) {
	err = ErrUNIMPLEMENTED
	return
}
func (c *Collection) DeleteWithXattr(key string, xattrKey string) (err error) {
	err = ErrUNIMPLEMENTED
	return
}
func (c *Collection) WriteUpdateWithXattr(key string, xattrKey string, userXattrKey string, exp uint32, opts *sgbucket.MutateInOptions, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut CAS, err error) {
	err = ErrUNIMPLEMENTED
	return
}

//////// Interface SubdocStore

func (c *Collection) SubdocInsert(docID string, fieldPath string, cas CAS, value any) (err error) {
	err = ErrUNIMPLEMENTED
	return
}

func (c *Collection) GetSubDocRaw(key string, subdocKey string) (value []byte, casOut uint64, err error) {
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
	if subdocKeyNesting(subdocKey) {
		err = &ErrUnimplemented{reason: "Rosmar does not support subdoc nesting"}
		return
	}

	// Get existing doc (if it exists) to change sub doc value in
	fullDoc := make(map[string]interface{})
	casOut, err = c.Get(key, &fullDoc)
	if err != nil && c.IsError(err, sgbucket.KeyNotFoundError) {
		return 0, err
	}
	if cas != 0 && casOut != cas {
		return 0, fmt.Errorf("error: cas mismatch: %d expected %d received. Unable to update document", cas, casOut)
	}

	// Set new subdoc value
	var subDocVal any
	if err = json.Unmarshal(value, &subDocVal); err != nil {
		return
	}
	fullDoc[subdocKey] = subDocVal

	// Write full doc body to collection
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
	switch feature {
	case sgbucket.BucketStoreFeatureCollections:
		return true
	case sgbucket.BucketStoreFeatureSubdocOperations:
		return true
	default:
		return false
	}
}

//////// Utilities:

type ErrUnimplemented struct{ reason string }

func (err *ErrUnimplemented) Error() string { return err.reason }

var ErrUNIMPLEMENTED = &ErrUnimplemented{reason: "Rosmar does not implement this method"}

func remapError(err error, key string) error {
	if err == sql.ErrNoRows {
		err = sgbucket.MissingError{Key: key}
	}
	return err
}

// Returns the last CAS assigned to any doc in _any_ collection.
func (bucket *Bucket) getLastCas(txn *sql.Tx) (cas CAS, err error) {
	row := txn.QueryRow("SELECT lastCas FROM bucket")
	err = row.Scan(&cas)
	return
}

// Returns the last CAS assigned to any doc in this collection.
func (c *Collection) getLastCas() (cas CAS, err error) {
	row := c.db.QueryRow("SELECT lastCas FROM collections WHERE id=$1", c.id)
	err = row.Scan(&cas)
	return
}

// Updates the collection's and the bucket's lastCas.
func (c *Collection) setLastCas(txn *sql.Tx, cas CAS) (err error) {
	_, err = txn.Exec(`UPDATE bucket SET lastCas=$1;
					   UPDATE collections SET lastCas=$1 WHERE id=$2`, cas, c.id)
	return
}

func (c *Collection) inTransaction(fn func(txn *sql.Tx) error) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	txn, err := c.db.Begin()
	if err != nil {
		return err
	}
	err = fn(txn)
	if err == nil {
		err = txn.Commit()
	} else {
		txn.Rollback()
	}
	return err
}

func (c *Collection) withNewCas(fn func(txn *sql.Tx, newCas CAS) error) error {
	return c.inTransaction(func(txn *sql.Tx) error {
		newCas, err := c.bucket.getLastCas(txn)
		if err == nil {
			newCas++
			err = fn(txn, newCas)
			if err == nil {
				err = c.setLastCas(txn, newCas)
			}
		}
		return err
	})
}

func encodeAsRaw(val interface{}, isJSON bool) (data []byte, err error) {
	if val != nil {
		if isJSON {
			// Check for already marshalled JSON
			switch typedVal := val.(type) {
			case []byte:
				data = typedVal
			case *[]byte:
				data = *typedVal
			default:
				data, err = json.Marshal(val)
			}
		} else {
			if typedVal, ok := val.([]byte); ok {
				data = typedVal
			} else {
				err = fmt.Errorf("raw value must be []byte")
			}
		}
	}
	return
}

func decodeRaw(raw []byte, rv any) error {
	if bytesPtr, ok := rv.(*[]byte); ok {
		*bytesPtr = raw
		return nil
	} else {
		return json.Unmarshal(raw, rv)
	}
}

var (
	// Enforce interface conformance:
	_ sgbucket.DataStore     = &Collection{}
	_ sgbucket.DataStoreName = &Collection{}
	_ sgbucket.ViewStore     = &Collection{}
)
