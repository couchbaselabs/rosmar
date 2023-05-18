package rosmar

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"

	sgbucket "github.com/couchbase/sg-bucket"
)

//////// SGBUCKET XATTR STORE INTERFACE

// Get a single xattr value.
func (c *Collection) GetXattr(
	key string,
	xattrKey string,
	xv interface{},
) (CAS, error) {
	if rawDoc, err := c.getRawWithXattr(key, xattrKey, ""); err != nil {
		return 0, err
	} else if rawDoc.Xattr == nil {
		return 0, sgbucket.XattrMissingError{Key: key, XattrKey: xattrKey}
	} else {
		return rawDoc.Cas, decodeRaw(rawDoc.Xattr, xv)
	}
}

// Set a single xattr value.
func (c *Collection) SetXattr(
	key string,
	xattrKey string,
	xv []byte,
) (casOut CAS, err error) {
	info("SetXattr")
	return c.writeWithXattr(key, nil, sgbucket.Xattr{xattrKey, xv}, nil, false, false, false)
}

// Remove a single xattr.
func (c *Collection) RemoveXattr(
	key string,
	xattrKey string,
	cas CAS,
) error {
	info("RemoveXattr")
	_, err := c.writeWithXattr(key, nil, sgbucket.Xattr{xattrKey, nil}, &cas, false, false, false)
	return err
}

// Remove one or more xattrs.
func (c *Collection) DeleteXattrs(
	key string,
	xattrKeys ...string,
) error {
	info("DeleteXattrs")
	var e *event
	return c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		e = &event{
			key: key,
			cas: newCas,
		}
		row := txn.QueryRow(`SELECT value, xattrs FROM documents WHERE collection=$1 AND key=$2`, c.id, key)
		var rawXattrs []byte
		err := row.Scan(&e.value, &rawXattrs)
		if err != nil {
			return nil, err
		}
		rawXattrs, e.changedXattrs, err = removeXattrs(rawXattrs, xattrKeys...)
		if err != nil {
			return nil, err
		}
		_, err = txn.Exec(`UPDATE documents SET xattrs=$1, cas=$2 WHERE collection=$2 AND key=$3`, rawXattrs, newCas, c.id, key)
		return e, err
	})
}

//////// SG UserXattrStore INTERFACE:

func (c *Collection) WriteUserXattr(key string, xattrKey string, xattrVal interface{}) (CAS, error) {
	info("WriteUserXattr(%q, %q, ...)", key, xattrKey)
	if xattrData, err := encodeAsRaw(xattrVal, true); err != nil {
		return 0, err
	} else {
		return c.writeWithXattr(key, nil, sgbucket.Xattr{xattrKey, xattrData}, nil, false, false, true)
	}
}

func (c *Collection) DeleteUserXattr(key string, xattrKey string) (CAS, error) {
	info("DeleteUserXattr(%q, %q)", key, xattrKey)
	return c.writeWithXattr(key, nil, sgbucket.Xattr{xattrKey, nil}, nil, false, false, true)

}

//////// BODY + XATTRS:

// Get the document's body and an xattr and a user xattr(?)
func (c *Collection) GetWithXattr(
	key string,
	xattrKey string,
	userXattrKey string, // TODO
	rv interface{},
	xv interface{},
	uxv interface{},
) (cas CAS, err error) {
	info("GetWithXattr(%q, %q, %q, ...)", key, xattrKey, userXattrKey)
	var rawDoc sgbucket.BucketDocument
	if rawDoc, err = c.getRawWithXattr(key, xattrKey, userXattrKey); err == nil {
		cas = rawDoc.Cas
		if rawDoc.Body == nil && rawDoc.Xattr == nil {
			err = sgbucket.MissingError{Key: key}
		} else if err = decodeRaw(rawDoc.Body, rv); err == nil {
			if err = decodeRaw(rawDoc.Xattr, xv); err == nil && userXattrKey != "" {
				err = decodeRaw(rawDoc.UserXattr, uxv)
			}
		}
	}
	debug("\tgetwithxattr--> %d, %v;  body=%s  xattr=%s", cas, err, rawDoc.Body, rawDoc.Xattr)
	return
}

// Single attempt to update a document and xattr.
// Setting isDelete=true and value=nil will delete the document body.
func (c *Collection) WriteWithXattr(
	key string,
	xattrKey string,
	exp uint32,
	cas CAS,
	opts *sgbucket.MutateInOptions,
	value []byte,
	xattrValue []byte,
	isDelete bool,
	deleteBody bool,
) (casOut CAS, err error) {
	info("WriteWithXattr(%q, %q, ...)", key, xattrKey)
	casOut, err = c.writeWithXattr(key, value, sgbucket.Xattr{xattrKey, xattrValue}, &cas, isDelete, deleteBody, false)
	debug("\twritewithxattr--> %d, %v", casOut, err)
	return
}

// CAS-safe write of a document and its associated named xattr
func (c *Collection) WriteCasWithXattr(
	key string,
	xattrKey string,
	exp uint32,
	cas CAS,
	opts *sgbucket.MutateInOptions,
	v interface{},
	xv interface{},
) (CAS, error) {
	info("WriteCasWithXattr(%q, %q, ...)", key, xattrKey)
	if value, err := encodeAsRaw(v, true); err != nil {
		return 0, err
	} else if xattrValue, err := encodeAsRaw(xv, true); err != nil {
		return 0, err
	} else {
		return c.WriteWithXattr(key, xattrKey, exp, cas, opts, value, xattrValue, false, false)
	}
}

// WriteUpdateWithXattr retrieves the existing doc from the c, invokes the callback to update
// the document, then writes the new document to the c.  Will repeat this process on CAS
// failure.  If `previous` is provided, will pass those values to the callback on the first
// iteration instead of retrieving from the c.
func (c *Collection) WriteUpdateWithXattr(
	key string,
	xattrKey string,
	userXattrKey string,
	exp uint32,
	opts *sgbucket.MutateInOptions,
	previous *sgbucket.BucketDocument,
	callback sgbucket.WriteUpdateWithXattrFunc,
) (casOut CAS, err error) {
	info("WriteUpdateWithXattr(%q, %q, %q, ...)", key, xattrKey, userXattrKey)
	for {
		if previous == nil {
			// Get current doc if no previous doc was provided:
			prevDoc, err := c.getRawWithXattr(key, xattrKey, userXattrKey)
			if err != nil {
				if _, ok := err.(sgbucket.MissingError); !ok {
					debug("\twriteupdatewithxattr--> %v  (Get failed)", err)
					return 0, err
				}
			}
			previous = &prevDoc
		}

		// Invoke the callback:
		updatedDoc, updatedXattr, deleteDoc, newExp, err := callback(previous.Body, previous.Xattr, previous.UserXattr, previous.Cas)
		if err != nil {
			if err == sgbucket.ErrCasFailureShouldRetry {
				// Callback wants us to retry:
				previous = nil
				continue
			}
			debug("\twriteupdatewithxattr--> %v  (callback error)", err)
			return previous.Cas, err
		}
		if newExp != nil {
			exp = *newExp
		}

		// Update body and/or xattr:
		casOut, err = c.WriteWithXattr(key, xattrKey, exp, previous.Cas, opts, updatedDoc, updatedXattr, deleteDoc, (updatedDoc == nil))

		if _, ok := err.(sgbucket.CasMismatchErr); !ok {
			// Exit loop on success or failure
			debug("\twriteupdatewithxattr--> %d, %v", casOut, err)
			return casOut, err
		}
		// ...else retry. Clear `previous` to force a Get this time.
		previous = nil
	}
}

// Delete a document's body and an xattr simultaneously.
func (c *Collection) DeleteWithXattr(
	key string,
	xattrKey string,
) error {
	info("DeleteWithXattr(%q, %q)", key, xattrKey)
	return c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		doc := &event{
			key:        key,
			cas:        newCas,
			isDeletion: true,
		}
		row := txn.QueryRow(`SELECT xattrs FROM documents WHERE collection=$1 AND key=$2`, c.id, key)
		var rawXattrs []byte
		err := row.Scan(&rawXattrs)
		if err != nil {
			return nil, err
		}
		rawXattrs, doc.changedXattrs, _ = removeXattrs(rawXattrs, xattrKey)

		_, err = txn.Exec(`UPDATE documents SET value=null, xattrs=$1, cas=$2 WHERE collection=$3 AND key=$4`, rawXattrs, newCas, c.id, key)
		return doc, err
	})
}

//////// INTERNALS:

// get doc's _uncopied_ raw body and an xattr.
func (c *Collection) getRawWithXattr(key string, xattrKey string, userXattrKey string) (rawDoc sgbucket.BucketDocument, err error) {
	row := c.db.QueryRow(`SELECT value, cas, xattrs -> $1, userXattrs -> $2 FROM documents WHERE collection=$3 AND key=$4`, xattrKey, userXattrKey, c.id, key)
	err = row.Scan(&rawDoc.Body, &rawDoc.Cas, &rawDoc.Xattr, &rawDoc.UserXattr)
	if err != nil {
		err = remapError(err, key)
	} else if rawDoc.Body == nil {
		err = sgbucket.MissingError{Key: key}
	}
	return
}

// Replaces a doc's body and/or an xattr.
func (c *Collection) writeWithXattr(
	key string, // doc key
	val []byte, // document body; nil means no change
	xattr sgbucket.Xattr, // xattr; nil value means delete
	ifCas *CAS, // if non-nil, must match current CAS; 0 for insert
	isDelete bool, // if true, doc must be a tombstone
	deleteBody bool, // if true, delete the doc body
	xattrIsUser bool, // true if this is a user xattr
) (casOut CAS, err error) {
	if xattrIsUser {
		panic("Unsupported!") //TODO
	}
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		e := &event{key: key}
		row := txn.QueryRow(`SELECT value, cas, xattrs FROM documents WHERE collection=$1 AND key=$2`, c.id, key)
		casOut = newCas
		var rawXattrs []byte
		err := row.Scan(&e.value, &e.cas, &rawXattrs)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}

		if ifCas != nil && *ifCas != e.cas && e.value != nil {
			return nil, sgbucket.CasMismatchErr{*ifCas, e.cas}
		}

		if isDelete && e.value != nil {
			return nil, fmt.Errorf("not a tombstone") //???
		}

		// Update the body:
		if deleteBody {
			e.value = nil
			e.isJSON = false
			e.isDeletion = true
			debug("\t\tSet doc %q body -> nil", key)
		} else if val != nil {
			e.value = val
			e.isJSON = true //???
			if Logging >= LevelTrace {
				trace("\t\tSet doc %q body = %s", key, val) // dump body; super verbose
			} else {
				debug("\t\tSet doc %q body", key)
			}
		}

		var xattrs map[string]any
		json.Unmarshal(rawXattrs, &xattrs)
		if xattr.Value != nil {
			var parsedXattr any
			if err := json.Unmarshal(xattr.Value, &parsedXattr); err != nil {
				return nil, fmt.Errorf("unparseable xattr: %w", err)
			}
			if !xattrIsUser && xattr.Name == "_sync" {
				// The "_sync" xattr has two properties that get macro-expanded:
				e.cas = newCas
				e.expandSyncXattrMacros(parsedXattr)
				xattr.Value, _ = json.Marshal(parsedXattr)
			}
			if xattrs == nil {
				xattrs = map[string]any{}
			}
			xattrs[xattr.Name] = parsedXattr
			if Logging >= LevelTrace {
				trace("\t\tSet doc %q xattr %q = %s", key, xattr.Name, xattr.Value)
			} else {
				debug("\t\tSet doc %q xattr %q", key, xattr.Name)
			}
		} else {
			if _, found := xattrs[xattr.Name]; found {
				delete(xattrs, xattr.Name)
				debug("\t\tDeleted doc %q xattr %s", key, xattr.Name)
			} else {
				return nil, sgbucket.MissingError{Key: key}
			}
		}
		rawXattrs, _ = json.Marshal(xattrs)

		if MaxDocSize > 0 && len(e.value)+len(rawXattrs) > MaxDocSize {
			return nil, sgbucket.DocTooBigErr{}
		}

		e.changedXattrs = []sgbucket.Xattr{xattr}
		e.cas = newCas
		_, err = txn.Exec(`UPDATE documents SET value=$1, cas=$2, xattrs=$3 WHERE collection=$4 AND key=$5`, e.value, e.cas, rawXattrs, c.id, key)
		return e, err
	})
	return
}

//////// HELPERS:

// Sets JSON properties "cas" to the given `cas`, and "value_crc" to CRC checksum of `docValue`.
func (e *event) expandSyncXattrMacros(xattr any) {
	if xattrMap, ok := xattr.(map[string]any); ok {
		// For some reason Server encodes `cas` as 8 hex bytes in little-endian order...
		casBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(casBytes, e.cas)
		xattrMap["cas"] = fmt.Sprintf("0x%x", casBytes)

		xattrMap["value_crc32c"] = e.encodedCRC32c()
	}
}

// Returns a CRC32c checksum of doc.value, formatted as a hex string.
func (e *event) encodedCRC32c() string {
	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(e.value, table)
	return fmt.Sprintf("0x%08x", checksum)
}

// Removes Xattrs from the raw JSON form.
func removeXattrs(rawXattrs []byte, xattrKeys ...string) (rawResult []byte, deletedAttrs []sgbucket.Xattr, err error) {
	var xattrs map[string]any
	if err = json.Unmarshal(rawXattrs, &xattrs); err != nil {
		return
	}
	for _, key := range xattrKeys {
		if xattrs[key] != nil {
			delete(xattrs, key)
			deletedAttrs = append(deletedAttrs, sgbucket.Xattr{key, nil})
		}
	}
	if len(xattrs) > 0 {
		rawResult, _ = json.Marshal(xattrs)
	}
	return
}
