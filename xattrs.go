package rosmar

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
)

type semiParsedXattrs = map[string]json.RawMessage

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
	traceEnter("SetXattr", "%q, %q", key, xattrKey)
	casOut, err = c.writeWithXattr(key, nil, sgbucket.Xattr{Name: xattrKey, Value: xv}, nil, 0, false, false, false)
	traceExit("SetXattr", err, "0x%x", casOut)
	return casOut, err
}

// Remove a single xattr.
func (c *Collection) RemoveXattr(
	key string,
	xattrKey string,
	cas CAS,
) error {
	traceEnter("RemoveXattr", "%q, %q", key, xattrKey)
	_, err := c.writeWithXattr(key, nil, sgbucket.Xattr{Name: xattrKey, Value: nil}, &cas, 0, false, false, false)
	traceExit("RemoveXattr", err, "ok")
	return err
}

// Remove one or more xattrs.
func (c *Collection) DeleteXattrs(
	key string,
	xattrKeys ...string,
) error {
	traceEnter("DeleteXattrs", "%q, %v", key, xattrKeys)
	var e *event
	err := c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		e = &event{
			key: key,
			cas: newCas,
		}
		row := txn.QueryRow(`SELECT value, xattrs FROM documents WHERE collection=?1 AND key=?2`, c.id, key)
		var rawXattrs []byte
		err := scan(row, &e.value, &rawXattrs)
		if err != nil {
			return nil, remapKeyError(err, key)
		}
		rawXattrs = removeXattrs(rawXattrs, xattrKeys...)
		e.xattrs = rawXattrs
		_, err = txn.Exec(`UPDATE documents SET xattrs=?1, cas=?2 WHERE collection=?2 AND key=?3`, rawXattrs, newCas, c.id, key)
		return e, err
	})
	traceExit("DeleteXattrs", err, "ok")
	return err
}

//////// SG UserXattrStore INTERFACE:

func (c *Collection) WriteUserXattr(key string, xattrKey string, xattrVal interface{}) (casOut CAS, err error) {
	traceEnter("WriteUserXattr", "%q, %q, ...", key, xattrKey)
	if xattrData, err := encodeAsRaw(xattrVal, true); err == nil {
		casOut, err = c.writeWithXattr(key, nil, sgbucket.Xattr{Name: xattrKey, Value: xattrData}, nil, 0, false, false, true)
	}
	traceExit("WriteUserXattr", err, "0x%x", casOut)
	return casOut, err
}

func (c *Collection) DeleteUserXattr(key string, xattrKey string) (casOut CAS, err error) {
	traceEnter("DeleteUserXattr", "%q, %q", key, xattrKey)
	casOut, err = c.writeWithXattr(key, nil, sgbucket.Xattr{Name: xattrKey, Value: nil}, nil, 0, false, false, true)
	traceExit("DeleteUserXattr", err, "0x%x", casOut)
	return casOut, err
}

//////// BODY + XATTRS:

// Get the document's body and an xattr and a user xattr(?)
func (c *Collection) GetWithXattr(
	key string,
	xattrKey string,
	userXattrKey string,
	rv interface{},
	xv interface{},
	uxv interface{},
) (cas CAS, err error) {
	traceEnter("GetWithXattr", "%q, %q, %q, ...", key, xattrKey, userXattrKey)
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
	traceExit("GetWithXattr", err, "cas=0x%x  body=%s  xattr=%s", cas, rawDoc.Body, rawDoc.Xattr)
	return
}

// Single attempt to update a document and xattr.
// Setting isDelete=true and value=nil will delete the document body.
func (c *Collection) WriteWithXattr(
	key string,
	xattrKey string,
	exp Exp,
	cas CAS,
	opts *sgbucket.MutateInOptions,
	value []byte,
	xattrValue []byte,
	isDelete bool,
	deleteBody bool,
) (casOut CAS, err error) {
	traceEnter("WriteWithXattr", "%q, %q, cas=%d, exp=%d, isDelete=%v, deleteBody=%v ...", key, xattrKey, cas, exp, isDelete, deleteBody)
	casOut, err = c.writeWithXattr(key, value, sgbucket.Xattr{Name: xattrKey, Value: xattrValue}, &cas, exp, isDelete, deleteBody, false)
	traceExit("WriteWithXattr", err, "0x%x", casOut)
	return
}

// CAS-safe write of a document and its associated named xattr
func (c *Collection) WriteCasWithXattr(
	key string,
	xattrKey string,
	exp Exp,
	cas CAS,
	opts *sgbucket.MutateInOptions,
	v interface{},
	xv interface{},
) (casOut CAS, err error) {
	traceEnter("WriteCasWithXattr", "%q, %q, cas=%d, exp=%d ...", key, xattrKey, cas, exp)
	var value, xattrValue []byte
	if value, err = encodeAsRaw(v, true); err == nil {
		if xattrValue, err = encodeAsRaw(xv, true); err == nil {
			casOut, err = c.writeWithXattr(key, value, sgbucket.Xattr{Name: xattrKey, Value: xattrValue}, &cas, exp, false, false, false)
		}
	}
	traceExit("WriteCasWithXattr", err, "0x%x", casOut)
	return
}

// WriteUpdateWithXattr retrieves the existing doc from the c, invokes the callback to update
// the document, then writes the new document to the c.  Will repeat this process on CAS
// failure.  If `previous` is provided, will pass those values to the callback on the first
// iteration instead of retrieving from the c.
func (c *Collection) WriteUpdateWithXattr(
	key string,
	xattrKey string,
	userXattrKey string,
	exp Exp,
	opts *sgbucket.MutateInOptions,
	previous *sgbucket.BucketDocument,
	callback sgbucket.WriteUpdateWithXattrFunc,
) (casOut CAS, err error) {
	traceEnter("WriteUpdateWithXattr", "%q, %q, %q, exp=%d, ...", key, xattrKey, userXattrKey, exp)
	defer func() { traceExit("WriteUpdateWithXattr", err, "0x%x", casOut) }()
	for {
		if previous == nil {
			// Get current doc if no previous doc was provided:
			prevDoc, err := c.getRawWithXattr(key, xattrKey, userXattrKey)
			if err != nil {
				if _, ok := err.(sgbucket.MissingError); !ok {
					return 0, err
				}
			}
			previous = &prevDoc
			trace("\tread BucketDocument{Body: %q, Xattr: %q, UserXattr: %q, Cas: %d}", previous.Body, previous.Xattr, previous.UserXattr, previous.Cas)
		} else {
			trace("\tprevious = BucketDocument{Body: %q, Xattr: %q, UserXattr: %q, Cas: %d}", previous.Body, previous.Xattr, previous.UserXattr, previous.Cas)
		}

		// Invoke the callback:
		updatedDoc, updatedXattr, deleteDoc, newExp, err := callback(previous.Body, previous.Xattr, previous.UserXattr, previous.Cas)
		if err != nil {
			if err == sgbucket.ErrCasFailureShouldRetry {
				// Callback wants us to retry:
				previous = nil
				continue
			}
			return previous.Cas, err
		}
		if newExp != nil {
			exp = *newExp
		}

		// Update body and/or xattr:
		casOut, err = c.WriteWithXattr(key, xattrKey, exp, previous.Cas, opts, updatedDoc, updatedXattr, false, deleteDoc)

		if _, ok := err.(sgbucket.CasMismatchErr); !ok {
			// Exit loop on success or failure
			return casOut, err
		}

		// ...else retry. Clear `previous` to force a Get this time.
		previous = nil
		trace("\twriteupdatewithxattr retrying...")
	}
}

// Delete a document's body and an xattr simultaneously.
func (c *Collection) DeleteWithXattr(
	key string,
	xattrKey string,
) error {
	traceEnter("DeleteWithXattr", "%q, %q", key, xattrKey)
	err := c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		e := &event{
			key:        key,
			cas:        newCas,
			isDeletion: true,
		}
		row := txn.QueryRow(`SELECT xattrs FROM documents WHERE collection=?1 AND key=?2`, c.id, key)
		var rawXattrs []byte
		err := scan(row, &rawXattrs)
		if err != nil {
			return nil, remapKeyError(err, key)
		}
		rawXattrs = removeXattrs(rawXattrs, xattrKey)
		e.xattrs = rawXattrs

		_, err = txn.Exec(`UPDATE documents SET value=null, xattrs=?1, cas=?2 WHERE collection=?3 AND key=?4`, rawXattrs, newCas, c.id, key)
		return e, err
	})
	traceExit("DeleteWithXattr", err, "ok")
	return err
}

//////// INTERNALS:

// just gets doc's raw xattrs during a transaction
func (c *Collection) getRawXattrs(txn *sql.Tx, key string) ([]byte, error) {
	var rawXattrs []byte
	row := txn.QueryRow(
		`SELECT xattrs FROM documents WHERE collection=?1 AND key=?2`, c.id, key)
	err := scan(row, &rawXattrs)
	return rawXattrs, err
}

// get doc's raw body and an xattr.
func (c *Collection) getRawWithXattr(key string, xattrKey string, userXattrKey string) (rawDoc sgbucket.BucketDocument, err error) {
	xattrIsDocument := xattrKey == "$document"
	var xattrPath, userXattrPath string
	if xattrIsDocument {
		xattrPath = "$.___"
	} else if xattrPath, err = xattrKeyToPath(xattrKey); err != nil {
		return
	}
	if userXattrKey == "" {
		userXattrPath = "$.___"
	} else if userXattrPath, err = xattrKeyToPath(userXattrKey); err != nil {
		return
	}
	row := c.db().QueryRow(`SELECT value, cas, xattrs -> ?1, xattrs -> ?2 FROM documents
							WHERE collection=?3 AND key=?4`, xattrPath, userXattrPath, c.id, key)
	var xattr, userXattr []byte
	err = scan(row, &rawDoc.Body, &rawDoc.Cas, &xattr, &userXattr)
	if err != nil {
		err = remapKeyError(err, key)
		return
	}
	if xattrIsDocument {
		rawDoc.Xattr = []byte(fmt.Sprintf(`{"value_crc32c":%q}`, encodedCRC32c(rawDoc.Body)))
	} else if len(xattr) > 0 {
		rawDoc.Xattr = xattr
	}
	if userXattrKey != "" && len(userXattr) > 0 {
		rawDoc.UserXattr = userXattr
	}
	return
}

// Replaces a doc's body and/or an xattr.
func (c *Collection) writeWithXattr(
	key string, // doc key
	val []byte, // document body; nil means no change
	xattr sgbucket.Xattr, // xattr; nil value means delete
	ifCas *CAS, // if non-nil, must match current CAS; 0 for insert
	exp Exp, // expiration
	isDelete bool, // ignored: I have no idea what exactly it does...
	deleteBody bool, // if true, delete the doc body
	xattrIsUser bool, // true if this is a user xattr
) (casOut CAS, err error) {
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		exp = absoluteExpiry(exp)
		e := &event{
			key: key,
			exp: exp,
		}
		row := txn.QueryRow(`SELECT value, isJSON, cas, xattrs FROM documents WHERE collection=?1 AND key=?2`,
			c.id, key)
		casOut = newCas
		var rawXattrs []byte
		err := scan(row, &e.value, &e.isJSON, &e.cas, &rawXattrs)
		if err != nil && err != sql.ErrNoRows {
			return nil, remapKeyError(err, key)
		} else if ifCas != nil && *ifCas != e.cas {
			return nil, sgbucket.CasMismatchErr{Expected: *ifCas, Actual: e.cas}
		}

		var xattrs semiParsedXattrs
		json.Unmarshal(rawXattrs, &xattrs)

		// Update the body:
		if deleteBody {
			e.value = nil
			e.isJSON = false
			e.isDeletion = true
			removeUserXattrs(xattrs)
			trace("\t\tSet doc %q body -> nil", key)
		} else if val != nil {
			e.value = val
			e.isJSON = true //???
			if Logging >= LevelTrace {
				trace("\t\tSet doc %q body = %s", key, val) // dump body; super verbose
			} else {
				trace("\t\tSet doc %q body", key)
			}
		}

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
				xattrs = semiParsedXattrs{}
			}
			xattrs[xattr.Name] = json.RawMessage(xattr.Value)
			trace("\t\tSet doc %q xattr %q = %s", key, xattr.Name, xattr.Value)
		} else {
			if _, found := xattrs[xattr.Name]; found {
				delete(xattrs, xattr.Name)
				trace("\t\tDeleted doc %q xattr %s", key, xattr.Name)
			} else {
				return nil, sgbucket.MissingError{Key: key}
			}
		}
		rawXattrs, _ = json.Marshal(xattrs)

		if err = checkDocSize(len(e.value) + len(rawXattrs)); err != nil {
			return nil, err
		}

		e.xattrs = rawXattrs
		e.cas = newCas
		_, err = txn.Exec(`INSERT INTO documents(collection,key,value,isJSON,cas,exp,xattrs)
							VALUES (?5,?6,?1,?2,?3,?7,?4)
							ON CONFLICT (collection,key) DO
								UPDATE SET value=?1, isJSON=?2, cas=?3, exp=?7, xattrs=?4
								WHERE collection=?5 AND key=?6`,
			e.value, e.isJSON, e.cas, rawXattrs, c.id, key, exp)
		return e, err
	})
	return
}

//////// HELPERS:

// Converts an Xattr key to a SQLite JSON path.
func xattrKeyToPath(xattrKey string) (path string, err error) {
	if strings.ContainsAny(xattrKey, `$.[]`) {
		// TODO: Support hierarchical paths
		err = fmt.Errorf("rosmar does not support Xattr key `%s`", xattrKey)
	} else {
		path = `$.` + xattrKey
	}
	return
}

func processXattrs(rawXattrs []byte, fn func(xattrs semiParsedXattrs)) []byte {
	if len(rawXattrs) > 0 {
		var xattrs semiParsedXattrs
		_ = json.Unmarshal(rawXattrs, &xattrs)
		fn(xattrs)
		if len(xattrs) > 0 {
			rawXattrs, _ = json.Marshal(xattrs)
		} else {
			rawXattrs = nil
		}
	}
	return rawXattrs
}

// Removes Xattrs from the raw JSON form.
func removeXattrs(rawXattrs []byte, xattrKeys ...string) (rawResult []byte) {
	rawResult = processXattrs(rawXattrs, func(xattrs semiParsedXattrs) {
		for _, key := range xattrKeys {
			delete(xattrs, key)
		}
	})
	return
}

func removeUserXattrs(xattrs semiParsedXattrs) {
	for k, _ := range xattrs {
		if k == "" || k[0] != '_' {
			delete(xattrs, k)
		}
	}
}

// Sets JSON properties "cas" to the given `cas`, and "value_crc" to CRC checksum of `docValue`.
func (e *event) expandSyncXattrMacros(xattr any) {
	if xattrMap, ok := xattr.(map[string]any); ok {
		// For some reason Server encodes `cas` as 8 hex bytes in little-endian order...
		casBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(casBytes, e.cas)
		xattrMap["cas"] = fmt.Sprintf("0x%x", casBytes)

		xattrMap["value_crc32c"] = encodedCRC32c(e.value)
	}
}
