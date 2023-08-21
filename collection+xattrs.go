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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
)

type semiParsedXattrs = map[string]json.RawMessage

// PersistedHybridLogicalVector is the persisted representation of the Hybrid Logical Vector, needed to update _sync xattrs
type PersistedHybridLogicalVector struct {
	CurrentVersionCAS string            `json:"cvCas,omitempty"`
	SourceID          string            `json:"src,omitempty"`
	Version           string            `json:"vrs,omitempty"`
	MergeVersions     map[string]string `json:"mv,omitempty"`
	PreviousVersions  map[string]string `json:"pv,omitempty"`
}

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
	casOut, err = c.writeWithXattr(key, nil, xattrKey, payload{marshaled: xv}, nil, nil, xNoOpts)
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
	_, err := c.writeWithXattr(key, nil, xattrKey, payload{}, &cas, nil, xNoOpts)
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
		if rawXattrs, err = removeXattrs(rawXattrs, xattrKeys...); err != nil {
			return nil, err
		}
		e.xattrs = rawXattrs
		_, err = txn.Exec(`UPDATE documents SET xattrs=?1, cas=?2 WHERE collection=?2 AND key=?3`, rawXattrs, newCas, c.id, key)
		return e, err
	})
	traceExit("DeleteXattrs", err, "ok")
	return err
}

//////// UserXattrStore INTERFACE:

func (c *Collection) WriteUserXattr(
	key string,
	xattrKey string,
	xv interface{},
) (casOut CAS, err error) {
	traceEnter("WriteUserXattr", "%q, %q, ...", key, xattrKey)
	casOut, err = c.writeWithXattr(key, nil, xattrKey, payload{parsed: xv}, nil, nil, xNoOpts)
	traceExit("WriteUserXattr", err, "0x%x", casOut)
	return casOut, err
}

func (c *Collection) DeleteUserXattr(
	key string,
	xattrKey string,
) (casOut CAS, err error) {
	traceEnter("DeleteUserXattr", "%q, %q", key, xattrKey)
	casOut, err = c.writeWithXattr(key, nil, xattrKey, payload{}, nil, nil, xNoOpts)
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
	xv []byte,
	isDelete bool, // ignored; unnecessary in Rosmar though it has meaning in CBS
	deleteBody bool,
) (casOut CAS, err error) {
	traceEnter("WriteWithXattr", "%q, %q, cas=%d, exp=%d, isDelete=%v, deleteBody=%v ...", key, xattrKey, cas, exp, isDelete, deleteBody)
	expP := ifelse(opts != nil && opts.PreserveExpiry, nil, &exp)
	vp := ifelse(value != nil || deleteBody, &payload{marshaled: value}, nil)
	casOut, err = c.writeWithXattr(key, vp, xattrKey, payload{marshaled: xv}, &cas, expP, xNoOpts)
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
	expP := &exp
	vp := ifelse(v != nil, &payload{parsed: v}, nil)
	if opts != nil && opts.PreserveExpiry {
		expP = nil
	}
	casOut, err = c.writeWithXattr(key, vp, xattrKey, payload{parsed: xv}, &cas, expP, xNoOpts)
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
) (err error) {
	traceEnter("DeleteWithXattr", "%q, %q", key, xattrKey)
	defer func() { traceExit("DeleteWithXattr", err, "ok") }()
	return c._deleteBodyAndXattr(key, xattrKey, false)
}

//////// XattrStore2 INTERFACE:

// Creates a tombstone doc (no value) with an xattr.
func (c *Collection) InsertXattr(
	key string,
	xattrKey string,
	exp uint32,
	cas uint64,
	xv interface{},
) (casOut uint64, err error) {
	traceEnter("InsertXattr", "%q, %q, exp=%d, ...", key, xattrKey, exp)
	defer func() { traceExit("InsertXattr", err, "0x%x", casOut) }()
	return c.writeWithXattr(key, nil, xattrKey, payload{parsed: xv}, &cas, &exp, xInsertDoc+xInsertXattr)
}

// Creates a document, with an xattr, only if it doesn't already exist.
func (c *Collection) InsertBodyAndXattr(
	key string,
	xattrKey string,
	exp uint32,
	v interface{},
	xv interface{},
) (casOut uint64, err error) {
	traceEnter("InsertBodyAndXattr", "%q, %q, exp=%d, ...", key, xattrKey, exp)
	defer func() { traceExit("InsertBodyAndXattr", err, "0x%x", casOut) }()
	return c.writeWithXattr(key, &payload{parsed: v}, xattrKey, payload{parsed: xv}, nil, &exp, xInsertDoc)
}

// Updates a document's xattr.
func (c *Collection) UpdateXattr(
	key string,
	xattrKey string,
	exp uint32,
	cas uint64,
	xv interface{},
) (casOut uint64, err error) {
	traceEnter("UpdateXattr", "%q, %q, exp=%d, cas=0x%x ...", key, xattrKey, exp, cas)
	defer func() { traceExit("UpdateXattr", err, "0x%x", casOut) }()
	return c.writeWithXattr(key, nil, xattrKey, payload{parsed: xv}, &cas, &exp, xNoOpts)
}

// Updates a document's value and an xattr.
func (c *Collection) UpdateBodyAndXattr(
	key string,
	xattrKey string,
	exp uint32,
	cas uint64,
	opts *sgbucket.MutateInOptions,
	v interface{},
	xv interface{},
) (casOut uint64, err error) {
	traceEnter("UpdateBodyAndXattr", "%q, %q, exp=%d, cas=0x%x ...", key, xattrKey, exp, cas)
	defer func() { traceExit("UpdateBodyAndXattr", err, "0x%x", casOut) }()
	vp := ifelse(v != nil, &payload{parsed: v}, nil)
	expP := &exp
	if opts != nil && opts.PreserveExpiry {
		expP = nil
	}
	return c.writeWithXattr(key, vp, xattrKey, payload{parsed: xv}, &cas, expP, xNoOpts)
}

// Updates an xattr and deletes the body (making the doc a tombstone.)
func (c *Collection) UpdateXattrDeleteBody(
	key string,
	xattrKey string,
	exp uint32,
	cas uint64,
	xv interface{},
) (casOut uint64, err error) {
	traceEnter("UpdateXattrDeleteBody", "%q, %q, exp=%d, cas=0x%x ...", key, xattrKey, exp, cas)
	defer func() { traceExit("UpdateXattrDeleteBody", err, "0x%x", casOut) }()
	return c.writeWithXattr(key, &payload{}, xattrKey, payload{parsed: xv}, &cas, &exp, xNoOpts)
}

// Deletes the document's body, and updates the CAS & CRC32 macros in the xattr.
func (c *Collection) DeleteBody(
	key string,
	xattrKey string,
	exp uint32,
	cas uint64,
) (casOut uint64, err error) {
	traceEnter("DeleteBody", "%q, %q, exp=%d, cas=0x%x ...", key, xattrKey, exp, cas)
	defer func() { traceExit("DeleteBody", err, "0x%x", casOut) }()
	return c.writeWithXattr(key, &payload{}, xattrKey, payload{}, &cas, &exp, xPreserveXattr)
}

// Deletes the document's body and an xattr.
// Unlike DeleteWithXattr, this fails if the doc is a tombstone (no body).
func (c *Collection) DeleteBodyAndXattr(key string, xattrKey string) (err error) {
	traceEnter("DeleteBodyAndXattr", "%q, %q", key, xattrKey)
	defer func() { traceExit("DeleteBodyAndXattr", err, "ok") }()
	return c._deleteBodyAndXattr(key, xattrKey, true)
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
	} else if xattrPath, err = xattrKeyToSQLitePath(xattrKey); err != nil {
		return
	}
	if userXattrKey == "" {
		userXattrPath = "$.___"
	} else if userXattrPath, err = xattrKeyToSQLitePath(userXattrKey); err != nil {
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

// Delete a document's body and an xattr simultaneously.
func (c *Collection) _deleteBodyAndXattr(
	key string,
	xattrKey string,
	bodyMustExist bool,
) error {
	err := c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		e := &event{
			key:        key,
			cas:        newCas,
			isDeletion: true,
		}
		var bodyExists bool
		row := txn.QueryRow(`SELECT xattrs, value NOT NULL FROM documents WHERE collection=?1 AND key=?2`, c.id, key)
		err := scan(row, &e.xattrs, &bodyExists)
		if err != nil {
			return nil, remapKeyError(err, key)
		} else if bodyMustExist && !bodyExists {
			return nil, sgbucket.MissingError{Key: key}
		} else if e.xattrs, err = removeXattrs(e.xattrs, xattrKey); err != nil {
			return nil, err
		}
		_, err = txn.Exec(`UPDATE documents SET value=null, xattrs=?1, cas=?2 WHERE collection=?3 AND key=?4`, e.xattrs, newCas, c.id, key)
		return e, err
	})
	return err
}

// Option flags for writeWithXattr
type writeXattrOpts uint8

const (
	xInsertDoc     = writeXattrOpts(1 << iota) // Create new doc; fail if it already exists
	xInsertXattr                               // Fail if `xattr` already exists
	xPreserveXattr                             // Leave xattr value alone, just expand macros
	xNoOpts        = writeXattrOpts(0)         // Default behavior
)

// Swiss Army knife method for modifying a document xattr, with or without changing the body.
func (c *Collection) writeWithXattr(
	key string, // doc key
	val *payload, // if non-nil, updates doc body; a nil payload means delete
	xattrKey string, // xattr key
	xattrVal payload, // xattr value; a nil payload deletes the xattr
	ifCas *CAS, // if non-nil, must match current CAS; 0 for insert
	exp *Exp, // if non-nil, sets expiration to this value
	opts writeXattrOpts, // option flags
) (casOut CAS, err error) {
	// Validate xattr key/value before going into the transaction:
	if err = validateXattrKey(xattrKey); err != nil {
		return
	}
	var parsedXattr any
	if !xattrVal.isNil() {
		if parsedXattr, err = xattrVal.unmarshalJSON(); err != nil {
			return 0, fmt.Errorf("unparseable xattr: %w", err)
		}
	}

	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		e := &event{
			key: key,
			cas: newCas,
		}

		// First read the existing doc, if any:
		row := txn.QueryRow(`SELECT value, isJSON, cas, exp, xattrs FROM documents WHERE collection=?1 AND key=?2`,
			c.id, key)
		var prevCas CAS
		if err := scan(row, &e.value, &e.isJSON, &prevCas, &e.exp, &e.xattrs); err == nil {
			if e.value == nil {
				e.xattrs = nil // xattrs are cleared whenever resurrecting a tombstone
			} else if opts&xInsertDoc != 0 {
				return nil, sgbucket.ErrKeyExists
			}
		} else if err != sql.ErrNoRows {
			return nil, remapKeyError(err, key)
		}

		if ifCas != nil && *ifCas != prevCas {
			if e.value == nil && *ifCas == 0 {
				// It's OK to use CAS 0 to refer to a tombstone.
			} else {
				return nil, sgbucket.CasMismatchErr{Expected: *ifCas, Actual: prevCas}
			}
		}

		var xattrs semiParsedXattrs
		if e.xattrs != nil {
			if err = json.Unmarshal(e.xattrs, &xattrs); err != nil {
				return nil, fmt.Errorf("document %q xattrs are unreadable: %w", key, err)
			}
		}

		if val != nil {
			if val.isNil() {
				// Delete body:
				e.value = nil
				e.isJSON = false
				e.isDeletion = true
				removeUserXattrs(xattrs) // Remove user xattrs when tombstoning doc
			} else {
				// Update body:
				e.isJSON = val.isJSON()
				e.value, err = val.asByteArray()
				if err != nil {
					return nil, err
				}
			}
		}

		if (opts & xPreserveXattr) != 0 {
			// The xPreserveXattr flag means to keep the xattr's value (but do macro expansion.)
			if opts&xInsertXattr != 0 {
				return nil, fmt.Errorf("illegal options to rosmar.Collection.writeWithXattr")
			}
			existingVal, ok := xattrs[xattrKey]
			if !ok {
				existingVal = json.RawMessage(`{}`)
			}
			xattrVal.setMarshaled(existingVal)
			if parsedXattr, err = xattrVal.unmarshalJSON(); err != nil {
				return nil, err
			}
		}

		if !xattrVal.isNil() {
			// Set xattr:
			if (opts&xInsertXattr != 0) && xattrs[xattrKey] != nil {
				return nil, sgbucket.ErrPathExists
			}
			if xattrKey == "_sync" {
				// The "_sync" xattr has two properties that get macro-expanded:
				e.expandSyncXattrMacros(parsedXattrc, c.bucket.uuid, opts)
				xattrVal.setParsed(parsedXattr)
			}
			var rawXattr []byte
			if rawXattr, err = xattrVal.marshalJSON(); err != nil {
				return nil, err
			}
			if xattrs == nil {
				xattrs = semiParsedXattrs{}
			}
			xattrs[xattrKey] = json.RawMessage(rawXattr)
			trace("\t\tSet doc %q xattr %q = %s", key, xattrKey, rawXattr)
		} else {
			// Delete xattr:
			if _, found := xattrs[xattrKey]; found {
				delete(xattrs, xattrKey)
				trace("\t\tDeleted doc %q xattr %s", key, xattrKey)
			} else {
				return nil, sgbucket.ErrPathNotFound
			}
		}
		e.xattrs, _ = json.Marshal(xattrs)

		if err = checkDocSize(len(e.value) + len(e.xattrs)); err != nil {
			return nil, err
		}

		casOut = newCas
		if exp != nil {
			e.exp = absoluteExpiry(*exp)
		}

		_, err = txn.Exec(`INSERT INTO documents(collection,key,value,isJSON,cas,exp,xattrs)
							VALUES (?5,?6,?1,?2,?3,?7,?4)
							ON CONFLICT (collection,key) DO
								UPDATE SET value=?1, isJSON=?2, cas=?3, exp=?7, xattrs=?4
								WHERE collection=?5 AND key=?6`,
			e.value, e.isJSON, e.cas, e.xattrs, c.id, key, e.exp)
		return e, err
	})
	return
}

//////// HELPERS:

// Checks an xattr key: Rosmar doesn't support multi-component key paths for xattrs.
func validateXattrKey(xattrKey string) error {
	if strings.ContainsAny(xattrKey, `$.[]`) {
		// TODO: Support hierarchical paths
		return fmt.Errorf("rosmar does not support Xattr key `%s`", xattrKey)
	} else {
		return nil
	}
}

// Converts an Xattr key to a SQLite JSON path.
func xattrKeyToSQLitePath(xattrKey string) (path string, err error) {
	return `$.` + xattrKey, validateXattrKey(xattrKey)
}

// Semi-parses the xattrs from JSON, passes that to the callback, then re-marshals and returns it.
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
func removeXattrs(rawXattrs []byte, xattrKeys ...string) (rawResult []byte, err error) {
	rawResult = processXattrs(rawXattrs, func(xattrs semiParsedXattrs) {
		for _, key := range xattrKeys {
			if err = validateXattrKey(key); err != nil {
				break
			}
			delete(xattrs, key)
		}
	})
	return
}

// Removes user (non-underscore-prefixed) Xattrs.
func removeUserXattrs(xattrs semiParsedXattrs) {
	for k := range xattrs {
		if k == "" || k[0] != '_' {
			delete(xattrs, k)
		}
	}
}

// macroExpand takes the macro and assigned 'value' the correct value based on what the macro is provided (this is gocb macro format)
func (e *event) macroExpand(value string, casServerFormat string, bucketUUID string) string {
	if value == "\"${Mutation.CAS}\"" {
		value = casServerFormat
	} else if value == "\"${Mutation.value_crc32c}\"" {
		value = encodedCRC32c(e.value)
	} else {
		// if the value isn't to be macro expanded to CAS or CRC hash then it is a source ID
		value = bucketUUID
	}
	return value
}

// Sets JSON properties "cas" to the given `cas`, and "value_crc" to CRC checksum of `docValue`.
func (e *event) expandSyncXattrMacros(xattr any, bucketUUID string, mutateSpec *sgbucket.MutateInOptions) error {
	var err error
	if xattrMap, ok := xattr.(map[string]any); ok {
		// create server format of cas value
		casBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(casBytes, e.cas)
		casServerFormat := fmt.Sprintf("0x%x", casBytes)

		// loop through provided mutate in spec and macro expand and insert into xattr
		for _, v := range mutateSpec.Spec {
			str := fmt.Sprint(v.Value)
			v.Value = e.macroExpand(str, casServerFormat, bucketUUID)
			err = addToXattr(xattrMap, v.Path, v.Value)
			if err != nil {
				fmt.Errorf("error during macro expansion process: %v", err)
			}
		}
	}
	return nil
}

// addToXattr will take the xattr provdied and a path, evaluate that path exists in the xattr provided and if so will add
// the value to the path specified
func addToXattr(xattr any, subdocKey string, value interface{}) error {
	path, err := parseSubdocPath(subdocKey)
	if err != nil {
		return err
	}
	// we provide the full xattr path from sync gateway, so here we need to cut the leading value from the path provided to rosmar
	// For example we provide full path `_sync.cas` but here we are only working with `_sync` xattrs thus we need to remove `_sync` from the path
	path = path[1:]
	// eval path exists
	subdoc, err := evalSubdocPath(xattr, path[0:len(path)-1])
	if err != nil {
		return err
	}

	parent, ok := subdoc.(map[string]any)
	if !ok {
		return sgbucket.ErrPathMismatch
	}
	// add value to map:
	lastPath := path[len(path)-1]
	if value != nil {
		parent[lastPath] = value
	} else {
		delete(parent, lastPath)
	}
	return nil
}

var (
	// Enforce interface conformance:
	_ sgbucket.UserXattrStore = &Collection{}
	_ sgbucket.XattrStore2    = &Collection{}
)
