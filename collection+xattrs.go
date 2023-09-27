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
	_ context.Context,
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
	_ context.Context,
	key string,
	xattrKey string,
	xv []byte,
) (casOut CAS, err error) {
	traceEnter("SetXattr", "%q, %q", key, xattrKey)
	casOut, err = c.writeWithXattr(key, nil, xattrKey, payload{marshaled: xv}, nil, nil, xNoOpts, nil)
	traceExit("SetXattr", err, "0x%x", casOut)
	return casOut, err
}

// Remove a single xattr.
func (c *Collection) RemoveXattr(
	_ context.Context,
	key string,
	xattrKey string,
	cas CAS,
) error {
	traceEnter("RemoveXattr", "%q, %q", key, xattrKey)
	_, err := c.writeWithXattr(key, nil, xattrKey, payload{}, &cas, nil, xNoOpts, nil)
	traceExit("RemoveXattr", err, "ok")
	return err
}

// Remove one or more xattrs.
func (c *Collection) DeleteXattrs(
	_ context.Context,
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
	casOut, err = c.writeWithXattr(key, nil, xattrKey, payload{parsed: xv}, nil, nil, xNoOpts, nil)
	traceExit("WriteUserXattr", err, "0x%x", casOut)
	return casOut, err
}

func (c *Collection) DeleteUserXattr(
	key string,
	xattrKey string,
) (casOut CAS, err error) {
	traceEnter("DeleteUserXattr", "%q, %q", key, xattrKey)
	casOut, err = c.writeWithXattr(key, nil, xattrKey, payload{}, nil, nil, xNoOpts, nil)
	traceExit("DeleteUserXattr", err, "0x%x", casOut)
	return casOut, err
}

//////// BODY + XATTRS:

// Get the document's body and an xattr and a user xattr(?)
func (c *Collection) GetWithXattr(
	_ context.Context,
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
	_ context.Context,
	key string,
	xattrKey string,
	exp Exp,
	cas CAS,
	value []byte,
	xv []byte,
	isDelete bool, // ignored; unnecessary in Rosmar though it has meaning in CBS
	deleteBody bool,
	opts *sgbucket.MutateInOptions,
) (casOut CAS, err error) {
	traceEnter("WriteWithXattr", "%q, %q, cas=%d, exp=%d, isDelete=%v, deleteBody=%v ...", key, xattrKey, cas, exp, isDelete, deleteBody)
	expP := ifelse(opts != nil && opts.PreserveExpiry, nil, &exp)
	vp := ifelse(value != nil || deleteBody, &payload{marshaled: value}, nil)
	casOut, err = c.writeWithXattr(key, vp, xattrKey, payload{marshaled: xv}, &cas, expP, xNoOpts, opts)
	traceExit("WriteWithXattr", err, "0x%x", casOut)
	return
}

// CAS-safe write of a document and its associated named xattr
func (c *Collection) WriteCasWithXattr(
	_ context.Context,
	key string,
	xattrKey string,
	exp Exp,
	cas CAS,
	v interface{},
	xv interface{},
	opts *sgbucket.MutateInOptions,
) (casOut CAS, err error) {
	traceEnter("WriteCasWithXattr", "%q, %q, cas=%d, exp=%d ...", key, xattrKey, cas, exp)
	expP := &exp
	vp := ifelse(v != nil, &payload{parsed: v}, nil)
	if opts != nil && opts.PreserveExpiry {
		expP = nil
	}
	casOut, err = c.writeWithXattr(key, vp, xattrKey, payload{parsed: xv}, &cas, expP, xNoOpts, opts)
	traceExit("WriteCasWithXattr", err, "0x%x", casOut)
	return
}

// WriteUpdateWithXattr retrieves the existing doc from the c, invokes the callback to update
// the document, then writes the new document to the c.  Will repeat this process on CAS
// failure.  If `previous` is provided, will pass those values to the callback on the first
// iteration instead of retrieving from the c.
func (c *Collection) WriteUpdateWithXattr(
	ctx context.Context,
	key string,
	xattrKey string,
	userXattrKey string,
	exp Exp,
	previous *sgbucket.BucketDocument,
	opts *sgbucket.MutateInOptions,
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
		updatedDoc, updatedXattr, deleteDoc, newExp, updatedOpts, err := callback(previous.Body, previous.Xattr, previous.UserXattr, opts, previous.Cas)
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
		// update the mutate in options if necessary
		if updatedOpts != nil {
			opts = updatedOpts
		}

		// Update body and/or xattr:
		casOut, err = c.WriteWithXattr(ctx, key, xattrKey, exp, previous.Cas, updatedDoc, updatedXattr, false, deleteDoc, opts)

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
	ctx context.Context,
	key string,
	xattrKey string,
) (err error) {
	traceEnter("DeleteWithXattr", "%q, %q", key, xattrKey)
	defer func() { traceExit("DeleteWithXattr", err, "ok") }()
	return c._deleteBodyAndXattr(key, xattrKey, false)
}

//////// XattrStore2 INTERFACE:

// Updates a document's xattr.
func (c *Collection) UpdateXattr(
	_ context.Context,
	key string,
	xattrKey string,
	exp uint32,
	cas uint64,
	xv interface{},
	opts *sgbucket.MutateInOptions,
) (casOut uint64, err error) {
	traceEnter("UpdateXattr", "%q, %q, exp=%d, cas=0x%x ...", key, xattrKey, exp, cas)
	defer func() { traceExit("UpdateXattr", err, "0x%x", casOut) }()
	return c.writeWithXattr(key, nil, xattrKey, payload{parsed: xv}, &cas, &exp, xNoOpts, opts)
}

// Updates an xattr and deletes the body (making the doc a tombstone.)
func (c *Collection) UpdateXattrDeleteBody(
	_ context.Context,
	key string,
	xattrKey string,
	exp uint32,
	cas uint64,
	xv interface{},
	opts *sgbucket.MutateInOptions,
) (casOut uint64, err error) {
	traceEnter("UpdateXattrDeleteBody", "%q, %q, exp=%d, cas=0x%x ...", key, xattrKey, exp, cas)
	defer func() { traceExit("UpdateXattrDeleteBody", err, "0x%x", casOut) }()
	return c.writeWithXattr(key, &payload{}, xattrKey, payload{parsed: xv}, &cas, &exp, xNoOpts, opts)
}

// Deletes the document's body, and updates the CAS & CRC32 macros in the xattr.
func (c *Collection) DeleteBody(
	_ context.Context,
	key string,
	xattrKey string,
	exp uint32,
	cas uint64,
	opts *sgbucket.MutateInOptions,
) (casOut uint64, err error) {
	traceEnter("DeleteBody", "%q, %q, exp=%d, cas=0x%x ...", key, xattrKey, exp, cas)
	defer func() { traceExit("DeleteBody", err, "0x%x", casOut) }()
	return c.writeWithXattr(key, &payload{}, xattrKey, payload{}, &cas, &exp, xPreserveXattr, opts)
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
	mutateOpts *sgbucket.MutateInOptions, // expiry and macro expansion options
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
			// Expand any macros specified in the mutateOpts
			if err := e.expandXattrMacros(xattrKey, parsedXattr, mutateOpts); err != nil {
				return nil, err
			}
			xattrVal.setParsed(parsedXattr)

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

// macroExpand returns the string representation for the specified expansion for the event
func (e *event) macroExpand(expansion sgbucket.MacroExpansionType) (string, error) {

	switch expansion {
	case sgbucket.MacroCas:
		return e.casAsString(), nil
	case sgbucket.MacroCrc32c:
		return encodedCRC32c(e.value), nil
	default:
		return "", fmt.Errorf("Unsupported MacroExpansionType: %v", expansion)
	}
}

// casAsString generates the string representation of CAS used by Couchbase Server's macro expansion
func casAsString(value CAS) string {
	casBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(casBytes, value)
	return fmt.Sprintf("0x%x", casBytes)
}

func (e *event) casAsString() string {
	return casAsString(e.cas)
}

// expandXattrMacros executes any macro expansions defined in mutateOpts
func (e *event) expandXattrMacros(xattrKey string, xattr any, mutateOpts *sgbucket.MutateInOptions) error {

	if mutateOpts == nil || len(mutateOpts.MacroExpansion) == 0 {
		return nil
	}

	xattrMap, ok := xattr.(map[string]any)
	if !ok {
		return fmt.Errorf("Unable to convert xattr to map for xattr key %s (type: %T)", xattrKey, xattr)
	}

	// loop through provided macro expansions, check they apply to this xattr, then upsert into the xattr
	// at the specified path
	for _, v := range mutateOpts.MacroExpansion {
		path, err := parseSubdocPath(v.Path)
		if err != nil {
			return err
		}
		if path[0] != xattrKey {
			return fmt.Errorf("Unknown xattr in macro expansion path, expect %s: %v ", xattrKey, path)
		}

		expandedValue, err := e.macroExpand(v.Type)
		if err != nil {
			return err
		}

		if err := upsertSubdocValue(xattrMap, path[1:], expandedValue); err != nil {
			return fmt.Errorf("Unable to set macro expansion value at path: %v: %w", v.Path, err)
		}
	}
	return nil
}

var (
	// Enforce interface conformance:
	_ sgbucket.UserXattrStore = &Collection{}
)
