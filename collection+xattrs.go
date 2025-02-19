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
	"errors"
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
)

type semiParsedXattrs = map[string]json.RawMessage

const (
	virtualXattrName     = "$document"
	virtualXattrRevSeqNo = "revid"
)

// ////// SGBUCKET XATTR STORE INTERFACE
func (c *Collection) GetXattrs(
	_ context.Context,
	key string,
	xattrKeys []string,
) (map[string][]byte, CAS, error) {
	rawDoc, err := c.getRawWithXattrs(key, xattrKeys)
	if err != nil {
		return nil, 0, err
	}
	if rawDoc.Xattrs == nil || len(rawDoc.Xattrs) == 0 {
		return nil, 0, sgbucket.XattrMissingError{Key: key, Xattrs: xattrKeys}
	}

	return rawDoc.Xattrs, rawDoc.Cas, nil
}

// SetWithMeta updates a document fully with xattrs and body and allows specification of a specific CAS (newCas). This update will always happen as long as oldCas matches the value of existing document. This simulates the kv op setWithMeta.
func (c *Collection) SetWithMeta(_ context.Context, key string, oldCas CAS, newCas CAS, exp uint32, xattrs []byte, body []byte, datatype sgbucket.FeedDataType) error {
	isJSON := datatype&sgbucket.FeedDataTypeJSON != 0
	isDeletion := false
	return c.writeWithMeta(key, body, xattrs, oldCas, newCas, exp, isJSON, isDeletion)
}

// writeWithMeta writes a document which will be stored with a cas value of newCas.  It still performs the standard CAS check for optimistic concurrency using oldCas, when specified.
func (c *Collection) writeWithMeta(key string, body []byte, xattrs []byte, oldCas CAS, newCas CAS, exp uint32, isJSON, isDeletion bool) error {
	var e *event
	err := c.bucket.inTransaction(func(txn *sql.Tx) error {
		var prevCas CAS
		var revSeqNo uint64
		row := txn.QueryRow(`SELECT cas, revSeqNo FROM documents WHERE collection=?1 AND key=?2`,
			c.id, key)
		err := scan(row, &prevCas, &revSeqNo)
		if err != nil && err != sql.ErrNoRows {
			return remapKeyError(err, key)
		}
		if oldCas != prevCas {
			return sgbucket.CasMismatchErr{Expected: oldCas, Actual: prevCas}
		}
		revSeqNo++
		e = &event{
			key:        key,
			value:      body,
			xattrs:     xattrs,
			cas:        newCas,
			exp:        exp,
			isDeletion: isDeletion,
			isJSON:     isJSON,
			revSeqNo:   revSeqNo,
		}
		return c.storeDocument(txn, e)
	})

	if err != nil {
		return err
	}
	if e != nil {
		c.postNewEvent(e)
	}
	return nil
}

// DeleteWithMeta tombstones a document and sets a specific cas. This update will always happen as long as oldCas matches the value of existing document. This simulates the kv op deleteWithMeta.
func (c *Collection) DeleteWithMeta(_ context.Context, key string, oldCas CAS, newCas CAS, exp uint32, xattrs []byte) error {
	var body []byte
	isJSON := false
	isDeletion := true
	return c.writeWithMeta(key, body, xattrs, oldCas, newCas, exp, isJSON, isDeletion)
}

// storeDocument performs a write to the underlying sqlite database of a document from a given event.
func (c *Collection) storeDocument(txn *sql.Tx, e *event) error {
	tombstone := 0
	if e.isDeletion {
		tombstone = 1
	}
	_, err := txn.Exec(`INSERT INTO documents(collection,key,value,isJSON,cas,exp,xattrs,tombstone,revSeqNo)
							VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
							ON CONFLICT (collection,key) DO
								UPDATE SET value=?3, isJSON=?4, cas=?5, exp=?6, xattrs=?7,tombstone=?8, revSeqNo=?9
								WHERE collection=?1 AND key=?2`,
		c.id, e.key, e.value, e.isJSON, e.cas, e.exp, e.xattrs, tombstone, e.revSeqNo)
	return err
}

// Set a single xattr value.
func (c *Collection) SetXattrs(_ context.Context, key string, xattrs map[string][]byte) (CAS, error) {
	traceEnter("SetXattrs", "%q, %q", key, xattrs)
	payloadXattrs := make(map[string]payload, len(xattrs))
	for k, v := range xattrs {
		payloadXattrs[k] = payload{marshaled: v}
	}
	casOut, err := c.writeWithXattrs(key, nil, payloadXattrs, nil, nil, writeXattrOptions{}, nil)
	traceExit("SetXattr", err, "0x%x", casOut)
	return casOut, err
}

func (c *Collection) RemoveXattrs(_ context.Context, key string, xattrKeys []string, cas CAS) error {
	removedXattrs := make(map[string]payload, len(xattrKeys))
	for _, xattrKey := range xattrKeys {
		removedXattrs[xattrKey] = payload{}
	}
	_, err := c.writeWithXattrs(key, nil, removedXattrs, &cas, nil, writeXattrOptions{}, nil)
	return err
}

// Remove one or more subdoc paths from a document.
func (c *Collection) DeleteSubDocPaths(
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
		var revSeqNo uint64
		row := txn.QueryRow(`SELECT value, xattrs, revSeqNo FROM documents WHERE collection=?1 AND key=?2`, c.id, key)
		var rawXattrs []byte
		err := scan(row, &e.value, &rawXattrs, &revSeqNo)
		if err != nil {
			return nil, remapKeyError(err, key)
		}
		if rawXattrs, err = removeXattrs(rawXattrs, xattrKeys...); err != nil {
			return nil, err
		}
		e.xattrs = rawXattrs
		_, err = txn.Exec(`UPDATE documents SET xattrs=?1, cas=?2, revSeqNo=?3 WHERE collection=?4 AND key=?5`, rawXattrs, newCas, revSeqNo, c.id, key)
		return e, err
	})
	traceExit("DeleteXattrs", err, "ok")
	return err
}

// ////// BODY + XATTRS:
func (c *Collection) GetWithXattrs(_ context.Context, key string, xattrKeys []string) (v []byte, xattrs map[string][]byte, cas CAS, err error) {
	rawDoc, err := c.getRawWithXattrs(key, xattrKeys)
	if err != nil {
		return nil, nil, 0, err
	}

	if rawDoc.Body == nil && len(rawDoc.Xattrs) == 0 {
		// Doc exists as tombstone, but not with any of the requested xattrs
		return nil, nil, rawDoc.Cas, sgbucket.MissingError{Key: key}
	}

	xattrs = make(map[string][]byte, len(xattrKeys))
	for _, xattrKey := range xattrKeys {
		encodedXattr, ok := rawDoc.Xattrs[xattrKey]
		if !ok {
			continue
		}
		var xattr []byte
		err = decodeRaw(encodedXattr, &xattr)
		if err != nil {
			return nil, nil, 0, err
		}
		xattrs[xattrKey] = xattr
	}
	return rawDoc.Body, xattrs, rawDoc.Cas, nil
}

// Writes a document and updates xattr values. Fails on a CAS mismatch.
// Parameters:
// - k: The key (document ID)
// - exp: Expiration timestamp (0 for never)
// - cas: Expected CAS value
// - opts: Options; use PreserveExpiry to avoid setting expiry
// - value: The raw value to set, or nil to *leave unchanged*
// - xattrValues: Each key represent a raw xattrs value to set, setting any of these values to nil will result in an error.
// - xattrsToDelete: The names of xattrs to delete.
func (c *Collection) WriteWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, value []byte, xattrsValues map[string][]byte, xattrsToDelete []string, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	expP := ifelse(opts != nil && opts.PreserveExpiry, nil, &exp)
	var vp *payload
	if value != nil {
		vp = &payload{marshaled: value}
	}
	xattrs := make(map[string]payload, len(xattrsValues))
	for xattrKey, xv := range xattrsValues {
		if xv == nil {
			return 0, fmt.Errorf("%s: %w", xattrKey, sgbucket.ErrNilXattrValue)
		}
		xattrs[xattrKey] = payload{marshaled: xv}
	}
	if cas == 0 && xattrsToDelete != nil {
		return 0, sgbucket.ErrDeleteXattrOnDocumentInsert
	} else if len(value) == 0 && len(xattrsValues) == 0 {
		return 0, sgbucket.ErrNeedXattrs
	}
	for _, xattrKey := range xattrsToDelete {
		if _, ok := xattrs[xattrKey]; ok {
			return 0, fmt.Errorf("%s: %w", xattrKey, sgbucket.ErrUpsertAndDeleteSameXattr)
		}
		xattrs[xattrKey] = payload{}
	}
	return c.writeWithXattrs(k, vp, xattrs, &cas, expP, writeXattrOptions{}, opts)
}

// WriteUpdateWithXattrs retrieves the existing doc from the c, invokes the callback to update
// the document, then writes the new document to the c.  Will repeat this process on CAS
// failure.  If `previous` is provided, will pass those values to the callback on the first
// iteration instead of retrieving from the c.
func (c *Collection) WriteUpdateWithXattrs(
	ctx context.Context,
	key string,
	xattrKeys []string,
	exp Exp,
	previous *sgbucket.BucketDocument,
	opts *sgbucket.MutateInOptions,
	callback sgbucket.WriteUpdateWithXattrsFunc,
) (casOut CAS, err error) {
	traceEnter("WriteUpdateWithXattr", "%q, %q, %q, exp=%d, ...", key, xattrKeys, exp)
	defer func() { traceExit("WriteUpdateWithXattr", err, "0x%x", casOut) }()
	for {
		if previous == nil {
			// Get current doc if no previous doc was provided:
			prevDoc, err := c.getRawWithXattrs(key, xattrKeys)
			if err != nil {
				if _, ok := err.(sgbucket.MissingError); !ok {
					return 0, err
				}
			}
			previous = &prevDoc
			trace("\tread BucketDocument{Body: %q, Xattrs: %q, UserXattr: %q, Cas: %d}", previous.Body, previous.Xattrs, previous.Cas)
		} else {
			trace("\tprevious = BucketDocument{Body: %q, Xattr: %q, UserXattr: %q, Cas: %d}", previous.Body, previous.Xattrs, previous.Cas)
		}

		// Invoke the callback:
		updatedDoc, err := callback(previous.Body, previous.Xattrs, previous.Cas)
		if err != nil {
			if err == sgbucket.ErrCasFailureShouldRetry {
				// Callback wants us to retry:
				previous = nil
				continue
			}
			return previous.Cas, err
		}
		var exp Exp
		if updatedDoc.Expiry != nil {
			exp = *updatedDoc.Expiry
		}
		// update the mutate in options if necessary
		if updatedDoc.Spec != nil {
			opts.MacroExpansion = append(opts.MacroExpansion, updatedDoc.Spec...)
		}
		if updatedDoc.IsTombstone {
			deleteBody := previous.Body != nil
			casOut, err = c.WriteTombstoneWithXattrs(ctx, key, exp, previous.Cas, updatedDoc.Xattrs, updatedDoc.XattrsToDelete, deleteBody, opts)
		} else {
			cas := previous.Cas
			if previous.IsTombstone {
				if len(updatedDoc.XattrsToDelete) > 0 {
					return 0, sgbucket.ErrDeleteXattrOnTombstone
				}
				casOut, err = c.WriteResurrectionWithXattrs(ctx, key, exp, updatedDoc.Doc, updatedDoc.Xattrs, opts)
			} else {
				// Update body and/or xattr:
				casOut, err = c.WriteWithXattrs(ctx, key, exp, cas, updatedDoc.Doc, updatedDoc.Xattrs, updatedDoc.XattrsToDelete, opts)
			}
		}

		if _, ok := err.(sgbucket.CasMismatchErr); !ok && !errors.Is(err, sgbucket.ErrKeyExists) {
			// Exit loop on success or failure
			return casOut, err
		}

		// ...else retry. Clear `previous` to force a Get this time.
		previous = nil
		trace("\twriteupdatewithxattr retrying...")
	}
}

// Updates a document's xattr.
func (c *Collection) UpdateXattrs(
	_ context.Context,
	key string,
	exp uint32,
	cas uint64,
	xattrs map[string][]byte,
	opts *sgbucket.MutateInOptions,
) (casOut uint64, err error) {
	traceEnter("UpdateXattr", "%q, %q, exp=%d, cas=0x%x ...", key, xattrs, exp, cas)
	defer func() { traceExit("UpdateXattr", err, "0x%x", casOut) }()
	xv := make(map[string]payload, len(xattrs))
	for xattrKey, xattrVal := range xattrs {
		xv[xattrKey] = payload{parsed: xattrVal}
	}
	return c.writeWithXattrs(key, nil, xv, &cas, &exp, writeXattrOptions{}, opts)
}

func (c *Collection) WriteTombstoneWithXattrs(
	_ context.Context,
	key string,
	exp uint32,
	cas uint64,
	xv map[string][]byte,
	xattrsToDelete []string,
	deleteBody bool, // rosmar doesn't require different handling depending on whether body is present
	opts *sgbucket.MutateInOptions,
) (casOut uint64, err error) {
	if len(xv) == 0 {
		return 0, sgbucket.ErrNeedXattrs
	}
	if !deleteBody && len(xattrsToDelete) == 0 && len(xv) == 0 {
		return 0, sgbucket.ErrNeedXattrs
	}

	if cas == 0 && xattrsToDelete != nil {
		return 0, sgbucket.ErrDeleteXattrOnDocumentInsert
	}

	xattrs := make(map[string]payload, len(xv))
	for xattrKey, xattrVal := range xv {
		if xattrVal == nil {
			return 0, fmt.Errorf("%s: %w", xattrKey, sgbucket.ErrNilXattrValue)
		}
		xattrs[xattrKey] = payload{marshaled: xattrVal}
	}
	for _, xattrKey := range xattrsToDelete {
		if _, ok := xattrs[xattrKey]; ok {
			return 0, fmt.Errorf("%s: %w", xattrKey, sgbucket.ErrUpsertAndDeleteSameXattr)
		}
		xattrs[xattrKey] = payload{}
	}
	checkCas := &cas
	requireExistingDoc := deleteBody || cas != 0
	return c.writeWithXattrs(key, &payload{}, xattrs, checkCas, &exp, writeXattrOptions{requireExistingDoc: requireExistingDoc, deleteBody: deleteBody}, opts)
}

// WriteResurrectionWithXattrs creates an alive document with a given tombstone and xattrs.
func (c *Collection) WriteResurrectionWithXattrs(ctx context.Context, k string, exp uint32, value []byte, xattrsValues map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	if value == nil {
		return 0, sgbucket.ErrNeedBody
	}
	expP := ifelse(opts != nil && opts.PreserveExpiry, nil, &exp)
	var vp *payload
	if value != nil {
		vp = &payload{marshaled: value}
	}
	xattrs := make(map[string]payload, len(xattrsValues))
	for xattrKey, xv := range xattrsValues {
		if xv == nil {
			return 0, fmt.Errorf("%s: %w", xattrKey, sgbucket.ErrNilXattrValue)
		}
		xattrs[xattrKey] = payload{marshaled: xv}
	}
	return c.writeWithXattrs(k, vp, xattrs, nil, expP, writeXattrOptions{insertDoc: true}, opts)
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
	xattrs := map[string]payload{xattrKey: payload{parsed: xv}}
	return c.writeWithXattrs(key, &payload{}, xattrs, &cas, &exp, writeXattrOptions{}, opts)
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
func (c *Collection) getRawWithXattrs(key string, xattrKeys []string) (sgbucket.BucketDocument, error) {
	var revSeqNo int64
	row := c.db().QueryRow(`SELECT value, cas, xattrs, tombstone, revSeqNo FROM documents WHERE collection=?1 AND key=?2`, c.id, key)
	rawDoc := sgbucket.BucketDocument{
		Xattrs: make(map[string][]byte, len(xattrKeys)),
	}
	var xattrs []byte
	err := scan(row, &rawDoc.Body, &rawDoc.Cas, &xattrs, &rawDoc.IsTombstone, &revSeqNo)
	if err != nil {
		return sgbucket.BucketDocument{}, remapKeyError(err, key)
	}
	var xattrMap map[string]json.RawMessage
	if xattrs != nil {
		err = json.Unmarshal(xattrs, &xattrMap)
		if err != nil {
			return sgbucket.BucketDocument{}, fmt.Errorf("document %q xattrs are unreadable: %w %s", key, err, xattrs)
		}
	}
	for _, xattrKey := range xattrKeys {
		if xattrKey == virtualXattrName {
			rawDoc.Xattrs[xattrKey] = []byte(fmt.Sprintf(`{"value_crc32c":%q,"%s":"%d"}`, encodedCRC32c(rawDoc.Body), virtualXattrRevSeqNo, revSeqNo))
			continue
		} else if xattrKey == virtualXattrName+"."+virtualXattrRevSeqNo {
			rawDoc.Xattrs[xattrKey] = []byte(fmt.Sprintf(`"%d"`, revSeqNo))
			continue
		}
		val, ok := xattrMap[xattrKey]
		if !ok {
			continue
		}
		rawDoc.Xattrs[xattrKey] = val
	}
	return rawDoc, nil
}

// DeleteWithXattrs a document's body and xattrs simultaneously.
func (c *Collection) DeleteWithXattrs(ctx context.Context, key string, xattrKeys []string) error {
	err := c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		e := &event{
			key:        key,
			cas:        newCas,
			isDeletion: true,
		}
		var bodyExists bool
		row := txn.QueryRow(`SELECT xattrs, value NOT NULL, revSeqNo FROM documents WHERE collection=?1 AND key=?2`, c.id, key)
		err := scan(row, &e.xattrs, &bodyExists, &e.revSeqNo)
		if err != nil {
			return nil, remapKeyError(err, key)
		} else if e.xattrs, err = removeXattrs(e.xattrs, xattrKeys...); err != nil {
			return nil, err
		}
		e.revSeqNo++
		_, err = txn.Exec(`UPDATE documents SET value=null, xattrs=?1, cas=?2, revSeqNo=?3 WHERE collection=?4 AND key=?5`, e.xattrs, newCas, e.revSeqNo, c.id, key)
		return e, err
	})
	return err
}

// Option flags for writeWithXattr
type writeXattrOptions struct {
	insertDoc          bool // Create new doc; fail if it already exists
	insertXattr        bool // Fail if `xattr` already exists
	preserveXattr      bool // Leave xattr value alone, just expand macros
	isDelete           bool // Allow ressurecting a tombstone
	requireExistingDoc bool // Return KeyNotFoundError if doc doesn't already exist
	deleteBody         bool // Delete the body along with updating tombstone
}

// checkCasXattr checks the cas supplied against the current cas of the document. existingCas is the current Cas of the document (will be 0 if no document) and expectedCas is the expected value. Returns CasMismatchErr on an unsuccesful CAS check.
func checkCasXattr(existingCas, expectedCas *CAS, opts writeXattrOptions) error {
	// no cas supplied, nothing to check, this is different than zero when used with SetXattr
	if expectedCas == nil {
		return nil
	}

	if *existingCas == *expectedCas {
		return nil
	}
	return sgbucket.CasMismatchErr{Expected: *expectedCas, Actual: *existingCas}
}

// Swiss Army knife method for modifying a document xattr, with or without changing the body.
func (c *Collection) writeWithXattrs(
	key string, // doc key
	val *payload, // if non-nil, updates doc body; a nil payload means delete
	xattrsPayload map[string]payload, // xattr key/val; a nil payload deletes the xattr
	ifCas *CAS, // if non-nil, must match current CAS; 0 for insert
	exp *Exp, // if non-nil, sets expiration to this value
	opts writeXattrOptions, // option flags
	mutateOpts *sgbucket.MutateInOptions, // expiry and macro expansion options
) (casOut CAS, err error) {
	parsedXattrs := make(map[string]any, len(xattrsPayload))
	for xattrKey, xattrVal := range xattrsPayload {
		// Validate xattr key/value before going into the transaction:
		if err = validateXattrKey(xattrKey); err != nil {
			return
		}
		if !xattrVal.isNil() {
			parsedXattr, err := xattrVal.unmarshalJSON()
			if err != nil {
				return 0, fmt.Errorf("unparseable xattr: %w", err)
			}
			parsedXattrs[xattrKey] = parsedXattr
		}
	}
	err = c.withNewCas(func(txn *sql.Tx, newCas CAS) (*event, error) {
		e := &event{
			key: key,
			cas: newCas,
		}
		var wasTombstone int
		// First read the existing doc, if any:
		row := txn.QueryRow(`SELECT value, isJSON, cas, exp, xattrs, tombstone, revSeqNo FROM documents WHERE collection=?1 AND key=?2`,
			c.id, key)
		var prevCas CAS
		if err := scan(row, &e.value, &e.isJSON, &prevCas, &e.exp, &e.xattrs, &wasTombstone, &e.revSeqNo); err == nil {
			if wasTombstone == 1 && (val != nil && !val.isNil()) {
				// couchbase server can't perform a cas check on a tombstone so we return ErrKeyExists
				if ifCas != nil && *ifCas != 0 {
					return nil, sgbucket.ErrKeyExists
				}
				e.xattrs = nil // xattrs are cleared whenever resurrecting a tombstone
			} else if opts.insertDoc {
				return nil, sgbucket.ErrKeyExists
			}
		} else if errors.Is(err, sql.ErrNoRows) {
			if opts.requireExistingDoc {
				return nil, sgbucket.MissingError{Key: key}
			} else if ifCas != nil && *ifCas != 0 {
				return nil, sgbucket.CasMismatchErr{Expected: *ifCas, Actual: 0}
			}
		} else {
			return nil, remapKeyError(err, key)
		}
		e.revSeqNo++
		if e.value == nil && opts.deleteBody && opts.requireExistingDoc {
			return nil, fmt.Errorf("Calling deleteBody=true when the document is a tombstone: %w", sgbucket.MissingError{Key: key})
		}

		err := checkCasXattr(&prevCas, ifCas, opts)
		if err != nil {
			return nil, err
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

		if opts.preserveXattr {
			// The xPreserveXattr flag means to keep the xattr's value (but do macro expansion.)
			if opts.insertXattr {
				return nil, fmt.Errorf("illegal options to rosmar.Collection.writeWithXattr")
			}
			for xattrKey := range xattrsPayload {
				existingVal, ok := xattrs[xattrKey]
				if !ok {
					existingVal = json.RawMessage(`{}`)
				}

				xattrPayload := xattrsPayload[xattrKey]
				xattrPayload.setMarshaled(existingVal)
				parsedXattr, err := xattrPayload.unmarshalJSON()
				if err != nil {
					return nil, err
				}
				parsedXattrs[xattrKey] = parsedXattr
			}
		}

		for xattrKey, xattrVal := range xattrsPayload {
			if !xattrVal.isNil() {
				// Set xattr:
				if opts.insertXattr && xattrs[xattrKey] != nil {
					return nil, sgbucket.ErrPathExists
				}
				// Expand any macros specified in the mutateOpts
				parsedXattr := parsedXattrs[xattrKey]
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
					return nil, fmt.Errorf("%s: %w", xattrKey, sgbucket.ErrPathNotFound)
				}
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

		err = c.storeDocument(txn, e)
		if err != nil {
			return nil, err
		}
		return e, nil
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
			continue
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
