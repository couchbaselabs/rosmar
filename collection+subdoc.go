// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"encoding/json"
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
)

func (c *Collection) GetSubDocRaw(key string, subdocKey string) (value []byte, casOut uint64, err error) {
	// TODO: Use SQLite JSON syntax to get the property
	traceEnter("SubdocGetRaw", "%q, %q", key, subdocKey)
	defer func() { traceExit("SubdocGetRaw", err, "0x%x, %s", casOut, value) }()

	path, err := parseSubdocPath(subdocKey)
	if err != nil {
		return
	}

	var fullDoc map[string]interface{}
	casOut, err = c.Get(key, &fullDoc)
	if err != nil {
		return
	}

	subdoc, err := evalSubdocPath(fullDoc, path)
	if err != nil {
		return
	}

	value, err = json.Marshal(subdoc)
	return value, casOut, err
}

func (c *Collection) SubdocInsert(key string, subdocKey string, cas CAS, value any) (err error) {
	traceEnter("SubdocInsert", "%q, %q, %d", key, subdocKey, cas)
	_, err = c.subdocWrite(key, subdocKey, cas, value, true)
	traceExit("SubdocInsert", err, "ok")
	return
}

func (c *Collection) WriteSubDoc(key string, subdocKey string, cas CAS, rawValue []byte) (casOut CAS, err error) {
	traceEnter("WriteSubDoc", "%q, %q, %d, %s", key, subdocKey, cas, rawValue)
	var value any
	if len(rawValue) > 0 {
		if err = json.Unmarshal(rawValue, &value); err != nil {
			return 0, err
		}
	}
	casOut, err = c.subdocWrite(key, subdocKey, cas, value, false)
	traceExit("WriteSubDoc", err, "0x%x", casOut)
	return
}

// common code of SubdocInsert and WriteSubDoc
func (c *Collection) subdocWrite(key string, subdocKey string, cas CAS, value any, insert bool) (casOut CAS, err error) {
	path, err := parseSubdocPath(subdocKey)
	if err != nil {
		return
	}

	for {
		// Get doc (if it exists) to change sub doc value in
		var fullDoc map[string]any
		casOut, err = c.Get(key, &fullDoc)
		if err != nil && !(!insert && c.IsError(err, sgbucket.KeyNotFoundError)) {
			return 0, err // SubdocInsert should fail if doc doesn't exist; WriteSubDoc doesn't
		}
		if cas != 0 && casOut != cas {
			return 0, sgbucket.CasMismatchErr{Expected: cas, Actual: casOut}
		}
		if fullDoc == nil {
			fullDoc = map[string]any{}
		}

		// Find the parent of the path:
		subdoc, err := evalSubdocPath(fullDoc, path[0:len(path)-1])
		if subdoc == nil {
			return 0, err
		}
		parent, ok := subdoc.(map[string]any)
		if !ok {
			return 0, sgbucket.ErrPathMismatch // Parent is not a map
		}
		// Now add the leaf property to the parent map:
		lastPath := path[len(path)-1]
		if insert && parent[lastPath] != nil {
			return 0, sgbucket.ErrPathExists // Insertion failed
		}
		if value != nil {
			parent[lastPath] = value
		} else {
			delete(parent, lastPath)
		}

		// Write full doc back to collection
		casOut, err = c.WriteCas(key, 0, 0, casOut, fullDoc, 0)

		if err != nil {
			if _, ok := err.(sgbucket.CasMismatchErr); ok && cas == 0 {
				continue // Doc has been updated but we're not matching CAS, so retry...
			}
			return 0, err
		}
		return casOut, nil
	}
}

// Parses a subdoc key into an array of JSON path components.
func parseSubdocPath(subdocKey string) ([]string, error) {
	if subdocKey == "" {
		return nil, fmt.Errorf("invalid subdoc key %q", subdocKey)
	}
	if strings.ContainsAny(subdocKey, "[]") {
		return nil, &ErrUnimplemented{reason: "Rosmar does not support arrays in subdoc keys: key is " + subdocKey}
	}
	if strings.ContainsAny(subdocKey, "\\`") {
		return nil, &ErrUnimplemented{reason: "Rosmar does not support escape characters in subdoc keys: key is " + subdocKey}
	}
	path := strings.Split(subdocKey, ".")
	return path, nil
}

// Evaluates a parsed JSON path on a value.
func evalSubdocPath(subdoc any, path []string) (any, error) {
	for _, prop := range path {
		if asMap, ok := subdoc.(map[string]any); ok {
			subdoc = asMap[prop]
			if subdoc == nil {
				return nil, sgbucket.ErrPathNotFound
			}
		} else {
			return nil, sgbucket.ErrPathMismatch
		}
	}
	return subdoc, nil
}

var (
	// Enforce interface conformance:
	_ sgbucket.SubdocStore = &Collection{}
)
