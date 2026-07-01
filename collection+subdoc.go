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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
)

func (c *Collection) GetSubDocRaw(ctx context.Context, key string, subdocKey string) (value []byte, casOut uint64, err error) {
	// TODO: Use SQLite JSON syntax to get the property
	traceEnter("SubdocGetRaw", "%q, %q", key, subdocKey)
	defer func() { traceExit("SubdocGetRaw", err, "0x%x, %s", casOut, value) }()

	path, err := parseSubdocPath(subdocKey)
	if err != nil {
		return
	}

	var fullDoc map[string]interface{}
	casOut, err = c.Get(ctx, key, &fullDoc)
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

func (c *Collection) SubdocInsert(ctx context.Context, key string, subdocKey string, cas CAS, value any) (err error) {
	traceEnter("SubdocInsert", "%q, %q, %d", key, subdocKey, cas)
	_, err = c.subdocWrite(ctx, key, subdocKey, cas, value, true)
	traceExit("SubdocInsert", err, "ok")
	return
}

func (c *Collection) WriteSubDoc(ctx context.Context, key string, subdocKey string, cas CAS, rawValue []byte) (casOut CAS, err error) {
	traceEnter("WriteSubDoc", "%q, %q, %d, %s", key, subdocKey, cas, rawValue)
	var value any
	if len(rawValue) > 0 {
		if err = json.Unmarshal(rawValue, &value); err != nil {
			return 0, err
		}
	}
	casOut, err = c.subdocWrite(ctx, key, subdocKey, cas, value, false)
	traceExit("WriteSubDoc", err, "0x%x", casOut)
	return
}

// common code of SubdocInsert and WriteSubDoc
func (c *Collection) subdocWrite(ctx context.Context, key string, subdocKey string, cas CAS, value any, insert bool) (casOut CAS, err error) {
	path, err := parseSubdocPath(subdocKey)
	if err != nil {
		return
	}

	for {
		// Get doc (if it exists) to change sub doc value in
		var fullDoc map[string]any
		casOut, err = c.Get(ctx, key, &fullDoc)
		var missingError sgbucket.MissingError
		if err != nil && !(!insert && errors.As(err, &missingError)) {
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
		if err != nil {
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
		casOut, err = c.WriteCas(ctx, key, 0, casOut, fullDoc, 0)

		if err != nil {
			if _, ok := err.(sgbucket.CasMismatchErr); ok && cas == 0 {
				continue // Doc has been updated but we're not matching CAS, so retry...
			}
			return 0, err
		}
		return casOut, nil
	}
}

// disallowedSubdocPathChars are characters rosmar does not support within a subdoc/xattr path
// component: `$` (CBS macro/virtual-attribute sigil), `[` `]` (array indexing), and backtick/
// backslash (CBS's escaping of literal dots or backticks within a path component, which rosmar
// does not implement). This is shared by parseSubdocPath and validateXattrPath so the two
// validators can't silently drift apart.
const disallowedSubdocPathChars = "$[]`\\"

// validateSubdocPath checks that a (possibly dotted) subdoc path is non-empty, free of
// disallowedSubdocPathChars, and has no empty components (e.g. from a leading, trailing, or
// doubled dot).
func validateSubdocPath(path string) error {
	if path == "" {
		return fmt.Errorf("invalid subdoc key %q", path)
	}
	if strings.ContainsAny(path, disallowedSubdocPathChars) {
		return &ErrUnimplemented{reason: "rosmar does not support this character in subdoc path: " + path}
	}
	for _, component := range strings.Split(path, ".") {
		if component == "" {
			return fmt.Errorf("subdoc path %q contains an empty path component", path)
		}
	}
	return nil
}

// Parses a subdoc key into an array of JSON path components.
func parseSubdocPath(subdocKey string) ([]string, error) {
	if err := validateSubdocPath(subdocKey); err != nil {
		return nil, err
	}
	return strings.Split(subdocKey, "."), nil
}

// Evaluates a parsed JSON path on a value.
func evalSubdocPath(subdoc any, path []string) (any, error) {
	for _, prop := range path {
		asMap, ok := subdoc.(map[string]any)
		if !ok {
			return nil, sgbucket.ErrPathMismatch
		}
		val, exists := asMap[prop]
		if !exists {
			return nil, sgbucket.ErrPathNotFound
		}
		subdoc = val
	}
	return subdoc, nil
}

// Upserts the value at the specified JSON path in the source.
// If createIntermediatePaths is true, creates intermediate maps for any missing path components,
// matching Couchbase Server subdoc upsert behavior. If false, a missing intermediate path
// component is reported as sgbucket.ErrPathNotFound instead of being silently created — used for
// macro expansion, where a missing intermediate object indicates a caller bug rather than
// something to paper over.
// Returns ErrPathMismatch if a non-leaf path entry exists but is not a map.
func upsertSubdocValue(source any, path []string, value any, createIntermediatePaths bool) error {
	if len(path) == 0 {
		return fmt.Errorf("subdoc path must not be empty")
	}
	current, ok := source.(map[string]any)
	if !ok {
		return sgbucket.ErrPathMismatch
	}
	for _, prop := range path[:len(path)-1] {
		child, exists := current[prop]
		if !exists || child == nil {
			if !createIntermediatePaths {
				return sgbucket.ErrPathNotFound
			}
			child = map[string]any{}
			current[prop] = child
		}
		current, ok = child.(map[string]any)
		if !ok {
			return sgbucket.ErrPathMismatch
		}
	}
	lastPath := path[len(path)-1]
	if value != nil {
		current[lastPath] = value
	} else {
		delete(current, lastPath)
	}
	return nil
}

var (
	// Enforce interface conformance:
	_ sgbucket.SubdocStore = &Collection{}
)
