// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
package rosmar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const syncXattrName = "_sync" // name of xattr used for sync gateway metadata
const vvXattrName = "_vv"     // name of xattr used for HLV

func TestDeleteThenAdd(t *testing.T) {
	ctx := t.Context()
	ensureNoLeaks(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx)

	var value interface{}
	_, err := coll.Get(ctx, "key", &value)
	assert.Equal(t, sgbucket.MissingError{Key: "key"}, err)
	addToCollection(t, coll, "key", 0, "value")
	_, err = coll.Get(ctx, "key", &value)
	assert.NoError(t, err, "Get")
	assert.Equal(t, "value", value)
	assert.NoError(t, coll.Delete(ctx, "key"), "Delete")
	_, err = coll.Get(ctx, "key", &value)
	assert.Equal(t, sgbucket.MissingError{Key: "key"}, err)
	addToCollection(t, coll, "key", 0, "value")
}

// TestIncr exercises the Incr contract Rosmar shares with Couchbase Server - cross-checked with GoCB/CB Server implementation.
//
//   - amt=0 on an existing key still rewrites the doc and bumps CAS (NOT a read-only op).
//   - Incr uses uint64 addition with wrap on overflow; "negative" amt
//     (e.g. uint64(-1)) decrements via wrap, with no clamp at 0.
//   - On a missing key, the delta is not applied — the stored value is def.
//   - def must fit in int64. CBS's gocb adapter casts def to int64; values
//     > math.MaxInt64 are interpreted as "do not create if absent" and return
//     KEY_ENOENT. Rosmar matches this by returning MissingError.
func TestIncr(t *testing.T) {
	ctx := t.Context()
	ensureNoLeaks(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx)

	// intentional underflows (follows GoCB semantics)
	maxU64 := uint64(math.MaxUint64)
	negTen := maxU64 - 9 // uint64(-10)

	cases := []struct {
		desc             string  // short English description of the scenario
		existingValue    *uint64 // if non-nil, seed the key via SetRaw before Incr
		amt              uint64
		def              uint64
		expectErr        bool
		expectValue      uint64
		expectPostRaw    string
		expectCASChanged bool // only checked when the doc existed pre-Incr
	}{
		{desc: "create zero counter when key missing", amt: 0, def: 0, expectValue: 0, expectPostRaw: "0"},
		{desc: "create counter at def when key missing", amt: 0, def: 5, expectValue: 5, expectPostRaw: "5"},
		{desc: "amt=0 on existing key returns current value and ignores def", existingValue: ptr(uint64(42)), amt: 0, def: 100, expectValue: 42, expectPostRaw: "42", expectCASChanged: true},
		{desc: "missing key returns def, delta is not applied to it", amt: 1, def: 5, expectValue: 5, expectPostRaw: "5"},
		{desc: "increment existing counter by amt", existingValue: ptr(uint64(42)), amt: 1, def: 5, expectValue: 43, expectPostRaw: "43", expectCASChanged: true},
		{desc: "amt=0 on existing key still rewrites doc and bumps CAS", existingValue: ptr(uint64(42)), amt: 0, def: 0, expectValue: 42, expectPostRaw: "42", expectCASChanged: true},
		{desc: "negative amt on missing key stores def (no wrap into def)", amt: maxU64, def: 0, expectValue: 0, expectPostRaw: "0"},
		{desc: "negative amt on existing key decrements via uint64 wrap", existingValue: ptr(uint64(42)), amt: maxU64, def: 0, expectValue: 41, expectPostRaw: "41", expectCASChanged: true},
		{desc: "amt=-10 on existing key decrements by 10", existingValue: ptr(uint64(100)), amt: negTen, def: 0, expectValue: 90, expectPostRaw: "90", expectCASChanged: true},
		{desc: "def > int64 max returns MissingError (matches CBS gocb sentinel)", amt: 1, def: maxU64, expectErr: true},
		{desc: "def > int64 max is ignored on existing key (delta still applied)", existingValue: ptr(uint64(42)), amt: 1, def: maxU64, expectValue: 43, expectPostRaw: "43", expectCASChanged: true},
	}

	// formatU64 renders a uint64 as "negN" when it represents a wrapped-negative int64,
	// so test names read as the caller-intended value (e.g. uint64(-10) → "neg10").
	formatU64 := func(v uint64) string {
		if int64(v) < 0 {
			return fmt.Sprintf("neg%d", -int64(v))
		}
		return strconv.FormatUint(v, 10)
	}

	for _, tc := range cases {
		state := "missing"
		if tc.existingValue != nil {
			state = fmt.Sprintf("existing%d", *tc.existingValue)
		}
		name := fmt.Sprintf("%s/amt=%s_def=%s_%s", tc.desc, formatU64(tc.amt), formatU64(tc.def), state)
		t.Run(name, func(t *testing.T) {
			key := name

			if tc.existingValue != nil {
				raw := []byte(strconv.FormatUint(*tc.existingValue, 10))
				require.NoError(t, coll.SetRaw(ctx, key, 0, nil, raw), "setup SetRaw")
			}
			_, preCAS, preErr := coll.GetRaw(ctx, key)

			value, incrErr := coll.Incr(ctx, key, tc.amt, tc.def, 0)

			if tc.expectErr {
				require.Error(t, incrErr, "expected Incr to error")
				require.ErrorAs(t, incrErr, &sgbucket.MissingError{}, "expected MissingError")
				if tc.existingValue == nil {
					_, _, postErr := coll.GetRaw(ctx, key)
					require.Error(t, postErr, "doc should not exist after a failed Incr on missing key")
				}
				return
			}

			require.NoError(t, incrErr)
			require.Equal(t, tc.expectValue, value, "returned counter value")

			postRaw, postCAS, postErr := coll.GetRaw(ctx, key)
			require.NoError(t, postErr, "GetRaw after Incr")
			require.Equal(t, tc.expectPostRaw, string(postRaw), "post-Incr stored value")

			if preErr == nil {
				if tc.expectCASChanged {
					require.NotEqual(t, preCAS, postCAS, "CAS should have changed")
				} else {
					require.Equal(t, preCAS, postCAS, "CAS should not have changed")
				}
			}
		})
	}
}

// Spawns 1000 goroutines that 'simultaneously' use Incr to increment the same counter by 1.
func TestIncrAtomic(t *testing.T) {
	ctx := t.Context()
	ensureNoLeaks(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx)
	var waiters sync.WaitGroup
	numIncrements := 5
	waiters.Add(numIncrements)
	for i := uint64(1); i <= uint64(numIncrements); i++ {
		numToAdd := i // lock down the value for the goroutine
		go func() {
			_, err := coll.Incr(ctx, "key", numToAdd, numToAdd, 0)
			assert.NoError(t, err, "Incr")
			waiters.Add(-1)
		}()
	}
	waiters.Wait()
	value, err := coll.Incr(ctx, "key", 0, 0, 0)
	assert.NoError(t, err, "Incr")
	assert.Equal(t, numIncrements*(numIncrements+1)/2, int(value))
}

func TestAppend(t *testing.T) {
	ctx := t.Context()
	ensureNoLeaks(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx)

	exists, err := coll.Exists(ctx, "key")
	assert.NoError(t, err)
	assert.False(t, exists)

	_, err = coll.WriteCas(ctx, "key", 0, 0, []byte(" World"), sgbucket.Append)
	assert.Equal(t, sgbucket.MissingError{Key: "key"}, err)

	err = coll.SetRaw(ctx, "key", 0, nil, []byte("Hello"))
	assert.NoError(t, err, "SetRaw")
	_, cas, err := coll.GetRaw(ctx, "key")
	assert.NoError(t, err, "GetRaw")

	_, err = coll.WriteCas(ctx, "key", 0, cas, []byte(" World"), sgbucket.Append)
	assert.NoError(t, err, "Append")
	value, _, err := coll.GetRaw(ctx, "key")
	assert.NoError(t, err, "GetRaw")
	assert.Equal(t, []byte("Hello World"), value)
}

func TestGets(t *testing.T) {
	ctx := t.Context()
	ensureNoLeaks(t)

	coll := makeTestBucket(t).DefaultDataStore(ctx)

	// Gets (JSON)
	addToCollection(t, coll, "key", 0, "value")

	var value interface{}
	cas, err := coll.Get(ctx, "key", &value)
	assert.NoError(t, err, "Gets")
	assert.True(t, cas > 0)
	assert.Equal(t, "value", value)

	// GetsRaw
	err = coll.SetRaw(ctx, "keyraw", 0, nil, []byte("Hello"))
	assert.NoError(t, err, "SetRaw")

	value, cas, err = coll.GetRaw(ctx, "keyraw")
	assert.NoError(t, err, "GetsRaw")
	assert.True(t, cas > 0)
	assert.Equal(t, []byte("Hello"), value)
}

func TestParseSubdocPaths(t *testing.T) {
	_, err := parseSubdocPath("")
	assert.Error(t, err)

	path, err := parseSubdocPath("foo")
	assert.NoError(t, err)
	assert.Equal(t, []string{"foo"}, path)

	path, err = parseSubdocPath("foo.bar")
	assert.NoError(t, err)
	assert.Equal(t, []string{"foo", "bar"}, path)

	_, err = parseSubdocPath("foo[5]")
	assert.Error(t, err)
	_, err = parseSubdocPath(`foo\"quoted`)
	assert.Error(t, err)
}

// TestParseSubdocPathSharesXattrPathRules is a regression test verifying that parseSubdocPath
// (used by WriteSubDoc/SubdocInsert/expandXattrMacros) and validateXattrPath (used by
// SetXattrs/WriteWithXattrs) reject the same set of characters and empty path components,
// since they now share a single validateSubdocPath implementation instead of drifting
// independently.
func TestParseSubdocPathSharesXattrPathRules(t *testing.T) {
	invalidPaths := []string{
		"",         // empty key
		"foo.",     // trailing dot -> empty component
		"foo..bar", // consecutive dots -> empty component
		"foo$bar",  // '$' is reserved for CBS macros/virtual attributes
		"foo[0]",   // array indexing unsupported
		"foo`bar`", // backtick escaping unsupported
		`foo\bar`,  // backslash escaping unsupported
	}
	for _, path := range invalidPaths {
		t.Run(fmt.Sprintf("path=%q", path), func(t *testing.T) {
			_, err := parseSubdocPath(path)
			require.Error(t, err, "parseSubdocPath should reject %q", path)
			if path != "" {
				require.Error(t, validateXattrPath(path), "validateXattrPath should also reject %q", path)
			}
		})
	}
}

// TestWriteSubDocRejectsDollarSign verifies that WriteSubDoc (a body subdoc path, not an xattr
// path) rejects '$' just like SetXattrs already does — a regression test for parseSubdocPath and
// validateXattrPath previously enforcing different rules.
func TestWriteSubDocRejectsDollarSign(t *testing.T) {
	ctx := t.Context()
	_, coll := initSubDocTest(t)

	_, err := coll.WriteSubDoc(ctx, "key", "rosmar.$foo", 0, []byte(`"x"`))
	require.Error(t, err)
}

func TestEvalSubdocPaths(t *testing.T) {
	rawJson := `{"one":1, "two":{"etc":2}, "array":[3,4]}`
	var doc map[string]any
	_ = json.Unmarshal([]byte(rawJson), &doc)

	// Valid 1-level paths:
	val, err := evalSubdocPath(doc, []string{"one"})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, val)
	val, err = evalSubdocPath(doc, []string{"two"})
	assert.NoError(t, err)
	assert.EqualValues(t, map[string]any{"etc": 2.0}, val)
	val, err = evalSubdocPath(doc, []string{"array"})
	assert.NoError(t, err)
	assert.EqualValues(t, []any{3.0, 4.0}, val)

	// Valid 2-level path:
	val, err = evalSubdocPath(doc, []string{"two", "etc"})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, val)

	// Missing paths:
	_, err = evalSubdocPath(doc, []string{"xxx"})
	assert.Error(t, err)
	_, err = evalSubdocPath(doc, []string{"two", "xxx", "yyy"})
	assert.Error(t, err)

	// Type mismatches:
	_, err = evalSubdocPath(doc, []string{"one", "xxx"})
	assert.Error(t, err)
	_, err = evalSubdocPath(doc, []string{"array", "xxx"})
	assert.Error(t, err)
}

func TestUpsertSubdocValue(t *testing.T) {
	doc := map[string]any{"a": map[string]any{"b": 1}}

	// Empty path must return an error, not panic.
	err := upsertSubdocValue(doc, []string{}, "v", true)
	require.Error(t, err)

	// Non-map source must return ErrPathMismatch.
	err = upsertSubdocValue("not-a-map", []string{"x"}, "v", true)
	require.ErrorIs(t, err, sgbucket.ErrPathMismatch)

	// Single-component path sets the key.
	require.NoError(t, upsertSubdocValue(doc, []string{"x"}, "hello", true))
	assert.Equal(t, "hello", doc["x"])

	// Multi-component path navigates into existing nested map.
	require.NoError(t, upsertSubdocValue(doc, []string{"a", "c"}, 99, true))
	assert.Equal(t, 99, doc["a"].(map[string]any)["c"])

	// Multi-component path creates intermediate maps for missing components when
	// createIntermediatePaths is true.
	require.NoError(t, upsertSubdocValue(doc, []string{"new", "nested", "key"}, true, true))
	assert.Equal(t, true, doc["new"].(map[string]any)["nested"].(map[string]any)["key"])

	// Nil value deletes the key.
	require.NoError(t, upsertSubdocValue(doc, []string{"x"}, nil, true))
	assert.NotContains(t, doc, "x")
}

// TestUpsertSubdocValueStrict verifies that createIntermediatePaths=false reports a missing
// intermediate path component as ErrPathNotFound instead of silently creating it — this is the
// mode expandXattrMacros uses, since a missing intermediate object during macro expansion
// indicates a caller bug rather than something to paper over.
func TestUpsertSubdocValueStrict(t *testing.T) {
	doc := map[string]any{"a": map[string]any{"b": 1}}

	// Existing intermediate path still works.
	require.NoError(t, upsertSubdocValue(doc, []string{"a", "c"}, 99, false))
	assert.Equal(t, 99, doc["a"].(map[string]any)["c"])

	// Missing intermediate path component is reported as ErrPathNotFound, not silently created.
	err := upsertSubdocValue(doc, []string{"missing", "nested", "key"}, true, false)
	require.ErrorIs(t, err, sgbucket.ErrPathNotFound)
	assert.NotContains(t, doc, "missing")
}

func initSubDocTest(t *testing.T) (CAS, sgbucket.DataStore) {
	ensureNoLeaks(t)

	coll := makeTestBucket(t).DefaultDataStore(t.Context())
	require.True(t, coll.IsSupported(sgbucket.BucketStoreFeatureSubdocOperations))

	rawJson := []byte(`{
        "rosmar":{
            "foo":"lol",
            "bar":"baz"}
        }`)

	addToCollection(t, coll, "key", 0, rawJson)

	var fullDoc map[string]any
	cas, err := coll.Get(t.Context(), "key", &fullDoc)
	assert.NoError(t, err)
	assert.Greater(t, cas, CAS(0))

	return cas, coll
}

func TestWriteSubDoc(t *testing.T) {
	ctx := t.Context()
	initialCas, coll := initSubDocTest(t)

	// update json
	rawJson := []byte(`"was here"`)
	// test update using incorrect cas value
	cas1, err := coll.WriteSubDoc(ctx, "key", "rosmar", 10, rawJson)
	assert.Error(t, err)
	assert.Equal(t, CAS(0), cas1)

	// test update using correct cas value
	cas2, err := coll.WriteSubDoc(ctx, "key", "rosmar", initialCas, rawJson)
	assert.NoError(t, err)
	assert.Greater(t, cas2, initialCas)

	var fullDoc map[string]any
	cas2Get, err := coll.Get(ctx, "key", &fullDoc)
	assert.NoError(t, err)
	assert.Equal(t, cas2, cas2Get)
	assert.EqualValues(t, map[string]any{"rosmar": "was here"}, fullDoc)

	// test update using 0 cas value
	cas3, err := coll.WriteSubDoc(ctx, "key", "rosmar", 0, rawJson)
	assert.NoError(t, err)
	assert.Greater(t, cas3, cas2)
}

func TestInsertSubDoc(t *testing.T) {
	ctx := t.Context()
	initialCas, coll := initSubDocTest(t)

	rosmarMap := map[string]any{"foo": "lol", "bar": "baz"}
	expectedDoc := map[string]any{"rosmar": rosmarMap}

	// test incorrect cas value
	err := coll.SubdocInsert(ctx, "key", "rosmar.kilroy", 10, "was here")
	assert.Error(t, err)

	// test update
	err = coll.SubdocInsert(ctx, "key", "rosmar.kilroy", initialCas, "was here")
	assert.NoError(t, err)

	var fullDoc map[string]any
	cas, err := coll.Get(ctx, "key", &fullDoc)
	assert.NoError(t, err)
	assert.Greater(t, cas, initialCas)

	rosmarMap["kilroy"] = "was here"
	assert.EqualValues(t, expectedDoc, fullDoc)

	// test failed update:
	err = coll.SubdocInsert(ctx, "key", "rosmar", cas, "wrong")
	assert.Error(t, err)
	err = coll.SubdocInsert(ctx, "key", "rosmar.foo.xxx.yyy", cas, "wrong")
	assert.Error(t, err)
}

func TestWriteCas(t *testing.T) {
	ctx := t.Context()
	ensureNoLeaks(t)

	coll := makeTestBucket(t).DefaultDataStore(ctx)

	// Add with WriteCas - JSON docs
	// Insert
	var obj interface{}
	mustUnmarshal(t, `{"value":"value1"}`, &obj)
	cas, err := coll.WriteCas(ctx, "key1", 0, 0, obj, 0)
	assert.NoError(t, err, "WriteCas")
	assert.True(t, cas > 0, "Cas value should be greater than zero")

	// Update document with wrong (zero) cas value
	mustUnmarshal(t, `{"value":"value2"}`, &obj)
	newCas, err := coll.WriteCas(ctx, "key1", 0, 0, obj, 0)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Update document with correct cas value
	mustUnmarshal(t, `{"value":"value2"}`, &obj)
	newCas, err = coll.WriteCas(ctx, "key1", 0, cas, obj, 0)
	assert.True(t, err == nil, "Valid cas should not have returned error.")
	assert.True(t, cas > 0, "Cas value should be greater than zero")
	assert.True(t, cas != newCas, "Cas value should change on successful update")
	var result interface{}
	getCas, err := coll.Get(ctx, "key1", &result)
	assert.NoError(t, err, "Get")
	assert.Equal(t, obj, result)
	assert.Equal(t, newCas, getCas)

	// Update document with obsolete case value
	mustUnmarshal(t, `{"value":"value3"}`, &obj)
	newCas, err = coll.WriteCas(ctx, "key1", 0, cas, obj, 0)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Add with WriteCas - raw docs
	// Insert
	cas, err = coll.WriteCas(ctx, "keyraw1", 0, 0, []byte("value1"), sgbucket.Raw)
	assert.NoError(t, err, "WriteCas")
	assert.True(t, cas > 0, "Cas value should be greater than zero")

	// Update document with wrong (zero) cas value
	newCas, err = coll.WriteCas(ctx, "keyraw1", 0, 0, []byte("value2"), sgbucket.Raw)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Update document with correct cas value
	newCas, err = coll.WriteCas(ctx, "keyraw1", 0, cas, []byte("value2"), sgbucket.Raw)
	assert.True(t, err == nil, "Valid cas should not have returned error.")
	assert.True(t, cas > 0, "Cas value should be greater than zero")
	assert.True(t, cas != newCas, "Cas value should change on successful update")
	value, getCas, err := coll.GetRaw(ctx, "keyraw1")
	assert.NoError(t, err, "GetRaw")
	assert.Equal(t, []byte("value2"), value)
	assert.Equal(t, newCas, getCas)

	// Update document with obsolete cas value
	newCas, err = coll.WriteCas(ctx, "keyraw1", 0, cas, []byte("value3"), sgbucket.Raw)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Delete document, attempt to recreate w/ cas set to 0
	err = coll.Delete(ctx, "keyraw1")
	assert.True(t, err == nil, "Delete failed")
	newCas, err = coll.WriteCas(ctx, "keyraw1", 0, 0, []byte("resurrectValue"), sgbucket.Raw)
	require.NoError(t, err, "Recreate with cas=0 should succeed.")
	assert.True(t, cas > 0, "Cas value should be greater than zero")
	value, getCas, err = coll.GetRaw(ctx, "keyraw1")
	assert.NoError(t, err, "GetRaw")
	assert.Equal(t, []byte("resurrectValue"), value)
	assert.Equal(t, newCas, getCas)

}

func TestRemove(t *testing.T) {
	ctx := t.Context()
	ensureNoLeaks(t)

	coll := makeTestBucket(t).DefaultDataStore(ctx)

	// Add with WriteCas - JSON docs
	// Insert
	var obj interface{}
	mustUnmarshal(t, `{"value":"value1"}`, &obj)
	cas, err := coll.WriteCas(ctx, "key1", 0, 0, obj, 0)
	assert.NoError(t, err, "WriteCas")
	assert.True(t, cas > 0, "Cas value should be greater than zero")

	// Update document with correct cas value
	mustUnmarshal(t, `{"value":"value2"}`, &obj)
	newCas, err := coll.WriteCas(ctx, "key1", 0, cas, obj, 0)
	assert.True(t, err == nil, "Valid cas should not have returned error.")
	assert.True(t, cas > 0, "Cas value should be greater than zero")
	assert.True(t, cas != newCas, "Cas value should change on successful update")
	var result interface{}
	getCas, err := coll.Get(ctx, "key1", &result)
	assert.NoError(t, err, "Get")
	assert.Equal(t, obj, result)
	assert.Equal(t, newCas, getCas)

	// Remove document with incorrect cas value
	newCas, err = coll.Remove(ctx, "key1", cas)
	assert.Error(t, err, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Remove document with correct cas value
	newCas, err = coll.Remove(ctx, "key1", getCas)
	assert.True(t, err == nil, "Valid cas should not have returned error on remove.")
	assert.True(t, newCas != uint64(0), "Remove should return non-zero cas")
}

// Test read and write of json as []byte
func TestNonRawBytes(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)

	coll := makeTestBucket(t).DefaultDataStore(ctx)

	byteBody := []byte(`{"value":"value1"}`)

	// Add with WriteCas - JSON doc as []byte and *[]byte
	_, err := coll.WriteCas(ctx, "writeCas1", 0, 0, byteBody, 0)
	assert.NoError(t, err, "WriteCas []byte")
	_, err = coll.WriteCas(ctx, "writeCas2", 0, 0, &byteBody, 0)
	assert.NoError(t, err, "WriteCas *[]byte")

	// Add with Add - JSON doc as []byte and *[]byte
	addToCollection(t, coll, "add1", 0, byteBody)
	addToCollection(t, coll, "add2", 0, &byteBody)

	// Set - JSON doc as []byte
	// Set - JSON doc as *[]byte
	// Add with Add - JSON doc as []byte and *[]byte
	err = coll.Set(ctx, "set1", 0, nil, byteBody)
	assert.NoError(t, err, "Set []byte")
	err = coll.Set(ctx, "set2", 0, nil, &byteBody)
	assert.NoError(t, err, "Set *[]byte")

	keySet := []string{"writeCas1", "writeCas2", "add1", "add2", "set1", "set2"}
	for _, key := range keySet {
		// Verify retrieval as map[string]interface{}
		var result map[string]interface{}
		cas, err := coll.Get(ctx, key, &result)
		assert.NoError(t, err, fmt.Sprintf("Error for Get %s", key))
		assert.True(t, cas > 0, fmt.Sprintf("CAS is zero for key: %s", key))
		assert.True(t, result != nil, fmt.Sprintf("result is nil for key: %s", key))
		if result != nil {
			assert.Equal(t, "value1", result["value"])
		}

		// Verify retrieval as *[]byte
		var rawResult []byte
		cas, err = coll.Get(ctx, key, &rawResult)
		assert.NoError(t, err, fmt.Sprintf("Error for Get %s", key))
		assert.True(t, cas > 0, fmt.Sprintf("CAS is zero for key: %s", key))
		assert.True(t, result != nil, fmt.Sprintf("result is nil for key: %s", key))
		if result != nil {
			matching := bytes.Compare(rawResult, byteBody)
			assert.Equal(t, 0, matching)
		}
	}

	// Verify values are stored as JSON and can be retrieved via view
	ddoc := sgbucket.DesignDoc{Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.value) emit(doc.key,doc.value)}`}}}
	err = coll.(*Collection).PutDDoc(ctx, "docname", &ddoc)
	assert.NoError(t, err, "PutDDoc failed")

	options := map[string]interface{}{"stale": false}
	result, err := coll.(*Collection).View(ctx, "docname", "view1", options)
	assert.NoError(t, err, "View call failed")
	assert.Equal(t, len(keySet), result.TotalRows)
}

//////// HELPERS:

func mustUnmarshal(t *testing.T, j string, obj any) {
	require.NoError(t, json.Unmarshal([]byte(j), &obj))
}

func setJSON(ctx context.Context, coll sgbucket.DataStore, docid string, jsonDoc string) error {
	var obj interface{}
	err := json.Unmarshal([]byte(jsonDoc), &obj)
	if err != nil {
		return err
	}
	return coll.Set(ctx, docid, 0, nil, obj)
}

func addToCollection(t *testing.T, coll sgbucket.DataStore, key string, exp uint32, value interface{}) {
	added, err := coll.Add(t.Context(), key, exp, value)
	require.NoError(t, err)
	require.True(t, added, "Expected doc to be added")
}

func ensureNoLeaks(t *testing.T) {
	t.Cleanup(func() { assert.Len(t, GetBucketNames(), 0) })
	ensureNoLeakedFeeds(t)
}

func ensureNoLeakedFeeds(t *testing.T) {
	if !assert.Equal(t, int32(0), atomic.LoadInt32(&activeFeedCount), "Previous test left unclosed Tap/DCP feeds") {
		return
	}

	t.Cleanup(func() {
		var count int32
		for i := 0; i < 100; i++ {
			count = atomic.LoadInt32(&activeFeedCount)
			if count == 0 {
				break
			}
			//log.Printf("Still %d feeds active; waiting...", count)
			time.Sleep(10 * time.Millisecond)
		}
		assert.Equal(t, int32(0), count, "Not all feed goroutines finished")
	})
}

func TestNoCasOnResurrection(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)
	const docID = "doc1"
	const exp = 0
	casOut, err := col.WriteCas(ctx, docID, exp, 0, []byte("{}"), sgbucket.Raw)
	require.NoError(t, err)
	require.NotEqual(t, 0, casOut)
	require.NoError(t, col.Delete(ctx, docID))

	// As on Couchbase Server, a non-zero CAS can't write to a tombstone, but a CAS of 0 resurrects it
	_, err = col.WriteCas(ctx, docID, exp, casOut, []byte("{}"), sgbucket.AddOnly)
	require.ErrorAs(t, err, &sgbucket.MissingError{})
	ressurectedCasOut, err := col.WriteCas(ctx, docID, exp, 0, []byte("{}"), sgbucket.AddOnly)
	require.NoError(t, err)
	require.NotEqual(t, 0, ressurectedCasOut)
}

func TestWriteCasWithXattrExistingXattr(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)

	const docID = "DocExistsXattrExists"

	val := make(map[string]interface{})
	val["type"] = docID

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 123
	xattrVal["rev"] = "1-1234"

	var exp uint32
	xattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, xattrVal)}
	cas := uint64(0)
	cas, err := col.WriteWithXattrs(ctx, docID, exp, cas, mustMarshalJSON(t, val), xattrs, nil, nil)
	require.NoError(t, err)

	updatedXattrVal := make(map[string]interface{})
	updatedXattrVal["seq"] = 123
	updatedXattrVal["rev"] = "2-1234"
	newXattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, updatedXattrVal)}

	const deleteBody = true
	// First attempt to update with a bad cas value, and ensure we're getting the expected error
	_, err = col.WriteTombstoneWithXattrs(ctx, docID, exp, uint64(1234), newXattrs, nil, deleteBody, nil)

	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	_, err = col.WriteTombstoneWithXattrs(ctx, docID, exp, cas, newXattrs, nil, deleteBody, nil)
	require.NoError(t, err)

	verifyEmptyBodyAndSyncXattr(t, col.(*Collection), docID)

}

// Test WriteWithXattr that only updates the xattr.
func TestWriteWithXattrNoBody(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)

	const docID = "WriteWithXattrNoBody"

	// Write a document with body
	val := make(map[string]interface{})
	val["type"] = docID

	xattrVal := make(map[string]interface{})
	xattrVal["rev"] = "1-1234"

	var exp uint32
	xattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, xattrVal)}
	cas := uint64(0)
	cas, err := col.WriteWithXattrs(ctx, docID, exp, cas, mustMarshalJSON(t, val), xattrs, nil, nil)
	require.NoError(t, err)

	// Update the xattr only
	updatedXattrVal := make(map[string]interface{})
	updatedXattrVal["rev"] = "2-1234"
	newXattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, updatedXattrVal)}

	cas, err = col.WriteWithXattrs(ctx, docID, exp, cas, nil, newXattrs, nil, nil)
	require.NoError(t, err)

	// Fetch, validate body and xattrs are correct
	getVal, getXattrs, _, err := col.GetWithXattrs(ctx, docID, []string{syncXattrName})
	var fetchedVal, fetchedXattr map[string]interface{}
	require.NoError(t, json.Unmarshal(getVal, &fetchedVal))
	require.Equal(t, val, fetchedVal)

	require.NoError(t, json.Unmarshal(getXattrs[syncXattrName], &fetchedXattr))
	require.Equal(t, updatedXattrVal, fetchedXattr)

}

// Test that WriteWithXattrs' xattrsToDelete supports a dotted subdoc path, removing only that
// field from the xattr and leaving the rest of the xattr intact.
func TestWriteWithXattrsDeleteSubDocPath(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)

	const docID = "WriteWithXattrsDeleteSubDocPath"

	val := map[string]interface{}{"type": docID}

	xattrVal := map[string]interface{}{"rev": "1-1234", "seq": float64(123)}
	xattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, xattrVal)}
	cas, err := col.WriteWithXattrs(ctx, docID, 0, 0, mustMarshalJSON(t, val), xattrs, nil, nil)
	require.NoError(t, err)

	// Update the body while deleting just the "rev" field from the _sync xattr, leaving "seq"
	// untouched. (WriteWithXattrs requires a body or xattr value to accompany a delete.)
	updatedVal := map[string]interface{}{"type": docID, "updated": true}
	_, err = col.WriteWithXattrs(ctx, docID, 0, cas, mustMarshalJSON(t, updatedVal), nil, []string{syncXattrName + ".rev"}, nil)
	require.NoError(t, err)

	getVal, getXattrs, _, err := col.GetWithXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)

	var fetchedVal, fetchedXattr map[string]interface{}
	require.NoError(t, json.Unmarshal(getVal, &fetchedVal))
	require.Equal(t, updatedVal, fetchedVal)

	require.NoError(t, json.Unmarshal(getXattrs[syncXattrName], &fetchedXattr))
	require.Equal(t, map[string]interface{}{"seq": float64(123)}, fetchedXattr)
}

func TestWriteCasWithXattrNoXattr(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)
	const docID = "DocExistsNoXattr"
	val := make(map[string]interface{})
	val["type"] = docID
	cas, err := col.WriteCas(ctx, docID, 0, 0, val, 0)
	require.NoError(t, err)

	updatedXattrVal := make(map[string]interface{})
	updatedXattrVal["seq"] = 123
	updatedXattrVal["rev"] = "2-1234"
	xattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, updatedXattrVal)}
	const deleteBody = true
	_, err = col.WriteTombstoneWithXattrs(ctx, docID, 0, uint64(1234), xattrs, nil, deleteBody, nil)

	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	_, err = col.WriteTombstoneWithXattrs(ctx, docID, 0, cas, xattrs, nil, deleteBody, nil)
	require.NoError(t, err)
	verifyEmptyBodyAndSyncXattr(t, col, docID)
}

func TestWriteCasWithXattrXattrExistsNoDoc(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)
	const docID = "XattrExistsNoDoc"

	val := make(map[string]interface{})
	val["type"] = docID

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 456
	xattrVal["rev"] = "1-1234"

	xattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, xattrVal)}
	// Create w/ XATTR
	cas := uint64(0)
	cas, err := col.WriteWithXattrs(ctx, docID, 0, cas, mustMarshalJSON(t, val), xattrs, nil, nil)
	require.NoError(t, err)

	// Delete the doc body
	cas, err = col.Remove(ctx, docID, cas)
	require.NoError(t, err)

	updatedXattrVal := make(map[string]interface{})
	updatedXattrVal["seq"] = 123
	updatedXattrVal["rev"] = "2-1234"
	xattrValBytes, err := json.Marshal(updatedXattrVal)
	require.NoError(t, err)

	updatedXattrs := map[string][]byte{syncXattrName: xattrValBytes}
	// First attempt to update with a bad cas value, and ensure we're getting the expected error
	const deleteBody = false
	_, err = col.WriteTombstoneWithXattrs(ctx, docID, 0, uint64(1234), updatedXattrs, nil, deleteBody, nil)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	_, err = col.WriteTombstoneWithXattrs(ctx, docID, 0, cas, updatedXattrs, nil, deleteBody, nil)
	require.NoError(t, err)
	verifyEmptyBodyAndSyncXattr(t, col, docID)
}

func TestWriteCasWithXattrOnTombstone(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)
	const docID = "XattrExistsNoDoc"

	val := make(map[string]interface{})
	val["type"] = docID

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 456
	xattrVal["rev"] = "1-1234"

	xattrs := map[string][]byte{syncXattrName: mustMarshalJSON(t, xattrVal)}
	cas, err := col.WriteWithXattrs(ctx, docID, 0, 0, mustMarshalJSON(t, val), xattrs, nil, nil)
	require.NoError(t, err)

	deleteCas, err := col.Remove(ctx, docID, cas)
	require.NoError(t, err)
	require.NotEqual(t, cas, deleteCas)

	// Verify attempted retrieval of non-existent xattrs still returns the correct cas
	retrievedVal, retrievedXattrs, getCas, err := col.GetWithXattrs(ctx, docID, []string{vvXattrName})
	require.ErrorAs(t, err, &sgbucket.MissingError{})
	require.Nil(t, retrievedVal)
	require.Equal(t, 0, len(retrievedXattrs))
	require.Equal(t, deleteCas, getCas)

	// A CAS of 0 resurrects the tombstone, as on Couchbase Server
	_, err = col.WriteWithXattrs(ctx, docID, 0, 0, mustMarshalJSON(t, val), xattrs, nil, nil)
	require.NoError(t, err)
	retrievedVal, retrievedXattrs, _, err = col.GetWithXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)
	require.JSONEq(t, string(mustMarshalJSON(t, val)), string(retrievedVal))
	require.JSONEq(t, string(xattrs[syncXattrName]), string(retrievedXattrs[syncXattrName]))
}

func verifyEmptyBodyAndSyncXattr(t *testing.T, store sgbucket.DataStore, key string) {
	xattrKeys := []string{syncXattrName}
	retrievedVal, retrievedXattrs, _, err := store.GetWithXattrs(t.Context(), key, xattrKeys)

	require.NoError(t, err)
	require.Nil(t, retrievedVal) // require that the doc body is empty
	syncXattrRaw, ok := retrievedXattrs[syncXattrName]
	require.True(t, ok)
	require.Greater(t, len(syncXattrRaw), 0)
}

func TestSetWithMetaNoDocument(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)
	const docID = "TestSetWithMeta"
	cas2 := CAS(1)
	body := []byte(`{"foo":"bar"}`)
	err := col.(*Collection).SetWithMeta(ctx, docID, 0, cas2, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.NoError(t, err)

	val, cas, err := col.GetRaw(ctx, docID)
	require.NoError(t, err)
	require.Equal(t, cas2, cas)
	require.JSONEq(t, string(body), string(val))
}

func TestSetWithMetaOverwriteJSON(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)
	docID := t.Name()
	cas1, err := col.WriteCas(ctx, docID, 0, 0, []byte("{}"), sgbucket.Raw)
	require.NoError(t, err)
	require.Greater(t, cas1, CAS(0))

	cas2 := CAS(1)
	body := []byte(`{"foo":"bar"}`)
	err = col.(*Collection).SetWithMeta(ctx, docID, cas1, cas2, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.NoError(t, err)

	val, cas, err := col.GetRaw(ctx, docID)
	require.NoError(t, err)
	require.Equal(t, cas2, cas)
	require.JSONEq(t, string(body), string(val))
}

func TestSetWithMetaOverwriteNotJSON(t *testing.T) {
	ctx := t.Context()
	bucket := makeTestBucket(t)
	col := bucket.DefaultDataStore(ctx)
	docID := t.Name()

	events, _ := startFeed(t, bucket)
	cas1, err := col.WriteCas(ctx, docID, 0, 0, []byte("{}"), 0)
	require.NoError(t, err)
	require.Greater(t, cas1, CAS(0))

	event1 := <-events
	require.Equal(t, docID, string(event1.Key))
	require.Equal(t, sgbucket.FeedOpMutation, event1.Opcode)
	require.Equal(t, sgbucket.FeedDataTypeJSON, event1.DataType)

	cas2 := CAS(1)
	body := []byte(`ABC`)
	err = col.(*Collection).SetWithMeta(ctx, docID, cas1, cas2, 0, nil, body, sgbucket.FeedDataTypeRaw)
	require.NoError(t, err)

	val, cas, err := col.GetRaw(ctx, docID)
	require.NoError(t, err)
	require.Equal(t, cas2, cas)
	require.Equal(t, body, val)

	event2 := <-events
	require.Equal(t, docID, string(event2.Key))
	require.Equal(t, sgbucket.FeedOpMutation, event2.Opcode)
	require.Equal(t, sgbucket.FeedDataTypeRaw, event2.DataType)
}

func TestSetWithMetaOverwriteTombstone(t *testing.T) {
	ctx := t.Context()
	bucket := makeTestBucket(t)
	col := bucket.DefaultDataStore(ctx)
	docID := t.Name()
	cas1, err := col.WriteCas(ctx, docID, 0, 0, []byte("{}"), sgbucket.Raw)
	require.NoError(t, err)
	require.Greater(t, cas1, CAS(0))
	deletedCas, err := col.Remove(ctx, docID, cas1)
	require.NoError(t, err)

	cas2 := CAS(1)
	body := []byte(`ABC`)

	// make sure there is a cas check even for tombstone
	err = col.(*Collection).SetWithMeta(ctx, docID, CAS(0), cas2, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	events, _ := startFeed(t, bucket)

	// cas check even on tombstone
	err = col.(*Collection).SetWithMeta(ctx, docID, deletedCas, cas2, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.NoError(t, err)

	event := <-events
	require.Equal(t, docID, string(event.Key))
	require.Equal(t, sgbucket.FeedOpMutation, event.Opcode)

	val, cas, err := col.GetRaw(ctx, docID)
	require.NoError(t, err)
	require.Equal(t, cas2, cas)
	require.Equal(t, body, val)
}

func TestSetWithMetaCas(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)
	docID := t.Name()

	body := []byte(`{"foo":"bar"}`)

	badStartingCas := CAS(1234)
	specifiedCas := CAS(1)

	// document doesn't exist, so cas mismatch will occur if CAS != 0
	err := col.(*Collection).SetWithMeta(ctx, docID, badStartingCas, specifiedCas, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	// document doesn't exist, but CAS 0 will allow writing
	err = col.(*Collection).SetWithMeta(ctx, docID, CAS(0), specifiedCas, 0, nil, body, sgbucket.FeedDataTypeJSON)
	require.NoError(t, err)

	val, cas, err := col.GetRaw(ctx, docID)
	require.NoError(t, err)
	require.Equal(t, specifiedCas, cas)
	require.JSONEq(t, string(body), string(val))
}

func TestDeleteWithMeta(t *testing.T) {
	testCases := []struct {
		name     string
		dataType sgbucket.WriteOptions
	}{
		{
			name:     "JSON",
			dataType: 0, // automatically determined
		},
		{
			name:     "Raw",
			dataType: sgbucket.Raw,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := t.Context()
			bucket := makeTestBucket(t)
			col := bucket.DefaultDataStore(ctx)
			docID := t.Name()

			startingCas, err := col.WriteCas(ctx, docID, 0, 0, []byte(`{"foo": "bar"}`), testCase.dataType)
			require.NoError(t, err)
			specifiedCas := CAS(1)

			events, _ := startFeed(t, bucket)

			// pass a bad CAS and document will not delete
			badStartingCas := CAS(1234)
			// document doesn't exist, but CAS 0 will allow writing
			err = col.(*Collection).DeleteWithMeta(ctx, docID, badStartingCas, specifiedCas, 0, nil)
			require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

			// tombstone with a good cas
			err = col.(*Collection).DeleteWithMeta(ctx, docID, startingCas, specifiedCas, 0, nil)
			require.NoError(t, err)

			event := <-events
			require.Equal(t, docID, string(event.Key))
			require.Equal(t, sgbucket.FeedOpDeletion, event.Opcode)
			require.Equal(t, sgbucket.FeedDataTypeRaw, event.DataType)

			_, err = col.Get(ctx, docID, nil)
			require.ErrorAs(t, err, &sgbucket.MissingError{})
		})
	}
}

func TestDeleteWithMetaXattr(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)
	docID := t.Name()

	val := make(map[string]interface{})
	val["type"] = docID

	xattrVal := make(map[string][]byte)
	const (
		userXattr      = "userXattr"
		systemXattr    = "_systemXattr"
		systemXattrVal = "bar"
	)
	xattrVal[userXattr] = mustMarshalJSON(t, "foo")
	xattrVal[systemXattr] = mustMarshalJSON(t, systemXattrVal)

	startingCas, err := col.WriteWithXattrs(ctx, docID, 0, 0, mustMarshalJSON(t, val), xattrVal, nil, nil)
	require.NoError(t, err)

	specifiedCas := CAS(1)
	// pass a bad CAS and document will not delete
	badStartingCas := CAS(1234)
	// document doesn't exist, but CAS 0 will allow writing
	err = col.DeleteWithMeta(ctx, docID, badStartingCas, specifiedCas, 0, nil)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	// tombstone with a good cas
	err = col.DeleteWithMeta(ctx, docID, startingCas, specifiedCas, 0, []byte(fmt.Sprintf(`{"%s": "%s"}`, systemXattr, systemXattrVal)))
	require.NoError(t, err)

	_, err = col.Get(ctx, docID, nil)
	require.ErrorAs(t, err, &sgbucket.MissingError{})

	xattrKeys := []string{syncXattrName, userXattr, systemXattr}

	xattrs, tombstoneCas, err := col.GetXattrs(ctx, docID, xattrKeys)
	require.NoError(t, err)
	require.Equal(t, specifiedCas, tombstoneCas)

	require.Contains(t, xattrs, systemXattr)
	require.NotContains(t, xattrs, userXattr)
}

func TestRevSeqNo(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)
	docID := t.Name()

	require.NoError(t, col.Set(ctx, docID, 0, nil, []byte(`{"foo": 1}`)))
	assertRevSeqNo(t, col, docID, `"1"`)

	require.NoError(t, col.Set(ctx, docID, 0, nil, []byte(`{"foo": 2}`)))
	assertRevSeqNo(t, col, docID, `"2"`)

	require.NoError(t, col.Delete(ctx, docID))
	assertRevSeqNo(t, col, docID, `"3"`)

	// ressurected doc
	require.NoError(t, col.Set(ctx, docID, 0, nil, []byte(`{"foo": 4`)))
	assertRevSeqNo(t, col, docID, `"4"`)

	// Doc was last written with exp=0. A Touch/GetAndTouchRaw that doesn't change
	// the expiry is a no-op on CB Server: CAS and revSeqNo are preserved.
	_, casBeforeNoOpTouch, err := col.GetRaw(ctx, docID)
	require.NoError(t, err)

	_, casAfterNoOpGAT, err := col.GetAndTouchRaw(ctx, docID, 0)
	require.NoError(t, err)
	assertRevSeqNo(t, col, docID, `"4"`)
	require.Equal(t, casBeforeNoOpTouch, casAfterNoOpGAT, "no-op GetAndTouchRaw should not bump CAS")

	casAfterNoOpTouch, err := col.Touch(ctx, docID, 0)
	require.NoError(t, err)
	assertRevSeqNo(t, col, docID, `"4"`)
	require.Equal(t, casBeforeNoOpTouch, casAfterNoOpTouch, "no-op Touch should not bump CAS")

	// Changing the expiry is a metadata mutation: CB Server bumps both CAS and revSeqNo.
	casAfterChangingTouch, err := col.Touch(ctx, docID, 60)
	require.NoError(t, err)
	assertRevSeqNo(t, col, docID, `"5"`)
	require.NotEqual(t, casBeforeNoOpTouch, casAfterChangingTouch, "Touch should bump CAS when exp changes")

	_, casAfterChangingGAT, err := col.GetAndTouchRaw(ctx, docID, 120)
	require.NoError(t, err)
	assertRevSeqNo(t, col, docID, `"6"`)
	require.NotEqual(t, casAfterChangingTouch, casAfterChangingGAT, "GetAndTouchRaw should bump CAS when exp changes")

	writeWithXattrsDocID := "writeWithXattrs"
	_, err = col.WriteWithXattrs(ctx, writeWithXattrsDocID, 0, 0, []byte(`{"foo": 1}`), nil, nil, nil)
	require.NoError(t, err)
	assertRevSeqNo(t, col, writeWithXattrsDocID, `"1"`)

	addRawDocID := "addRaw"
	_, err = col.AddRaw(ctx, addRawDocID, 0, []byte(`{"foo": 1}`))
	require.NoError(t, err)
	assertRevSeqNo(t, col, addRawDocID, `"1"`)

	setRawDocID := "setRaw"
	require.NoError(t, col.SetRaw(ctx, setRawDocID, 0, nil, []byte(`{"foo": 1}`)))
	assertRevSeqNo(t, col, setRawDocID, `"1"`)

	writeCasDocID := "writeCas"
	_, err = col.WriteCas(ctx, writeCasDocID, 0, 0, []byte(`{"foo": 1}`), 0)
	require.NoError(t, err)
	assertRevSeqNo(t, col, writeCasDocID, `"1"`)
}

func TestVirtualXattr(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)
	docID := t.Name()

	require.NoError(t, col.Set(ctx, docID, 0, nil, []byte(`{"foo": 1}`)))
	assertRevSeqNo(t, col, docID, `"1"`)

	// $document returns an object — unmarshal into a struct.
	t.Run("default virtual xattr", func(t *testing.T) {
		type virtualXattrDoc struct {
			RevNo   string `json:"revid,omitempty"`
			Crc32   string `json:"value_crc32c,omitempty"`
			CAS     string `json:"cas,omitempty"`
			Exptime uint32 `json:"exptime"`
			Deleted bool   `json:"deleted"`
		}
		xattrs, cas, err := col.GetXattrs(ctx, docID, []string{virtualXattrName})
		require.NoError(t, err)
		require.Contains(t, xattrs, virtualXattrName)
		var vx virtualXattrDoc
		require.NoError(t, json.Unmarshal(xattrs[virtualXattrName], &vx))
		expectedCAS := fmt.Sprintf(`0x%s`, strconv.FormatUint(cas, 16))
		require.Equal(t, virtualXattrDoc{RevNo: "1", Crc32: "0xe9a4f542", CAS: expectedCAS}, vx)
	})

	// $document.revid returns a raw JSON string.
	t.Run("rev seq no", func(t *testing.T) {
		xattrKey := fmt.Sprintf("%s.%s", virtualXattrName, virtualXattrRevSeqNo)
		xattrs, _, err := col.GetXattrs(ctx, docID, []string{xattrKey})
		require.NoError(t, err)
		require.Contains(t, xattrs, xattrKey)
		var revNo string
		require.NoError(t, json.Unmarshal(xattrs[xattrKey], &revNo))
		require.Equal(t, "1", revNo)
	})

	// $document.CAS returns a raw JSON number equal to the document's CAS.
	t.Run("cas", func(t *testing.T) {
		xattrKey := fmt.Sprintf("%s.%s", virtualXattrName, virtualXattrCAS)
		xattrs, cas, err := col.GetXattrs(ctx, docID, []string{xattrKey})
		require.NoError(t, err)
		require.Contains(t, xattrs, xattrKey)
		var fetchedCAS string
		require.NoError(t, json.Unmarshal(xattrs[xattrKey], &fetchedCAS))
		expectedCAS := fmt.Sprintf(`0x%s`, strconv.FormatUint(cas, 16))
		require.Equal(t, expectedCAS, fetchedCAS)
	})

	// $document.exptime returns the doc expiry as a JSON number (always present, even when 0).
	t.Run("expiry", func(t *testing.T) {
		xattrKey := fmt.Sprintf("%s.%s", virtualXattrName, virtualXattrExpiry)

		// Doc written above has no expiry — should return 0.
		xattrs, _, err := col.GetXattrs(ctx, docID, []string{xattrKey})
		require.NoError(t, err)
		require.Contains(t, xattrs, xattrKey)
		var fetchedExpiry uint32
		require.NoError(t, json.Unmarshal(xattrs[xattrKey], &fetchedExpiry))
		require.Equal(t, uint32(0), fetchedExpiry)

		// Write a doc with a real expiry and confirm it round-trips.
		expDocID := docID + "_exp"
		expiry := uint32(time.Now().Add(1 * time.Hour).Unix())
		require.NoError(t, col.Set(ctx, expDocID, expiry, nil, []byte(`{"foo": 2}`)))

		xattrs, _, err = col.GetXattrs(ctx, expDocID, []string{xattrKey})
		require.NoError(t, err)
		require.Contains(t, xattrs, xattrKey)
		require.NoError(t, json.Unmarshal(xattrs[xattrKey], &fetchedExpiry))
		require.Equal(t, expiry, fetchedExpiry)
	})
}

func assertRevSeqNo(t *testing.T, col *Collection, docID string, expectedRevSeqNo string) {
	xattrName := "$document.revid"
	ctx := t.Context()
	xattrs, _, err := col.GetXattrs(ctx, docID, []string{xattrName})
	require.NoError(t, err)

	require.Equal(t, expectedRevSeqNo, string(xattrs[xattrName]))
}

// TestTombstoneRemovesUserXattrs checks that each way of deleting a doc keeps system xattrs and removes user xattrs.
func TestTombstoneRemovesUserXattrs(t *testing.T) {
	ctx := t.Context()
	dataStore := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	deletes := map[string]func(t *testing.T, key string, cas uint64){
		"Delete": func(t *testing.T, key string, _ uint64) { require.NoError(t, dataStore.Delete(ctx, key)) },
		"Remove": func(t *testing.T, key string, cas uint64) {
			_, err := dataStore.Remove(ctx, key, cas)
			require.NoError(t, err)
		},
		"UpdateDelete": func(t *testing.T, key string, _ uint64) {
			_, err := dataStore.Update(ctx, key, 0, func([]byte) ([]byte, *uint32, bool, error) { return nil, nil, true, nil })
			require.NoError(t, err)
		},
	}
	for name, deleteFn := range deletes {
		t.Run(name, func(t *testing.T) {
			key := t.Name()
			cas, err := dataStore.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"foo":"bar"}`),
				map[string][]byte{"_sync": []byte(`{"rev":"1-a"}`), "user": []byte(`{"a":1}`)}, nil, nil)
			require.NoError(t, err)

			deleteFn(t, key, cas)

			xattrs, _, err := dataStore.GetXattrs(ctx, key, []string{"_sync"})
			require.NoError(t, err)
			require.JSONEq(t, `{"rev":"1-a"}`, string(xattrs["_sync"]))
			_, _, err = dataStore.GetXattrs(ctx, key, []string{"user"})
			require.ErrorAs(t, err, &sgbucket.XattrMissingError{})
		})
	}
}

func mustMarshalJSON(t *testing.T, obj any) []byte {
	bytes, err := json.Marshal(obj)
	require.NoError(t, err)
	return bytes
}

// TestDataStoreOperationErrors checks that each write, delete and remove operation matches Couchbase Server against a
// missing key, a tombstone and a live document, with a CAS of 0 (no CAS check), the current CAS and a stale CAS.
func TestDataStoreOperationErrors(t *testing.T) {
	const (
		success     = "success"
		notFound    = "not found"
		casMismatch = "cas mismatch"
		xattrName   = "_testxattr"
	)
	type docState string
	const (
		missing   docState = "missing"
		tombstone docState = "tombstone"
		live      docState = "live"
	)
	type casArg string
	const (
		zeroCas    casArg = "zeroCas"
		currentCas casArg = "currentCas"
		staleCas   casArg = "staleCas"
	)

	ctx := t.Context()
	ensureNoLeaks(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx)

	createDoc := func(t *testing.T, state docState) (key string, cas uint64) {
		key = t.Name()
		if state == missing {
			return key, 0
		}
		cas, err := coll.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"foo":"bar"}`), map[string][]byte{xattrName: []byte(`{"seq":1}`)}, nil, nil)
		require.NoError(t, err)
		if state == tombstone {
			cas, err = coll.Remove(ctx, key, cas)
			require.NoError(t, err)
		}
		return key, cas
	}

	testCases := []struct {
		name     string
		op       func(key string, cas uint64) error
		expected map[docState]map[casArg]string                 // a single zeroCas entry for operations that take no CAS
		verify   func(t *testing.T, key string, state docState) // optional check of the document after a success
	}{
		{
			name: "Delete",
			op:   func(key string, _ uint64) error { return coll.Delete(ctx, key) },
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: notFound},
				tombstone: {zeroCas: notFound},
				live:      {zeroCas: success},
			},
		},
		{
			name: "Remove",
			op: func(key string, cas uint64) error {
				_, err := coll.Remove(ctx, key, cas)
				return err
			},
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: notFound},
				tombstone: {zeroCas: notFound, currentCas: notFound, staleCas: notFound},
				live:      {zeroCas: success, currentCas: success, staleCas: casMismatch},
			},
		},
		{
			name: "DeleteWithXattrs",
			op:   func(key string, _ uint64) error { return coll.DeleteWithXattrs(ctx, key, []string{xattrName}) },
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: notFound},
				tombstone: {zeroCas: success},
				live:      {zeroCas: success},
			},
		},
		{
			name: "WriteTombstoneWithXattrs",
			op: func(key string, cas uint64) error {
				_, err := coll.WriteTombstoneWithXattrs(ctx, key, 0, cas, map[string][]byte{xattrName: []byte(`{"seq":2}`)}, nil, true, nil)
				return err
			},
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: notFound},
				tombstone: {zeroCas: notFound, currentCas: notFound, staleCas: notFound},
				live:      {zeroCas: success, currentCas: success, staleCas: casMismatch},
			},
		},
		{
			name: "RemoveXattrs",
			op:   func(key string, cas uint64) error { return coll.RemoveXattrs(ctx, key, []string{xattrName}, cas) },
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: notFound},
				tombstone: {zeroCas: success, currentCas: success, staleCas: casMismatch},
				live:      {zeroCas: success, currentCas: success, staleCas: casMismatch},
			},
		},
		{
			name: "DeleteSubDocPaths",
			op:   func(key string, _ uint64) error { return coll.DeleteSubDocPaths(ctx, key, xattrName) },
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: notFound},
				tombstone: {zeroCas: notFound},
				live:      {zeroCas: success},
			},
		},
		{
			name: "WriteCas",
			op: func(key string, cas uint64) error {
				_, err := coll.WriteCas(ctx, key, 0, cas, map[string]any{"foo": "baz"}, 0)
				return err
			},
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: success},
				tombstone: {zeroCas: success, currentCas: notFound, staleCas: notFound},
				live:      {zeroCas: casMismatch, currentCas: success, staleCas: casMismatch},
			},
		},
		{
			name: "WriteSubDoc",
			op: func(key string, cas uint64) error {
				_, err := coll.WriteSubDoc(ctx, key, "foo", cas, []byte(`"baz"`))
				return err
			},
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: success},
				tombstone: {zeroCas: success, currentCas: success, staleCas: success},
				live:      {zeroCas: success, currentCas: success, staleCas: casMismatch},
			},
		},
		{
			name: "WriteWithXattrs",
			op: func(key string, cas uint64) error {
				_, err := coll.WriteWithXattrs(ctx, key, 0, cas, []byte(`{"foo":"baz"}`), map[string][]byte{xattrName: []byte(`{"seq":2}`)}, nil, nil)
				return err
			},
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: success},
				tombstone: {zeroCas: success, currentCas: notFound, staleCas: notFound},
				live:      {zeroCas: casMismatch, currentCas: success, staleCas: casMismatch},
			},
		},
		{
			name: "WriteResurrectionWithXattrs",
			op: func(key string, _ uint64) error {
				_, err := coll.WriteResurrectionWithXattrs(ctx, key, 0, []byte(`{"foo":"baz"}`), map[string][]byte{xattrName: []byte(`{"seq":2}`)}, nil)
				return err
			},
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: success},
				tombstone: {zeroCas: success},
				live:      {zeroCas: casMismatch},
			},
		},
		{
			name: "SetXattrs",
			op: func(key string, _ uint64) error {
				_, err := coll.SetXattrs(ctx, key, map[string][]byte{xattrName: []byte(`{"seq":2}`)})
				return err
			},
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: success},
				tombstone: {zeroCas: success},
				live:      {zeroCas: success},
			},
			verify: func(t *testing.T, key string, state docState) {
				body, _, err := coll.GetRaw(ctx, key)
				switch state {
				case missing:
					require.NoError(t, err)
					require.JSONEq(t, `{}`, string(body))
				case tombstone:
					require.ErrorAs(t, err, &sgbucket.MissingError{})
				case live:
					require.NoError(t, err)
					require.JSONEq(t, `{"foo":"bar"}`, string(body))
				}
			},
		},
		{
			name: "UpdateXattrs",
			op: func(key string, cas uint64) error {
				_, err := coll.UpdateXattrs(ctx, key, 0, cas, map[string][]byte{xattrName: []byte(`{"seq":2}`)}, nil)
				return err
			},
			expected: map[docState]map[casArg]string{
				missing:   {zeroCas: notFound},
				tombstone: {zeroCas: success, currentCas: success, staleCas: casMismatch},
				live:      {zeroCas: success, currentCas: success, staleCas: casMismatch},
			},
		},
	}
	for _, tc := range testCases {
		for _, state := range []docState{missing, tombstone, live} {
			for _, arg := range []casArg{zeroCas, currentCas, staleCas} {
				expected, ok := tc.expected[state][arg]
				if !ok {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", tc.name, state, arg), func(t *testing.T) {
					key, cas := createDoc(t, state)
					switch arg {
					case zeroCas:
						cas = 0
					case staleCas:
						cas--
					}
					err := tc.op(key, cas)
					switch expected {
					case success:
						require.NoError(t, err)
					case notFound:
						require.ErrorAs(t, err, &sgbucket.MissingError{})
					case casMismatch:
						require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})
					}
					if expected == success && tc.verify != nil {
						tc.verify(t, key, state)
					}
				})
			}
		}
	}
}

// TestDeleteWithXattrsMissingXattr checks that DeleteWithXattrs matches Couchbase Server, in the result and the
// remaining document, when it is asked to delete an xattr that the document does not have.
func TestDeleteWithXattrsMissingXattr(t *testing.T) {
	const (
		success     = "success"
		notFound    = "not found"
		otherError  = "other error"
		missingName = "_missingxattr"
	)
	ctx := t.Context()
	ensureNoLeaks(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx)

	testCases := []struct {
		name              string
		tombstone         bool
		xattrs            []string
		deleteXattrs      []string
		expected          string
		expectedRemaining []string
	}{
		{
			name:              "live doc, delete present and missing xattr",
			xattrs:            []string{"_xattr1"},
			deleteXattrs:      []string{"_xattr1", missingName},
			expected:          success,
			expectedRemaining: []string{"_xattr1"},
		},
		{
			name:              "live doc, delete one of two xattrs and a missing xattr",
			xattrs:            []string{"_xattr1", "_xattr2"},
			deleteXattrs:      []string{"_xattr1", missingName},
			expected:          success,
			expectedRemaining: []string{"_xattr1", "_xattr2"},
		},
		{
			name:              "live doc, delete missing xattr",
			xattrs:            []string{"_xattr1"},
			deleteXattrs:      []string{missingName},
			expected:          success,
			expectedRemaining: []string{"_xattr1"},
		},
		{
			name:         "live doc with no xattrs, delete missing xattr",
			deleteXattrs: []string{missingName},
			expected:     success,
		},
		{
			name:              "tombstone, delete present and missing xattr",
			tombstone:         true,
			xattrs:            []string{"_xattr1"},
			deleteXattrs:      []string{"_xattr1", missingName},
			expected:          otherError,
			expectedRemaining: []string{"_xattr1"},
		},
		{
			name:              "tombstone, delete missing xattr",
			tombstone:         true,
			xattrs:            []string{"_xattr1"},
			deleteXattrs:      []string{missingName},
			expected:          notFound,
			expectedRemaining: []string{"_xattr1"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := t.Name()
			xattrs := make(map[string][]byte, len(tc.xattrs))
			for _, xattrName := range tc.xattrs {
				xattrs[xattrName] = []byte(`{"seq":1}`)
			}
			var cas uint64
			var err error
			if len(xattrs) > 0 {
				cas, err = coll.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"foo":"bar"}`), xattrs, nil, nil)
			} else {
				cas, err = coll.WriteCas(ctx, key, 0, 0, map[string]any{"foo": "bar"}, 0)
			}
			require.NoError(t, err)
			if tc.tombstone {
				_, err = coll.Remove(ctx, key, cas)
				require.NoError(t, err)
			}

			err = coll.DeleteWithXattrs(ctx, key, tc.deleteXattrs)
			switch tc.expected {
			case success:
				require.NoError(t, err)
			case notFound:
				require.ErrorAs(t, err, &sgbucket.MissingError{})
			case otherError:
				require.Error(t, err)
				require.NotErrorAs(t, err, &sgbucket.MissingError{})
			}

			_, _, err = coll.GetRaw(ctx, key)
			require.ErrorAs(t, err, &sgbucket.MissingError{})
			var remaining []string
			for _, xattrName := range append(tc.xattrs, missingName) {
				xattrValues, _, err := coll.GetXattrs(ctx, key, []string{xattrName})
				if err == nil && len(xattrValues[xattrName]) > 0 {
					remaining = append(remaining, xattrName)
				}
			}
			require.Equal(t, tc.expectedRemaining, remaining)
		})
	}
}

// TestWriteCasNil checks that a nil WriteCas value never creates a tombstone: an untyped nil writes a null JSON body
// and a nil []byte writes an empty body.
func TestWriteCasNil(t *testing.T) {
	type docState string
	const (
		missing   docState = "missing"
		tombstone docState = "tombstone"
		live      docState = "live"
	)
	ctx := t.Context()
	dataStore := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	getRevSeqNo := func(t *testing.T, key string) uint64 {
		xattrs, _, err := dataStore.GetXattrs(ctx, key, []string{"$document.revid"})
		require.NoError(t, err)
		var revSeqNo string
		require.NoError(t, json.Unmarshal(xattrs["$document.revid"], &revSeqNo))
		n, err := strconv.ParseUint(revSeqNo, 10, 64)
		require.NoError(t, err)
		return n
	}
	testCases := []struct {
		name         string
		state        docState
		value        any
		opt          sgbucket.WriteOptions
		expectedBody string
	}{
		{name: "untypedNil", state: missing, expectedBody: "null"},
		{name: "untypedNil", state: tombstone, expectedBody: "null"},
		{name: "untypedNil", state: live, expectedBody: "null"},
		{name: "nilBytes", state: missing, value: []byte(nil)},
		{name: "nilBytes", state: live, value: []byte(nil)},
		{name: "nilBytesRaw", state: missing, value: []byte(nil), opt: sgbucket.Raw},
		{name: "nilBytesRaw", state: live, value: []byte(nil), opt: sgbucket.Raw},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/%s", tc.name, tc.state), func(t *testing.T) {
			key := t.Name()
			var cas, revSeqNo uint64
			if tc.state != missing {
				var err error
				cas, err = dataStore.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"foo":"bar"}`), map[string][]byte{"_sync": []byte(`{"rev":"1-a"}`)}, nil, nil)
				require.NoError(t, err)
				if tc.state == tombstone {
					require.NoError(t, dataStore.Delete(ctx, key))
					cas = 0
				}
				revSeqNo = getRevSeqNo(t, key)
			}
			_, err := dataStore.WriteCas(ctx, key, 0, cas, tc.value, tc.opt)
			require.NoError(t, err)

			body, _, err := dataStore.GetRaw(ctx, key)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBody, string(body))
			require.Equal(t, revSeqNo+1, getRevSeqNo(t, key))
			xattrs, _, err := dataStore.GetXattrs(ctx, key, []string{"_sync"})
			if tc.state == live {
				require.NoError(t, err)
				require.JSONEq(t, `{"rev":"1-a"}`, string(xattrs["_sync"]))
			} else {
				require.ErrorAs(t, err, &sgbucket.XattrMissingError{})
			}
		})
	}
}

// TestTombstoneFlagMatchesBody checks that after each write over a tombstone, the document is live if it has a body
// and a tombstone if it does not, and that the write increases the revision sequence number.
func TestTombstoneFlagMatchesBody(t *testing.T) {
	ctx := t.Context()
	dataStore := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	getRevSeqNo := func(t *testing.T, key string) uint64 {
		xattrs, _, err := dataStore.GetXattrs(ctx, key, []string{"$document.revid"})
		require.NoError(t, err)
		var revSeqNo string
		require.NoError(t, json.Unmarshal(xattrs["$document.revid"], &revSeqNo))
		n, err := strconv.ParseUint(revSeqNo, 10, 64)
		require.NoError(t, err)
		return n
	}
	syncXattr := map[string][]byte{"_sync": []byte(`{"rev":"2-a"}`)}
	testCases := []struct {
		name         string
		expectedLive bool
		writeFn      func(t *testing.T, key string, cas uint64)
	}{
		{
			name:         "Add",
			expectedLive: true,
			writeFn: func(t *testing.T, key string, _ uint64) {
				added, err := dataStore.Add(ctx, key, 0, map[string]any{"new": true})
				require.NoError(t, err)
				require.True(t, added)
			},
		},
		{
			name:         "Set",
			expectedLive: true,
			writeFn: func(t *testing.T, key string, _ uint64) {
				require.NoError(t, dataStore.Set(ctx, key, 0, nil, map[string]any{"new": true}))
			},
		},
		{
			name:         "AddRaw",
			expectedLive: true,
			writeFn: func(t *testing.T, key string, _ uint64) {
				added, err := dataStore.AddRaw(ctx, key, 0, []byte(`{"new":true}`))
				require.NoError(t, err)
				require.True(t, added)
			},
		},
		{
			name:         "SetRaw",
			expectedLive: true,
			writeFn: func(t *testing.T, key string, _ uint64) {
				require.NoError(t, dataStore.SetRaw(ctx, key, 0, nil, []byte(`{"new":true}`)))
			},
		},
		{
			name:         "Incr",
			expectedLive: true,
			writeFn: func(t *testing.T, key string, _ uint64) {
				_, err := dataStore.Incr(ctx, key, 1, 1, 0)
				require.NoError(t, err)
			},
		},
		{
			name:         "WriteCas",
			expectedLive: true,
			writeFn: func(t *testing.T, key string, _ uint64) {
				_, err := dataStore.WriteCas(ctx, key, 0, 0, map[string]any{"new": true}, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "SetXattrs",
			writeFn: func(t *testing.T, key string, _ uint64) {
				_, err := dataStore.SetXattrs(ctx, key, syncXattr)
				require.NoError(t, err)
			},
		},
		{
			name: "UpdateXattrs",
			writeFn: func(t *testing.T, key string, cas uint64) {
				_, err := dataStore.UpdateXattrs(ctx, key, 0, cas, syncXattr, nil)
				require.NoError(t, err)
			},
		},
		{
			name: "WriteWithXattrsNilBody",
			writeFn: func(t *testing.T, key string, cas uint64) {
				_, err := dataStore.WriteWithXattrs(ctx, key, 0, cas, nil, syncXattr, nil, nil)
				require.NoError(t, err)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := t.Name()
			_, err := dataStore.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"foo":"bar"}`), map[string][]byte{"_sync": []byte(`{"rev":"1-a"}`)}, nil, nil)
			require.NoError(t, err)
			require.NoError(t, dataStore.Delete(ctx, key))
			_, cas, err := dataStore.GetXattrs(ctx, key, []string{"_sync"})
			require.NoError(t, err)
			revSeqNo := getRevSeqNo(t, key)

			tc.writeFn(t, key, cas)

			require.Greater(t, getRevSeqNo(t, key), revSeqNo)
			_, _, err = dataStore.GetRaw(ctx, key)
			if tc.expectedLive {
				require.NoError(t, err)
				require.NoError(t, dataStore.Delete(ctx, key))
			} else {
				require.ErrorAs(t, err, &sgbucket.MissingError{})
				require.ErrorAs(t, dataStore.Delete(ctx, key), &sgbucket.MissingError{})
			}
		})
	}
}

// TestDeleteAfterTombstoneResurrection checks that a document written over a tombstone is live again, and that a
// document removed by DeleteWithXattrs is a tombstone, by deleting it afterwards.
func TestDeleteAfterTombstoneResurrection(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	body := map[string]any{"foo": "bar"}
	rawBody := []byte(`{"foo":"bar"}`)
	writes := map[string]func(key string) error{
		"Set":    func(key string) error { return col.Set(ctx, key, 0, nil, body) },
		"SetRaw": func(key string) error { return col.SetRaw(ctx, key, 0, nil, rawBody) },
		"Incr": func(key string) error {
			_, err := col.Incr(ctx, key, 1, 1, 0)
			return err
		},
		"Add": func(key string) error {
			added, err := col.Add(ctx, key, 0, body)
			require.True(t, added)
			return err
		},
		"AddRaw": func(key string) error {
			added, err := col.AddRaw(ctx, key, 0, rawBody)
			require.True(t, added)
			return err
		},
		"WriteCas": func(key string) error {
			_, err := col.WriteCas(ctx, key, 0, 0, body, 0)
			return err
		},
		"Update": func(key string) error {
			_, err := col.Update(ctx, key, 0, func([]byte) ([]byte, *uint32, bool, error) {
				return rawBody, nil, false, nil
			})
			return err
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			key := t.Name()
			require.NoError(t, col.SetRaw(ctx, key, 0, nil, rawBody))
			require.NoError(t, col.Delete(ctx, key))

			require.NoError(t, write(key))
			require.NoError(t, col.Delete(ctx, key))
		})
	}

	t.Run("DeleteWithXattrs", func(t *testing.T) {
		key := t.Name()
		_, err := col.WriteWithXattrs(ctx, key, 0, 0, rawBody, map[string][]byte{"_testxattr": []byte(`{"seq":1}`)}, nil, nil)
		require.NoError(t, err)
		require.NoError(t, col.DeleteWithXattrs(ctx, key, []string{"_testxattr"}))
		require.ErrorAs(t, col.Delete(ctx, key), &sgbucket.MissingError{})
	})
}

// TestUpdateByDocState checks the document after an Update over a missing, tombstone or live document, when the
// callback returns a value or nil, with and without a delete.
func TestUpdateByDocState(t *testing.T) {
	type docState string
	const (
		missing   docState = "missing"
		tombstone docState = "tombstone"
		live      docState = "live"
	)
	ctx := t.Context()
	dataStore := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	testCases := []struct {
		state             docState
		value             []byte
		delete            bool
		expectedTombstone bool   // otherwise a live document with expectedBody
		expectedBody      string // empty for an empty body
		expectedExpiry    bool
		expectedSync      bool
	}{
		{state: missing, expectedExpiry: true},
		{state: tombstone, expectedExpiry: true},
		{state: live, expectedExpiry: true, expectedSync: true},
		{state: missing, delete: true, expectedExpiry: true},
		{state: tombstone, delete: true, expectedExpiry: true},
		{state: live, delete: true, expectedTombstone: true, expectedSync: true},
		{state: tombstone, value: []byte(`{"new":true}`), expectedBody: `{"new":true}`, expectedExpiry: true},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/value=%t/delete=%t", tc.state, tc.value != nil, tc.delete), func(t *testing.T) {
			key := t.Name()
			if tc.state != missing {
				cas, err := dataStore.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"foo":"bar"}`), map[string][]byte{"_sync": []byte(`{"seq":1}`)}, nil, nil)
				require.NoError(t, err)
				if tc.state == tombstone {
					_, err = dataStore.Remove(ctx, key, cas)
					require.NoError(t, err)
					_, cas, err = dataStore.GetRaw(ctx, key)
					require.ErrorAs(t, err, &sgbucket.MissingError{})
					require.Zero(t, cas, "the read of a tombstone returned a CAS")
				}
			}

			exp := uint32(3600)
			_, err := dataStore.Update(ctx, key, 0, func([]byte) ([]byte, *uint32, bool, error) {
				return tc.value, &exp, tc.delete, nil
			})
			require.NoError(t, err)

			body, _, err := dataStore.GetRaw(ctx, key)
			switch {
			case tc.expectedTombstone:
				require.ErrorAs(t, err, &sgbucket.MissingError{})
			case tc.expectedBody == "":
				require.NoError(t, err)
				require.Empty(t, body)
			default:
				require.NoError(t, err)
				require.JSONEq(t, tc.expectedBody, string(body))
			}
			docExp, err := dataStore.GetExpiry(ctx, key)
			require.NoError(t, err)
			require.Equal(t, tc.expectedExpiry, docExp != 0, "unexpected expiry %d", docExp)
			xattrs, _, err := dataStore.GetXattrs(ctx, key, []string{"_sync"})
			if tc.expectedSync {
				require.NoError(t, err)
				require.JSONEq(t, `{"seq":1}`, string(xattrs["_sync"]))
			} else {
				require.ErrorAs(t, err, &sgbucket.XattrMissingError{})
			}
		})
	}
}

// TestTombstoneExpiry checks the expiry of the tombstone that each operation creates, when the document and the
// operation have expiries far in the future, and that a write which resurrects it with PreserveExpiry has no expiry.
func TestTombstoneExpiry(t *testing.T) {
	type docState string
	const (
		missing   docState = "missing"
		tombstone docState = "tombstone"
		live      docState = "live"
	)
	ctx := t.Context()
	dataStore := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	docExpiry := uint32(time.Now().Add(5 * 365 * 24 * time.Hour).Unix())
	opExpiry := uint32(time.Now().Add(10 * 365 * 24 * time.Hour).Unix())
	syncXattr := func(seq int) map[string][]byte {
		return map[string][]byte{"_sync": fmt.Appendf(nil, `{"seq":%d}`, seq)}
	}
	writeUpdateTombstone := func(t *testing.T, key string, exp uint32, callbackExp *uint32, opts *sgbucket.MutateInOptions) {
		_, err := dataStore.WriteUpdateWithXattrs(ctx, key, []string{"_sync"}, exp, nil, opts, func([]byte, map[string][]byte, uint64) (sgbucket.UpdatedDoc, error) {
			return sgbucket.UpdatedDoc{Xattrs: syncXattr(2), IsTombstone: true, Expiry: callbackExp}, nil
		})
		require.NoError(t, err)
	}

	testCases := []struct {
		name       string
		state      docState
		takesExp   bool // the operation takes an expiry
		takesOpts  bool // the operation takes MutateInOptions
		keepsOpExp bool // the tombstone has the expiry passed to the operation
		op         func(t *testing.T, key string, cas uint64, exp uint32, opts *sgbucket.MutateInOptions)
	}{
		{
			name:  "Delete",
			state: live,
			op: func(t *testing.T, key string, _ uint64, _ uint32, _ *sgbucket.MutateInOptions) {
				require.NoError(t, dataStore.Delete(ctx, key))
			},
		},
		{
			name:  "Remove",
			state: live,
			op: func(t *testing.T, key string, cas uint64, _ uint32, _ *sgbucket.MutateInOptions) {
				_, err := dataStore.Remove(ctx, key, cas)
				require.NoError(t, err)
			},
		},
		{
			name:  "DeleteWithXattrs",
			state: live,
			op: func(t *testing.T, key string, _ uint64, _ uint32, _ *sgbucket.MutateInOptions) {
				require.NoError(t, dataStore.DeleteWithXattrs(ctx, key, nil))
			},
		},
		{
			name:     "UpdateDelete",
			state:    live,
			takesExp: true,
			op: func(t *testing.T, key string, _ uint64, exp uint32, _ *sgbucket.MutateInOptions) {
				_, err := dataStore.Update(ctx, key, 0, func([]byte) ([]byte, *uint32, bool, error) { return nil, &exp, true, nil })
				require.NoError(t, err)
			},
		},
		{
			name:       "WriteTombstoneWithXattrsDeleteBody",
			state:      live,
			takesExp:   true,
			takesOpts:  true,
			keepsOpExp: true,
			op: func(t *testing.T, key string, cas uint64, exp uint32, opts *sgbucket.MutateInOptions) {
				_, err := dataStore.WriteTombstoneWithXattrs(ctx, key, exp, cas, syncXattr(2), nil, true, opts)
				require.NoError(t, err)
			},
		},
		{
			name:       "WriteTombstoneWithXattrsMissing",
			state:      missing,
			takesExp:   true,
			takesOpts:  true,
			keepsOpExp: true,
			op: func(t *testing.T, key string, _ uint64, exp uint32, opts *sgbucket.MutateInOptions) {
				_, err := dataStore.WriteTombstoneWithXattrs(ctx, key, exp, 0, syncXattr(2), nil, false, opts)
				require.NoError(t, err)
			},
		},
		{
			name:       "WriteTombstoneWithXattrsTombstone",
			state:      tombstone,
			takesExp:   true,
			takesOpts:  true,
			keepsOpExp: true,
			op: func(t *testing.T, key string, cas uint64, exp uint32, opts *sgbucket.MutateInOptions) {
				_, err := dataStore.WriteTombstoneWithXattrs(ctx, key, exp, cas, syncXattr(2), nil, false, opts)
				require.NoError(t, err)
			},
		},
		{
			name:       "WriteUpdateWithXattrs",
			state:      live,
			takesExp:   true,
			takesOpts:  true,
			keepsOpExp: true,
			op: func(t *testing.T, key string, _ uint64, exp uint32, opts *sgbucket.MutateInOptions) {
				writeUpdateTombstone(t, key, exp, nil, opts)
			},
		},
		{
			name:       "WriteUpdateWithXattrsMissing",
			state:      missing,
			takesExp:   true,
			takesOpts:  true,
			keepsOpExp: true,
			op: func(t *testing.T, key string, _ uint64, exp uint32, opts *sgbucket.MutateInOptions) {
				writeUpdateTombstone(t, key, exp, nil, opts)
			},
		},
		{
			name:       "WriteUpdateWithXattrsCallbackExpiry",
			state:      live,
			takesExp:   true,
			takesOpts:  true,
			keepsOpExp: true,
			op: func(t *testing.T, key string, _ uint64, exp uint32, opts *sgbucket.MutateInOptions) {
				writeUpdateTombstone(t, key, 0, &exp, opts)
			},
		},
	}
	resurrections := map[string]func(t *testing.T, key string){
		"Set": func(t *testing.T, key string) {
			require.NoError(t, dataStore.Set(ctx, key, 0, &sgbucket.UpsertOptions{PreserveExpiry: true}, map[string]any{"foo": "baz"}))
		},
		"WriteWithXattrs": func(t *testing.T, key string) {
			_, err := dataStore.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"foo":"baz"}`), syncXattr(3), nil, &sgbucket.MutateInOptions{PreserveExpiry: true})
			require.NoError(t, err)
		},
		"WriteResurrectionWithXattrs": func(t *testing.T, key string) {
			_, err := dataStore.WriteResurrectionWithXattrs(ctx, key, 0, []byte(`{"foo":"baz"}`), syncXattr(3), &sgbucket.MutateInOptions{PreserveExpiry: true})
			require.NoError(t, err)
		},
	}
	type documentXattr struct {
		Exptime uint32 `json:"exptime"`
		Deleted bool   `json:"deleted"`
	}
	getDocumentXattr := func(t *testing.T, key string) documentXattr {
		xattrs, _, err := dataStore.GetXattrs(ctx, key, []string{virtualXattrName})
		require.NoError(t, err)
		var document documentXattr
		require.NoError(t, json.Unmarshal(xattrs[virtualXattrName], &document))
		return document
	}
	requireExpiry := func(t *testing.T, expected, actual uint32, msg string) {
		if expected == 0 {
			require.Zero(t, actual, msg)
		} else {
			// Couchbase Server gets an absolute expiry as a duration, which can round it down.
			require.InDelta(t, expected, actual, 2, msg)
		}
	}
	for _, tc := range testCases {
		docExpiries := []uint32{0, docExpiry}
		if tc.state == missing {
			docExpiries = []uint32{0}
		}
		opExpiries := []uint32{0}
		if tc.takesExp {
			opExpiries = append(opExpiries, opExpiry)
		}
		preserves := []bool{false}
		if tc.takesOpts {
			preserves = append(preserves, true)
		}
		for _, docExp := range docExpiries {
			for _, opExp := range opExpiries {
				for _, preserve := range preserves {
					for resurrectionName, resurrect := range resurrections {
						t.Run(fmt.Sprintf("%s/docExp=%t/opExp=%t/preserve=%t/%s", tc.name, docExp != 0, opExp != 0, preserve, resurrectionName), func(t *testing.T) {
							key := t.Name()
							var cas uint64
							if tc.state != missing {
								var err error
								cas, err = dataStore.WriteWithXattrs(ctx, key, docExp, 0, []byte(`{"foo":"bar"}`), syncXattr(1), nil, nil)
								require.NoError(t, err)
								exp, err := dataStore.GetExpiry(ctx, key)
								require.NoError(t, err)
								requireExpiry(t, docExp, exp, "live document expiry")
								require.Equal(t, documentXattr{Exptime: exp}, getDocumentXattr(t, key))
								if tc.state == tombstone {
									cas, err = dataStore.Remove(ctx, key, cas)
									require.NoError(t, err)
								}
							}
							var opts *sgbucket.MutateInOptions
							if preserve {
								opts = &sgbucket.MutateInOptions{PreserveExpiry: true}
							}
							tc.op(t, key, cas, opExp, opts)

							var expected uint32
							if tc.keepsOpExp {
								expected = opExp
							}
							_, _, err := dataStore.GetRaw(ctx, key)
							require.ErrorAs(t, err, &sgbucket.MissingError{})
							exp, err := dataStore.GetExpiry(ctx, key)
							require.NoError(t, err)
							requireExpiry(t, expected, exp, "tombstone expiry")
							document := getDocumentXattr(t, key)
							require.True(t, document.Deleted)
							requireExpiry(t, expected, document.Exptime, "$document.exptime")

							resurrect(t, key)
							exp, err = dataStore.GetExpiry(ctx, key)
							require.NoError(t, err)
							require.Zero(t, exp, "document written over the tombstone has an expiry")
						})
					}
				}
			}
		}
	}
}
