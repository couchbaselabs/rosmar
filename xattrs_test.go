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
	"errors"
	"fmt"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTouchXattrWithCas(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const docID = "doc1"
	bodyBytes := []byte(`{"hello":"world"}`)
	xattrsInput := map[string][]byte{
		syncXattrName: []byte(`{"existing":"value"}`),
	}
	originalCas, err := coll.WriteWithXattrs(ctx, docID, 0, 0, bodyBytes, xattrsInput, nil, nil)
	require.NoError(t, err)

	// Stale CAS should fail.
	_, err = coll.TouchXattrWithCas(ctx, docID, syncXattrName, "name", "db1", originalCas+1)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})

	// Correct CAS succeeds and bumps CAS.
	newCas, err := coll.TouchXattrWithCas(ctx, docID, syncXattrName, "name", "db1", originalCas)
	require.NoError(t, err)
	require.NotEqual(t, originalCas, newCas)

	// Verify the xattr property is now visible and the existing properties were preserved.
	gotBody, gotXattrs, getCas, err := coll.GetWithXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)
	require.Equal(t, newCas, getCas)
	require.Equal(t, bodyBytes, gotBody)
	var xattr map[string]string
	require.NoError(t, json.Unmarshal(gotXattrs[syncXattrName], &xattr))
	require.Equal(t, "db1", xattr["name"])
	require.Equal(t, "value", xattr["existing"])

	// The old CAS is now stale.
	_, err = coll.TouchXattrWithCas(ctx, docID, syncXattrName, "name", "db2", originalCas)
	require.ErrorAs(t, err, &sgbucket.CasMismatchErr{})
}

// TestTouchXattrWithCasCreatesXattr verifies that TouchXattrWithCas can populate an xattr on a doc
// that does not yet have one.
func TestTouchXattrWithCasCreatesXattr(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const docID = "doc1"
	require.NoError(t, coll.SetRaw(ctx, docID, 0, nil, []byte(`{"a":1}`)))
	_, cas, err := coll.GetRaw(ctx, docID)
	require.NoError(t, err)

	newCas, err := coll.TouchXattrWithCas(ctx, docID, syncXattrName, "name", "db1", cas)
	require.NoError(t, err)
	require.NotEqual(t, cas, newCas)

	_, gotXattrs, _, err := coll.GetWithXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)
	var xattr map[string]string
	require.NoError(t, json.Unmarshal(gotXattrs[syncXattrName], &xattr))
	require.Equal(t, "db1", xattr["name"])
}

func TestSetXattrs(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	addToCollection(t, coll, "key", 0, "value")

	const (
		key1 = "xfiles1"
		key2 = "boring"
	)
	xattrs := map[string][]byte{
		key1: []byte(`{"truth":"out_there"}`),
		key2: []byte(`{"foo": "bar"}`),
	}
	cas, err := coll.SetXattrs(ctx, "key", xattrs)
	require.NoError(t, err)

	val, outputXattrs, gotCas, err := coll.GetWithXattrs(ctx, "key", []string{key1, key2})
	require.NoError(t, err)
	assert.Equal(t, cas, gotCas)
	assert.Equal(t, `"value"`, string(val))
	assert.Equal(t, string(mustMarshalJSON(t, map[string]string{"truth": "out_there"})), string(outputXattrs[key1]))
}

func TestMacroExpansion(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	// Successful case - sets cas and crc32c in the _sync xattr
	opts := &sgbucket.MutateInOptions{}
	opts.MacroExpansion = []sgbucket.MacroExpansionSpec{
		{Path: "_sync.testcas", Type: sgbucket.MacroCas},
		{Path: "_sync.testcrc32c", Type: sgbucket.MacroCrc32c},
	}
	bodyBytes := []byte(`{"a":123}`)

	xattrsInput := map[string][]byte{
		"_sync": []byte(`{"x":456}`),
	}
	casOut, err := coll.WriteWithXattrs(ctx, "key", 0, 0, bodyBytes, xattrsInput, nil, opts)
	require.NoError(t, err)

	_, xattrs, getCas, err := coll.GetWithXattrs(ctx, "key", []string{syncXattrName})
	require.NoError(t, err)
	require.Equal(t, getCas, casOut)

	marshalledXval, ok := xattrs[syncXattrName]
	require.True(t, ok)
	var xval map[string]any
	err = json.Unmarshal(marshalledXval, &xval)
	require.NoError(t, err)
	casVal, ok := xval["testcas"]
	require.True(t, ok)
	require.Equal(t, casAsString(casOut), casVal)

	_, ok = xval["testcrc32c"]
	require.True(t, ok)

	// This would be the behavior in CBS <7.6
	/*
		// Unsuccessful - target unknown xattr
		opts.MacroExpansion = []sgbucket.MacroExpansionSpec{
			{Path: "_unknown.testcas", Type: sgbucket.MacroCas},
		}

		opts.MacroExpansion = []sgbucket.MacroExpansionSpec{
			{Path: "_sync.unknownPath.testcas", Type: sgbucket.MacroCas},
		}
		_, err = coll.WriteWithXattrs(ctx, "pathError", 0, 0, bodyBytes, xattrsInput, opts)
		require.Error(t, err)
	*/
}

func TestMacroExpansionMultipleXattrs(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	// Successful case - sets cas and crc32c in the _sync xattr
	opts := &sgbucket.MutateInOptions{}
	opts.MacroExpansion = []sgbucket.MacroExpansionSpec{
		{Path: "_xattr1.testcas", Type: sgbucket.MacroCas},
		{Path: "_xattr2.testcas", Type: sgbucket.MacroCas},
	}
	bodyBytes := []byte(`{"a":123}`)

	xattrsInput := map[string][]byte{
		"_xattr1": []byte(`{"x":"abc"}`),
		"_xattr2": []byte(`{"x":"def"}`),
		"_xattr3": []byte(`{"x":"ghi"}`),
	}
	casOut, err := coll.WriteWithXattrs(ctx, "key", 0, 0, bodyBytes, xattrsInput, nil, opts)
	require.NoError(t, err)

	_, xattrs, getCas, err := coll.GetWithXattrs(ctx, "key", []string{"_xattr1", "_xattr2", "_xattr3"})
	require.NoError(t, err)
	require.Equal(t, getCas, casOut)

	for _, xattr := range []string{"_xattr1", "_xattr2", "_xattr3"} {
		marshalledXval, ok := xattrs[xattr]
		require.True(t, ok, "xattr %s not found", xattr)
		var xattr1 map[string]string
		err = json.Unmarshal(marshalledXval, &xattr1)
		require.NoError(t, err)
		if xattr == "_xattr1" || xattr == "_xattr2" {
			require.Equal(t, casAsString(casOut), xattr1["testcas"])
		} else {
			require.NotContains(t, xattr1, "testcas")
		}
	}
}

func TestWriteWithXattrsSetAndDeleteError(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)
	docID := t.Name()

	fakeCas := uint64(1)
	_, err := col.WriteWithXattrs(ctx, docID, 0, fakeCas, []byte(`{"foo": "bar"}`), map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)}, []string{"xattr1"}, nil)
	require.ErrorIs(t, err, sgbucket.ErrUpsertAndDeleteSameXattr)
}

func TestWriteUpdateDeleteXattrTombstone(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)

	key := t.Name()
	xattrKey := "_xattr1"
	xattrBody := []byte(`{"foo": "bar"}`)

	_, err := col.WriteTombstoneWithXattrs(ctx, key, 0, 0, map[string][]byte{xattrKey: xattrBody}, nil, false, nil)
	require.NoError(t, err)

	xattrs, _, err := col.GetXattrs(ctx, key, []string{xattrKey})
	require.NoError(t, err)
	require.JSONEq(t, string(xattrBody), string(xattrs[xattrKey]))

	writeUpdateFunc := func(doc []byte, xattrs map[string][]byte, cas uint64) (sgbucket.UpdatedDoc, error) {
		return sgbucket.UpdatedDoc{
			XattrsToDelete: []string{xattrKey},
			Doc:            []byte(`{"foo":"bar"}`),
		}, nil
	}

	_, err = col.WriteUpdateWithXattrs(ctx, key, []string{xattrKey}, 0, nil, nil, writeUpdateFunc)
	require.ErrorIs(t, err, sgbucket.ErrDeleteXattrOnTombstone)
}

func TestWriteTombstoneWithXattrs(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore(t.Context())

	type casOption uint32

	const (
		zeroCas casOption = iota
		incorrectCas
		previousCas
	)
	type testCase struct {
		name           string
		previousDoc    *sgbucket.BucketDocument
		deleteBody     bool
		finalBody      []byte
		finalXattrs    map[string][]byte
		updatedXattrs  map[string][]byte
		xattrsToDelete []string
		cas            casOption
		writeErrorFunc func(testing.TB, error)
	}
	tests := []testCase{
		// alive document with xattrs
		/* CBG-3918, should be a cas mismatch error
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:        zeroCas,
			deleteBody: true,
			writeErrorFunc: requireCasMismatchError,
		},
		*/
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=incorrect,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            incorrectCas,
			deleteBody:     true,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: true,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
		},
		/* CBG-3918, should be a cas mismatch error
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:        zeroCas,
			deleteBody: true,
			writeErrorFunc: requireCasMismatchError,
		},
		*/
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=incorrect,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            incorrectCas,
			deleteBody:     true,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: true,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
				"_xattr2": []byte(`{"c" : "d"}`),
			},
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=incorrect,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            incorrectCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			finalBody: []byte(`{"foo": "bar"}`),
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=incorrect,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            incorrectCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalBody:  []byte(`{"foo": "bar"}`),
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
				"_xattr2": []byte(`{"c" : "d"}`),
			},
		},
		// alive document without xattrs
		/* CBG-3918, should be a cas mismatch error
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:        zeroCas,
			deleteBody: true,
			writeErrorFunc: requireCasMismatchError,
		},
		*/
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=incorrect,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            incorrectCas,
			deleteBody:     true,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:        previousCas,
			deleteBody: true,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
		},
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=incorrect,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            incorrectCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalBody:  []byte(`{"foo": "bar"}`),
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
		},
		// tombstone
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=incorrect,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            incorrectCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            previousCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},

		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=incorrect,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            incorrectCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
		},

		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=incorrect,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            incorrectCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            previousCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},

		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=incorrect,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            incorrectCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
				"_xattr2": []byte(`{"c" : "d"}`),
			},
		},
		// nodoc
		{
			name: "previousDoc=nodoc,updatedXattrs=_xattr1,cas=0,deleteBody=true",
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            zeroCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},
		{
			name: "previousDoc=nodoc,updatedXattrs=_xattr1,cas=incorrect,deleteBody=true",
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            incorrectCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},
		{
			name: "previousDoc=nodoc,updatedXattrs=_xattr1,cas=0,deleteBody=false",
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:        zeroCas,
			deleteBody: false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
		},
		{
			name: "previousDoc=nodoc,updatedXattrs=_xattr1,cas=incorrect,deleteBody=false",
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            incorrectCas,
			writeErrorFunc: requireDocNotFoundError,
		},
		// alive document with multiple xattrs
		/* CBG-3918, should be a cas mismatch error
		{
			name: "previousDoc=body+_xattr1,xattrsToUpdate=_xattr1+_xattr2,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:        zeroCas,
			deleteBody: true,
			writeErrorFunc: requireCasMismatchError,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
		},
		*/
		{
			name: "previousDoc=body+_xattr1,xattrsToUpdate=_xattr1+_xattr2,cas=incorrect,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:            incorrectCas,
			deleteBody:     true,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,xattrsToUpdate=_xattr1+_xattr2,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:        previousCas,
			deleteBody: true,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
		},
		{
			name: "previousDoc=body+_xattr1+_xattr2,xattrsToUpdate=_xattr1+_xattr2,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1+_xattr2,xattrsToUpdate=_xattr1+_xattr2,cas=incorrect,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:            incorrectCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1+_xattr2,xattrsToUpdate=_xattr1+_xattr2,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalBody:  []byte(`{"foo": "bar"}`),
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
		},
		// alive document with no xattrs
		/* CBG-3918, should be a cas mismatch error
		{
			name: "previousDoc=body,xattrsToUpdate=_xattr1+_xattr2,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:        zeroCas,
			deleteBody: true,
			writeErrorFunc: requireCasMismatchError,
			},
		},
		*/
		{
			name: "previousDoc=body,xattrsToUpdate=_xattr1+_xattr2,cas=incorrect,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:            incorrectCas,
			deleteBody:     true,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body,xattrsToUpdate=_xattr1,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:        previousCas,
			deleteBody: true,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
		},
		{
			name: "previousDoc=body,xattrsToUpdate=_xattr1,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body,xattrsToUpdate=_xattr1,cas=incorrect,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:            incorrectCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body,xattrsToUpdate=_xattr1,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalBody:  []byte(`{"foo": "bar"}`),
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
		},
		// tombstone with multiple xattrs
		{
			name:        "previousDoc=nil,xattrsToUpdate=_xattr1+_xattr2,cas=0,deleteBody=true",
			previousDoc: nil,
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:            zeroCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},
		{
			name:        "previousDoc=nil,xattrsToUpdate=_xattr1+_xattr2,cas=incorrect,deleteBody=true",
			previousDoc: nil,
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:            incorrectCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},
		{
			name:        "previousDoc=nil,xattrsToUpdate=_xattr1,cas=0,deleteBody=false",
			previousDoc: nil,
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:        zeroCas,
			deleteBody: false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
		},
		{
			name:        "previousDoc=nil,xattrsToUpdate=_xattr1,cas=incorrect,deleteBody=false",
			previousDoc: nil,
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			cas:            incorrectCas,
			deleteBody:     false,
			writeErrorFunc: requireDocNotFoundError,
		},
		{
			name:        "previousDoc=nil,xattrsToUpdate=_xattr1,xattrsToDelete=_xattr2,cas=0,deleteBody=false",
			previousDoc: nil,
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			xattrsToDelete: []string{"_xattr2"},
			cas:            zeroCas,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
		},
		{
			name:        "previousDoc=nil,xattrsToUpdate=_xattr1,xattrsToDelete=_xattr2,cas=0,deleteBody=true",
			previousDoc: nil,
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			xattrsToDelete: []string{"_xattr2"},
			cas:            zeroCas,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundError,
		},
		{
			name: "previousDoc=body+_xattr1,_xattr2,xattrsToUpdate=_xattr1,xattrsToDelete=_xattr2,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			xattrsToDelete: []string{"_xattr2"},
			cas:            previousCas,
			deleteBody:     false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			finalBody: []byte(`{"foo": "bar"}`),
		},
		{
			name: "previousDoc=body+_xattr1,_xattr2,xattrsToUpdate=_xattr1,xattrsToDelete=_xattr2,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			xattrsToDelete: []string{"_xattr2"},
			cas:            previousCas,
			deleteBody:     true,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			var exp uint32
			docID := t.Name()
			cas := uint64(0)
			if test.cas == incorrectCas {
				cas = 1
			}
			if test.previousDoc != nil {
				var casOut uint64
				var err error
				if test.previousDoc.Body == nil {
					casOut, err = col.WriteTombstoneWithXattrs(ctx, docID, exp, 0, test.previousDoc.Xattrs, nil, false, nil)
				} else {
					casOut, err = col.WriteWithXattrs(ctx, docID, exp, 0, test.previousDoc.Body, test.previousDoc.Xattrs, nil, nil)
				}
				require.NoError(t, err)
				if test.cas == previousCas {
					cas = casOut
				}
			}
			_, err := col.WriteTombstoneWithXattrs(ctx, docID, exp, cas, test.updatedXattrs, nil, test.deleteBody, nil)
			if test.writeErrorFunc != nil {
				test.writeErrorFunc(t, err)
				if test.finalBody != nil {
					require.Fail(t, "finalBody should not be set when expecting an error")
				}
				return
			}
			require.NoError(t, err)

			xattrKeys := make([]string, 0, len(test.updatedXattrs))
			for k := range test.updatedXattrs {
				xattrKeys = append(xattrKeys, k)
			}
			if test.previousDoc != nil {
				for xattrKey := range test.previousDoc.Xattrs {
					_, ok := test.updatedXattrs[xattrKey]
					if !ok {
						xattrKeys = append(xattrKeys, xattrKey)
					}
				}
			}

			body, xattrs, _, err := col.GetWithXattrs(ctx, docID, xattrKeys)
			require.NoError(t, err)
			require.Equal(t, "", string(body))
			requireXattrsEqual(t, test.finalXattrs, xattrs)
		})
	}
}

func TestWriteUpdateWithXattrs(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore(t.Context())

	type testCase struct {
		name         string
		previousDoc  *sgbucket.BucketDocument
		updatedDoc   sgbucket.UpdatedDoc
		finalBody    []byte
		finalXattrs  map[string][]byte
		errorsIs     error
		errorFunc    func(testing.TB, error)
		getErrorFunc func(testing.TB, error)
	}
	tests := []testCase{
		// alive document with xattrs
		/* this fails in rosmar with a getError and doesn't write null doc body
		{
			name:      "previousDoc=nil,updatedDoc=nil",
			finalBody: []byte("null"),
		},
		*/
		{
			name: "previousDoc=body+_xattr1,updatedDoc=nil",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			errorsIs: sgbucket.ErrNeedXattrs,
		},
		{
			name: "previousDoc=null,updatedDoc=body",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`null`),
			},
			updatedDoc: sgbucket.UpdatedDoc{
				Doc: []byte(`{"foo": "bar"}`),
			},
			finalBody: []byte(`{"foo": "bar"}`),
		},

		/* This should fail, and does under rosmar but not CBS
		{
			name: "previousDoc=_xattr1,updatedDoc=nil",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
		},
		*/
		{
			name: "previousDoc=nil,updatedDoc=_xattr1",
			updatedDoc: sgbucket.UpdatedDoc{
				Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
				IsTombstone: true,
			},
			finalXattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
		},
		{
			name: "previousDoc=body+_xattr1,updatedDoc=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedDoc: sgbucket.UpdatedDoc{
				Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
				IsTombstone: true,
			},
			finalXattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
		},
		{
			name: "previousDoc=_xattr1,updatedDoc=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedDoc: sgbucket.UpdatedDoc{
				Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
				IsTombstone: true,
			},
			finalXattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
		},

		{
			name: "previousDoc=body+_xattr1,_xattr2,updatedDoc=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
					"_xattr2": []byte(`{"e" : "f"}`),
				},
			},
			updatedDoc: sgbucket.UpdatedDoc{
				Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
				IsTombstone: true,
			},
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
				"_xattr2": []byte(`{"e" : "f"}`),
			},
		},
		{
			name: "previousDoc=_xattr1,xattr2,updatedDoc=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
					"_xattr2": []byte(`{"e" : "f"}`),
				},
			},
			updatedDoc: sgbucket.UpdatedDoc{
				Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
				IsTombstone: true,
			},
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
				"_xattr2": []byte(`{"e" : "f"}`),
			},
		},
		{
			name: "previousDoc=_xattr1,xattr2,updatedDoc=tombstone+_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
					"_xattr2": []byte(`{"e" : "f"}`),
				},
			},
			updatedDoc: sgbucket.UpdatedDoc{
				IsTombstone: true,
				Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)}},
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
				"_xattr2": []byte(`{"e" : "f"}`),
			},
		},
		{
			name: "delete xattr on tombstone resurection",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"foo": "bar"}`),
				},
			},
			updatedDoc: sgbucket.UpdatedDoc{
				Doc:            []byte(`{"foo": "bar"}`),
				XattrsToDelete: []string{"xattr1"},
			},
			errorsIs: sgbucket.ErrDeleteXattrOnTombstone,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			docID := t.Name()
			if test.previousDoc != nil {
				var exp uint32
				var cas uint64
				var mutateInOptions *sgbucket.MutateInOptions
				if len(test.previousDoc.Body) == 0 {
					deleteBody := false
					var mutateInOptions *sgbucket.MutateInOptions
					_, err := col.WriteTombstoneWithXattrs(ctx, docID, exp, cas, test.previousDoc.Xattrs, nil, deleteBody, mutateInOptions)
					require.NoError(t, err)
				} else {
					_, err := col.WriteWithXattrs(ctx, docID, exp, cas, test.previousDoc.Body, test.previousDoc.Xattrs, nil, mutateInOptions)
					require.NoError(t, err)
				}
			}
			writeUpdateFunc := func(_ []byte, _ map[string][]byte, _ uint64) (sgbucket.UpdatedDoc, error) {
				return test.updatedDoc, nil
			}

			cas, err := col.WriteUpdateWithXattrs(ctx, docID, nil, 0, nil, nil, writeUpdateFunc)
			if test.errorsIs != nil {
				require.ErrorIs(t, err, test.errorsIs)
				require.Equal(t, uint64(0), cas)
				return
			} else if test.errorFunc != nil {
				test.errorFunc(t, err)
				require.Equal(t, uint64(0), cas)
				return
			}
			require.NoError(t, err)
			require.NotEqual(t, uint64(0), cas)

			// assemble names of any possible xattrs
			xattrNames := make([]string, 0, len(test.updatedDoc.Xattrs))
			for k := range test.updatedDoc.Xattrs {
				xattrNames = append(xattrNames, k)
			}
			if test.previousDoc != nil {
				for k := range test.previousDoc.Xattrs {
					if _, ok := test.updatedDoc.Xattrs[k]; !ok {
						xattrNames = append(xattrNames, k)
					}
				}
			}
			body, xattrs, _, err := col.GetWithXattrs(ctx, docID, xattrNames)
			if test.getErrorFunc != nil {
				test.getErrorFunc(t, err)
				return
			}
			require.NoError(t, err)
			if test.updatedDoc.IsTombstone {
				require.Equal(t, "", string(body))
			} else if len(test.finalBody) > 0 {
				require.JSONEq(t, string(test.finalBody), string(body))
			} else {
				require.Equal(t, string(test.finalBody), string(body))
			}
			requireXattrsEqual(t, test.finalXattrs, xattrs)
		})
	}
}

func TestWriteWithXattrs(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore(t.Context())

	type testCase struct {
		name           string
		body           []byte
		cas            uint64
		xattrs         map[string][]byte
		xattrsToDelete []string
		errorIs        error
		errorFunc      func(testing.TB, error)
	}

	tests := []testCase{
		{
			name: "body=true,xattrs=nil,xattrsToDelete=nil,cas=0",
			body: []byte(`{"foo": "bar"}`),
		},
		{
			name:      "body=true,xattrs=nil,xattrsToDelete=nil,cas=incorrect",
			body:      []byte(`{"foo": "bar"}`),
			cas:       uint64(1),
			errorFunc: requireCasMismatchError,
		},
		{
			name:           "body=true,xattrs=nil,xattrsToDelete=xattr1,cas=0",
			body:           []byte(`{"foo": "bar"}`),
			xattrsToDelete: []string{"xattr1"},
			errorIs:        sgbucket.ErrDeleteXattrOnDocumentInsert,
		},
		{
			name:           "body=true,xattrs=nil,xattrsToDelete=xattr1,cas=incorrect",
			body:           []byte(`{"foo": "bar"}`),
			xattrsToDelete: []string{"xattr1"},
			cas:            uint64(1),
			errorFunc:      requireCasMismatchError,
		},
		{
			name:           "body=true,xattrs=xattr1,xattrsToDelete=xattr1,cas=0",
			xattrs:         map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
			body:           []byte(`{"foo": "bar"}`),
			xattrsToDelete: []string{"xattr1"},
			errorIs:        sgbucket.ErrDeleteXattrOnDocumentInsert,
		},
		{
			name:           "body=true,xattrs=xattr1,xattrsToDelete=xattr1,cas=incorrect",
			body:           []byte(`{"foo": "bar"}`),
			xattrs:         map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
			xattrsToDelete: []string{"xattr1"},
			cas:            uint64(1),
			errorIs:        sgbucket.ErrUpsertAndDeleteSameXattr,
		},
		{
			name:   "body=true,xattrs=xattr1,xattrsToDelete=nil,cas=0",
			body:   []byte(`{"foo": "bar"}`),
			xattrs: map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
		},
		{
			name:      "body=true,xattrs=xattr1,xattrsToDelete=nil,cas=incorrect",
			body:      []byte(`{"foo": "bar"}`),
			xattrs:    map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
			cas:       uint64(1),
			errorFunc: requireCasMismatchError,
		},
		{
			name:    "xattr_nil_value",
			body:    []byte(`{"foo": "bar"}`),
			xattrs:  map[string][]byte{"xattr1": nil},
			errorIs: sgbucket.ErrNilXattrValue,
		},
		{
			name:           "body=true,xattrs=nil,xattrsToDelete=xattr1,xattr2,cas=0",
			body:           []byte(`{"foo": "bar"}`),
			xattrsToDelete: []string{"xattr1", "xattr2"},
			errorIs:        sgbucket.ErrDeleteXattrOnDocumentInsert,
		},
		{
			name:           "body=true,xattrs=nil,xattrsToDelete=xattr1,xattr2,cas=incorrect",
			body:           []byte(`{"foo": "bar"}`),
			xattrsToDelete: []string{"xattr1", "xattr2"},
			cas:            uint64(1),
			errorFunc:      requireCasMismatchError,
		},
		{
			name:           "body=true,xattrs=xattr1,xattrsToDelete=xattr1,xattr2,cas=0",
			body:           []byte(`{"foo": "bar"}`),
			xattrs:         map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
			xattrsToDelete: []string{"xattr1", "xattr2"},
			errorIs:        sgbucket.ErrDeleteXattrOnDocumentInsert,
		},

		{
			name:           "body=true,xattrs=xattr1,xattrsToDelete=xattr1,xattr2,cas=incorrect",
			body:           []byte(`{"foo": "bar"}`),
			xattrs:         map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
			xattrsToDelete: []string{"xattr1", "xattr2"},
			cas:            uint64(1),
			errorIs:        sgbucket.ErrUpsertAndDeleteSameXattr,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			if test.errorFunc != nil && test.errorIs != nil {
				require.FailNow(t, "test case should specify errFunc  xor errorIs")
			}
			docID := t.Name()
			cas, err := col.WriteWithXattrs(ctx, docID, 0, test.cas, test.body, test.xattrs, test.xattrsToDelete, nil)
			if test.errorFunc != nil {
				test.errorFunc(t, err)
				require.Equal(t, uint64(0), cas)
				return
			}
			require.ErrorIs(t, err, test.errorIs)
			if test.errorIs != nil {
				require.Equal(t, uint64(0), cas)
				return
			}
			require.NoError(t, err)
			require.NotEqual(t, uint64(0), cas)

			xattrKeys := make([]string, 0, len(test.xattrs))
			for k := range test.xattrs {
				xattrKeys = append(xattrKeys, k)
			}
			doc, xattrs, getCas, err := col.GetWithXattrs(ctx, docID, xattrKeys)
			require.NoError(t, err)
			require.Equal(t, cas, getCas)
			require.JSONEq(t, string(test.body), string(doc))
			require.Equal(t, len(test.xattrs), len(xattrs), "Length of output doesn't match xattrs=%+v doesn't match input xattrs=%+v", xattrs, test.xattrs)
			for k, v := range test.xattrs {
				require.Contains(t, xattrs, k)
				require.JSONEq(t, string(v), string(xattrs[k]))
			}
		})
	}
}

func TestWriteWithXattrsSetXattrNil(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)
	docID := t.Name()

	for _, cas := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("cas=%d", cas), func(t *testing.T) {
			_, err := col.WriteWithXattrs(ctx, docID, 0, cas, []byte(`{"foo": "bar"}`), map[string][]byte{"xattr1": nil}, nil, nil)
			require.ErrorIs(t, err, sgbucket.ErrNilXattrValue)
		})
	}
}

func TestWriteTombstoneWithXattrsSetXattrNil(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)
	docID := t.Name()

	for _, cas := range []uint64{0, 1} {
		for _, deleteBody := range []bool{false, true} {
			t.Run(fmt.Sprintf("cas=%d, deleteBody=%v", cas, deleteBody), func(t *testing.T) {
				_, err := col.WriteTombstoneWithXattrs(ctx, docID, 0, cas, map[string][]byte{"_xattr1": nil}, nil, deleteBody, nil)
				require.ErrorIs(t, err, sgbucket.ErrNilXattrValue)
			})
		}
	}
}

func TestWriteWithXattrsInsertAndDeleteError(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)
	docID := t.Name()

	_, err := col.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{"foo": "bar"}`), map[string][]byte{"xattr1": []byte(`{"foo": "bar"}`)}, []string{"xattr2"}, nil)
	require.ErrorIs(t, err, sgbucket.ErrDeleteXattrOnDocumentInsert)
}

// TestWriteWithXattrsInvalidPaths verifies that writeWithXattrs rejects unsupported or
// malformed xattr key paths before making any changes.
func TestWriteWithXattrsInvalidPaths(t *testing.T) {
	ctx := t.Context()
	col := makeTestBucket(t).DefaultDataStore(ctx)
	body := []byte(`{"foo": "bar"}`)

	invalidPaths := []string{
		"",           // empty key
		"$document",  // starts with $
		"xattr$key",  // $ in key
		"xattr[0]",   // [ in key
		"xattr]key",  // ] in key
		"_sync..rev", // empty component from consecutive dots
		"_sync.",     // trailing dot → empty component
		"._sync",     // leading dot → empty top-level key
		"_sync.$rev", // $ in sub-path
		"_sync.[0]",  // [ in sub-path
		"_sync.rev]", // ] in sub-path
	}

	for _, path := range invalidPaths {
		t.Run(fmt.Sprintf("path=%q", path), func(t *testing.T) {
			_, err := col.WriteWithXattrs(ctx, t.Name(), 0, 0, body,
				map[string][]byte{path: []byte(`{"a":"b"}`)}, nil, nil)
			require.Error(t, err, "expected error for xattr path %q", path)
		})
	}
}

// TestSetXattrsInvalidPaths verifies SetXattrs also rejects bad paths.
func TestSetXattrsInvalidPaths(t *testing.T) {
	ctx := t.Context()
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)
	require.NoError(t, coll.SetRaw(ctx, t.Name(), 0, nil, []byte(`{"a":1}`)))

	for _, path := range []string{"$key", "key[0]", "_sync..rev", "_sync."} {
		t.Run(fmt.Sprintf("path=%q", path), func(t *testing.T) {
			_, err := coll.SetXattrs(ctx, t.Name(), map[string][]byte{path: []byte(`{}`)})
			require.Error(t, err, "expected error for xattr path %q", path)
		})
	}
}

func requireXattrsEqual(t testing.TB, expected map[string][]byte, actual map[string][]byte) {
	require.Len(t, actual, len(expected), "Expected xattrs to be the same length %v, got %v", expected, actual)
	for k, v := range expected {
		actualV, ok := actual[k]
		if !ok {
			require.Fail(t, "Missing expected xattr %s", k)
		}
		require.JSONEq(t, string(v), string(actualV))
	}
}

func TestWriteResurrectionWithXattrs(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore(t.Context())

	type testCase struct {
		name          string
		previousDoc   *sgbucket.BucketDocument
		updatedXattrs map[string][]byte
		updatedBody   []byte
		errorIs       error
		errorFunc     func(testing.TB, error)
	}
	tests := []testCase{
		/* this writes literal null on Couchbase server and no document on rosmar
		{
			name:      "previousDoc=nil",
			finalBody: []byte("null"),
		},
		*/
		{
			name: "previousDoc=_xattr1,xattrsToUpdate=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
				IsTombstone: true,
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			errorIs: sgbucket.ErrNeedBody,
		},
		{
			name: "previousDoc=_xattr1,xattrsToUpdate=_xattr1,updatedBody=nil",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			errorIs: sgbucket.ErrNeedBody,
		},
		{
			name: "previousDoc=_xattr1,xattrsToUpdate=_xattr1,updatedBody=body",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			updatedBody: []byte(`{"foo": "bar"}`),
		},
		{
			name: "previousDoc=_xattr1,xattrsToUpdate=nil,updatedBody=body",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedBody: []byte(`{"foo": "bar"}`),
		},
		// if there is no doc, WriteResurrectionWithXattrs behaves like WriteWithXattrs with cas=0
		{
			name:        "previousDoc=nil,xattrsToUpdate=nil,updatedBody=body",
			previousDoc: nil,
			updatedBody: []byte(`{"foo": "bar"}`),
		},
		{
			name: "previousDoc=alive,xattrsToUpdate=_xattr1,updatedBody=body",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			updatedBody: []byte(`{"foo": "bar"}`),
			errorFunc:   requireDocFoundError,
		},
		{
			name: "previousDoc=_xattr1,xattrsToUpdate=_xattr1+_xattr2,updatedBody=body",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
				"_xattr2": []byte(`{"f": "g"}`),
			},
			updatedBody: []byte(`{"foo": "bar"}`),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			docID := t.Name()
			exp := uint32(0)
			if test.previousDoc != nil {
				if test.previousDoc.Body == nil {
					_, err := col.WriteTombstoneWithXattrs(ctx, docID, exp, 0, test.previousDoc.Xattrs, nil, false, nil)
					require.NoError(t, err)
				} else {
					_, err := col.WriteWithXattrs(ctx, docID, exp, 0, test.previousDoc.Body, test.previousDoc.Xattrs, nil, nil)
					require.NoError(t, err)
				}
			}
			_, err := col.WriteResurrectionWithXattrs(ctx, docID, exp, test.updatedBody, test.updatedXattrs, nil)
			if test.errorIs != nil {
				require.ErrorIs(t, err, test.errorIs)
				return
			} else if test.errorFunc != nil {
				test.errorFunc(t, err)
				return
			}
			require.NoError(t, err)

			xattrKeys := make([]string, 0, len(test.updatedXattrs))
			for k := range test.updatedXattrs {
				xattrKeys = append(xattrKeys, k)
			}
			if test.previousDoc != nil {
				for xattrKey := range test.previousDoc.Xattrs {
					_, ok := test.updatedXattrs[xattrKey]
					if !ok {
						xattrKeys = append(xattrKeys, xattrKey)
					}
				}
			}

			body, xattrs, _, err := col.GetWithXattrs(ctx, docID, xattrKeys)
			require.NoError(t, err)
			require.JSONEq(t, string(test.updatedBody), string(body))
			requireXattrsEqual(t, test.updatedXattrs, xattrs)
		})
	}
}

func TestUpdateXattrs(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore(t.Context())

	type testCase struct {
		name           string
		previousDoc    *sgbucket.BucketDocument
		updatedXattrs  map[string][]byte
		finalXattrs    map[string][]byte
		writeErrorFunc func(testing.TB, error)
	}
	tests := []testCase{
		/* passes on CBS but not rosmar
		{
			name: "previousDoc=nil,updatedXattrs=_xattr1",
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a": "b"}`),
			},
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a": "b"}`),
			},
			writeErrorFunc: requireDocNotFoundError,
		},
		*/
		{
			name: "previousDoc=body,updatedXattrs=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a": "b"}`),
			},
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a": "b"}`),
			},
		},
		{
			name: "previousDoc=body,_xattr1,updatedXattrs=_xattr2",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a": "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c": "d"}`),
			},
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a": "b"}`),
				"_xattr2": []byte(`{"c": "d"}`),
			},
		},
		{
			name: "previousDoc=tombstone,_xattr1,updatedXattrs=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a": "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
		},
		{
			name: "previousDoc=tombstone,_xattr1,updatedXattrs=_xattr2",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a": "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c": "d"}`),
			},
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a": "b"}`),
				"_xattr2": []byte(`{"c": "d"}`),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			var exp uint32
			docID := t.Name()
			cas := uint64(0)
			if test.previousDoc != nil {
				if test.previousDoc.Body == nil {
					var err error
					cas, err = col.WriteTombstoneWithXattrs(ctx, docID, exp, 0, test.previousDoc.Xattrs, nil, false, nil)
					require.NoError(t, err)
				} else {
					var err error
					cas, err = col.WriteWithXattrs(ctx, docID, exp, 0, test.previousDoc.Body, test.previousDoc.Xattrs, nil, nil)
					require.NoError(t, err)
				}
			}

			updatedCas, err := col.UpdateXattrs(ctx, docID, exp, cas, test.updatedXattrs, nil)
			if test.writeErrorFunc != nil {
				test.writeErrorFunc(t, err)
				return
			}
			require.NoError(t, err)
			require.NotEqual(t, cas, updatedCas)

			xattrKeys := make([]string, 0, len(test.updatedXattrs))
			for k := range test.updatedXattrs {
				xattrKeys = append(xattrKeys, k)
			}
			if test.previousDoc != nil {
				for xattrKey := range test.previousDoc.Xattrs {
					_, ok := test.updatedXattrs[xattrKey]
					if !ok {
						xattrKeys = append(xattrKeys, xattrKey)
					}
				}
			}
			body, xattrs, _, err := col.GetWithXattrs(ctx, docID, xattrKeys)
			require.NoError(t, err)
			if test.previousDoc.Body == nil {
				require.Nil(t, body)
			} else {
				require.JSONEq(t, string(test.previousDoc.Body), string(body))
			}
			requireXattrsEqual(t, test.finalXattrs, xattrs)

		})
	}
}

func requireCasMismatchError(t testing.TB, err error) {
	require.Error(t, err, "Expected an error of type IsCasMismatch %+v\n", err)
	var casMismatchErr sgbucket.CasMismatchErr
	require.ErrorAs(t, err, &casMismatchErr)
}

func requireDocNotFoundError(t testing.TB, err error) {
	var missingError sgbucket.MissingError
	require.ErrorAs(t, err, &missingError)
}

func requireDocFoundError(t testing.TB, err error) {
	require.ErrorIs(t, err, sgbucket.ErrKeyExists)
}

func TestSetHierarchicalPath(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	addToCollection(t, coll, "key", 0, "value")

	_, err := coll.SetXattrs(ctx, "key", map[string][]byte{"_sync.foo": []byte(`"bar"`)})
	require.NoError(t, err)

	xattrs, _, err := coll.GetXattrs(ctx, "key", []string{"_sync"})
	require.NoError(t, err)
	var syncXattr map[string]string
	require.NoError(t, json.Unmarshal(xattrs["_sync"], &syncXattr))
	require.Equal(t, map[string]string{"foo": "bar"}, syncXattr)
}

func TestDeleteSubDocPaths(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	// Dotted path: delete a field within an xattr.
	const docID = "doc1"
	_, err := coll.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{"val":"subdoc"}`),
		map[string][]byte{syncXattrName: []byte(`{"rev":"1-a","other":"keep"}`)}, nil, nil)
	require.NoError(t, err)

	err = coll.DeleteSubDocPaths(ctx, docID, syncXattrName+".rev")
	require.NoError(t, err)

	xvMap, _, err := coll.GetXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(xvMap[syncXattrName], &got))
	assert.NotContains(t, got, "rev")
	assert.Equal(t, "keep", got["other"])

	// Simple path: delete an entire xattr.
	const docID2 = "doc2"
	_, err = coll.WriteWithXattrs(ctx, docID2, 0, 0, []byte(`{"val":"simple"}`),
		map[string][]byte{syncXattrName: []byte(`{"rev":"1-a"}`)}, nil, nil)
	require.NoError(t, err)

	err = coll.DeleteSubDocPaths(ctx, docID2, syncXattrName)
	require.NoError(t, err)

	_, _, err = coll.GetXattrs(ctx, docID2, []string{syncXattrName})
	require.Error(t, err)
	var missingErr sgbucket.XattrMissingError
	assert.ErrorAs(t, err, &missingErr)
}

// TestDeleteSubDocPathsEmptyComponents verifies that paths with empty components
// (trailing dot, consecutive dots) are rejected rather than silently misbehaving.
func TestDeleteSubDocPathsEmptyComponents(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const docID = "doc1"
	_, err := coll.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{"val":1}`),
		map[string][]byte{syncXattrName: []byte(`{"rev":"1-a"}`)}, nil, nil)
	require.NoError(t, err)

	for _, path := range []string{
		syncXattrName + ".",     // trailing dot → empty field component
		syncXattrName + "..rev", // consecutive dots → empty intermediate component
		syncXattrName + ".rev.", // trailing dot after field name
	} {
		t.Run(path, func(t *testing.T) {
			err := coll.DeleteSubDocPaths(ctx, docID, path)
			require.Error(t, err, "expected error for path %q", path)
		})
	}

	// Document should be untouched.
	xvMap, _, err := coll.GetXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(xvMap[syncXattrName], &got))
	assert.Equal(t, "1-a", got["rev"])
}

// TestSetXattrsNestedPath verifies that SetXattrs with a dotted path creates intermediate
// maps when they don't exist, matching Couchbase Server subdoc upsert behavior.
// This is the behavior exercised by attachment compaction mark phase, which stamps a nested
// path like "_sync-compact.compactID.<runID>" on attachment docs that may not yet have
// the xattr at all.
func TestSetXattrsNestedPath(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const (
		docID        = "att-doc"
		xattrKey     = "_sync-compact"
		compactIDKey = "compactID"
		runID        = "run1"
	)
	nestedPath := xattrKey + "." + compactIDKey + "." + runID

	// Create a raw doc with no xattrs, like a legacy attachment document.
	require.NoError(t, coll.SetRaw(ctx, docID, 0, nil, []byte(`{}`)))

	// SetXattrs with a three-level dot path on a doc that has no xattrs yet should
	// create all intermediate maps.
	_, err := coll.SetXattrs(ctx, docID, map[string][]byte{
		nestedPath: []byte(`"1234567890"`),
	})
	require.NoError(t, err)

	// The top-level xattr should now contain the nested structure.
	xattrs, _, err := coll.GetXattrs(ctx, docID, []string{xattrKey})
	require.NoError(t, err)
	require.Contains(t, xattrs, xattrKey)

	var xattrValue map[string]any
	require.NoError(t, json.Unmarshal(xattrs[xattrKey], &xattrValue))
	compactID, ok := xattrValue[compactIDKey]
	require.True(t, ok, "expected %q key in xattr", compactIDKey)
	runMap, ok := compactID.(map[string]any)
	require.True(t, ok)
	assert.Contains(t, runMap, runID)

	// A second SetXattrs call with a different runID should add to the existing map
	// without overwriting the first entry.
	const runID2 = "run2"
	nestedPath2 := xattrKey + "." + compactIDKey + "." + runID2
	_, err = coll.SetXattrs(ctx, docID, map[string][]byte{
		nestedPath2: []byte(`"9999999999"`),
	})
	require.NoError(t, err)

	xattrs, _, err = coll.GetXattrs(ctx, docID, []string{xattrKey})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(xattrs[xattrKey], &xattrValue))
	compactID, ok = xattrValue[compactIDKey]
	require.True(t, ok)
	runMap, ok = compactID.(map[string]any)
	require.True(t, ok)
	assert.Contains(t, runMap, runID, "first run entry should still be present")
	assert.Contains(t, runMap, runID2, "second run entry should be present")
}

// TestDeleteSubDocPathsDeepNested verifies that DeleteSubDocPaths supports three-level
// dotted paths (xattrName.fieldName.subFieldName), as used by attachment compaction cleanup.
func TestDeleteSubDocPathsDeepNested(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const (
		docID        = "att-doc"
		xattrKey     = "_sync-compact"
		compactIDKey = "compactID"
		run1         = "run1"
		run2         = "run2"
	)

	// Set up a document with a three-level nested xattr structure:
	// _sync-compact = {"compactID": {"run1": 111, "run2": 222}}
	_, err := coll.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{}`),
		map[string][]byte{
			xattrKey: []byte(`{"` + compactIDKey + `":{"` + run1 + `":111,"` + run2 + `":222}}`),
		}, nil, nil)
	require.NoError(t, err)

	// Delete just run1 via a three-level path.
	err = coll.DeleteSubDocPaths(ctx, docID, xattrKey+"."+compactIDKey+"."+run1)
	require.NoError(t, err)

	xvMap, _, err := coll.GetXattrs(ctx, docID, []string{xattrKey})
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(xvMap[xattrKey], &got))
	compactIDs, ok := got[compactIDKey].(map[string]any)
	require.True(t, ok, "compactID field should be a map")
	assert.NotContains(t, compactIDs, run1, "run1 should have been deleted")
	assert.Contains(t, compactIDs, run2, "run2 should still be present")

	// Delete run2; compactID map is now empty but xattr still exists.
	err = coll.DeleteSubDocPaths(ctx, docID, xattrKey+"."+compactIDKey+"."+run2)
	require.NoError(t, err)

	xvMap, _, err = coll.GetXattrs(ctx, docID, []string{xattrKey})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(xvMap[xattrKey], &got))
	compactIDs, ok = got[compactIDKey].(map[string]any)
	require.True(t, ok, "compactID field should still be an (empty) map")
	assert.Empty(t, compactIDs)
}

// TestGetXattrsSubPath verifies that GetXattrs supports dotted sub-path keys such as
// "_sync-compact.compactID", returning the value at that nested location keyed by the
// full dotted path — matching Couchbase Server subdoc read behaviour.
func TestGetXattrsSubPath(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const (
		docID        = "att-doc"
		xattrKey     = "_sync-compact"
		compactIDKey = "compactID"
		run1         = "run1"
	)
	subPath := xattrKey + "." + compactIDKey

	// Create a document with _sync-compact = {"compactID": {"run1": 111}}
	require.NoError(t, coll.SetRaw(ctx, docID, 0, nil, []byte(`{}`)))
	_, err := coll.SetXattrs(ctx, docID, map[string][]byte{
		xattrKey + "." + compactIDKey + "." + run1: []byte(`111`),
	})
	require.NoError(t, err)

	// Read the sub-path _sync-compact.compactID — should return {"run1": 111}.
	xvMap, _, err := coll.GetXattrs(ctx, docID, []string{subPath})
	require.NoError(t, err)
	require.Contains(t, xvMap, subPath)
	var compactIDs map[string]any
	require.NoError(t, json.Unmarshal(xvMap[subPath], &compactIDs))
	assert.Equal(t, float64(111), compactIDs[run1])

	// After removing the whole xattr, GetXattrs on the sub-path should return XattrMissingError.
	_, cas, err := coll.GetXattrs(ctx, docID, []string{xattrKey})
	require.NoError(t, err)
	require.NoError(t, coll.RemoveXattrs(ctx, docID, []string{xattrKey}, cas))
	_, _, err = coll.GetXattrs(ctx, docID, []string{subPath})
	require.Error(t, err)
	var missingErr sgbucket.XattrMissingError
	assert.ErrorAs(t, err, &missingErr)
}

// TestGetXattrsSubPathErrors verifies that getRawWithXattrs returns real errors for corrupt
// xattr JSON and for path mismatches, rather than silently returning XattrMissingError.
func TestGetXattrsSubPathErrors(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const (
		docID    = "doc"
		xattrKey = "_sync"
	)

	// Write a doc with a valid xattr so we have a document to corrupt.
	require.NoError(t, coll.SetRaw(ctx, docID, 0, nil, []byte(`{}`)))
	_, err := coll.SetXattrs(ctx, docID, map[string][]byte{
		xattrKey: []byte(`{"seq":1}`),
	})
	require.NoError(t, err)

	t.Run("corrupt parent xattr JSON returns error", func(t *testing.T) {
		_, dbErr := coll.db().Exec(
			`UPDATE documents SET xattrs=? WHERE collection=? AND key=?`,
			[]byte(`{"`+xattrKey+`": not-json}`), coll.id, docID)
		require.NoError(t, dbErr)

		_, _, getErr := coll.GetXattrs(ctx, docID, []string{xattrKey + ".seq"})
		require.Error(t, getErr, "expected error for corrupt parent xattr JSON")
		// Must NOT silently become XattrMissingError.
		var corruptMissingErr sgbucket.XattrMissingError
		assert.False(t, errors.As(getErr, &corruptMissingErr), "corrupt JSON should not produce XattrMissingError, got: %v", getErr)
	})

	// Restore a valid xattr for the remaining subtests.
	_, err = coll.db().Exec(
		`UPDATE documents SET xattrs=? WHERE collection=? AND key=?`,
		[]byte(`{"`+xattrKey+`":{"seq":1}}`), coll.id, docID)
	require.NoError(t, err)

	t.Run("missing sub-key returns XattrMissingError", func(t *testing.T) {
		// ErrPathNotFound is expected and should surface as XattrMissingError.
		_, _, getErr := coll.GetXattrs(ctx, docID, []string{xattrKey + ".nonexistent"})
		require.Error(t, getErr)
		var notFoundMissingErr sgbucket.XattrMissingError
		assert.ErrorAs(t, getErr, &notFoundMissingErr)
	})

	t.Run("path mismatch returns error", func(t *testing.T) {
		// _sync.seq is a number, not an object; asking for _sync.seq.child is ErrPathMismatch.
		_, _, getErr := coll.GetXattrs(ctx, docID, []string{xattrKey + ".seq.child"})
		require.Error(t, getErr, "expected error for path mismatch")
		var mismatchMissingErr sgbucket.XattrMissingError
		assert.False(t, errors.As(getErr, &mismatchMissingErr), "path mismatch should not produce XattrMissingError, got: %v", getErr)
	})
}

// TestDeleteSubDocPathsFeedEvent verifies that the DCP feed event emitted by DeleteSubDocPaths
// carries the correct Expiry and Opcode — a regression test for the bug where exp and tombstone
// were not read from the database so events always had Expiry=0 and Opcode=FeedOpMutation.
func TestDeleteSubDocPathsFeedEvent(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	bucket := makeTestBucket(t)
	coll := bucket.DefaultDataStore(ctx).(*Collection)

	const (
		aliveDocID     = "alive-doc"
		tombstoneDocID = "tombstone-doc"
		xattrKey       = syncXattrName
		exp            = uint32(0x70000000) // far-future absolute expiry
	)
	xattrBody := []byte(`{"rev":"1-a","extra":"keep"}`)

	// Create an alive doc with a non-zero expiry.
	_, err := coll.WriteWithXattrs(ctx, aliveDocID, exp, 0, []byte(`{"val":1}`),
		map[string][]byte{xattrKey: xattrBody}, nil, nil)
	require.NoError(t, err)

	// Create a tombstone with the same xattr.
	_, err = coll.WriteTombstoneWithXattrs(ctx, tombstoneDocID, exp, 0,
		map[string][]byte{xattrKey: xattrBody}, nil, false, nil)
	require.NoError(t, err)

	events, _ := startFeed(t, bucket)

	// DeleteSubDocPaths on the alive doc — event must be FeedOpMutation with the correct expiry.
	err = coll.DeleteSubDocPaths(ctx, aliveDocID, xattrKey+".rev")
	require.NoError(t, err)
	e := <-events
	assert.Equal(t, sgbucket.FeedOpMutation, e.Opcode, "alive doc should emit FeedOpMutation")
	assert.Equal(t, uint32(exp), e.Expiry, "alive doc expiry should be preserved in feed event")

	// DeleteSubDocPaths on the tombstone — event must be FeedOpDeletion.
	err = coll.DeleteSubDocPaths(ctx, tombstoneDocID, xattrKey+".rev")
	require.NoError(t, err)
	e = <-events
	assert.Equal(t, sgbucket.FeedOpDeletion, e.Opcode, "tombstone doc should emit FeedOpDeletion")
}

// TestRemoveXattrsDeleteSubPath verifies that RemoveXattrs only removes the targeted nested field
// when given a dotted sub-path, rather than deleting the entire top-level xattr — a regression
// test for a bug where the top-level xattr key was always deleted regardless of any sub-path
// suffix.
func TestRemoveXattrsDeleteSubPath(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const docID = "doc"
	_, err := coll.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{"val":1}`),
		map[string][]byte{syncXattrName: []byte(`{"rev":"1-a","other":"keep"}`)}, nil, nil)
	require.NoError(t, err)

	// Deleting the dotted sub-path should remove only that field...
	_, cas, err := coll.GetXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)
	require.NoError(t, coll.RemoveXattrs(ctx, docID, []string{syncXattrName + ".rev"}, cas))

	xvMap, _, err := coll.GetXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(xvMap[syncXattrName], &got))
	assert.NotContains(t, got, "rev", "sub-path delete should remove only the targeted field")
	assert.Equal(t, "keep", got["other"], "sub-path delete must not remove sibling fields")

	// ...while deleting the plain xattr name still removes the whole xattr.
	_, cas, err = coll.GetXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)
	require.NoError(t, coll.RemoveXattrs(ctx, docID, []string{syncXattrName}, cas))
	_, _, err = coll.GetXattrs(ctx, docID, []string{syncXattrName})
	require.Error(t, err)
	var missingErr sgbucket.XattrMissingError
	assert.ErrorAs(t, err, &missingErr)
}

// TestValidateXattrPathRejectsBackticks verifies that xattr paths containing backticks or
// backslashes are rejected rather than silently mishandled, since rosmar does not implement
// Couchbase Server's backtick escaping of sub-path components.
func TestValidateXattrPathRejectsBackticks(t *testing.T) {
	ctx := t.Context()
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)
	require.NoError(t, coll.SetRaw(ctx, t.Name(), 0, nil, []byte(`{"a":1}`)))

	for _, path := range []string{"_sync.`a.b`", "_sync.a\\.b", "`_sync`.rev"} {
		t.Run(path, func(t *testing.T) {
			_, err := coll.SetXattrs(ctx, t.Name(), map[string][]byte{path: []byte(`"v"`)})
			require.Error(t, err, "expected error for xattr path %q", path)
		})
	}
}

// TestGetXattrsSubPathNullValue is a regression test verifying that GetXattrs on a dotted path
// pointing at an explicit JSON null leaf returns the null value, rather than conflating "value is
// null" with "path not found" and returning XattrMissingError.
func TestGetXattrsSubPathNullValue(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const docID = "doc"
	_, err := coll.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{"val":1}`),
		map[string][]byte{syncXattrName: []byte(`{"rev":null,"other":"keep"}`)}, nil, nil)
	require.NoError(t, err)

	xvMap, _, err := coll.GetXattrs(ctx, docID, []string{syncXattrName + ".rev"})
	require.NoError(t, err, "an explicit null xattr field should be returned, not treated as missing")
	require.Contains(t, xvMap, syncXattrName+".rev")
	assert.JSONEq(t, "null", string(xvMap[syncXattrName+".rev"]))
}

// TestWriteWithXattrsDeleteSubPathMissingLeaf is a regression test verifying that deleting a
// dotted xattr sub-path via WriteWithXattrs/RemoveXattrs reports sgbucket.ErrPathNotFound when
// the leaf field doesn't exist, matching the behavior of deleting a whole (non-dotted) xattr name
// that doesn't exist, instead of silently succeeding.
func TestWriteWithXattrsDeleteSubPathMissingLeaf(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const docID = "doc"
	_, err := coll.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{"val":1}`),
		map[string][]byte{syncXattrName: []byte(`{"other":"keep"}`)}, nil, nil)
	require.NoError(t, err)

	_, cas, err := coll.GetXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)

	err = coll.RemoveXattrs(ctx, docID, []string{syncXattrName + ".nonexistent"}, cas)
	require.ErrorIs(t, err, sgbucket.ErrPathNotFound)

	// Sibling field must be untouched.
	xvMap, _, err := coll.GetXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(xvMap[syncXattrName], &got))
	assert.Equal(t, "keep", got["other"])
}

// TestMacroExpansionMissingIntermediatePath is a regression test verifying that macro expansion
// targeting a multi-level path whose intermediate object doesn't already exist in the xattr
// fails loudly (ErrPathNotFound) instead of silently fabricating the missing intermediate object,
// which would produce an unexpected nested xattr shape.
func TestMacroExpansionMissingIntermediatePath(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	opts := &sgbucket.MutateInOptions{
		MacroExpansion: []sgbucket.MacroExpansionSpec{
			{Path: "_xattr1.meta.testcas", Type: sgbucket.MacroCas},
		},
	}
	// "_xattr1" exists but has no "meta" sub-object, so the macro path's intermediate
	// component is missing.
	xattrsInput := map[string][]byte{"_xattr1": []byte(`{"x":"abc"}`)}
	_, err := coll.WriteWithXattrs(ctx, t.Name(), 0, 0, []byte(`{"a":123}`), xattrsInput, nil, opts)
	require.ErrorIs(t, err, sgbucket.ErrPathNotFound)
}

// TestWriteWithXattrsPreserveXattrHierarchicalPath is a regression test verifying that the
// preserveXattr option keeps the existing xattr value (only applying macro expansion) for a
// hierarchical (dotted) xattr path, rather than silently discarding the preserved value and
// applying the incoming payload's value instead.
func TestWriteWithXattrsPreserveXattrHierarchicalPath(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	const docID = "doc"
	_, err := coll.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{"foo":"bar"}`),
		map[string][]byte{syncXattrName: []byte(`{"nested":{"keep":"me"}}`)}, nil, nil)
	require.NoError(t, err)
	_, _, cas, err := coll.GetWithXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)

	xattrs := map[string]payload{
		syncXattrName + ".nested.keep": {marshaled: []byte(`"clobber"`)},
	}
	_, err = coll.writeWithXattrs(docID, nil, xattrs, &cas, nil, writeXattrOptions{preserveXattr: true}, nil)
	require.NoError(t, err)

	_, xv, _, err := coll.GetWithXattrs(ctx, docID, []string{syncXattrName})
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(xv[syncXattrName], &got))
	nested, ok := got["nested"].(map[string]any)
	require.True(t, ok, "nested field should still be a map")
	assert.Equal(t, "me", nested["keep"], "preserveXattr should keep the existing value, not the incoming payload's value")
}
