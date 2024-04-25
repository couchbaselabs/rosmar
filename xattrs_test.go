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
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetXattrs(t *testing.T) {
	ctx := testCtx(t)
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore().(*Collection)

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
	ctx := testCtx(t)
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore().(*Collection)

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
	ctx := testCtx(t)
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore().(*Collection)

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
	col := makeTestBucket(t).DefaultDataStore()
	docID := t.Name()

	ctx := testCtx(t)
	fakeCas := uint64(1)
	_, err := col.WriteWithXattrs(ctx, docID, 0, fakeCas, []byte(`{"foo": "bar"}`), map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)}, []string{"xattr1"}, nil)
	require.ErrorIs(t, err, sgbucket.ErrUpsertAndDeleteSameXattr)
}

func TestWriteWithXattrsSetXattrNil(t *testing.T) {
	col := makeTestBucket(t).DefaultDataStore()
	docID := t.Name()

	ctx := testCtx(t)
	for _, fakeCas := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("cas=%d", fakeCas), func(t *testing.T) {

			_, err := col.WriteWithXattrs(ctx, docID, 0, fakeCas, []byte(`{"foo": "bar"}`), map[string][]byte{"xattr1": nil}, nil, nil)
			require.ErrorIs(t, err, sgbucket.ErrNilXattrValue)
		})
	}
}

// TestXattrWriteUpdateXattr.  Validates basic write of document with xattr, and retrieval of the same doc w/ xattr.
func TestWriteUpdateWithXattrs(t *testing.T) {
	ctx := testCtx(t)
	col := makeTestBucket(t).DefaultDataStore()

	key := t.Name()
	xattr1 := "xattr1"
	xattr2 := "xattr2"
	xattr3 := "xattr3"
	xattrNames := []string{xattr1, xattr2, xattr3}
	body := `{"counter": 1}`

	xattrsToModify := xattrNames
	var xattrsToDelete []string
	// Dummy write update function that increments 'counter' in the doc and 'seq' in the xattr
	writeUpdateFunc := func(doc []byte, xattrs map[string][]byte, cas uint64) (sgbucket.UpdatedDoc, error) {
		var docMap map[string]float64
		if doc == nil {
			docMap = map[string]float64{"counter": 1}
		} else {
			require.NoError(t, json.Unmarshal(doc, &docMap))
			docMap["counter"]++
		}
		updatedDoc := sgbucket.UpdatedDoc{
			Doc:            mustMarshalJSON(t, docMap),
			Xattrs:         make(map[string][]byte),
			XattrsToDelete: xattrsToDelete,
		}
		for _, xattrName := range xattrsToModify {
			xattr := xattrs[xattrName]
			var xattrMap map[string]float64
			if xattr == nil {
				xattrMap = map[string]float64{"seq": 1}
			} else {
				require.NoError(t, json.Unmarshal(xattr, &xattrMap))
				xattrMap["seq"]++
			}
			updatedDoc.Xattrs[xattrName] = mustMarshalJSON(t, xattrMap)
		}
		return updatedDoc, nil
	}

	// Insert
	_, err := col.WriteUpdateWithXattrs(ctx, key, xattrNames, 0, nil, nil, writeUpdateFunc)
	require.NoError(t, err)

	rawVal, xattrs, _, err := col.GetWithXattrs(ctx, key, xattrNames)
	require.NoError(t, err)

	require.JSONEq(t, body, string(rawVal))
	for _, xattrName := range xattrNames {
		require.Contains(t, xattrs, xattrName)
		var xattr map[string]float64
		require.NoError(t, json.Unmarshal(xattrs[xattrName], &xattr))
		assert.Equal(t, float64(1), xattr["seq"])
	}

	// Update
	xattrsToModify = []string{xattr1, xattr2}
	xattrsToDelete = []string{xattr3}
	_, err = col.WriteUpdateWithXattrs(ctx, key, xattrNames, 0, nil, nil, writeUpdateFunc)
	require.NoError(t, err)

	rawVal, xattrs, _, err = col.GetWithXattrs(ctx, key, xattrNames)
	require.NoError(t, err)

	require.JSONEq(t, `{"counter": 2}`, string(rawVal))
	for _, xattrName := range []string{xattr1, xattr2} {
		require.Contains(t, xattrs, xattrName)
		var xattr map[string]float64
		require.NoError(t, json.Unmarshal(xattrs[xattrName], &xattr))
		assert.Equal(t, float64(2), xattr["seq"])
	}
	require.NotContains(t, xattrs, xattr3)
}

func TestWriteUpdateDeleteXattrTombstone(t *testing.T) {
	ctx := testCtx(t)
	col := makeTestBucket(t).DefaultDataStore()

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
