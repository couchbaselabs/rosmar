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
	casOut, err := coll.WriteWithXattrs(ctx, "key", 0, 0, bodyBytes, xattrsInput, opts)
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

	// Unsuccessful - target unknown xattr
	opts.MacroExpansion = []sgbucket.MacroExpansionSpec{
		{Path: "_unknown.testcas", Type: sgbucket.MacroCas},
	}
	_, err = coll.WriteWithXattrs(ctx, "xattrMismatch", 0, 0, bodyBytes, xattrsInput, opts)
	require.Error(t, err)

	opts.MacroExpansion = []sgbucket.MacroExpansionSpec{
		{Path: "_sync.unknownPath.testcas", Type: sgbucket.MacroCas},
	}
	_, err = coll.WriteWithXattrs(ctx, "pathError", 0, 0, bodyBytes, xattrsInput, opts)
	require.Error(t, err)
}
