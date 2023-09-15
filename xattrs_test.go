// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetXattr(t *testing.T) {
	ctx := testCtx(t)
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore()

	addToCollection(t, coll, "key", 0, "value")

	cas, err := coll.SetXattr(ctx, "key", "xfiles", []byte(`{"truth":"out_there"}`))
	require.NoError(t, err)

	var val, xval any
	gotCas, err := coll.GetWithXattr(ctx, "key", "xfiles", "", &val, &xval, nil)
	require.NoError(t, err)
	assert.Equal(t, cas, gotCas)
	assert.Equal(t, "value", val)
	assert.Equal(t, map[string]any{"truth": "out_there"}, xval)
}

func TestMacroExpansion(t *testing.T) {
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore()

	// Successful case - sets cas and crc32c in the _sync xattr
	opts := &sgbucket.MutateInOptions{}
	opts.MacroExpansion = []sgbucket.MacroExpansionSpec{
		{Path: "_sync.testcas", Type: sgbucket.MacroCas},
		{Path: "_sync.testcrc32c", Type: sgbucket.MacroCrc32c},
	}
	bodyBytes := []byte(`{"a":123}`)
	xattrBytes := []byte(`{"x":456}`)

	casOut, err := coll.WriteWithXattr("key", "_sync", 0, 0, opts, bodyBytes, xattrBytes, false, false)
	require.NoError(t, err)

	var val, xval map[string]any
	getCas, err := coll.GetWithXattr("key", "_sync", "", &val, &xval, nil)
	require.NoError(t, err)
	require.Equal(t, getCas, casOut)

	casVal, ok := xval["testcas"]
	require.True(t, ok)
	require.Equal(t, casAsString(casOut), casVal)

	_, ok = xval["testcrc32c"]
	require.True(t, ok)

	// Unsuccessful - target unknown xattr
	opts.MacroExpansion = []sgbucket.MacroExpansionSpec{
		{Path: "_unknown.testcas", Type: sgbucket.MacroCas},
	}
	_, err = coll.WriteWithXattr("xattrMismatch", "_sync", 0, 0, opts, bodyBytes, xattrBytes, false, false)
	require.Error(t, err)

	opts.MacroExpansion = []sgbucket.MacroExpansionSpec{
		{Path: "_sync.unknownPath.testcas", Type: sgbucket.MacroCas},
	}
	_, err = coll.WriteWithXattr("pathError", "_sync", 0, 0, opts, bodyBytes, xattrBytes, false, false)
	require.Error(t, err)
}
