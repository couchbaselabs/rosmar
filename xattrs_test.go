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
