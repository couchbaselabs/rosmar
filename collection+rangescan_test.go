// Copyright 2025-Present Couchbase, Inc.
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

func TestRangeScan(t *testing.T) {
	ensureNoLeaks(t)
	ctx := t.Context()
	coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)

	docs := map[string]string{
		"doc_a": `{"name":"alpha"}`,
		"doc_b": `{"name":"bravo"}`,
		"doc_c": `{"name":"charlie"}`,
		"doc_d": `{"name":"delta"}`,
		"doc_e": `{"name":"echo"}`,
	}
	for k, v := range docs {
		require.NoError(t, coll.SetRaw(ctx, k, 0, nil, []byte(v)))
	}

	collectIDs := func(t *testing.T, iter sgbucket.ScanResultIterator, idsOnly bool) []string {
		t.Helper()
		defer func() { assert.NoError(t, iter.Close(ctx)) }()
		var ids []string
		for item := iter.Next(ctx); item != nil; item = iter.Next(ctx) {
			ids = append(ids, item.ID)
			assert.NotZero(t, item.Cas)
			if idsOnly {
				assert.Nil(t, item.Body)
			} else {
				assert.NotNil(t, item.Body)
			}
		}
		return ids
	}

	t.Run("FullRange", func(t *testing.T) {
		iter, err := coll.Scan(ctx, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{})
		require.NoError(t, err)
		require.Equal(t, []string{"doc_a", "doc_b", "doc_c", "doc_d", "doc_e"}, collectIDs(t, iter, false))
	})

	t.Run("PartialRange", func(t *testing.T) {
		scan := sgbucket.RangeScan{
			From: &sgbucket.ScanTerm{Term: "doc_b"},
			To:   &sgbucket.ScanTerm{Term: "doc_d", Exclusive: true},
		}
		iter, err := coll.Scan(ctx, scan, sgbucket.ScanOptions{})
		require.NoError(t, err)
		require.Equal(t, []string{"doc_b", "doc_c"}, collectIDs(t, iter, false))
	})

	t.Run("ExclusiveFrom", func(t *testing.T) {
		scan := sgbucket.RangeScan{
			From: &sgbucket.ScanTerm{Term: "doc_a", Exclusive: true},
			To:   &sgbucket.ScanTerm{Term: "doc_c"},
		}
		iter, err := coll.Scan(ctx, scan, sgbucket.ScanOptions{})
		require.NoError(t, err)
		require.Equal(t, []string{"doc_b", "doc_c"}, collectIDs(t, iter, false))
	})

	t.Run("IDsOnly", func(t *testing.T) {
		iter, err := coll.Scan(ctx, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{IDsOnly: true})
		require.NoError(t, err)
		require.Equal(t, []string{"doc_a", "doc_b", "doc_c", "doc_d", "doc_e"}, collectIDs(t, iter, true))
	})

	t.Run("EmptyRange", func(t *testing.T) {
		iter, err := coll.Scan(ctx, sgbucket.NewRangeScanForPrefix("zzz_nonexistent_"), sgbucket.ScanOptions{})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close(ctx)) }()
		assert.Nil(t, iter.Next(ctx))
	})

	t.Run("PrefixScan", func(t *testing.T) {
		iter, err := coll.Scan(ctx, sgbucket.NewRangeScanForPrefix("doc_c"), sgbucket.ScanOptions{})
		require.NoError(t, err)
		require.Equal(t, []string{"doc_c"}, collectIDs(t, iter, false))
	})

	t.Run("TombstonesExcluded", func(t *testing.T) {
		require.NoError(t, coll.Delete(ctx, "doc_b"))

		iter, err := coll.Scan(ctx, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{})
		require.NoError(t, err)
		assert.Equal(t, []string{"doc_a", "doc_c", "doc_d", "doc_e"}, collectIDs(t, iter, false))
	})

	t.Run("NoBounds", func(t *testing.T) {
		iter, err := coll.Scan(ctx, sgbucket.RangeScan{}, sgbucket.ScanOptions{IDsOnly: true})
		require.NoError(t, err)
		// doc_b was deleted above
		assert.Equal(t, []string{"doc_a", "doc_c", "doc_d", "doc_e"}, collectIDs(t, iter, true))
	})

	t.Run("UnsupportedScanType", func(t *testing.T) {
		_, err := coll.Scan(ctx, nil, sgbucket.ScanOptions{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported scan type")
	})
}
