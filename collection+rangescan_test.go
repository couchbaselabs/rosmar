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
	coll := makeTestBucket(t).DefaultDataStore().(*Collection)

	// Seed test documents
	docs := map[string]string{
		"doc_a": `{"name":"alpha"}`,
		"doc_b": `{"name":"bravo"}`,
		"doc_c": `{"name":"charlie"}`,
		"doc_d": `{"name":"delta"}`,
		"doc_e": `{"name":"echo"}`,
	}
	for k, v := range docs {
		require.NoError(t, coll.SetRaw(k, 0, nil, []byte(v)))
	}

	t.Run("FullRange", func(t *testing.T) {
		scan := sgbucket.NewRangeScanForPrefix("doc_")
		iter, err := coll.Scan(scan, sgbucket.ScanOptions{})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for item := iter.Next(); item != nil; item = iter.Next() {
			ids = append(ids, item.ID)
			assert.NotNil(t, item.Body)
			assert.NotZero(t, item.Cas)
			assert.False(t, item.IDOnly)
		}
		require.Equal(t, []string{"doc_a", "doc_b", "doc_c", "doc_d", "doc_e"}, ids)
	})

	t.Run("PartialRange", func(t *testing.T) {
		scan := sgbucket.RangeScan{
			From: &sgbucket.ScanTerm{Term: "doc_b"},
			To:   &sgbucket.ScanTerm{Term: "doc_d", Exclusive: true},
		}
		iter, err := coll.Scan(scan, sgbucket.ScanOptions{})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for item := iter.Next(); item != nil; item = iter.Next() {
			ids = append(ids, item.ID)
		}
		require.Equal(t, []string{"doc_b", "doc_c"}, ids)
	})

	t.Run("ExclusiveFrom", func(t *testing.T) {
		scan := sgbucket.RangeScan{
			From: &sgbucket.ScanTerm{Term: "doc_a", Exclusive: true},
			To:   &sgbucket.ScanTerm{Term: "doc_c"},
		}
		iter, err := coll.Scan(scan, sgbucket.ScanOptions{})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for item := iter.Next(); item != nil; item = iter.Next() {
			ids = append(ids, item.ID)
		}
		require.Equal(t, []string{"doc_b", "doc_c"}, ids)
	})

	t.Run("IDsOnly", func(t *testing.T) {
		scan := sgbucket.NewRangeScanForPrefix("doc_")
		iter, err := coll.Scan(scan, sgbucket.ScanOptions{IDsOnly: true})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for item := iter.Next(); item != nil; item = iter.Next() {
			ids = append(ids, item.ID)
			assert.True(t, item.IDOnly)
			assert.Nil(t, item.Body)
		}
		require.Equal(t, []string{"doc_a", "doc_b", "doc_c", "doc_d", "doc_e"}, ids)
	})

	t.Run("EmptyRange", func(t *testing.T) {
		scan := sgbucket.NewRangeScanForPrefix("zzz_nonexistent_")
		iter, err := coll.Scan(scan, sgbucket.ScanOptions{})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		assert.Nil(t, iter.Next())
	})

	t.Run("PrefixScan", func(t *testing.T) {
		scan := sgbucket.NewRangeScanForPrefix("doc_c")
		iter, err := coll.Scan(scan, sgbucket.ScanOptions{})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for item := iter.Next(); item != nil; item = iter.Next() {
			ids = append(ids, item.ID)
		}
		require.Equal(t, []string{"doc_c"}, ids)
	})

	t.Run("TombstonesExcluded", func(t *testing.T) {
		require.NoError(t, coll.Delete("doc_b"))

		scan := sgbucket.NewRangeScanForPrefix("doc_")
		iter, err := coll.Scan(scan, sgbucket.ScanOptions{})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for item := iter.Next(); item != nil; item = iter.Next() {
			ids = append(ids, item.ID)
		}
		assert.Equal(t, []string{"doc_a", "doc_c", "doc_d", "doc_e"}, ids)
	})

	t.Run("NoBounds", func(t *testing.T) {
		scan := sgbucket.RangeScan{}
		iter, err := coll.Scan(scan, sgbucket.ScanOptions{IDsOnly: true})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for item := iter.Next(); item != nil; item = iter.Next() {
			ids = append(ids, item.ID)
		}
		// doc_b was deleted above, remaining 4 docs should all be returned in order
		assert.Equal(t, []string{"doc_a", "doc_c", "doc_d", "doc_e"}, ids)
	})

	t.Run("UnsupportedScanType", func(t *testing.T) {
		_, err := coll.Scan(nil, sgbucket.ScanOptions{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported scan type")
	})
}
