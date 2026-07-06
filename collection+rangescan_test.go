// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"errors"
	"sort"
	"strings"
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

	// Sort returned IDs so tests can use Equal against an ordered expected slice.
	// Range scan returns no ordering guarantee; consumers must be order-agnostic.
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
		// A clean drain must leave no error, distinguishable from an error via Err().
		assert.NoError(t, iter.Err())
		sort.Strings(ids)
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
		// An empty range is clean EOF, not an error.
		assert.NoError(t, iter.Err())
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

// TestRangeScanErrAndClose exercises the gocb-parity Err/Close contract: a
// clean Close returns nil but records ErrScanCancelled for a later Err, Close is
// idempotent, and a real iteration error wins over the cancellation sentinel.
// It runs against both the streaming (on-disk) scanIterator and the eager
// (in-memory) preRecordedScanIterator.
func TestRangeScanErrAndClose(t *testing.T) {
	ensureNoLeaks(t)
	ctx := t.Context()

	seed := func(t *testing.T, coll *Collection) {
		t.Helper()
		for _, k := range []string{"doc_a", "doc_b", "doc_c"} {
			require.NoError(t, coll.SetRaw(ctx, k, 0, nil, []byte(`{}`)))
		}
	}

	assertCloseSemantics := func(t *testing.T, coll *Collection) {
		t.Helper()

		t.Run("CleanCloseRecordsCancellation", func(t *testing.T) {
			iter, err := coll.Scan(ctx, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{IDsOnly: true})
			require.NoError(t, err)
			for item := iter.Next(ctx); item != nil; item = iter.Next(ctx) {
				require.NotEmpty(t, item.ID)
			}
			// Clean end-of-stream: no error until Close is called.
			require.NoError(t, iter.Err())
			require.NoError(t, iter.Close(ctx))
			// gocb parity: a clean Close cancels the scan, surfaced by a later Err.
			require.ErrorIs(t, iter.Err(), sgbucket.ErrScanCancelled)
			// Idempotent: a second Close reports the recorded cancellation.
			require.ErrorIs(t, iter.Close(ctx), sgbucket.ErrScanCancelled)
		})

		t.Run("CloseBeforeDrain", func(t *testing.T) {
			iter, err := coll.Scan(ctx, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{IDsOnly: true})
			require.NoError(t, err)
			require.NotNil(t, iter.Next(ctx))
			require.NoError(t, iter.Close(ctx))
			require.ErrorIs(t, iter.Err(), sgbucket.ErrScanCancelled)
			// No further items are returned after Close.
			require.Nil(t, iter.Next(ctx))
		})
	}

	t.Run("Streaming", func(t *testing.T) {
		coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)
		require.False(t, coll.bucket.inMemory)
		seed(t, coll)
		assertCloseSemantics(t, coll)
	})

	t.Run("PreRecorded", func(t *testing.T) {
		bucket, err := OpenBucket(InMemoryURL, strings.ToLower(t.Name()), CreateNew)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, bucket.CloseAndDelete(ctx)) })
		coll := bucket.DefaultDataStore(ctx).(*Collection)
		require.True(t, coll.bucket.inMemory)
		seed(t, coll)
		assertCloseSemantics(t, coll)
	})

	// A real iteration error must win over the cancellation sentinel and be
	// reported identically by Err and Close. A genuine sql.Rows scan failure is
	// impractical to trigger through the public API, so set the error directly.
	t.Run("RealErrorWinsOverCancellation", func(t *testing.T) {
		boom := errors.New("boom")

		t.Run("preRecorded", func(t *testing.T) {
			it := &preRecordedScanIterator{err: boom}
			require.ErrorIs(t, it.Close(ctx), boom)
			require.ErrorIs(t, it.Err(), boom)
			require.NotErrorIs(t, it.Err(), sgbucket.ErrScanCancelled)
		})

		t.Run("streaming", func(t *testing.T) {
			coll := makeTestBucket(t).DefaultDataStore(ctx).(*Collection)
			seed(t, coll)
			iter, err := coll.Scan(ctx, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{IDsOnly: true})
			require.NoError(t, err)
			si := iter.(*scanIterator)
			si.err = boom
			require.ErrorIs(t, si.Close(ctx), boom)
			require.ErrorIs(t, si.Err(), boom)
			require.NotErrorIs(t, si.Err(), sgbucket.ErrScanCancelled)
		})
	})
}
