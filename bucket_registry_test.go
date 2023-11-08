// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReuseInMemoryBucket(t *testing.T) {
	ensureNoLeaks(t)
	bucketName := strings.ToLower(t.Name())
	bucket1, err := OpenBucket(InMemoryURL, bucketName, CreateOrOpen)
	require.NoError(t, err)
	key := "foo"
	body := []byte("bar")
	require.NoError(t, bucket1.DefaultDataStore().Set("foo", 0, nil, body))
	require.False(t, bucket1.closed)
	bucket1.Close(testCtx(t))
	defer func() {
		assert.NoError(t, bucket1.CloseAndDelete(testCtx(t)))
		assert.Len(t, getBucketNames(), 0)
	}()

	require.True(t, bucket1.closed)
	require.Equal(t, []string{bucketName}, getBucketNames())

	bucket2, err := OpenBucket(InMemoryURL, bucketName, CreateOrOpen)
	require.NoError(t, err)
	require.False(t, bucket2.closed)
	require.Equal(t, []string{bucketName}, getBucketNames())
	var bucket2Body []byte
	_, err = bucket2.DefaultDataStore().Get(key, &bucket2Body)
	require.NoError(t, err)
	require.Equal(t, body, bucket2Body)
}

func TestBucketRegistryRefCountPersistentBucket(t *testing.T) {
	ensureNoLeaks(t)

	bucketName := strings.ToLower(t.Name())
	bucket, err := OpenBucket(uriFromPath(t.TempDir()+"/"+bucketName), bucketName, CreateOrOpen)
	require.NoError(t, err)
	require.Equal(t, []string{bucketName}, getBucketNames())
	bucket.Close(testCtx(t))
	require.Len(t, getBucketNames(), 0)
}

func TestDuplicateBucketNamesDifferentPath(t *testing.T) {

	ensureNoLeaks(t)

	bucketName := strings.ToLower(t.Name())
	path1 := uriFromPath(t.TempDir() + "/" + bucketName + "1")
	path2 := uriFromPath(t.TempDir() + "/" + bucketName + "2")

	bucket1, err := OpenBucket(path1, bucketName, CreateOrOpen)
	require.NoError(t, err)
	require.Equal(t, []string{bucketName}, getBucketNames())

	bucket2, err := OpenBucket(path2, bucketName, CreateOrOpen)
	require.ErrorContains(t, err, "already exists")
	require.Equal(t, []string{bucketName}, getBucketNames())

	bucket1.Close(testCtx(t))
	require.Len(t, getBucketNames(), 0)

	// Close bucket1, should allow bucket2 to open
	bucket2, err = OpenBucket(path2, bucketName, CreateOrOpen)
	require.NoError(t, err)
	defer bucket2.Close(testCtx(t))
	require.Equal(t, []string{bucketName}, getBucketNames())

}
