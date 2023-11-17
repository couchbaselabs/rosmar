// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	if GetLogLevel() == LevelNone {
		SetLogLevel(LevelInfo)
	}
}

func testCtx(t *testing.T) context.Context {
	return context.Background() // match sync gateway interfaces for logging
}

const testBucketDirName = "RosmarTest"

func testBucketPath(t *testing.T) string {
	return fmt.Sprintf("%s%c%s", t.TempDir(), os.PathSeparator, testBucketDirName)
}

func makeTestBucket(t *testing.T) *Bucket {
	LoggingCallback = func(level LogLevel, fmt string, args ...any) {
		t.Logf(logLevelNamesPrint[level]+fmt, args...)
	}
	bucket, err := OpenBucket(uriFromPath(testBucketPath(t)), strings.ToLower(t.Name()), CreateNew)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, bucket.CloseAndDelete(testCtx(t)))
	})

	return bucket
}

func dsName(scope string, coll string) sgbucket.DataStoreName {
	return sgbucket.DataStoreNameImpl{Scope: scope, Collection: coll}
}

func requireAddRaw(t *testing.T, c sgbucket.DataStore, key string, exp Exp, value []byte) {
	added, err := c.AddRaw(key, exp, value)
	require.NoError(t, err)
	require.True(t, added, "Doc was not added")
}

func bucketCount(name string) uint {
	cluster.lock.Lock()
	defer cluster.lock.Unlock()
	return cluster.bucketCount[name]
}

func TestNewBucket(t *testing.T) {
	ensureNoLeaks(t)
	bucket := makeTestBucket(t)
	bucketName := strings.ToLower(t.Name())
	assert.Equal(t, bucketName, bucket.GetName())
	assert.Contains(t, bucket.GetURL(), testBucketDirName)

	require.Equal(t, uint(1), bucketCount(bucketName))

	require.NoError(t, bucket.CloseAndDelete(testCtx(t)))
	require.Equal(t, uint(0), bucketCount(bucketName))
}

func TestGetMissingBucket(t *testing.T) {
	ensureNoLeaks(t)
	path := uriFromPath(testBucketPath(t))
	require.NoError(t, DeleteBucketAt(path))
	bucket, err := OpenBucket(path, strings.ToLower(t.Name()), ReOpenExisting)
	if runtime.GOOS == "windows" {
		assert.ErrorContains(t, err, "unable to open database file: The system cannot find the path specified")
	} else {
		assert.ErrorContains(t, err, "unable to open database file: no such file or directory")
	}
	assert.Nil(t, bucket)
}

func TestCallClosedBucket(t *testing.T) {
	ensureNoLeaks(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore()
	bucket.Close(testCtx(t))
	defer func() {
		assert.NoError(t, bucket.CloseAndDelete(testCtx(t)))
	}()
	_, err := bucket.ListDataStores()
	assert.ErrorContains(t, err, "bucket has been closed")
	_, _, err = c.GetRaw("foo")
	assert.ErrorContains(t, err, "bucket has been closed")
}

func TestNewBucketInMemory(t *testing.T) {
	ensureNoLeaks(t)
	assert.NoError(t, DeleteBucketAt(InMemoryURL))

	testCases := []struct {
		name string
		mode OpenMode
	}{
		{
			name: "CreateNew",
			mode: CreateNew,
		},
		{
			name: "CreateOrOpen",
			mode: CreateOrOpen,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			bucket, err := OpenBucket(InMemoryURL, strings.ToLower(t.Name()), testCase.mode)
			require.NoError(t, err)
			require.NotNil(t, bucket)

			require.Equal(t, uint(1), bucketCount(bucket.GetName()))

			err = bucket.CloseAndDelete(testCtx(t))
			assert.NoError(t, err)

			assert.Empty(t, bucketCount(bucket.GetName()))
		})
	}
}

var defaultCollection = dsName("_default", "_default")

func TestTwoBucketsOneURL(t *testing.T) {
	ensureNoLeaks(t)
	bucket1 := makeTestBucket(t)
	url := bucket1.url

	bucketName := strings.ToLower(t.Name())
	bucket2, err := OpenBucket(url, bucketName, CreateNew)
	require.ErrorContains(t, err, "already exists")
	require.Nil(t, bucket2)

	bucket2, err = OpenBucket(url, bucketName, ReOpenExisting)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, bucket2.CloseAndDelete(testCtx(t)))
	})

	require.Equal(t, uint(2), bucketCount(bucketName))

	bucket1.Close(testCtx(t))
	require.Equal(t, uint(1), bucketCount(bucketName))

	err = DeleteBucketAt(url)
	require.Error(t, err)

	require.NoError(t, bucket2.CloseAndDelete(testCtx(t)))
	assert.Empty(t, bucketCount(bucketName))

	err = DeleteBucketAt(url)
	assert.NoError(t, err)
}

func TestDefaultCollection(t *testing.T) {
	ensureNoLeaks(t)
	bucket := makeTestBucket(t)

	// Initially one collection:
	colls, err := bucket.ListDataStores()
	assert.NoError(t, err)
	assert.Equal(t, []sgbucket.DataStoreName{defaultCollection.(sgbucket.DataStoreNameImpl)}, colls)

	coll := bucket.DefaultDataStore()
	assert.NotNil(t, coll)
	assert.Equal(t, strings.ToLower(t.Name())+"._default._default", coll.GetName())
}

func TestCreateCollection(t *testing.T) {
	ensureNoLeaks(t)
	bucket := makeTestBucket(t)

	collName := dsName("_default", "foo")
	err := bucket.CreateDataStore(testCtx(t), collName)
	assert.NoError(t, err)

	coll, err := bucket.NamedDataStore(collName)
	assert.NoError(t, err)
	assert.NotNil(t, coll)
	assert.Equal(t, strings.ToLower(t.Name())+"._default.foo", coll.GetName())

	colls, err := bucket.ListDataStores()
	assert.NoError(t, err)
	assert.Equal(t, colls, []sgbucket.DataStoreName{defaultCollection, collName})
}

//////// MULTI-COLLECTION:

func TestMultiCollectionBucket(t *testing.T) {
	ensureNoLeaks(t)
	ensureNoLeakedFeeds(t)

	huddle := makeTestBucket(t)
	c1, err := huddle.NamedDataStore(dsName("scope1", "collection1"))
	require.NoError(t, err)
	ok, err := c1.Add("doc1", 0, "c1_value")
	require.NoError(t, err)
	require.True(t, ok)
	c2, err := huddle.NamedDataStore(dsName("scope1", "collection2"))
	require.NoError(t, err)
	ok, err = c2.Add("doc1", 0, "c2_value")
	require.True(t, ok)
	require.NoError(t, err)

	var value interface{}
	_, err = c1.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)
	_, err = c2.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c2_value", value)

	// reopen collection, verify retrieval
	c1copy, err := huddle.NamedDataStore(dsName("scope1", "collection1"))
	require.NoError(t, err)
	_, err = c1copy.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)

	// drop collection
	err = huddle.DropDataStore(dsName("scope1", "collection1"))
	require.NoError(t, err)

	// reopen collection, verify that previous data is not present
	newC1, err := huddle.NamedDataStore(dsName("scope1", "collection1"))
	require.NoError(t, err)
	_, err = newC1.Get("doc1", &value)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))
}

func TestGetPersistentMultiCollectionBucket(t *testing.T) {
	ensureNoLeakedFeeds(t)

	huddle := makeTestBucket(t)
	huddleURL := huddle.GetURL()

	c1, _ := huddle.NamedDataStore(dsName("scope1", "collection1"))
	ok, err := c1.Add("doc1", 0, "c1_value")
	require.True(t, ok)
	require.NoError(t, err)
	c2, _ := huddle.NamedDataStore(dsName("scope1", "collection2"))
	ok, err = c2.Add("doc1", 0, "c2_value")
	require.True(t, ok)
	require.NoError(t, err)

	var value interface{}
	_, err = c1.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)
	_, err = c2.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c2_value", value)

	// reopen collection, verify retrieval
	c1copy, _ := huddle.NamedDataStore(dsName("scope1", "collection1"))
	_, err = c1copy.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)

	// Close collection bucket
	huddle.Close(testCtx(t))

	// Reopen persisted collection bucket
	loadedHuddle, loadedErr := OpenBucket(huddleURL, strings.ToLower(t.Name()), ReOpenExisting)
	require.NoError(t, loadedErr)

	// validate contents
	var loadedValue interface{}
	c1Loaded, _ := loadedHuddle.NamedDataStore(dsName("scope1", "collection1"))
	_, err = c1Loaded.Get("doc1", &loadedValue)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", loadedValue)

	// drop collection, should remove persisted value
	err = loadedHuddle.DropDataStore(dsName("scope1", "collection1"))
	require.NoError(t, err)

	// reopen collection, verify that previous data is not present
	newC1, _ := loadedHuddle.NamedDataStore(dsName("scope1", "collection1"))
	_, err = newC1.Get("doc1", &loadedValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))

	// verify that non-dropped collection (collection2) values are still present
	c2Loaded, _ := loadedHuddle.NamedDataStore(dsName("scope1", "collection2"))
	_, err = c2Loaded.Get("doc1", &loadedValue)
	require.NoError(t, err)
	assert.Equal(t, "c2_value", loadedValue)

	// Close collection bucket
	loadedHuddle.Close(testCtx(t))

	// Reopen persisted collection bucket again to ensure dropped collection is not present
	reloadedHuddle, reloadedErr := OpenBucket(huddleURL, strings.ToLower(t.Name()), ReOpenExisting)
	require.NoError(t, reloadedErr)

	// reopen dropped collection, verify that previous data is not present
	var reloadedValue interface{}
	reloadedC1, _ := reloadedHuddle.NamedDataStore(dsName("scope1", "collection1"))
	_, err = reloadedC1.Get("doc1", &reloadedValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))

	// reopen non-dropped collection, verify that previous data is present
	reloadedC2, _ := reloadedHuddle.NamedDataStore(dsName("scope1", "collection2"))
	_, err = reloadedC2.Get("doc1", &reloadedValue)
	require.NoError(t, err)
	assert.Equal(t, "c2_value", reloadedValue)

	// Close and Delete the bucket, should delete underlying collections
	require.NoError(t, reloadedHuddle.CloseAndDelete(testCtx(t)))

	// Attempt to reopen deleted bucket
	_, err = OpenBucket(huddleURL, strings.ToLower(t.Name()), ReOpenExisting)
	assert.Error(t, err)

	// Create new bucket at same path:
	postDeleteHuddle, err := OpenBucket(huddleURL, strings.ToLower(t.Name()), CreateNew)
	require.NoError(t, err)
	var postDeleteValue interface{}
	postDeleteC2, err := postDeleteHuddle.NamedDataStore(dsName("scope1", "collection2"))
	require.NoError(t, err)
	_, err = postDeleteC2.Get("doc1", &postDeleteValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))
	require.NoError(t, postDeleteHuddle.CloseAndDelete(testCtx(t)))
}

func TestExpiration(t *testing.T) {
	ensureNoLeaks(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore()

	exp, err := bucket.nextExpiration()
	require.NoError(t, err)
	require.Equal(t, Exp(0), exp)

	exp2 := Exp(time.Now().Add(-5 * time.Second).Unix())
	exp4 := Exp(time.Now().Add(2 * time.Second).Unix())

	requireAddRaw(t, c, "k1", 0, []byte("v1"))
	requireAddRaw(t, c, "k2", exp2, []byte("v2"))
	requireAddRaw(t, c, "k3", 0, []byte("v3"))
	requireAddRaw(t, c, "k4", exp4, []byte("v4"))

	exp, err = bucket.nextExpiration()
	require.NoError(t, err)
	// Usually this will return exp2, but if this is slow enough that the expiration goroutine runs to expire document k2, it can return exp4.
	require.Contains(t, []Exp{exp2, exp4}, exp)

	log.Printf("... waiting 1 sec ...")
	time.Sleep(1 * time.Second)

	exp, err = bucket.nextExpiration()
	require.NoError(t, err)
	require.Equal(t, int(exp4), int(exp))

	_, _, err = c.GetRaw("k1")
	assert.NoError(t, err)
	_, _, err = c.GetRaw("k2")
	assert.Error(t, err) // k2 is gone
	_, _, err = c.GetRaw("k3")
	assert.NoError(t, err)
	_, _, err = c.GetRaw("k4")
	assert.NoError(t, err)

	log.Printf("... waiting 2 secs ...")
	time.Sleep(2 * time.Second)

	exp, err = bucket.nextExpiration()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), exp)

	_, _, err = c.GetRaw("k4")
	assert.Error(t, err)

	n, err := bucket.PurgeTombstones()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

func TestExpirationAfterClose(t *testing.T) {
	bucket, err := OpenBucket(InMemoryURL, strings.ToLower(t.Name()), CreateNew)
	ctx := testCtx(t)
	defer func() {
		assert.NoError(t, bucket.CloseAndDelete(ctx))
	}()
	require.NoError(t, err)
	c := bucket.DefaultDataStore()

	// set expiry long enough that Close will happen first
	exp := Exp(time.Now().Add(1 * time.Second).Unix())
	requireAddRaw(t, c, "docID", exp, []byte("v1"))
	bucket.Close(ctx)
	// sleep to ensure we won't panic
	time.Sleep(2 * time.Second)
}

func TestUriFromPathWindows(t *testing.T) {
	ensureNoLeaks(t)
	if runtime.GOOS != "windows" {
		t.Skip("This test is only for windows")
	}
	testCases := []struct {
		name   string
		input  string
		output string
	}{
		{
			name:   "absolute path, backslash",
			input:  `c:\foo\bar`,
			output: `rosmar:///c:/foo/bar`,
		},
		{
			name:   "absolute path, forward slash",
			input:  `c:/foo/bar`,
			output: `rosmar:///c:/foo/bar`,
		},
		{
			name:   "relative path forward slash",
			input:  "foo/bar",
			output: "rosmar://foo/bar",
		},
		{
			name:   "relative path black slash",
			input:  `foo/bar`,
			output: "rosmar://foo/bar",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.output, uriFromPath(testCase.input))
		})
	}
}

func TestUriFromPathNonWindows(t *testing.T) {
	ensureNoLeaks(t)
	if runtime.GOOS == "windows" {
		t.Skip("This test is only for non-windows")
	}
	testCases := []struct {
		name   string
		input  string
		output string
	}{
		{
			name:   "absolute path",
			input:  "/foo/bar",
			output: "rosmar:///foo/bar",
		},
		{
			name:   "relative path",
			input:  "foo/bar",
			output: "rosmar://foo/bar",
		},
		{
			name:   "has blackslash",
			input:  `foo\bar`,
			output: `rosmar://foo\bar`,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.output, uriFromPath(testCase.input))
		})
	}
}
