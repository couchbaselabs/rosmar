// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
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

const testBucketDirName = "RosmarTest"

func testBucketPath(t *testing.T) string {
	return fmt.Sprintf("%s%c%s", t.TempDir(), os.PathSeparator, testBucketDirName)
}

func makeTestBucket(t *testing.T) *Bucket {
	return makeTestBucketWithName(t, strings.ToLower(t.Name()))
}

func makeTestBucketWithName(t *testing.T, name string) *Bucket {
	logToTest(t)
	bucket, err := OpenBucket(uriFromPath(testBucketPath(t)), name, CreateNew)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, bucket.CloseAndDelete(t.Context()))
	})

	return bucket
}

// logToTest routes rosmar's logging to t.Logf for the duration of the test.  LoggingCallback is a package
// global, so the previous callback has to be put back afterwards: left bound to a finished test's t, the next
// log - from a later test, or from a feed or expiry goroutine - panics with "Log in goroutine after ... has
// completed".  Registered before the bucket's cleanup so that it is restored after the bucket has closed.
func logToTest(t *testing.T) {
	previous := LoggingCallback
	t.Cleanup(func() { LoggingCallback = previous })
	LoggingCallback = func(level LogLevel, fmt string, args ...any) {
		t.Helper()
		t.Logf(logLevelNamesPrint[level]+fmt, args...)
	}
}

func dsName(scope string, coll string) sgbucket.DataStoreName {
	return sgbucket.DataStoreNameImpl{Scope: scope, Collection: coll}
}

func requireAddRaw(t *testing.T, c sgbucket.DataStore, key string, exp Exp, value []byte) {
	added, err := c.AddRaw(t.Context(), key, exp, value)
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

	require.NoError(t, bucket.CloseAndDelete(t.Context()))
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
	ctx := t.Context()
	ensureNoLeaks(t)
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore(ctx)
	bucket.Close(ctx)
	defer func() {
		assert.NoError(t, bucket.CloseAndDelete(ctx))
	}()
	_, err := bucket.ListDataStores(ctx)
	assert.ErrorContains(t, err, "bucket has been closed")
	_, _, err = c.GetRaw(ctx, "foo")
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
			ctx := t.Context()
			bucket, err := OpenBucket(InMemoryURL, strings.ToLower(t.Name()), testCase.mode)
			require.NoError(t, err)
			require.NotNil(t, bucket)

			require.Equal(t, uint(1), bucketCount(bucket.GetName()))

			err = bucket.CloseAndDelete(ctx)
			assert.NoError(t, err)

			assert.Empty(t, bucketCount(bucket.GetName()))
		})
	}
}

var defaultCollection = dsName("_default", "_default")

func TestTwoBucketsOneURL(t *testing.T) {
	ctx := t.Context()
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
		assert.NoError(t, bucket2.CloseAndDelete(ctx))
	})

	require.Equal(t, uint(2), bucketCount(bucketName))

	bucket1.Close(ctx)
	require.Equal(t, uint(1), bucketCount(bucketName))

	err = DeleteBucketAt(url)
	require.Error(t, err)

	require.NoError(t, bucket2.CloseAndDelete(ctx))
	assert.Empty(t, bucketCount(bucketName))

	err = DeleteBucketAt(url)
	assert.NoError(t, err)
}

func TestDefaultCollection(t *testing.T) {
	ctx := t.Context()
	ensureNoLeaks(t)
	bucket := makeTestBucket(t)

	// Initially one collection:
	colls, err := bucket.ListDataStores(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []sgbucket.DataStoreName{defaultCollection.(sgbucket.DataStoreNameImpl)}, colls)

	coll := bucket.DefaultDataStore(ctx)
	assert.NotNil(t, coll)
	assert.Equal(t, strings.ToLower(t.Name())+"._default._default", coll.GetName())
}

func TestCreateCollection(t *testing.T) {
	ctx := t.Context()
	ensureNoLeaks(t)
	bucket := makeTestBucket(t)

	collName := dsName("_default", "foo")
	err := bucket.CreateDataStore(ctx, collName)
	assert.NoError(t, err)

	coll, err := bucket.NamedDataStore(ctx, collName)
	assert.NoError(t, err)
	assert.NotNil(t, coll)
	assert.Equal(t, strings.ToLower(t.Name())+"._default.foo", coll.GetName())

	colls, err := bucket.ListDataStores(ctx)
	assert.NoError(t, err)
	assert.Equal(t, colls, []sgbucket.DataStoreName{defaultCollection, collName})
}

//////// MULTI-COLLECTION:

func TestMultiCollectionBucket(t *testing.T) {
	ctx := t.Context()
	ensureNoLeaks(t)
	ensureNoLeakedFeeds(t)

	huddle := makeTestBucket(t)
	c1, err := huddle.NamedDataStore(ctx, dsName("scope1", "collection1"))
	require.NoError(t, err)
	ok, err := c1.Add(ctx, "doc1", 0, "c1_value")
	require.NoError(t, err)
	require.True(t, ok)
	c2, err := huddle.NamedDataStore(ctx, dsName("scope1", "collection2"))
	require.NoError(t, err)
	ok, err = c2.Add(ctx, "doc1", 0, "c2_value")
	require.True(t, ok)
	require.NoError(t, err)

	var value interface{}
	_, err = c1.Get(ctx, "doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)
	_, err = c2.Get(ctx, "doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c2_value", value)

	// reopen collection, verify retrieval
	c1copy, err := huddle.NamedDataStore(ctx, dsName("scope1", "collection1"))
	require.NoError(t, err)
	_, err = c1copy.Get(ctx, "doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)

	// drop collection
	err = huddle.DropDataStore(ctx, dsName("scope1", "collection1"))
	require.NoError(t, err)

	// reopen collection, verify that previous data is not present
	newC1, err := huddle.NamedDataStore(ctx, dsName("scope1", "collection1"))
	require.NoError(t, err)
	_, err = newC1.Get(ctx, "doc1", &value)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))
}

func TestGetPersistentMultiCollectionBucket(t *testing.T) {
	ctx := t.Context()
	ensureNoLeakedFeeds(t)

	huddle := makeTestBucket(t)
	huddleURL := huddle.GetURL()

	c1, _ := huddle.NamedDataStore(ctx, dsName("scope1", "collection1"))
	ok, err := c1.Add(ctx, "doc1", 0, "c1_value")
	require.True(t, ok)
	require.NoError(t, err)
	c2, _ := huddle.NamedDataStore(ctx, dsName("scope1", "collection2"))
	ok, err = c2.Add(ctx, "doc1", 0, "c2_value")
	require.True(t, ok)
	require.NoError(t, err)

	var value interface{}
	_, err = c1.Get(ctx, "doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)
	_, err = c2.Get(ctx, "doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c2_value", value)

	// reopen collection, verify retrieval
	c1copy, _ := huddle.NamedDataStore(ctx, dsName("scope1", "collection1"))
	_, err = c1copy.Get(ctx, "doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)

	// Close collection bucket
	huddle.Close(ctx)

	// Reopen persisted collection bucket
	loadedHuddle, loadedErr := OpenBucket(huddleURL, strings.ToLower(t.Name()), ReOpenExisting)
	require.NoError(t, loadedErr)

	// validate contents
	var loadedValue interface{}
	c1Loaded, _ := loadedHuddle.NamedDataStore(ctx, dsName("scope1", "collection1"))
	_, err = c1Loaded.Get(ctx, "doc1", &loadedValue)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", loadedValue)

	// drop collection, should remove persisted value
	err = loadedHuddle.DropDataStore(ctx, dsName("scope1", "collection1"))
	require.NoError(t, err)

	// reopen collection, verify that previous data is not present
	newC1, _ := loadedHuddle.NamedDataStore(ctx, dsName("scope1", "collection1"))
	_, err = newC1.Get(ctx, "doc1", &loadedValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))

	// verify that non-dropped collection (collection2) values are still present
	c2Loaded, _ := loadedHuddle.NamedDataStore(ctx, dsName("scope1", "collection2"))
	_, err = c2Loaded.Get(ctx, "doc1", &loadedValue)
	require.NoError(t, err)
	assert.Equal(t, "c2_value", loadedValue)

	// Close collection bucket
	loadedHuddle.Close(ctx)

	// Reopen persisted collection bucket again to ensure dropped collection is not present
	reloadedHuddle, reloadedErr := OpenBucket(huddleURL, strings.ToLower(t.Name()), ReOpenExisting)
	require.NoError(t, reloadedErr)

	// reopen dropped collection, verify that previous data is not present
	var reloadedValue interface{}
	reloadedC1, _ := reloadedHuddle.NamedDataStore(ctx, dsName("scope1", "collection1"))
	_, err = reloadedC1.Get(ctx, "doc1", &reloadedValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))

	// reopen non-dropped collection, verify that previous data is present
	reloadedC2, _ := reloadedHuddle.NamedDataStore(ctx, dsName("scope1", "collection2"))
	_, err = reloadedC2.Get(ctx, "doc1", &reloadedValue)
	require.NoError(t, err)
	assert.Equal(t, "c2_value", reloadedValue)

	// Close and Delete the bucket, should delete underlying collections
	require.NoError(t, reloadedHuddle.CloseAndDelete(ctx))

	// Attempt to reopen deleted bucket
	_, err = OpenBucket(huddleURL, strings.ToLower(t.Name()), ReOpenExisting)
	assert.Error(t, err)

	// Create new bucket at same path:
	postDeleteHuddle, err := OpenBucket(huddleURL, strings.ToLower(t.Name()), CreateNew)
	require.NoError(t, err)
	var postDeleteValue interface{}
	postDeleteC2, err := postDeleteHuddle.NamedDataStore(ctx, dsName("scope1", "collection2"))
	require.NoError(t, err)
	_, err = postDeleteC2.Get(ctx, "doc1", &postDeleteValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))
	require.NoError(t, postDeleteHuddle.CloseAndDelete(ctx))
}

func TestExpiration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		ensureNoLeaks(t)
		bucket := makeTestBucket(t)
		c := bucket.DefaultDataStore(ctx)

		exp, err := bucket.nextExpiration()
		require.NoError(t, err)
		require.Equal(t, Exp(0), exp)

		pastExp := Exp(time.Now().Add(-5 * time.Second).Unix())
		futureExp := Exp(time.Now().Add(2 * time.Second).Unix())

		requireAddRaw(t, c, "k1", 0, []byte("v1"))
		requireAddRaw(t, c, "k3", 0, []byte("v3"))
		requireAddRaw(t, c, "k4", futureExp, []byte("v4"))

		exp, err = bucket.nextExpiration()
		require.NoError(t, err)
		require.Equal(t, int(futureExp), int(exp))

		// k2 is already expired, so wait for the expiration goroutine to remove it
		requireAddRaw(t, c, "k2", pastExp, []byte("v2"))
		synctest.Wait()

		exp, err = bucket.nextExpiration()
		require.NoError(t, err)
		require.Equal(t, int(futureExp), int(exp))

		_, _, err = c.GetRaw(ctx, "k1")
		assert.NoError(t, err)
		_, _, err = c.GetRaw(ctx, "k2")
		assert.Error(t, err) // k2 is gone
		_, _, err = c.GetRaw(ctx, "k3")
		assert.NoError(t, err)
		_, _, err = c.GetRaw(ctx, "k4")
		assert.NoError(t, err)

		time.Sleep(3 * time.Second)
		synctest.Wait()

		exp, err = bucket.nextExpiration()
		require.NoError(t, err)
		assert.Equal(t, uint32(0), exp)

		_, _, err = c.GetRaw(ctx, "k4")
		assert.Error(t, err)

		n, err := bucket.PurgeTombstones()
		assert.NoError(t, err)
		assert.Equal(t, int64(2), n)
	})
}

func TestExpirationAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		ensureNoLeaks(t)
		// a file-based bucket is required: Close leaves the sqlite database open for an in-memory bucket
		bucket := makeTestBucket(t)
		c := bucket.DefaultDataStore(ctx)

		// set expiry long enough that Close will happen first
		exp := Exp(time.Now().Add(1 * time.Second).Unix())
		requireAddRaw(t, c, "docID", exp, []byte("v1"))
		bucket.Close(ctx)
		// pass the expiry time: an expiration left running would panic on the closed database
		time.Sleep(2 * time.Second)
	})
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

// TestTestLoggingIsRestored verifies that a test's logging callback is unbound once it finishes.  Left bound to
// a finished test, rosmar logging from a later test panics with "Log in goroutine after ... has completed",
// which is why the package could not be run with -count 2.
func TestTestLoggingIsRestored(t *testing.T) {
	previousLevel := GetLogLevel()
	SetLogLevel(LevelInfo)
	previousCallback := LoggingCallback
	t.Cleanup(func() {
		LoggingCallback = previousCallback
		SetLogLevel(previousLevel)
	})

	var logged int
	LoggingCallback = func(LogLevel, string, ...any) { logged++ }

	t.Run("makes a bucket", func(t *testing.T) {
		makeTestBucket(t) // binds LoggingCallback to this subtest
	})

	// The subtest has finished, so logging must have come back to this test's callback:
	info("logging after the subtest finished")
	require.NotZero(t, logged, "makeTestBucket left LoggingCallback bound to a finished test")
}

type docRow struct {
	cas       CAS
	exp       Exp
	tombstone bool
	revSeqNo  uint64
	hasValue  bool
}

func getDocRow(t *testing.T, c *Collection, key string) docRow {
	var row docRow
	require.NoError(t, scan(c.db().QueryRow(
		`SELECT cas, exp, tombstone, revSeqNo, value IS NOT NULL FROM documents WHERE collection=? AND key=?`, c.id, key),
		&row.cas, &row.exp, &row.tombstone, &row.revSeqNo, &row.hasValue))
	return row
}

func TestExpirationSkipsTombstones(t *testing.T) {
	testCases := []struct {
		name     string
		deleteFn func(t *testing.T, c *Collection, key string, cas CAS, exp Exp)
		keepsExp bool // tombstone keeps the expiry passed to the delete call
	}{
		{
			name: "Delete",
			deleteFn: func(t *testing.T, c *Collection, key string, _ CAS, _ Exp) {
				require.NoError(t, c.Delete(t.Context(), key))
			},
		},
		{
			name: "DeleteWithXattrs",
			deleteFn: func(t *testing.T, c *Collection, key string, _ CAS, _ Exp) {
				require.NoError(t, c.DeleteWithXattrs(t.Context(), key, nil))
			},
		},
		{
			name: "UpdateDelete",
			deleteFn: func(t *testing.T, c *Collection, key string, _ CAS, _ Exp) {
				_, err := c.Update(t.Context(), key, 0, func([]byte) ([]byte, *uint32, bool, error) { return nil, nil, true, nil })
				require.NoError(t, err)
			},
		},
		{
			name:     "UpdateXattrDeleteBody",
			keepsExp: true,
			deleteFn: func(t *testing.T, c *Collection, key string, cas CAS, exp Exp) {
				_, err := c.UpdateXattrDeleteBody(t.Context(), key, "_sync", exp, cas, map[string]any{"foo": "bar"}, nil)
				require.NoError(t, err)
			},
		},
		{
			name:     "WriteTombstoneWithXattrs",
			keepsExp: true,
			deleteFn: func(t *testing.T, c *Collection, key string, cas CAS, exp Exp) {
				_, err := c.WriteTombstoneWithXattrs(t.Context(), key, exp, cas, map[string][]byte{"_sync": []byte(`{"foo":"bar"}`)}, nil, true, nil)
				require.NoError(t, err)
			},
		},
		{
			name:     "DeleteWithMeta",
			keepsExp: true,
			deleteFn: func(t *testing.T, c *Collection, key string, cas CAS, exp Exp) {
				require.NoError(t, c.DeleteWithMeta(t.Context(), key, cas, cas+1, exp, nil))
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ensureNoLeaks(t)
				bucket := makeTestBucket(t)
				c := bucket.DefaultDataStore(t.Context()).(*Collection)
				key := "doc1"

				exp := Exp(time.Now().Add(2 * time.Second).Unix())
				cas, err := c.WriteCas(t.Context(), key, exp, 0, []byte(`{"foo":"bar"}`), 0)
				require.NoError(t, err)

				tc.deleteFn(t, c, key, cas, exp)
				before := getDocRow(t, c, key)
				t.Logf("tombstone row: %+v", before)
				require.False(t, before.hasValue)
				assert.True(t, before.tombstone, "tombstone flag not set")
				if tc.keepsExp {
					assert.Equal(t, exp, before.exp)
				} else {
					assert.Equal(t, Exp(0), before.exp)
				}

				time.Sleep(3 * time.Second)
				synctest.Wait()

				require.Equal(t, before, getDocRow(t, c, key), "expirer modified tombstone")
			})
		})
	}
}
