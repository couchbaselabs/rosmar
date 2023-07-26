package rosmar

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	if Logging == LevelNone {
		Logging = LevelInfo
	}
}

const testBucketDirName = "RosmarTest"

func testBucketPath(t *testing.T) string {
	dir := fmt.Sprintf("%s%c%s", t.TempDir(), os.PathSeparator, testBucketDirName)
	return dir
}

func makeTestBucket(t *testing.T) *Bucket {
	bucket, err := OpenBucket(testBucketPath(t), CreateNew)
	require.NoError(t, err)
	t.Cleanup(bucket.Close)

	LoggingCallback = func(level LogLevel, fmt string, args ...any) {
		t.Logf(logLevelNamesPrint[level]+fmt, args...)
	}
	return bucket
}

func dsName(scope string, coll string) sgbucket.DataStoreName {
	return sgbucket.DataStoreNameImpl{Scope: scope, Collection: coll}
}

func TestNewBucket(t *testing.T) {
	bucket := makeTestBucket(t)
	assert.Equal(t, testBucketDirName, bucket.GetName())
	assert.Contains(t, bucket.GetURL(), testBucketDirName)

	url := bucket.url
	buckets := bucketsAtURL(url)
	assert.Contains(t, buckets, bucket)

	bucket.Close()
	assert.Empty(t, bucketsAtURL(url))
}

func TestGetMissingBucket(t *testing.T) {
	path := testBucketPath(t)
	require.NoError(t, DeleteBucketAt(path))
	bucket, err := OpenBucket(path, ReOpenExisting)
	assert.ErrorContains(t, err, "unable to open database file: no such file or directory")
	assert.Nil(t, bucket)
}

func TestCallClosedBucket(t *testing.T) {
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore()
	bucket.Close()
	_, err := bucket.ListDataStores()
	assert.ErrorContains(t, err, "bucket has been closed")
	_, _, err = c.GetRaw("foo")
	assert.ErrorContains(t, err, "bucket has been closed")
}

func TestNewBucketInMemory(t *testing.T) {
	assert.NoError(t, DeleteBucketAt(InMemoryURL))

	modes := []OpenMode{CreateNew, CreateOrOpen}
	for _, mode := range modes {
		bucket, err := OpenBucket(InMemoryURL, mode)
		require.NoError(t, err)
		require.NotNil(t, bucket)

		assert.Empty(t, bucketsAtURL(bucket.url))

		err = bucket.CloseAndDelete()
		assert.NoError(t, err)
	}

	_, err := OpenBucket(InMemoryURL, ReOpenExisting)
	assert.Error(t, err)
	assert.Equal(t, err, fs.ErrNotExist)
}

var defaultCollection = dsName("_default", "_default")

func TestTwoBucketsOneURL(t *testing.T) {
	bucket1 := makeTestBucket(t)
	url := bucket1.url

	bucket2, err := OpenBucket(url, CreateNew)
	require.ErrorContains(t, err, "already exists")
	require.Nil(t, bucket2)

	bucket2, err = OpenBucket(url, ReOpenExisting)
	require.NoError(t, err)
	t.Cleanup(bucket2.Close)

	buckets := bucketsAtURL(url)
	assert.Len(t, buckets, 2)
	assert.Contains(t, buckets, bucket1)
	assert.Contains(t, buckets, bucket2)

	bucket1.Close()
	buckets = bucketsAtURL(url)
	assert.Len(t, buckets, 1)
	assert.Contains(t, buckets, bucket2)

	err = DeleteBucketAt(url)
	assert.ErrorContains(t, err, "there is a Bucket open at that URL")

	bucket2.Close()
	assert.Empty(t, bucketsAtURL(url))

	err = DeleteBucketAt(url)
	assert.NoError(t, err)
}

func TestDefaultCollection(t *testing.T) {
	bucket := makeTestBucket(t)

	// Initially one collection:
	colls, err := bucket.ListDataStores()
	assert.NoError(t, err)
	assert.Equal(t, []sgbucket.DataStoreName{defaultCollection.(sgbucket.DataStoreNameImpl)}, colls)

	coll := bucket.DefaultDataStore()
	assert.NotNil(t, coll)
	assert.Equal(t, "RosmarTest._default._default", coll.GetName())
}

func TestCreateCollection(t *testing.T) {
	bucket := makeTestBucket(t)

	collName := dsName("_default", "foo")
	err := bucket.CreateDataStore(collName)
	assert.NoError(t, err)

	coll, err := bucket.NamedDataStore(collName)
	assert.NoError(t, err)
	assert.NotNil(t, coll)
	assert.Equal(t, "RosmarTest._default.foo", coll.GetName())

	colls, err := bucket.ListDataStores()
	assert.NoError(t, err)
	assert.Equal(t, colls, []sgbucket.DataStoreName{defaultCollection, collName})
}

//////// MULTI-COLLECTION:

func TestMultiCollectionBucket(t *testing.T) {
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
	huddle.Close()

	// Reopen persisted collection bucket
	loadedHuddle, loadedErr := OpenBucket(huddleURL, ReOpenExisting)
	assert.NoError(t, loadedErr)

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
	loadedHuddle.Close()

	// Reopen persisted collection bucket again to ensure dropped collection is not present
	reloadedHuddle, reloadedErr := OpenBucket(huddleURL, ReOpenExisting)
	assert.NoError(t, reloadedErr)

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
	require.NoError(t, reloadedHuddle.CloseAndDelete())

	// Attempt to reopen deleted bucket
	_, err = OpenBucket(huddleURL, ReOpenExisting)
	assert.Error(t, err)

	// Create new bucket at same path:
	postDeleteHuddle, err := OpenBucket(huddleURL, CreateNew)
	require.NoError(t, err)
	var postDeleteValue interface{}
	postDeleteC2, err := postDeleteHuddle.NamedDataStore(dsName("scope1", "collection2"))
	require.NoError(t, err)
	_, err = postDeleteC2.Get("doc1", &postDeleteValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))
	require.NoError(t, postDeleteHuddle.CloseAndDelete())
}

func TestExpiration(t *testing.T) {
	Logging = LevelTrace
	bucket := makeTestBucket(t)
	c := bucket.DefaultDataStore()

	exp, err := bucket.NextExpiration()
	if assert.NoError(t, err) {
		assert.Equal(t, Exp(0), exp)
	}

	exp2 := Exp(time.Now().Add(-5 * time.Second).Unix())
	exp4 := Exp(time.Now().Add(2 * time.Second).Unix())

	c.AddRaw("k1", 0, []byte("v1"))
	c.AddRaw("k2", exp2, []byte("v2"))
	c.AddRaw("k3", 0, []byte("v3"))
	c.AddRaw("k4", exp4, []byte("v4"))

	exp, err = bucket.NextExpiration()
	require.NoError(t, err)
	assert.Equal(t, exp2, exp)

	log.Printf("... waiting 1 sec ...")
	time.Sleep(1 * time.Second)

	exp, err = bucket.NextExpiration()
	require.NoError(t, err)
	assert.Equal(t, exp4, exp)

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

	exp, err = bucket.NextExpiration()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), exp)

	_, _, err = c.GetRaw("k4")
	assert.Error(t, err)

	n, err := bucket.PurgeTombstones()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), n)
}
