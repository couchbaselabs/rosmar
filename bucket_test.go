package rosmar

import (
	"errors"
	"fmt"
	"os"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	if Logging == LevelNone {
		Logging = LevelInfo
	}
}

const testBucketFilename = "rosmar_test"
const testBucketName = "RosmarTest"

func testBucketPath(t *testing.T) string {
	return fmt.Sprintf("%s%c%s", t.TempDir(), os.PathSeparator, testBucketFilename)
}

func makeTestBucket(t *testing.T) *Bucket {
	bucket, err := NewBucket(testBucketPath(t), testBucketName)
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
	assert.Equal(t, testBucketName, bucket.GetName())
	assert.Contains(t, bucket.GetURL(), testBucketFilename)
}

func TestGetMissingBucket(t *testing.T) {
	bucket, err := GetBucket(testBucketPath(t))
	assert.Error(t, err)
	assert.Nil(t, bucket)
}

func TestNewBucketInMemory(t *testing.T) {
	bucket, err := NewBucket(InMemoryURL, "Rosmar")
	require.NoError(t, err)
	require.NotNil(t, bucket)

	err = bucket.CloseAndDelete()
	assert.NoError(t, err)

	// This should fail:
	_, err = GetBucket(InMemoryURL)
	assert.Error(t, err)

	assert.NoError(t, DeleteBucket(InMemoryURL))
}

var defaultCollection = dsName("_default", "_default")

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
	loadedHuddle, loadedErr := GetBucket(huddleURL)
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
	reloadedHuddle, reloadedErr := GetBucket(huddleURL)
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

	// Attempt to reopen persisted collectionBucket
	postDeleteHuddle, err := NewBucket(huddleURL, testBucketName)
	assert.NoError(t, err)

	var postDeleteValue interface{}
	postDeleteC2, err := postDeleteHuddle.NamedDataStore(dsName("scope1", "collection2"))
	require.NoError(t, err)
	_, err = postDeleteC2.Get("doc1", &postDeleteValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))
	require.NoError(t, postDeleteHuddle.CloseAndDelete())
}
