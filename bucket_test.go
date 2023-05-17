package rosmar

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	Logging = LevelInfo
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

func TestNewBucket(t *testing.T) {
	bucket := makeTestBucket(t)
	assert.Equal(t, testBucketName, bucket.GetName())
	assert.Contains(t, bucket.GetURL(), testBucketFilename)
}

func TestGetMissingBucket(t *testing.T) {
	bucket, err := GetBucket(testBucketPath(t), testBucketName)
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
	_, err = GetBucket(InMemoryURL, "Rosmar")
	assert.Error(t, err)

	assert.NoError(t, deleteBucketPath(InMemoryURL))
}

var defaultCollection = DataStoreName{"_default", "_default"}

func TestDefaultCollection(t *testing.T) {
	bucket := makeTestBucket(t)

	// Initially one collection:
	colls, err := bucket.ListDataStores()
	assert.NoError(t, err)
	assert.Equal(t, colls, []sgbucket.DataStoreName{defaultCollection})

	coll := bucket.DefaultDataStore()
	assert.NotNil(t, coll)
	assert.Equal(t, "RosmarTest._default._default", coll.GetName())
}

func TestCreateCollection(t *testing.T) {
	bucket := makeTestBucket(t)

	collName := DataStoreName{"_default", "foo"}
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
	c1, err := huddle.NamedDataStore(DataStoreName{"scope1", "collection1"})
	require.NoError(t, err)
	ok, err := c1.Add("doc1", 0, "c1_value")
	require.NoError(t, err)
	require.True(t, ok)
	c2, err := huddle.NamedDataStore(DataStoreName{"scope1", "collection2"})
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
	c1copy, err := huddle.NamedDataStore(DataStoreName{"scope1", "collection1"})
	require.NoError(t, err)
	_, err = c1copy.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)

	// drop collection
	err = huddle.DropDataStore(DataStoreName{"scope1", "collection1"})
	require.NoError(t, err)

	// reopen collection, verify that previous data is not present
	newC1, err := huddle.NamedDataStore(DataStoreName{"scope1", "collection1"})
	require.NoError(t, err)
	_, err = newC1.Get("doc1", &value)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))
}

func TestValidDataStoreName(t *testing.T) {

	validDataStoreNames := [][2]string{
		{"myScope", "myCollection"},
		{"ABCabc123_-%", "ABCabc123_-%"},
		{"_default", "myCollection"},
		{"_default", "_default"},
	}

	invalidDataStoreNames := [][2]string{
		{"a:1", "a:1"},
		{"_a", "b"},
		{"a", "_b"},
		{"%a", "b"},
		{"%a", "b"},
		{"a", "%b"},
		{"myScope", "_default"},
		{"_default", "a:1"},
	}

	for _, validPair := range validDataStoreNames {
		assert.True(t, isValidDataStoreName(validPair[0], validPair[1]))
	}
	for _, invalidPair := range invalidDataStoreNames {
		assert.False(t, isValidDataStoreName(invalidPair[0], invalidPair[1]))
	}
}

func TestCollectionMutations(t *testing.T) {
	ensureNoLeakedFeeds(t)

	huddle := makeTestBucket(t)
	defer huddle.Close()

	collection1, err := huddle.NamedDataStore(DataStoreName{"scope1", "collection1"})
	require.NoError(t, err)
	collection2, err := huddle.NamedDataStore(DataStoreName{"scope1", "collection2"})
	require.NoError(t, err)
	numDocs := 50

	collectionID_1, _ := huddle.GetCollectionID("scope1", "collection1")
	collectionID_2, _ := huddle.GetCollectionID("scope1", "collection2")

	// Add n docs to two collections
	for i := 1; i <= numDocs; i++ {
		ok, err := collection1.Add(fmt.Sprintf("doc%d", i), 0, fmt.Sprintf("value%d", i))
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = collection2.Add(fmt.Sprintf("doc%d", i), 0, fmt.Sprintf("value%d", i))
		require.NoError(t, err)
		require.True(t, ok)
	}

	var callbackMutex sync.Mutex
	var c1Count, c2Count int
	c1Keys := make(map[string]struct{})
	c2Keys := make(map[string]struct{})

	callback := func(event sgbucket.FeedEvent) bool {
		if event.Opcode != sgbucket.FeedOpMutation {
			return false
		}
		callbackMutex.Lock()
		defer callbackMutex.Unlock()
		if CollectionID(event.CollectionID) == collectionID_1 {
			c1Count++
			key := string(event.Key)
			_, ok := c1Keys[key]
			assert.False(t, ok)
			c1Keys[key] = struct{}{}
		} else if CollectionID(event.CollectionID) == collectionID_2 {
			c2Count++
			key := string(event.Key)
			_, ok := c2Keys[key]
			assert.False(t, ok)
			c2Keys[key] = struct{}{}
		}
		return true
	}

	args := sgbucket.FeedArguments{
		Scopes: map[string][]string{
			"scope1": {"collection1", "collection2"},
		},
		Terminator: make(chan bool),
	}
	defer close(args.Terminator)
	err = huddle.StartDCPFeed(args, callback, nil)
	require.NoError(t, err, "StartTapFeed failed")

	// wait for mutation counts to reach expected
	expectedCountReached := false
	for i := 0; i < 100; i++ {
		callbackMutex.Lock()
		if c1Count == numDocs && c2Count == numDocs {
			callbackMutex.Unlock()
			expectedCountReached = true
			break
		}
		callbackMutex.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	assert.True(t, expectedCountReached)
	assert.Equal(t, len(c1Keys), numDocs)
	assert.Equal(t, len(c2Keys), numDocs)
}

func TestGetPersistentMultiCollectionBucket(t *testing.T) {
	ensureNoLeakedFeeds(t)

	huddle := makeTestBucket(t)
	huddleURL := huddle.GetURL()

	c1, _ := huddle.NamedDataStore(DataStoreName{"scope1", "collection1"})
	ok, err := c1.Add("doc1", 0, "c1_value")
	require.True(t, ok)
	require.NoError(t, err)
	c2, _ := huddle.NamedDataStore(DataStoreName{"scope1", "collection2"})
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
	c1copy, _ := huddle.NamedDataStore(DataStoreName{"scope1", "collection1"})
	_, err = c1copy.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)

	// Close collection bucket
	huddle.Close()

	// Reopen persisted collection bucket
	loadedHuddle, loadedErr := GetBucket(huddleURL, testBucketName)
	assert.NoError(t, loadedErr)

	// validate contents
	var loadedValue interface{}
	c1Loaded, _ := loadedHuddle.NamedDataStore(DataStoreName{"scope1", "collection1"})
	_, err = c1Loaded.Get("doc1", &loadedValue)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", loadedValue)

	// drop collection, should remove persisted value
	err = loadedHuddle.DropDataStore(DataStoreName{"scope1", "collection1"})
	require.NoError(t, err)

	// reopen collection, verify that previous data is not present
	newC1, _ := loadedHuddle.NamedDataStore(DataStoreName{"scope1", "collection1"})
	_, err = newC1.Get("doc1", &loadedValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))

	// verify that non-dropped collection (collection2) values are still present
	c2Loaded, _ := loadedHuddle.NamedDataStore(DataStoreName{"scope1", "collection2"})
	_, err = c2Loaded.Get("doc1", &loadedValue)
	require.NoError(t, err)
	assert.Equal(t, "c2_value", loadedValue)

	// Close collection bucket
	loadedHuddle.Close()

	// Reopen persisted collection bucket again to ensure dropped collection is not present
	reloadedHuddle, reloadedErr := GetBucket(huddleURL, testBucketName)
	assert.NoError(t, reloadedErr)

	// reopen dropped collection, verify that previous data is not present
	var reloadedValue interface{}
	reloadedC1, _ := reloadedHuddle.NamedDataStore(DataStoreName{"scope1", "collection1"})
	_, err = reloadedC1.Get("doc1", &reloadedValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))

	// reopen non-dropped collection, verify that previous data is present
	reloadedC2, _ := reloadedHuddle.NamedDataStore(DataStoreName{"scope1", "collection2"})
	_, err = reloadedC2.Get("doc1", &reloadedValue)
	require.NoError(t, err)
	assert.Equal(t, "c2_value", reloadedValue)

	// Close and Delete the bucket, should delete underlying collections
	require.NoError(t, reloadedHuddle.CloseAndDelete())

	// Attempt to reopen persisted collectionBucket
	postDeleteHuddle, err := NewBucket(huddleURL, testBucketName)
	assert.NoError(t, err)

	var postDeleteValue interface{}
	postDeleteC2, err := postDeleteHuddle.NamedDataStore(DataStoreName{"scope1", "collection2"})
	require.NoError(t, err)
	_, err = postDeleteC2.Get("doc1", &postDeleteValue)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))
	require.NoError(t, postDeleteHuddle.CloseAndDelete())
}
