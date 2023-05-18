package rosmar

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetXattr(t *testing.T) {
	ensureNoLeakedFeeds(t)
	coll := makeTestBucket(t).DefaultDataStore()

	addToCollection(t, coll, "key", 0, "value")

	cas, err := coll.SetXattr("key", "xfiles", []byte(`{"truth":"out_there"}`))
	require.NoError(t, err)

	var val, xval any
	gotCas, err := coll.GetWithXattr("key", "xfiles", "", &val, &xval, nil)
	require.NoError(t, err)
	assert.Equal(t, cas, gotCas)
	assert.Equal(t, "value", val)
	assert.Equal(t, map[string]any{"truth": "out_there"}, xval)
}
