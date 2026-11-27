// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueuePullReportsClose verifies that pull tells a closed queue apart from a nil value. A feed relies
// on this to tell a terminated feed from one that pulled its end-of-feed marker.
func TestQueuePullReportsClose(t *testing.T) {
	t.Run("nil value", func(t *testing.T) {
		var q queue[*int]
		q.init()
		q.push(nil)
		v, ok := q.pull()
		assert.Nil(t, v)
		assert.True(t, ok)
	})

	t.Run("closed with values queued", func(t *testing.T) {
		var q queue[*int]
		q.init()
		q.push(new(int))
		q.close()
		v, ok := q.pull()
		assert.Nil(t, v)
		assert.False(t, ok, "close drops queued values")
	})

	t.Run("closed from another goroutine", func(t *testing.T) {
		var q queue[*int]
		q.init()
		pulled := make(chan bool)
		go func() {
			_, ok := q.pull()
			pulled <- ok
		}()
		q.close()
		select {
		case ok := <-pulled:
			assert.False(t, ok)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "pull did not return after close")
		}
	})
}
