// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHybridLogicalClockNow(t *testing.T) {
	clock := hybridLogicalClock{clock: &systemClock{}}
	timestamp1 := clock.Now()
	timestamp2 := clock.Now()
	require.Greater(t, timestamp2, timestamp1)
}

func generateTimestamps(wg *sync.WaitGroup, clock *hybridLogicalClock, n int, result chan []timestamp) {
	defer wg.Done()
	timestamps := make([]timestamp, n)
	for i := 0; i < n; i++ {
		timestamps[i] = clock.Now()
	}
	result <- timestamps
}

func TestHLCNowConcurrent(t *testing.T) {
	clock := hybridLogicalClock{clock: &systemClock{}}
	goroutines := 100
	timestampCount := 100

	wg := sync.WaitGroup{}
	results := make(chan []timestamp)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go generateTimestamps(&wg, &clock, timestampCount, results)
	}

	doneChan := make(chan struct{})
	go func() {
		wg.Wait()
		doneChan <- struct{}{}
	}()
	allTimestamps := make([]timestamp, 0, goroutines*timestampCount)
loop:
	for {
		select {
		case timestamps := <-results:
			allTimestamps = append(allTimestamps, timestamps...)
		case <-doneChan:
			break loop
		}
	}
	uniqueTimestamps := make(map[timestamp]struct{})
	for _, timestamp := range allTimestamps {
		if _, ok := uniqueTimestamps[timestamp]; ok {
			t.Errorf("Timestamp %d is not unique", timestamp)
		}
		uniqueTimestamps[timestamp] = struct{}{}
	}
}

type fakeClock struct {
	time timestamp
}

func (c *fakeClock) getTime() timestamp {
	return c.time
}

func TestHLCReverseTime(t *testing.T) {
	clock := &fakeClock{}
	hlc := hybridLogicalClock{clock: clock}
	require.Equal(t, timestamp(1), hlc.Now())
	require.Equal(t, timestamp(2), hlc.Now())

	// reverse time no counter
	clock.time = timestamp(0)
	require.Equal(t, timestamp(3), hlc.Now())

	// reset time to normal
	clock.time = timestamp(6)
	require.Equal(t, timestamp(6), hlc.Now())

	// reverse time again
	clock.time = timestamp(1)
	require.Equal(t, timestamp(7), hlc.Now())

	// jump to a value we had previously
	clock.time = timestamp(6)
	require.Equal(t, int(timestamp(8)), int(hlc.Now()))
	require.Equal(t, int(timestamp(9)), int(hlc.Now()))
}
