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
	time uint64
}

func (c *fakeClock) getTime() uint64 {
	return c.time
}

func TestHLCReverseTime(t *testing.T) {
	clock := &fakeClock{}
	hlc := hybridLogicalClock{clock: clock}
	startTime := uint64(1000000) // 1 second
	clock.time = startTime
	require.Equal(t, timestamp(0xf0000), hlc.Now())
	require.Equal(t, timestamp(0xf0001), hlc.Now())

	// reverse time no counter
	clock.time = 0
	require.Equal(t, timestamp(0xf0002), hlc.Now())

	// reset time to normal
	clock.time = startTime
	require.Equal(t, timestamp(0xf0003), hlc.Now())

	// reverse time again
	clock.time = 1
	require.Equal(t, timestamp(0xf0004), hlc.Now())

	// jump to a value we had previously
	clock.time = startTime * 2
	require.Equal(t, timestamp(0x1e0000), hlc.Now())
	require.Equal(t, timestamp(0x1e0001), hlc.Now())

	// continue forward
	clock.time *= 2
	require.Equal(t, timestamp(0x3d0000), hlc.Now())

}
