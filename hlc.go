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
	"time"
)

type timestamp uint64

// hybridLogicalClock is a hybrid logical clock implementation for rosmar that produces timestamps that will always be increasing regardless of clock changes.
type hybridLogicalClock struct {
	clock       clock
	highestTime uint64
	mutex       sync.Mutex
}

// clock interface is used to abstract the system clock for testing purposes.
type clock interface {
	// getTime returns the current time in nanoseconds.
	getTime() uint64
}

type systemClock struct{}

// getTime returns the current time in nanoseconds.
func (c *systemClock) getTime() uint64 {
	return uint64(time.Now().UnixNano())
}

// NewHybridLogicalClock returns a new HLC from a previously initialized time.
func NewHybridLogicalClock(lastTime timestamp) *hybridLogicalClock {
	return &hybridLogicalClock{
		highestTime: uint64(lastTime),
		clock:       &systemClock{},
	}
}

// Now returns the next time represented in nanoseconds. This can be the current timestamp, or if multiple occur in the same nanosecond, an increasing timestamp.
func (c *hybridLogicalClock) Now() timestamp {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	physicalTime := c.clock.getTime() &^ 0xFFFF // round to 48 bits
	if c.highestTime >= physicalTime {
		c.highestTime++
	} else {
		c.highestTime = physicalTime
	}
	return timestamp(c.highestTime)
}
