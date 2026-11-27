// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build deadlock

package rosmar

import "github.com/sasha-s/go-deadlock"

// mutex reports lock order inversions, so that tests find deadlocks even when the goroutines do not interleave to deadlock.
type mutex = deadlock.Mutex

func init() {
	// only check lock order, since timeout goroutines and timers do not work inside synctest bubbles
	deadlock.Opts.DeadlockTimeout = 0
}
