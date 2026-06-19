//go:build windows

// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"golang.org/x/sys/windows"
)

// rosmarWallClock returns the current wall-clock time in nanoseconds since the Unix epoch. On Windows,
// time.Now() is backed by the coarse system timer (~0.5-15ms resolution), which is too low for the HLC:
// successive writes land in the same physical slot, forcing logical increments. GetSystemTimePreciseAsFileTime
// reads the high-resolution system clock (~100ns) directly, read live on every call so it stays in lockstep
// with any other clock in the same process reading the same system time (e.g. Sync Gateway's HLC during tests).
func rosmarWallClock() uint64 {
	var ft windows.Filetime
	windows.GetSystemTimePreciseAsFileTime(&ft)
	return uint64(ft.Nanoseconds())
}
