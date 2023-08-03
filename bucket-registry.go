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

	"golang.org/x/exp/slices"
)

// The bucket registry tracks all open Buckets (except in-memory ones) by their URL.
// This is used by feeds, to forward events from the Bucket that created them to other Buckets on
// the same file that should also be notified.
//
// The registry is not bulletproof; it can be fooled into thinking Buckets on the same file aren't,
// if their URLs aren't identical. This can happen if:
//
// * one of the file paths goes through (different) symlinks
// * the paths are capitalized differently, on a case-preserving filesystem like macOS or Windows
// * there's some kind of character-encoding difference that the filesystem ignores
//
// The way to fix this is to ask the filesystem to canonicalize the path (e.g. by calling
// `realpath` or `fcntl(F_GETPATH)`) and use the canonical path as the key.
// Unfortunately Go doesn't seem to have an API for that.

var bucketRegistry = map[string][]*Bucket{} // Maps URL to slice of Buckets at that URL
var bucketRegistryMutex sync.Mutex          // Thread-safe access to bucketRegistry

// Adds a newly opened Bucket to the registry.
func registerBucket(bucket *Bucket) {
	url := bucket.url
	if isInMemoryURL(url) {
		return
	}
	debug("registerBucket %v at %s", bucket, url)
	bucketRegistryMutex.Lock()
	bucketRegistry[url] = append(bucketRegistry[url], bucket)
	bucketRegistryMutex.Unlock()
}

// Removes a Bucket from the registry. Must be called before closing.
func unregisterBucket(bucket *Bucket) {
	url := bucket.url
	if isInMemoryURL(url) {
		return
	}
	debug("UNregisterBucket %v at %s", bucket, url)
	bucketRegistryMutex.Lock()
	defer bucketRegistryMutex.Unlock()

	buckets := bucketRegistry[url]
	i := slices.Index(buckets, bucket)
	if i < 0 {
		warn("unregisterBucket couldn't find %v", bucket)
		return
	}
	if len(buckets) == 1 {
		delete(bucketRegistry, url)
	} else {
		// Copy the slice before mutating, in case a client is iterating it:
		buckets = slices.Clone(buckets)
		buckets[i] = nil // remove ptr that might be left in the underlying array, for gc
		buckets = slices.Delete(buckets, i, i+1)
		bucketRegistry[url] = buckets
	}
}

// Returns the array of Bucket instances open on a given URL.
func bucketsAtURL(url string) (buckets []*Bucket) {
	if !isInMemoryURL(url) {
		bucketRegistryMutex.Lock()
		buckets = bucketRegistry[url]
		bucketRegistryMutex.Unlock()
	}
	return
}
