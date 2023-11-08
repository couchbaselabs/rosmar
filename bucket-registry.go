// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"context"
	"slices"
	"sync"
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

type bucketRegistry struct {
	byURL       map[string][]*Bucket
	inMemoryRef map[string]*Bucket
	lock        sync.Mutex
}

var cluster *bucketRegistry // global cluster registry
func init() {
	cluster = &bucketRegistry{
		byURL:       make(map[string][]*Bucket),
		inMemoryRef: make(map[string]*Bucket),
	}
}

// registryBucket adds a newly opened Bucket to the registry.
func (r *bucketRegistry) registerBucket(bucket *Bucket) {
	url := bucket.url
	name := bucket.GetName()
	debug("registerBucket %v %s at %s", bucket, name, url)
	r.lock.Lock()
	defer r.lock.Unlock()
	_, ok := r.inMemoryRef[name]
	if !ok {
		b := bucket.copy()
		r.inMemoryRef[name] = b
	}
	r.byURL[url] = append(r.byURL[url], bucket)
}

func (r *bucketRegistry) getInMemoryBucket(name string) *Bucket {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.inMemoryRef[name]
}

// Removes a Bucket from the registry. Must be called before closing.
func (r *bucketRegistry) unregisterBucket(bucket *Bucket) {
	url := bucket.url
	name := bucket.name
	debug("UNregisterBucket %v at %s", bucket, name, url)
	r.lock.Lock()
	defer r.lock.Unlock()

	buckets := r.byURL[url]
	i := slices.Index(buckets, bucket)
	if i < 0 {
		warn("unregisterBucket couldn't find %v", bucket)
		return
	}
	if len(buckets) == 1 {
		delete(r.byURL, url)
		if !bucket.inMemory {
			bucket._closeAllInstances()
			delete(r.inMemoryRef, name)
		}
		return
	}
	// Copy the slice before mutating, in case a client is iterating it:
	buckets = slices.Clone(buckets)
	buckets[i] = nil // remove ptr that might be left in the underlying array, for gc
	buckets = slices.Delete(buckets, i, i+1)
	r.byURL[url] = buckets
	return
}

// Delete bucket from the registry and disk. Closes all existing buckets.
func (r *bucketRegistry) deleteBucket(ctx context.Context, bucket *Bucket) error {
	url := bucket.url
	name := bucket.name
	r.lock.Lock()
	defer r.lock.Unlock()

	_, ok := r.inMemoryRef[name]
	if ok {
		delete(r.inMemoryRef, name)
	}
	delete(r.byURL, url)
	return DeleteBucketAt(url)
}

func (r *bucketRegistry) getBucketNames() []string {
	r.lock.Lock()
	defer r.lock.Unlock()

	names := make([]string, 0, len(r.inMemoryRef))
	for name := range r.inMemoryRef {
		names = append(names, name)
	}
	return names
}

// getInMemoryBucket returns an instance of a bucket. If there are other copies of this bucket already in memory, it will return this version. If this is an in memory bucket, the bucket will not be removed until deleteBucket is called.
func getInMemoryBucket(name string) *Bucket {
	return cluster.getInMemoryBucket(name)
}

func registerBucket(bucket *Bucket) {
	cluster.registerBucket(bucket)
}

// eeregisterBucket removes a Bucket from the registry. Must be called before closing.
func unregisterBucket(bucket *Bucket) {
	cluster.unregisterBucket(bucket)
}

// deleteBucket will delete a bucket from the registry and from disk.
func deleteBucket(ctx context.Context, bucket *Bucket) error {
	return cluster.deleteBucket(ctx, bucket)
}

// getBucketNames returns a list of all bucket names.
func getBucketNames() []string {
	return cluster.getBucketNames()
}
