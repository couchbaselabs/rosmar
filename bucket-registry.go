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
	"sync"
)

// The bucket registry tracks all open Buckets and refcounts them.
// The canonical version of the bucket is in the buckets member of the bucketRegistry. This will remain in memory for the following length of time:
//
// * In Memory bucket: bucket is not deleted until any Bucket's CloseAndDelete is closed.
// * On disk bucket: bucket is deleted from registry when all there are no open copies of the bucket in memory. Unlike in memory bucket, the bucket will stay persisted on disk to be reopened.

// bucketRegistry tracks all open buckets
type bucketRegistry struct {
	bucketCount map[string]uint    // stores a reference count of open buckets
	buckets     map[string]*Bucket // stores a reference to each open bucket
	lock        sync.Mutex
}

var cluster *bucketRegistry // global cluster registry
func init() {
	cluster = &bucketRegistry{
		bucketCount: make(map[string]uint),
		buckets:     make(map[string]*Bucket),
	}
}

// registryBucket adds a newly opened Bucket to the registry.
func (r *bucketRegistry) registerBucket(bucket *Bucket) {
	name := bucket.GetName()
	debug("registerBucket %v %s at %s", bucket, name, bucket.url)
	r.lock.Lock()
	defer r.lock.Unlock()
	_, ok := r.buckets[name]
	if !ok {
		b := bucket.copy()
		r.buckets[name] = b
	}
	r.bucketCount[name] += 1
}

// getCachedBucket returns a bucket from the registry if it exists.
func (r *bucketRegistry) getCachedBucket(name string) *Bucket {
	r.lock.Lock()
	defer r.lock.Unlock()
	bucket := r.buckets[name]
	if bucket == nil {
		return nil
	}
	return bucket.copy()
}

// unregisterBucket removes a Bucket from the registry. Must be called before closing.
func (r *bucketRegistry) unregisterBucket(bucket *Bucket) {
	name := bucket.name
	debug("UNregisterBucket %v %s at %s", bucket, name, bucket.url)
	r.lock.Lock()
	defer r.lock.Unlock()

	bucketCount := r.bucketCount[name]
	if bucketCount < 0 {
		warn("unregisterBucket couldn't find %v", bucket)
		return
	}
	if bucketCount == 1 {
		delete(r.bucketCount, name)
		// if an in memory bucket, don't close the sqlite db since it will vanish
		if !bucket.inMemory {
			bucket._closeSqliteDB()
			delete(r.buckets, name)
		}
		return
	}
	r.bucketCount[name] -= 1
	return
}

// deleteBucket deletes a bucket from the registry and disk. Closes all existing buckets of the same name.
func (r *bucketRegistry) deleteBucket(ctx context.Context, bucket *Bucket) error {
	name := bucket.name
	r.lock.Lock()
	defer r.lock.Unlock()

	_, ok := r.buckets[name]
	if ok {
		delete(r.buckets, name)
	}
	delete(r.bucketCount, name)
	return DeleteBucketAt(bucket.url)
}

// getBucketNames returns a list of all bucket names in the bucketRegistry.
func (r *bucketRegistry) getBucketNames() []string {
	r.lock.Lock()
	defer r.lock.Unlock()

	names := make([]string, 0, len(r.buckets))
	for name := range r.buckets {
		names = append(names, name)
	}
	return names
}

// getCachedBucket returns an instance of a bucket. If there are other copies of this bucket already in memory, it will return this version. If this is an in memory bucket, the bucket will not be removed until deleteBucket is called.
func getCachedBucket(name string) *Bucket {
	return cluster.getCachedBucket(name)
}

// registryBucket adds a newly opened Bucket to the registry.
func registerBucket(bucket *Bucket) {
	cluster.registerBucket(bucket)
}

// unregisterBucket removes a Bucket from the registry. Must be called before closing.
func unregisterBucket(bucket *Bucket) {
	cluster.unregisterBucket(bucket)
}

// deleteBucket will delete a bucket from the registry and from disk.
func deleteBucket(ctx context.Context, bucket *Bucket) error {
	return cluster.deleteBucket(ctx, bucket)
}

// GetBucketNames returns a list of all bucket names.
func GetBucketNames() []string {
	return cluster.getBucketNames()
}
