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
	"fmt"
	"io/fs"
	"sync"
)

// The bucket registry tracks all open Buckets and refcounts them. This represents a cluster of buckets, one per bucket name. When OpenBucket is called, a Bucket instance is added to bucketRegistry, representing the canonical bucket object. This object will not be removed from the bucket registry until:

// * In Memory bucket: bucket is not deleted until any Bucket's CloseAndDelete is closed.
// * On disk bucket: bucket is deleted from registry when all there are no open copies of the bucket in memory. Unlike in memory bucket, the bucket will stay persisted on disk to be reopened.
//
// Any Buckets returned by OpenBucket will be a copy of the canonical bucket object, which shares pointers to all mutable objects and copies of immutable objects. The difference between the canonical copy of the bucket is the `closed` state, representing when the bucket is no longer writeable. Sharing the data structures allows a single DCP prodcuer and expiry framework.

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

// registerBucket adds a newly opened Bucket to the registry. Returns true if the bucket already exists, and a copy of the bucket to use.
func (r *bucketRegistry) registerBucket(bucket *Bucket) (bool, *Bucket) {
	name := bucket.GetName()
	debug("_registerBucket %v %s at %s", bucket, name, bucket.url)
	r.lock.Lock()
	defer r.lock.Unlock()

	_, ok := r.buckets[name]
	if !ok {
		r.buckets[name] = bucket
	}
	r.bucketCount[name] += 1
	return ok, r.buckets[name].copy()
}

// getCachedBucket returns a bucket from the registry if it exists.
func (r *bucketRegistry) getCachedBucket(name, url string, mode OpenMode) (*Bucket, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	bucket := r.buckets[name]
	if bucket == nil {
		return nil, nil
	}
	if mode == CreateNew {
		return nil, fs.ErrExist
	}
	if url != bucket.url {
		return nil, fmt.Errorf("bucket %q already exists at %q, will not open at %q", name, bucket.url, url)
	}

	r.bucketCount[name] += 1
	return r.buckets[name].copy(), nil
}

// unregisterBucket removes a Bucket from the registry. Must be called before closing.
func (r *bucketRegistry) unregisterBucket(bucket *Bucket) error {
	name := bucket.name
	debug("UNregisterBucket %v %s at %s", bucket, name, bucket.url)
	r.lock.Lock()
	defer r.lock.Unlock()

	bucketCount := r.bucketCount[name]
	if bucketCount == 0 {
		warn("unregisterBucket couldn't find %v", bucket)
		return nil
	}
	if bucketCount == 1 {
		delete(r.bucketCount, name)
		// if an in memory bucket, don't close the sqlite db since it will vanish
		if bucket.inMemory {
			return nil
		}
		err := bucket._closeSqliteDB()
		if err != nil {
			return err
		}
		delete(r.buckets, name)
		return nil
	}
	r.bucketCount[name] -= 1
	return nil
}

// deleteBucket deletes a bucket from the registry and disk. Closes all existing buckets of the same name.
func (r *bucketRegistry) deleteBucket(_ context.Context, bucket *Bucket) error {
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

// getCachedBucket returns an instance of a bucket. If there are other copies of this bucket already in memory, it will return this version. If this is an in memory bucket, the bucket will not be removed until deleteBucket is called. Returns an error if the bucket can not be opened, but nil error and nil bucket if there is no registered bucket.
func getCachedBucket(name, url string, mode OpenMode) (*Bucket, error) {
	return cluster.getCachedBucket(name, url, mode)
}

// registryBucket adds a copy of a Bucket to the registry. Returns true if the bucket already exists.
func registerBucket(bucket *Bucket) (bool, *Bucket) {
	return cluster.registerBucket(bucket)
}

// unregisterBucket removes a Bucket from the registry. Must be called before closing.
func unregisterBucket(bucket *Bucket) error {
	return cluster.unregisterBucket(bucket)
}

// deleteBucket will delete a bucket from the registry and from disk.
func deleteBucket(ctx context.Context, bucket *Bucket) error {
	return cluster.deleteBucket(ctx, bucket)
}

// GetBucketNames returns a list of all bucket names.
func GetBucketNames() []string {
	return cluster.getBucketNames()
}
