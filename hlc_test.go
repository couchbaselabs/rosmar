// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"fmt"
	"sync"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/require"
)

func TestHLCCrossBucket(t *testing.T) {
	ctx := t.Context()
	goroutines := 10
	documentCount := 10

	collection1 := makeTestBucketWithName(t, "bucket1").DefaultDataStore(ctx)
	collection2 := makeTestBucketWithName(t, "bucket2").DefaultDataStore(ctx)

	wg := sync.WaitGroup{}
	results := make(chan []uint64)

	createDocuments := func(goroutineIdx int, collection sgbucket.DataStore) {

		defer wg.Done()
		casValues := make([]uint64, documentCount)
		for i := 0; i < documentCount; i++ {
			cas, err := collection.WriteCas(ctx, fmt.Sprintf("key_%d_%d", goroutineIdx, i), 0, 0, []byte(" World"), sgbucket.AddOnly)
			require.NoError(t, err)
			casValues[i] = cas
		}
		results <- casValues
	}
	for i := 0; i < goroutines; i++ {
		for _, collection := range []sgbucket.DataStore{collection1, collection2} {
			wg.Add(1)
			go createDocuments(i, collection)
		}
	}

	doneChan := make(chan struct{})
	go func() {
		wg.Wait()
		doneChan <- struct{}{}
	}()
	allCas := make([]uint64, 0, goroutines*documentCount)
loop:
	for {
		select {
		case casValues := <-results:
			allCas = append(allCas, casValues...)
		case <-doneChan:
			break loop
		}
	}
	uniqueCas := make(map[uint64]struct{})
	for _, cas := range allCas {
		if _, ok := uniqueCas[cas]; ok {
			t.Errorf("cas %d is not unique", cas)
		}
		uniqueCas[cas] = struct{}{}
	}

}
