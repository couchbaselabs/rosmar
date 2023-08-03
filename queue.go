// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"container/list"
	"sync"
)

// Thread-safe producer/consumer queue.
type queue[T any] struct {
	list *list.List
	cond *sync.Cond
}

// Initializes a queue struct
func (q *queue[T]) init() {
	q.list = list.New()
	q.cond = sync.NewCond(&sync.Mutex{})
}

// Pushes a value into the queue. (Never blocks: the queue has no size limit.)
// Returns false if the queue has been closed.
func (q *queue[T]) push(value T) (ok bool) {
	q.cond.L.Lock()
	if q.list != nil {
		q.list.PushFront(value)
		if q.list.Len() == 1 {
			q.cond.Signal()
		}
		ok = true
	}
	q.cond.L.Unlock()
	return ok
}

// Removes the last/oldest value from the queue; if the queue is empty, blocks.
// If the queue is closed while blocking, returns a default-initialized T.
func (q *queue[T]) pull() (result T) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.list != nil && q.list.Len() == 0 {
		q.cond.Wait()
	}
	if q.list != nil {
		last := q.list.Back()
		q.list.Remove(last)
		result = last.Value.(T)
	}
	return
}

func (q *queue[T]) close() {
	q.cond.L.Lock()
	if q.list != nil {
		q.list = nil
		q.cond.Broadcast()
	}
	q.cond.L.Unlock()
}
