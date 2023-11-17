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

// expiryManager handles expiration for a given bucket. It stores a timer which will call expirationFunc to delete documents. The value of when the timer
type expiryManager struct {
	mutex          *sync.Mutex // mutex for synchronized access to expiryManager
	timer          *time.Timer // Schedules expiration of docs
	nextExp        *uint32     // Timestamp when expTimer will run (0 if never)
	expirationFunc func()      // Function to call when timer expires
}

func newExpirationManager(expiractionFunc func()) *expiryManager {
	var nextExp uint32
	return &expiryManager{
		mutex:          &sync.Mutex{},
		nextExp:        &nextExp,
		expirationFunc: expiractionFunc,
	}
}

// stop stops existing timers and waits for any expiration processes to complete
func (e *expiryManager) stop() {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if e.timer != nil {
		e.timer.Stop()
	}
}

// _getNext returns the next expiration time, 0 if there is no scheduled expiration.
func (e *expiryManager) _getNext() uint32 {
	return *e.nextExp
}

// setNext sets the next expiration time and schedules an expiration to occur after that time.
func (e *expiryManager) setNext(exp uint32) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e._setNext(exp)
}

// _clearNext clears the next expiration time.
func (e *expiryManager) _clearNext() {
	var exp uint32
	e.nextExp = &exp
}

// setNext sets the next expiration time and schedules an expiration to occur after that time. Requires caller to have acquired mutex.
func (e *expiryManager) _setNext(exp uint32) {
	info("_setNext ", exp)
	e.nextExp = &exp
	if exp == 0 {
		e.timer = nil
		return
	}
	dur := expDuration(exp)
	if dur < 0 {
		dur = 0
	}
	debug("EXP: Scheduling in %s", dur)
	if e.timer == nil {
		e.timer = time.AfterFunc(dur, e.runExpiry)
	} else {
		e.timer.Reset(dur)
	}
}

// scheduleExpirationAtOrBefore schedules the next expiration of documents to occur, from the minimum expiration value in the bucket.
func (e *expiryManager) scheduleExpirationAtOrBefore(exp uint32) {
	if exp == 0 {
		return
	}
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e._scheduleExpirationAtOrBefore(exp)
}

// _scheduleExpirationAtOrBefore schedules the next expiration of documents to occur, from the minimum expiration value in the bucket. Requires the mutext to be held.
func (e *expiryManager) _scheduleExpirationAtOrBefore(exp uint32) {
	if exp == 0 {
		return
	}
	currentNextExp := e._getNext()
	// if zero will unset the timer.
	if currentNextExp == 0 || exp < currentNextExp {
		e._setNext(exp)
	}
}

// runExpiry is called when the timer expires. It calls the expirationFunc and then reschedules the timer if necessary.
func (e *expiryManager) runExpiry() {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.expirationFunc()
}
