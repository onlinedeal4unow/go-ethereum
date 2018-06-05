// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"runtime"
)

// finalizer is a concurrent state trie hasher.
var finalizer = newStateFinalizer(runtime.NumCPU())

// stateFinalizerRequest is a request for hashing dirty state tries, caching the
// results back into the state objects and tries themselves.
//
// The inc field defines the number of state objects to skip after each hashing,
// which is used to feed the same underlying input array to different threads but
// ensure they process the early objects fast.
type stateFinalizerRequest struct {
	db   Database
	objs []*stateObject
	done chan int
	off  int
	inc  int
}

// stateFinalizer is a helper structure to concurrently hash dirty state tries.
type stateFinalizer struct {
	threads int
	tasks   chan *stateFinalizerRequest
}

// newStateFinalizer creates a new state object background hasher.
func newStateFinalizer(threads int) *stateFinalizer {
	f := &stateFinalizer{
		tasks:   make(chan *stateFinalizerRequest, threads),
		threads: threads,
	}
	for i := 0; i < threads; i++ {
		go f.loop()
	}
	return f
}

// loop is an infinite loop, finalizing state objects.
func (f *stateFinalizer) loop() {
	for task := range f.tasks {
		for i := task.off; i < len(task.objs); i += task.inc {
			task.objs[i].updateRoot(task.db)
			task.done <- i
		}
	}
}

// finalize recovers the senders from a batch of transactions and caches them
// back into the same data structures. There is no validation being done, nor
// any reaction to invalid signatures. That is up to calling code later.
func (f *stateFinalizer) finalize(db Database, objs []*stateObject, callback func(*stateObject)) {
	// If there's not much to do, execute inline
	if len(objs) < 1 {
		for _, obj := range objs {
			obj.updateRoot(db)
			callback(obj)
		}
		return
	}
	// Ensure we have meaningful task sizes and schedule the hashings
	tasks := f.threads
	if len(objs) < tasks {
		tasks = len(objs)
	}
	done := make(chan int, len(objs))
	for i := 0; i < tasks; i++ {
		f.tasks <- &stateFinalizerRequest{
			db:   db,
			objs: objs,
			done: done,
			off:  i,
			inc:  tasks,
		}
	}
	// Wait for the results to trickle back and deliver them
	for i := 0; i < len(objs); i++ {
		callback(objs[<-done])
	}
}
