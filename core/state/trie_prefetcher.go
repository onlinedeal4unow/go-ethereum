// Copyright 2020 The go-ethereum Authors
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
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	// trieDeliveryMeter counts how many times the prefetcher was unable to supply
	// the statedb with a prefilled trie. This meter should be zero -- if it's not, that
	// needs to be investigated
	trieDeliveryMissMeter = metrics.NewRegisteredMeter("trie/prefetch/deliverymiss", nil)

	triePrefetchAccountLoadMeter  = metrics.NewRegisteredMeter("trie/prefetch/account/load", nil)
	triePrefetchAccountDupMeter   = metrics.NewRegisteredMeter("trie/prefetch/account/dup", nil)
	triePrefetchAccountSkipMeter  = metrics.NewRegisteredMeter("trie/prefetch/account/skip", nil)
	triePrefetchAccountWasteMeter = metrics.NewRegisteredMeter("trie/prefetch/account/waste", nil)

	triePrefetchStorageLoadMeter  = metrics.NewRegisteredMeter("trie/prefetch/storage/load", nil)
	triePrefetchStorageDupMeter   = metrics.NewRegisteredMeter("trie/prefetch/storage/dup", nil)
	triePrefetchStorageSkipMeter  = metrics.NewRegisteredMeter("trie/prefetch/storage/skip", nil)
	triePrefetchStorageWasteMeter = metrics.NewRegisteredMeter("trie/prefetch/storage/waste", nil)
)

// triePrefetcher is an active prefetcher, which receives accounts or storage
// items and does trie-loading of them. The goal is to get as much useful content
// into the caches as possible.
//
// Note, the prefetcher's API is not thread safe.
type triePrefetcher struct {
	db       Database                    // Database to fetch trie nodes through
	root     common.Hash                 // Root hash of theaccount trie for metrics
	fetchers map[common.Hash]*subfetcher // Subfetchers for each trie
}

func newTriePrefetcher(db Database, root common.Hash) *triePrefetcher {
	return &triePrefetcher{
		db:       db,
		root:     root,
		fetchers: make(map[common.Hash]*subfetcher),
	}
}

// report iterates over all the subfetchers, aborts any that were left spinning
// and reports the stats to the metrics subsystem.
func (p *triePrefetcher) report() {
	for _, fetcher := range p.fetchers {
		fetcher.abort() // safe to do multiple times

		if fetcher.root == p.root {
			triePrefetchAccountLoadMeter.Mark(int64(len(fetcher.seen)))
			triePrefetchAccountDupMeter.Mark(int64(fetcher.dups))
			triePrefetchAccountSkipMeter.Mark(int64(len(fetcher.tasks)))
			triePrefetchAccountWasteMeter.Mark(int64(len(fetcher.seen) - fetcher.used))
		} else {
			triePrefetchStorageLoadMeter.Mark(int64(len(fetcher.seen)))
			triePrefetchStorageDupMeter.Mark(int64(fetcher.dups))
			triePrefetchStorageSkipMeter.Mark(int64(len(fetcher.tasks)))
			triePrefetchStorageWasteMeter.Mark(int64(len(fetcher.seen) - fetcher.used))
		}
	}
	// Clear out all fetchers (will crash on a second call, deliberate)
	p.fetchers = nil
}

// prefetch schedules a batch of trie items to prefetch.
func (p *triePrefetcher) prefetch(root common.Hash, keys [][]byte) {
	fetcher := p.fetchers[root]
	if fetcher == nil {
		fetcher = newSubfetcher(p.db, root)
		p.fetchers[root] = fetcher
	}
	fetcher.schedule(keys)
}

// trie returns the trie matching the root hash, or nil if the prefetcher doesn't
// have it.
func (p *triePrefetcher) trie(root common.Hash) Trie {
	// Bail out if no trie was prefetched for this root
	fetcher := p.fetchers[root]
	if fetcher == nil {
		trieDeliveryMissMeter.Mark(1)
		return nil
	}
	// Interrupt the prefetcher if it's by any chance still running and return
	// a copy of any pre-loaded trie.
	fetcher.abort() // safe to do multiple times

	if fetcher.trie == nil {
		trieDeliveryMissMeter.Mark(1)
		return nil
	}
	// Two accounts may well have the same storage root, but we cannot allow
	// them both to make updates to the same trie instance. Therefore, we need
	// to either delete the trie now, or deliver a copy of the trie.
	original := fetcher.trie
	fetcher.trie = nil

	return original
}

// used marks a batch of state items used to allow creating statistics as to
// how useful or wasteful the prefetcher is.
func (p *triePrefetcher) used(root common.Hash, used int) {
	if fetcher := p.fetchers[root]; fetcher != nil {
		fetcher.used = used
	}
}

// subfetcher is a trie fetcher goroutine responsible for pulling entries for a
// single trie. It is spawned when a new root is encountered and lives until the
// main prefetcher is paused and either all requested items are processed or if
// the trie being worked on is retrieved from the prefetcher.
type subfetcher struct {
	db   Database    // Database to load trie nodes through
	root common.Hash // Root hash of the trie to prefetch
	trie Trie        // Trie being populated with nodes

	tasks [][]byte   // Items queued up for retrieval
	lock  sync.Mutex // Lock protecting the task queue

	wake chan struct{} // Wake channel if a new task is scheduled
	stop chan struct{} // Channel to interrupt processing
	term chan struct{} // Channel to signal iterruption

	seen map[string]struct{} // Tracks the entries already loaded
	dups int                 // Number of duplicate preload tasks
	used int                 // Tracks the entries used in the end
}

// newSubfetcher creates a goroutine to prefetch state items belonging to a
// particular root hash.
func newSubfetcher(db Database, root common.Hash) *subfetcher {
	sf := &subfetcher{
		db:   db,
		root: root,
		wake: make(chan struct{}, 1),
		stop: make(chan struct{}),
		term: make(chan struct{}),
		seen: make(map[string]struct{}),
	}
	go sf.loop()
	return sf
}

// schedule adds a batch of trie keys to the queue to prefetch.
func (sf *subfetcher) schedule(keys [][]byte) {
	// Append the tasks to the current queue
	sf.lock.Lock()
	sf.tasks = append(sf.tasks, keys...)
	sf.lock.Unlock()

	// Notify the prefetcher, it's fine if it's already terminated
	select {
	case sf.wake <- struct{}{}:
	default:
	}
}

// abort interrupts the subfetcher immediately. It is safe to call abort multiple
// times but it is not thread safe.
func (sf *subfetcher) abort() {
	select {
	case <-sf.stop:
	default:
		close(sf.stop)
	}
	<-sf.term
}

// loop waits for new tasks to be scheduled and keeps loading them until it runs
// out of tasks or its underlying trie is retrieved for committing.
func (sf *subfetcher) loop() {
	// No matter how the loop stops, signal anyone waiting that it's terminated
	defer close(sf.term)

	// Start by opening the trie and stop processing if it fails
	trie, err := sf.db.OpenTrie(sf.root)
	if err != nil {
		log.Warn("Trie prefetcher failed opening trie", "root", sf.root, "err", err)
		return
	}
	sf.trie = trie

	// Trie opened successfully, keep prefetching items
	for {
		select {
		case <-sf.wake:
			// Subfetcher was woken up, retrieve any tasks to avoid spinning the lock
			sf.lock.Lock()
			tasks := sf.tasks
			sf.tasks = nil
			sf.lock.Unlock()

			// Prefetch any tasks until the loop is interrupted
			for i, task := range tasks {
				select {
				case <-sf.stop:
					// If termination is requested, add any leftover back and return
					sf.lock.Lock()
					sf.tasks = append(sf.tasks, tasks[i:]...)
					sf.lock.Unlock()
					return

				default:
					// No termination request yet, prefetch the next entry
					taskid := string(task)
					if _, ok := sf.seen[taskid]; ok {
						sf.dups++
					} else {
						sf.trie.TryGet(task)
						sf.seen[taskid] = struct{}{}
					}
				}
			}

		case <-sf.stop:
			// Termination is requested, abort and leave remaining tasks
			return
		}
	}
}
