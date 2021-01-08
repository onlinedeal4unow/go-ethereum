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
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	// trieDeliveryMeter counts how many times the prefetcher was unable to supply
	// the statedb with a prefilled trie. This meter should be zero -- if it's not, that
	// needs to be investigated
	trieDeliveryMissMeter = metrics.NewRegisteredMeter("trie/prefetch/deliverymiss", nil)

	triePrefetchFetchMeter = metrics.NewRegisteredMeter("trie/prefetch/fetch", nil)
	triePrefetchSkipMeter  = metrics.NewRegisteredMeter("trie/prefetch/skip", nil)
)

// TriePrefetcher is an active prefetcher, which receives accounts or storage
// items and does trie-loading of them. The goal is to get as much useful content
// into the caches as possible.
//
// Note, the prefetcher's API is not thread safe.
type TriePrefetcher struct {
	db Database

	cmdCh  chan *command // Control channel to pause or reset the root
	reqCh  chan struct{} // Notification channel for new preload requests
	quitCh chan struct{}

	accountTasks []common.Address              // Set of accounts to prefetch
	storageTasks map[common.Hash][]common.Hash // Set of storage slots to prefetch
	taskLock     sync.Mutex                    // Lock protecting the task sets

	paused uint32 // Whether the prefetcher is actively loading data or serving tries

	storageTries    map[common.Hash]Trie
	accountTrie     Trie
	accountTrieRoot common.Hash
}

func NewTriePrefetcher(db Database) *TriePrefetcher {
	return &TriePrefetcher{
		db:           db,
		cmdCh:        make(chan *command),
		reqCh:        make(chan struct{}, 1), // 1 to notify, no need to track multiple notifications
		quitCh:       make(chan struct{}),
		storageTasks: make(map[common.Hash][]common.Hash),
		paused:       1, // User needs to call Resume() before allowing to preload data
	}
}

type command struct {
	root common.Hash
	done chan struct{}
}

func (p *TriePrefetcher) Loop() {
	var (
		accountTrieRoot common.Hash
		accountTrie     Trie
		storageTries    map[common.Hash]Trie

		err error
		// Some tracking of performance
		skipped int64
		fetched int64

		paused = true
	)
	// The prefetcher loop has two distinct phases:
	// 1: Paused: when in this state, the accumulated tries are accessible to outside
	// callers.
	// 2: Active prefetching, awaiting slots and accounts to prefetch
	for {
		select {
		case <-p.quitCh:
			return
		case cmd := <-p.cmdCh:
			// Clear out any pending update notification
			select {
			case <-p.reqCh: // Skip stale notification
			default: // No notification queued
			}

			if paused {
				// Prefetcher is being resumed, clear old data
				p.storageTries = nil
				p.accountTrie = nil
				p.accountTrieRoot = common.Hash{}

				// Start with a new set of tries
				storageTries = make(map[common.Hash]Trie)
				accountTrieRoot = cmd.root
				accountTrie, err = p.db.OpenTrie(accountTrieRoot)
				if err != nil {
					log.Error("Trie prefetcher failed opening trie", "root", accountTrieRoot, "err", err)
				}
				if accountTrieRoot == (common.Hash{}) {
					log.Error("Trie prefetcher unpaused with bad root")
				}
				paused = false
			} else {
				// Prefetcher is being paused, abort all unfulfilled requests
				p.taskLock.Lock()
				skipped += int64(len(p.accountTasks))
				for storageTasks := range p.storageTasks {
					skipped += int64(len(storageTasks))
				}
				p.accountTasks = nil
				p.storageTasks = make(map[common.Hash][]common.Hash)
				p.taskLock.Unlock()

				// Update metrics at new block events
				triePrefetchFetchMeter.Mark(fetched)
				triePrefetchSkipMeter.Mark(skipped)
				fetched, skipped = 0, 0

				// Make the tries accessible
				p.accountTrie = accountTrie
				p.storageTries = storageTries
				p.accountTrieRoot = accountTrieRoot

				if cmd.root != (common.Hash{}) {
					log.Error("Trie prefetcher paused with non-empty root")
				}
				paused = true
			}
			close(cmd.done)

		case <-p.reqCh:
			if paused {
				log.Error("Prefetch request arrived whilst paused")
				continue
			}
			// Retrieve all the tasks queued up and reset the sets for new insertions
			p.taskLock.Lock()
			accountTasks, storageTasks := p.accountTasks, p.storageTasks
			if len(accountTasks) > 0 {
				p.accountTasks = nil
			}
			if len(storageTasks) > 0 {
				p.storageTasks = make(map[common.Hash][]common.Hash)
			}
			p.taskLock.Unlock()

			// Keep prefetching the data until an interruption is triggered
			for _, addr := range accountTasks {
				if atomic.LoadUint32(&p.paused) == 1 {
					break
				}
				accountTrie.TryGet(addr[:])
				fetched++
			}
			for root, slots := range storageTasks {
				if atomic.LoadUint32(&p.paused) == 1 {
					break
				}
				if _, ok := storageTries[root]; !ok {
					storageTrie, err := p.db.OpenTrie(root)
					if err != nil {
						log.Warn("Trie prefetcher failed opening storage trie", "root", root, "err", err)
						skipped += int64(len(slots))
						continue
					}
					storageTries[root] = storageTrie
				}
				storageTrie := storageTries[root]
				for _, hash := range slots {
					if atomic.LoadUint32(&p.paused) == 1 {
						break
					}
					storageTrie.TryGet(hash[:])
				}
				fetched += int64(len(slots))
			}
			// If pre-fetching was interrupted, mark all remaining asks as skipped
			if atomic.LoadUint32(&p.paused) == 1 {
				skipped += int64(len(accountTasks))
				for tasks := range storageTasks {
					skipped += int64(len(tasks))
				}
			}
		}
	}
}

// Close stops the prefetcher
func (p *TriePrefetcher) Close() {
	if p.quitCh != nil {
		close(p.quitCh)
		p.quitCh = nil
	}
}

// Resume causes the prefetcher to clear out old data, and get ready to
// fetch data concerning the new root.
func (p *TriePrefetcher) Resume(root common.Hash) {
	// Abort if the prefetcher is not paused
	if atomic.LoadUint32(&p.paused) == 0 {
		log.Error("Trie prefetcher already resumed")
		return
	}
	atomic.StoreUint32(&p.paused, 0)

	cmd := &command{
		root: root,
		done: make(chan struct{}),
	}
	p.cmdCh <- cmd
	<-cmd.done
}

// Pause causes the prefetcher to pause prefetching, and make tries
// accessible to callers via GetTrie.
func (p *TriePrefetcher) Pause() {
	// Abort if the prefetcher is already paused
	if atomic.LoadUint32(&p.paused) == 1 {
		log.Error("Trie prefetcher already paused")
		return
	}
	atomic.StoreUint32(&p.paused, 1)

	// Request a pause and wait until it's done
	cmd := &command{
		done: make(chan struct{}),
	}
	p.cmdCh <- cmd
	<-cmd.done
}

// PrefetchAddresses adds an address for prefetching
func (p *TriePrefetcher) PrefetchAddresses(addresses []common.Address) {
	// Abort if the prefetcher is already paused
	if atomic.LoadUint32(&p.paused) == 1 {
		log.Error("Attempted account trie-prefetch whilst paused")
		return
	}
	// Inject the addresses into the task queue and notify the prefetcher
	p.taskLock.Lock()
	defer p.taskLock.Unlock()

	p.accountTasks = append(p.accountTasks, addresses...)
	select {
	case p.reqCh <- struct{}{}:
	default:
		// Already notified
	}
}

// PrefetchStorage adds a storage root and a set of keys for prefetching
func (p *TriePrefetcher) PrefetchStorage(root common.Hash, slots []common.Hash) {
	// Abort if the prefetcher is already paused
	if atomic.LoadUint32(&p.paused) == 1 {
		log.Error("Attempted storage trie-prefetch whilst paused")
		return
	}
	// Inject the storage hashes into the task queue and notify the prefetcher
	p.taskLock.Lock()
	defer p.taskLock.Unlock()

	p.storageTasks[root] = append(p.storageTasks[root], slots...)
	select {
	case p.reqCh <- struct{}{}:
	default:
		// Already notified
	}
}

// GetTrie returns the trie matching the root hash, or nil if the prefetcher
// doesn't have it.
//
// Note, if snapshots are not enabled, we'll hit this code path during normal
// transaction processing, so don't check for that invariant.
func (p *TriePrefetcher) GetTrie(root common.Hash) Trie {
	// Abort if the prefetcher is not paused
	//if atomic.LoadUint32(&p.paused) == 0 {
	//	log.Error("Attempted trie-prefetcher retrieval whilst not paused")
	//	return nil
	//}
	if root == p.accountTrieRoot {
		return p.accountTrie
	}
	if storageTrie, ok := p.storageTries[root]; ok {
		// Two accounts may well have the same storage root, but we cannot allow
		// them both to make updates to the same trie instance. Therefore,
		// we need to either delete the trie now, or deliver a copy of the trie.
		delete(p.storageTries, root)
		return storageTrie
	}
	trieDeliveryMissMeter.Mark(1)
	return nil
}
