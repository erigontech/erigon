// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package v4

import (
	"bytes"
	"context"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

const hashParallelMin = 2048

type feedEntry struct {
	plainKey  string
	hashedKey []byte
	update    *commitment.Update
}

func hashFeed(items []feedEntry, workers int) {
	if workers <= 1 || len(items) < hashParallelMin {
		var cache commitment.AddrHashCache
		for i := range items {
			items[i].hashedKey = commitment.KeyToHexNibbleHashCached(common.ToBytesZeroCopy(items[i].plainKey), &cache)
		}
		return
	}
	chunk := (len(items) + workers - 1) / workers
	var wg sync.WaitGroup
	for start := 0; start < len(items); start += chunk {
		end := min(start+chunk, len(items))
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			var cache commitment.AddrHashCache
			for i := lo; i < hi; i++ {
				items[i].hashedKey = commitment.KeyToHexNibbleHashCached(common.ToBytesZeroCopy(items[i].plainKey), &cache)
			}
		}(start, end)
	}
	wg.Wait()
}

func compareFeed(a, b feedEntry) int {
	if c := bytes.Compare(a.hashedKey, b.hashedKey); c != 0 {
		return c
	}
	return strings.Compare(a.plainKey, b.plainKey)
}

func bucketBy[T any](items []T, bucket func(*T) int) [257]int {
	var bounds [257]int
	for i := range items {
		bounds[bucket(&items[i])+1]++
	}
	for b := 1; b < len(bounds); b++ {
		bounds[b] += bounds[b-1]
	}
	next := bounds
	for b := range 256 {
		for next[b] < bounds[b+1] {
			for c := bucket(&items[next[b]]); c != b; c = bucket(&items[next[b]]) {
				items[next[b]], items[next[c]] = items[next[c]], items[next[b]]
				next[c]++
			}
			next[b]++
		}
	}
	return bounds
}

func warmSorted(warmuper *commitment.Warmuper, items []feedEntry) {
	if warmuper != nil {
		warmuper.WarmSorted(len(items), func(i int) []byte { return items[i].hashedKey })
	}
}

func partitionFeed(items []feedEntry, workers int, warmuper *commitment.Warmuper) ([]storageTask, []accountEntry, int, error) {
	bounds := bucketBy(items, func(e *feedEntry) int { return int(e.hashedKey[0])<<4 | int(e.hashedKey[1]) })
	var parts [256]*partitioner
	var errs [256]error
	parallelFor(256, workers, 1, func(b int) {
		bucket := items[bounds[b]:bounds[b+1]]
		slices.SortFunc(bucket, compareFeed)
		p := newPartitioner()
		for i := range bucket {
			if err := p.add(bucket[i].hashedKey, bucket[i].update); err != nil {
				errs[b] = err
				return
			}
		}
		parts[b] = p
	})
	warmSorted(warmuper, items)
	var storage [256][]storageTask
	var accounts [256][]accountEntry
	seen := 0
	for b := range parts {
		if errs[b] != nil {
			return nil, nil, 0, errs[b]
		}
		storage[b], accounts[b] = parts[b].done()
		seen += parts[b].seen
	}
	return slices.Concat(storage[:]...), slices.Concat(accounts[:]...), seen, nil
}

func partitionUpdates(ctx context.Context, updates *commitment.Updates, workers int, warmuper *commitment.Warmuper) ([]storageTask, []accountEntry, int, error) {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	items := make([]feedEntry, 0, updates.Size())
	if err := updates.Drain(func(plainKey string, update *commitment.Update) error {
		items = append(items, feedEntry{plainKey: plainKey, update: update})
		return nil
	}); err != nil {
		return nil, nil, 0, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, 0, err
	}

	hashFeed(items, workers)
	if workers > 1 && len(items) >= hashParallelMin {
		return partitionFeed(items, workers, warmuper)
	}
	slices.SortFunc(items, compareFeed)
	warmSorted(warmuper, items)

	p := newPartitioner()
	for i := range items {
		if err := p.add(items[i].hashedKey, items[i].update); err != nil {
			return nil, nil, 0, err
		}
	}
	storage, accounts := p.done()
	return storage, accounts, p.seen, nil
}

func partitionAccounts(feed []commitment.FeedAccount, workers int) ([]storageTask, []accountEntry) {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	bounds := bucketBy(feed, func(a *commitment.FeedAccount) int { return int(a.Hash[0]) })
	var storage [256][]storageTask
	var accounts [256][]accountEntry
	parallelFor(256, workers, 1, func(b int) {
		bucket := feed[bounds[b]:bounds[b+1]]
		if len(bucket) == 0 {
			return
		}
		slices.SortFunc(bucket, func(x, y commitment.FeedAccount) int { return bytes.Compare(x.Hash[:], y.Hash[:]) })
		keys := len(bucket)
		for i := range bucket {
			keys += len(bucket[i].Slots)
		}
		nibs := make([]byte, 64*keys)
		entries := make([]storageEntry, keys-len(bucket))
		expand := func(hash *[32]byte) []byte {
			out := nibs[:64:64]
			nibs = nibs[64:]
			nibbles.Expand(hash[:], out)
			return out
		}
		p := &partitioner{accounts: make([]accountEntry, len(bucket))}
		for i := range bucket {
			fa := &bucket[i]
			p.accounts[i] = accountEntry{hashedKey: expand(&fa.Hash), update: fa.Update, storageDirty: len(fa.Slots) != 0}
			if len(fa.Slots) == 0 {
				continue
			}
			slices.SortFunc(fa.Slots, func(x, y commitment.FeedSlot) int { return bytes.Compare(x.Hash[:], y.Hash[:]) })
			task := storageTask{addrHash: fa.Hash, entries: entries[:len(fa.Slots):len(fa.Slots)]}
			entries = entries[len(fa.Slots):]
			for j := range fa.Slots {
				task.entries[j] = storageEntry{path: expand(&fa.Slots[j].Hash), value: fa.Slots[j].Value, op: storagePut}
				if len(fa.Slots[j].Value) == 0 {
					task.entries[j].op = storageDelete
				}
			}
			p.storage = append(p.storage, task)
		}
		storage[b], accounts[b] = p.done()
	})
	return slices.Concat(storage[:]...), slices.Concat(accounts[:]...)
}
