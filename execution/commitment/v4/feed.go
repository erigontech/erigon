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

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type addrPrefixCache struct {
	addr  [length.Addr]byte
	nibs  [64]byte
	valid bool
}

func (c *addrPrefixCache) hash(key []byte) []byte {
	if len(key) <= length.Addr {
		return commitment.KeyToHexNibbleHash(key)
	}
	out := make([]byte, 128)
	addr := [length.Addr]byte(key[:length.Addr])
	if c.valid && c.addr == addr {
		copy(out[:64], c.nibs[:])
	} else {
		h := keccak.Sum256(key[:length.Addr])
		nibbles.Expand(h[:], out[:64])
		c.addr = addr
		copy(c.nibs[:], out[:64])
		c.valid = true
	}
	h := keccak.Sum256(key[length.Addr:])
	nibbles.Expand(h[:], out[64:])
	return out
}

const hashParallelMin = 2048

type feedEntry struct {
	plainKey  string
	hashedKey []byte
	update    *commitment.Update
}

func hashFeed(items []feedEntry, workers int) {
	if workers <= 1 || len(items) < hashParallelMin {
		var cache addrPrefixCache
		for i := range items {
			items[i].hashedKey = cache.hash(common.ToBytesZeroCopy(items[i].plainKey))
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
			var cache addrPrefixCache
			for i := lo; i < hi; i++ {
				items[i].hashedKey = cache.hash(common.ToBytesZeroCopy(items[i].plainKey))
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

func bucketFeed(items []feedEntry) [257]int {
	bucket := func(e *feedEntry) int {
		if len(e.hashedKey) < 2 {
			return 0
		}
		return int(e.hashedKey[0])<<4 | int(e.hashedKey[1])
	}
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
	bounds := bucketFeed(items)
	var parts [256]*partitioner
	var errs [256]error
	parallelFor(256, workers, 1, func(b int) {
		bucket := items[bounds[b]:bounds[b+1]]
		slices.SortFunc(bucket, compareFeed)
		p := newPartitioner()
		for i := range bucket {
			if err := p.add(bucket[i].hashedKey, common.ToBytesZeroCopy(bucket[i].plainKey), bucket[i].update); err != nil {
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
		if err := p.add(items[i].hashedKey, common.ToBytesZeroCopy(items[i].plainKey), items[i].update); err != nil {
			return nil, nil, 0, err
		}
	}
	storage, accounts := p.done()
	return storage, accounts, p.seen, nil
}
