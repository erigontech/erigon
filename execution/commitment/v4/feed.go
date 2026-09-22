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
	slices.SortFunc(items, func(a, b feedEntry) int {
		if c := bytes.Compare(a.hashedKey, b.hashedKey); c != 0 {
			return c
		}
		return strings.Compare(a.plainKey, b.plainKey)
	})

	p := newPartitioner()
	var prevKey []byte
	for i := range items {
		if warmuper != nil {
			hk := items[i].hashedKey
			startDepth := 0
			if prevKey != nil {
				minLen := min(len(prevKey), len(hk))
				for startDepth < minLen && prevKey[startDepth] == hk[startDepth] {
					startDepth++
				}
			}
			warmuper.WarmKey(hk, startDepth, 0)
			prevKey = hk
		}
		if err := p.add(items[i].hashedKey, common.ToBytesZeroCopy(items[i].plainKey), items[i].update); err != nil {
			return nil, nil, 0, err
		}
	}
	storage, accounts := p.done()
	return storage, accounts, p.seen, nil
}
