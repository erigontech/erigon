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

package eip8297

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common"
)

var ErrParallelStreamRootWorkers = errors.New("eip8297: parallel stream root worker count is not positive")

type streamRootSubtree struct {
	firstBits []byte
	branches  []streamRootBranch
	lastLeaf  common.Hash
	direct    bool
	hasLeaf   bool
}

type parallelStreamRootLeaf struct {
	keyStart    int
	keyLength   int
	valueStart  int
	valueLength int
}

type parallelStreamRootBatch struct {
	shard  int
	arena  []byte
	leaves []parallelStreamRootLeaf
}

type ParallelStreamRoot struct {
	sum         HashFn
	workers     int
	inputs      chan *parallelStreamRootBatch
	parts       []map[int]streamRootSubtree
	workerErrs  []error
	wg          sync.WaitGroup
	closed      bool
	lastShard   int
	hasShard    bool
	prevKey     []byte
	prevBits    []byte
	err         error
	root        common.Hash
	rootReady   bool
	current     *parallelStreamRootBatch
	primed      bool
	buildActive *atomic.Int32
	buildMax    *atomic.Int32
	buildStart  func()
}

func NewParallelStreamRoot(sum HashFn, workers int) (*ParallelStreamRoot, error) {
	if sum == nil {
		return nil, ErrStreamRootHash
	}
	if workers <= 0 {
		return nil, ErrParallelStreamRootWorkers
	}
	b := &ParallelStreamRoot{
		sum:        sum,
		workers:    workers,
		inputs:     make(chan *parallelStreamRootBatch, workers+2),
		parts:      make([]map[int]streamRootSubtree, workers),
		workerErrs: make([]error, workers),
	}
	for worker := range workers {
		b.parts[worker] = make(map[int]streamRootSubtree)
		b.wg.Add(1)
		go b.run(worker)
	}
	return b, nil
}

func (b *ParallelStreamRoot) Add(key, value []byte) error {
	return b.AddContext(context.Background(), key, value)
}

func (b *ParallelStreamRoot) AddContext(ctx context.Context, key, value []byte) error {
	if b.err != nil {
		return b.err
	}
	if b.closed {
		return errors.New("eip8297: parallel stream root is closed")
	}
	if err := validateStreamRootLeaf(key, value); err != nil {
		b.err = err
		return err
	}
	bits := bytesToBits(key)
	if b.hasShard && bytes.Compare(key, b.prevKey) <= 0 {
		b.err = errors.New("eip8297: stream keys are not strictly ascending")
		return b.err
	}
	if b.hasShard {
		commonBits := streamRootCommonPrefix(b.prevBits, bits)
		if commonBits == len(b.prevBits) || commonBits == len(bits) {
			b.err = errors.New("eip8297: stream keys are not prefix-free")
			return nil
		}
	}
	shard := streamRootShard(key)
	if b.hasShard && shard < b.lastShard {
		b.err = errors.New("eip8297: stream keys are not strictly ascending")
		return b.err
	}
	if b.current == nil || b.current.shard != shard {
		if err := b.flush(ctx); err != nil {
			return err
		}
		b.current = &parallelStreamRootBatch{shard: shard, arena: make([]byte, 0, len(key)+len(value)), leaves: make([]parallelStreamRootLeaf, 0, 1024)}
	}
	keyStart := len(b.current.arena)
	b.current.arena = append(b.current.arena, key...)
	valueStart := len(b.current.arena)
	b.current.arena = append(b.current.arena, value...)
	b.current.leaves = append(b.current.leaves, parallelStreamRootLeaf{keyStart: keyStart, keyLength: len(key), valueStart: valueStart, valueLength: len(value)})
	b.prevKey = append(b.prevKey[:0], key...)
	b.prevBits = append(b.prevBits[:0], bits...)
	b.lastShard = shard
	b.hasShard = true
	if b.workers == 1 && !b.primed {
		probe := &parallelStreamRootBatch{shard: b.current.shard, arena: slices.Clone(b.current.arena), leaves: slices.Clone(b.current.leaves)}
		select {
		case b.inputs <- probe:
			b.primed = true
		case <-ctx.Done():
			b.err = ctx.Err()
			return b.err
		}
	}
	return nil
}

func (b *ParallelStreamRoot) SetBuildConcurrencyCounters(active, max *atomic.Int32) {
	b.buildActive = active
	b.buildMax = max
}

func (b *ParallelStreamRoot) SetBuildStartHook(hook func()) {
	b.buildStart = hook
}

func (b *ParallelStreamRoot) flush(ctx context.Context) error {
	if b.current == nil {
		return nil
	}
	batch := b.current
	b.current = nil
	select {
	case b.inputs <- batch:
		return nil
	case <-ctx.Done():
		b.err = ctx.Err()
		return b.err
	}
}

func (b *ParallelStreamRoot) RootHash() (common.Hash, error) {
	if b.rootReady {
		return b.root, b.err
	}
	if b.closed {
		return common.Hash{}, b.err
	}
	b.closed = true
	if b.err == nil {
		_ = b.flush(context.Background())
	}
	close(b.inputs)
	b.wg.Wait()
	if b.err != nil {
		return common.Hash{}, b.err
	}
	for _, err := range b.workerErrs {
		if err != nil {
			b.err = err
			return common.Hash{}, err
		}
	}
	var nodes [1 << pbinStreamRootPartitionBits]streamRootSubtree
	for worker := range b.parts {
		for shard, subtree := range b.parts[worker] {
			nodes[shard] = subtree
		}
	}
	for level := pbinStreamRootPartitionBits - 1; level >= 0; level-- {
		groupSize := 1 << (pbinStreamRootPartitionBits - level)
		half := groupSize / 2
		for base := 0; base < len(nodes); base += groupSize {
			nodes[base] = combineStreamRootSubtrees(b.sum, level, nodes[base], nodes[base+half])
		}
	}
	root := nodes[0]
	if len(root.branches) == 0 && !root.hasLeaf {
		b.root = EmptyTreeHash
	} else {
		b.root = root.hashAt(b.sum, -1)
	}
	b.rootReady = true
	return b.root, nil
}

func (b *ParallelStreamRoot) run(worker int) {
	defer b.wg.Done()
	for batch := range b.inputs {
		if b.workerErrs[worker] != nil {
			return
		}
		active := int32(0)
		if b.buildActive != nil {
			active = b.buildActive.Add(1)
			if b.buildMax != nil {
				for max := b.buildMax.Load(); active > max && !b.buildMax.CompareAndSwap(max, active); max = b.buildMax.Load() {
				}
			}
		}
		if b.buildStart != nil {
			b.buildStart()
		}
		builder, err := NewStreamRootBuilder(b.sum)
		if err == nil {
			for _, leaf := range batch.leaves {
				key := batch.arena[leaf.keyStart : leaf.keyStart+leaf.keyLength]
				value := batch.arena[leaf.valueStart : leaf.valueStart+leaf.valueLength]
				if err = builder.Add(key, value); err != nil {
					break
				}
			}
		}
		if err != nil {
			if b.buildActive != nil {
				b.buildActive.Add(-1)
			}
			b.workerErrs[worker] = err
			return
		}
		if b.buildActive != nil {
			b.buildActive.Add(-1)
		}
		b.parts[worker][batch.shard] = builder.subtree()
	}
}

func streamRootShard(key []byte) int {
	shard := int(key[0]) << 4
	if len(key) > 1 {
		shard |= int(key[1] >> 4)
	}
	return shard
}

func combineStreamRootSubtrees(sum HashFn, level int, left, right streamRootSubtree) streamRootSubtree {
	if len(left.branches) == 0 && !left.hasLeaf {
		return right
	}
	if len(right.branches) == 0 && !right.hasLeaf {
		return left
	}
	return streamRootSubtree{
		firstBits: left.firstBits,
		branches: []streamRootBranch{{
			split: level,
			left:  left.hashAt(sum, level),
			right: right.hashAt(sum, level),
		}},
		direct: true,
	}
}

func (s streamRootSubtree) hashAt(sum HashFn, parentDepth int) common.Hash {
	if len(s.branches) == 0 {
		return s.lastLeaf
	}
	if s.direct {
		branch := s.branches[0]
		prefix := s.firstBits[parentDepth+1 : branch.split]
		return hashStreamRootBranch(sum, prefix, branch.left, branch.right)
	}
	root := s.lastLeaf
	for i, branch := range slices.Backward(s.branches) {
		prefix := branch.prefix
		if i == 0 {
			prefix = s.firstBits[parentDepth+1 : branch.split]
		}
		root = hashStreamRootBranch(sum, prefix, branch.left, root)
	}
	return root
}

func hashStreamRootBranch(sum HashFn, prefix []byte, left, right common.Hash) common.Hash {
	preimage := make([]byte, 0, 1+2+(len(prefix)+7)/8+2*len(left))
	preimage = append(preimage, BranchTag)
	preimage = append(preimage, EncodeBitPrefix(prefix)...)
	preimage = append(preimage, left[:]...)
	preimage = append(preimage, right[:]...)
	return common.Hash(sum(preimage))
}

const pbinStreamRootPartitionBits = 12
