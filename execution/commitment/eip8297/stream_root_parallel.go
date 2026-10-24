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
	"context"
	"errors"
	"sync"

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
	shard int
	key   []byte
	value []byte
}

type ParallelStreamRoot struct {
	sum        HashFn
	workers    int
	inputs     []chan parallelStreamRootLeaf
	parts      []map[int]streamRootSubtree
	workerErrs []error
	wg         sync.WaitGroup
	closed     bool
	lastShard  int
	hasShard   bool
	err        error
	root       common.Hash
	rootReady  bool
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
		inputs:     make([]chan parallelStreamRootLeaf, workers),
		parts:      make([]map[int]streamRootSubtree, workers),
		workerErrs: make([]error, workers),
	}
	for worker := range workers {
		b.inputs[worker] = make(chan parallelStreamRootLeaf, 1024)
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
	shard := streamRootShard(key)
	if b.hasShard && shard < b.lastShard {
		b.err = errors.New("eip8297: stream keys are not strictly ascending")
		return b.err
	}
	b.lastShard = shard
	b.hasShard = true
	select {
	case b.inputs[shard%b.workers] <- parallelStreamRootLeaf{shard: shard, key: key, value: value}:
	case <-ctx.Done():
		b.err = ctx.Err()
		return b.err
	}
	return nil
}

func (b *ParallelStreamRoot) RootHash() (common.Hash, error) {
	if b.rootReady {
		return b.root, b.err
	}
	if b.closed {
		return common.Hash{}, b.err
	}
	b.closed = true
	for _, input := range b.inputs {
		close(input)
	}
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
	builders := make(map[int]*StreamRootBuilder)
	for leaf := range b.inputs[worker] {
		builder := builders[leaf.shard]
		if builder == nil {
			var err error
			builder, err = NewStreamRootBuilder(b.sum)
			if err != nil {
				b.workerErrs[worker] = err
				continue
			}
			builders[leaf.shard] = builder
		}
		if err := builder.Add(leaf.key, leaf.value); err != nil {
			b.workerErrs[worker] = err
		}
	}
	for shard, builder := range builders {
		if b.workerErrs[worker] != nil {
			return
		}
		b.parts[worker][shard] = builder.subtree()
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
	for i := len(s.branches) - 1; i >= 0; i-- {
		branch := s.branches[i]
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
