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
	"unsafe"

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
	shard         int
	arena         []byte
	leaves        []parallelStreamRootLeaf
	residentBytes uint64
	queueTokens   int
}

type ParallelStreamRoot struct {
	sum          HashFn
	workers      int
	inputs       []chan *parallelStreamRootBatch
	queueTokens  chan struct{}
	stopped      chan struct{}
	stopOnce     sync.Once
	parts        []map[int]streamRootSubtree
	workerErrs   []error
	workerErrMu  sync.Mutex
	workerFailed atomic.Bool
	wg           sync.WaitGroup
	closed       bool
	lastShard    int
	hasShard     bool
	prevKey      []byte
	prevBits     []byte
	err          error
	root         common.Hash
	rootReady    bool
	current      *parallelStreamRootBatch
}

func NewParallelStreamRoot(sum HashFn, workers int) (*ParallelStreamRoot, error) {
	return newParallelStreamRoot(sum, workers, parallelStreamRootQueueBytes)
}

func newParallelStreamRoot(sum HashFn, workers int, queueBytes uint64) (*ParallelStreamRoot, error) {
	if sum == nil {
		return nil, ErrStreamRootHash
	}
	if workers <= 0 {
		return nil, ErrParallelStreamRootWorkers
	}
	queueTokenCount := int(queueBytes / parallelStreamRootQueueTokenBytes)
	if queueTokenCount == 0 {
		return nil, errors.New("eip8297: parallel stream root queue budget is too small")
	}
	b := &ParallelStreamRoot{
		sum:         sum,
		workers:     workers,
		inputs:      make([]chan *parallelStreamRootBatch, workers),
		queueTokens: make(chan struct{}, queueTokenCount),
		stopped:     make(chan struct{}),
		parts:       make([]map[int]streamRootSubtree, workers),
		workerErrs:  make([]error, workers),
	}
	for range cap(b.queueTokens) {
		b.queueTokens <- struct{}{}
	}
	for worker := range workers {
		b.inputs[worker] = make(chan *parallelStreamRootBatch, parallelStreamRootChannelBatches)
		b.parts[worker] = make(map[int]streamRootSubtree)
		b.wg.Add(1)
		go b.run(worker)
	}
	return b, nil
}

func (b *ParallelStreamRoot) AddContext(ctx context.Context, key, value []byte) error {
	if b.err != nil {
		return b.err
	}
	if b.workerFailed.Load() {
		err := b.workerError()
		b.err = err
		return err
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
			return b.err
		}
	}
	shard := streamRootShard(key)
	if b.hasShard && shard < b.lastShard {
		b.err = errors.New("eip8297: stream keys are not strictly ascending")
		return b.err
	}
	if b.current == nil || b.current.shard != shard || !b.current.canAppend(key, value) {
		if err := b.flush(ctx); err != nil {
			return err
		}
		b.current = newParallelStreamRootBatch(shard)
		b.current.residentBytes = b.current.capacityBytes()
	}
	keyStart := len(b.current.arena)
	b.current.arena = append(b.current.arena, key...)
	valueStart := len(b.current.arena)
	b.current.arena = append(b.current.arena, value...)
	b.current.leaves = append(b.current.leaves, parallelStreamRootLeaf{keyStart: keyStart, keyLength: len(key), valueStart: valueStart, valueLength: len(value)})
	b.current.residentBytes = max(b.current.residentBytes, b.current.capacityBytes())
	b.prevKey = append(b.prevKey[:0], key...)
	b.prevBits = append(b.prevBits[:0], bits...)
	b.lastShard = shard
	b.hasShard = true
	return nil
}

func (b *ParallelStreamRoot) flush(ctx context.Context) error {
	if b.current == nil {
		return nil
	}
	batch := b.current
	b.current = nil
	tokens, err := b.acquireQueueTokens(ctx, batch.residentBytes)
	if err != nil {
		b.releaseBatch(batch)
		b.err = err
		return err
	}
	batch.queueTokens = tokens
	input := b.inputs[streamRootWorker(batch.shard, b.workers)]
	select {
	case input <- batch:
		return nil
	case <-b.stopped:
		b.releaseBatch(batch)
		err := b.workerError()
		b.err = err
		return err
	case <-ctx.Done():
		b.releaseBatch(batch)
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
	} else if b.current != nil {
		b.releaseBatch(b.current)
		b.current = nil
	}
	for _, input := range b.inputs {
		close(input)
	}
	b.wg.Wait()
	if b.err != nil {
		return common.Hash{}, b.err
	}
	if err := b.workerError(); err != nil {
		b.err = err
		return common.Hash{}, err
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
	input := b.inputs[worker]
	var builder *StreamRootBuilder
	currentShard := -1
	for {
		var batch *parallelStreamRootBatch
		var ok bool
		select {
		case batch, ok = <-input:
			if !ok {
				if builder != nil {
					b.parts[worker][currentShard] = builder.subtree()
				}
				return
			}
		case <-b.stopped:
			return
		}
		if b.workerFailed.Load() {
			b.releaseBatch(batch)
			return
		}
		var err error
		if builder == nil || currentShard != batch.shard {
			if builder != nil {
				b.parts[worker][currentShard] = builder.subtree()
			}
			builder, err = NewStreamRootBuilder(b.sum)
			currentShard = batch.shard
		}
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
			b.releaseBatch(batch)
			b.workerErrMu.Lock()
			b.workerErrs[worker] = err
			b.workerErrMu.Unlock()
			b.workerFailed.Store(true)
			b.stopOnce.Do(func() { close(b.stopped) })
			return
		}
		b.releaseBatch(batch)
	}
}

func (b *ParallelStreamRoot) releaseBatch(batch *parallelStreamRootBatch) {
	for range batch.queueTokens {
		b.queueTokens <- struct{}{}
	}
	batch.queueTokens = 0
}

func (b *parallelStreamRootBatch) capacityBytes() uint64 {
	return uint64(cap(b.arena)) + uint64(cap(b.leaves))*uint64(unsafe.Sizeof(parallelStreamRootLeaf{}))
}

func newParallelStreamRootBatch(shard int) *parallelStreamRootBatch {
	return &parallelStreamRootBatch{
		shard:  shard,
		arena:  make([]byte, 0, parallelStreamRootBatchBytes),
		leaves: make([]parallelStreamRootLeaf, 0, parallelStreamRootBatchBytes/int(unsafe.Sizeof(parallelStreamRootLeaf{}))),
	}
}

func (b *parallelStreamRootBatch) canAppend(key, value []byte) bool {
	return len(b.arena)+len(key)+len(value) <= cap(b.arena) && len(b.leaves) < cap(b.leaves)
}

func (b *ParallelStreamRoot) acquireQueueTokens(ctx context.Context, bytes uint64) (int, error) {
	tokens := int((bytes + parallelStreamRootQueueTokenBytes - 1) / parallelStreamRootQueueTokenBytes)
	if tokens > cap(b.queueTokens) {
		return 0, errors.New("eip8297: parallel stream root batch exceeds queue budget")
	}
	acquired := 0
	for acquired < tokens {
		select {
		case <-b.queueTokens:
			acquired++
		case <-b.stopped:
			for range acquired {
				b.queueTokens <- struct{}{}
			}
			return 0, b.workerError()
		case <-ctx.Done():
			for range acquired {
				b.queueTokens <- struct{}{}
			}
			return 0, ctx.Err()
		}
	}
	return acquired, nil
}

func (b *ParallelStreamRoot) workerError() error {
	b.workerErrMu.Lock()
	defer b.workerErrMu.Unlock()
	for _, err := range b.workerErrs {
		if err != nil {
			return err
		}
	}
	return nil
}

func streamRootShard(key []byte) int {
	shard := int(key[0]) << 4
	if len(key) > 1 {
		shard |= int(key[1] >> 4)
	}
	return shard
}

func streamRootWorker(shard, workers int) int {
	return shard % workers
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

const (
	parallelStreamRootBatchBytes         = 4 * 1024 * 1024
	parallelStreamRootBatchResidentBytes = parallelStreamRootBatchBytes + parallelStreamRootBatchBytes
	parallelStreamRootChannelBatches     = 1024
	parallelStreamRootQueueBytes         = 4 * 1024 * 1024 * 1024
	parallelStreamRootQueueTokenBytes    = parallelStreamRootBatchResidentBytes
	pbinStreamRootPartitionBits          = 12
)
