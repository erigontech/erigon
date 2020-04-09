// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty off
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Pruning of the Merkle Patricia trees

package trie

import (
	"fmt"
	"sort"
	"strings"
)

type AccountEvicter interface {
	EvictNode([]byte)
}

type generations struct {
	blockNumToGeneration map[uint64]*generation
	keyToBlockNum        map[string]uint64
	oldestBlockNum       uint64
	totalSize            int64
}

func newGenerations() *generations {
	return &generations{
		make(map[uint64]*generation),
		make(map[string]uint64),
		0,
		0,
	}
}

func (gs *generations) add(blockNum uint64, key []byte, size uint) {
	if _, ok := gs.keyToBlockNum[string(key)]; ok {
		gs.updateSize(blockNum, key, size)
		return
	}

	generation, ok := gs.blockNumToGeneration[blockNum]
	if !ok {
		generation = newGeneration()
		gs.blockNumToGeneration[blockNum] = generation
	}
	generation.add(key, size)
	gs.keyToBlockNum[string(key)] = blockNum
	if gs.oldestBlockNum > blockNum {
		gs.oldestBlockNum = blockNum
	}
	gs.totalSize += int64(size)
}

func (gs *generations) touch(blockNum uint64, key []byte) {
	if len(gs.blockNumToGeneration) == 0 {
		return
	}

	oldBlockNum, ok := gs.keyToBlockNum[string(key)]
	if !ok {
		return
	}

	oldGeneration, ok := gs.blockNumToGeneration[oldBlockNum]
	if !ok {
		return
	}

	currentGeneration, ok := gs.blockNumToGeneration[blockNum]
	if !ok {
		currentGeneration = newGeneration()
		gs.blockNumToGeneration[blockNum] = currentGeneration
	}

	currentGeneration.grabFrom(key, oldGeneration)

	gs.keyToBlockNum[string(key)] = blockNum

	if gs.oldestBlockNum > blockNum {
		gs.oldestBlockNum = blockNum
	}

	if oldGeneration.empty() {
		delete(gs.blockNumToGeneration, oldBlockNum)
	}
}

func (gs *generations) remove(key []byte) {
	oldBlockNum, ok := gs.keyToBlockNum[string(key)]
	if !ok {
		return
	}
	generation, ok := gs.blockNumToGeneration[oldBlockNum]
	if !ok {
		return
	}
	sizeDiff := generation.remove(key)
	gs.totalSize += sizeDiff
	delete(gs.keyToBlockNum, string(key))
}

func (gs *generations) updateSize(blockNum uint64, key []byte, newSize uint) {
	oldBlockNum, ok := gs.keyToBlockNum[string(key)]
	if !ok {
		gs.add(blockNum, key, newSize)
		return
	}
	generation, ok := gs.blockNumToGeneration[oldBlockNum]
	if !ok {
		gs.add(blockNum, key, newSize)
		return
	}

	sizeDiff := generation.updateAccountSize(key, newSize)
	gs.totalSize += sizeDiff
	gs.touch(blockNum, key)
}

// popKeysToEvict returns the keys to evict from the trie,
// also removing them from generations
func (gs *generations) popKeysToEvict(threshold uint64) []string {
	keys := make([]string, 0)
	for uint64(gs.totalSize) > threshold && len(gs.blockNumToGeneration) > 0 {
		generation, ok := gs.blockNumToGeneration[gs.oldestBlockNum]
		if !ok {
			gs.oldestBlockNum++
			continue
		}

		gs.totalSize -= generation.totalSize
		if gs.totalSize < 0 {
			gs.totalSize = 0
		}
		keysToEvict := generation.keys()
		keys = append(keys, keysToEvict...)
		for _, k := range keysToEvict {
			delete(gs.keyToBlockNum, k)
		}
		delete(gs.blockNumToGeneration, gs.oldestBlockNum)
		gs.oldestBlockNum++
	}
	return keys
}

type generation struct {
	sizesByKey map[string]uint
	totalSize  int64
}

func newGeneration() *generation {
	return &generation{
		make(map[string]uint),
		0,
	}
}

func (g *generation) empty() bool {
	return len(g.sizesByKey) == 0
}

func (g *generation) grabFrom(key []byte, other *generation) {
	if g == other {
		return
	}

	keyStr := string(key)
	size, ok := other.sizesByKey[keyStr]
	if !ok {
		return
	}

	g.sizesByKey[keyStr] = size
	g.totalSize += int64(size)
	other.totalSize -= int64(size)

	if other.totalSize < 0 {
		other.totalSize = 0
	}

	delete(other.sizesByKey, keyStr)
}

func (g *generation) add(key []byte, size uint) {
	g.sizesByKey[string(key)] = size
	g.totalSize += int64(size)
}

func (g *generation) updateAccountSize(key []byte, size uint) int64 {
	oldSize := g.sizesByKey[string(key)]
	g.sizesByKey[string(key)] = size
	diff := int64(size) - int64(oldSize)
	g.totalSize += diff
	return diff
}

func (g *generation) remove(key []byte) int64 {
	oldSize := g.sizesByKey[string(key)]
	delete(g.sizesByKey, string(key))
	g.totalSize -= int64(oldSize)
	if g.totalSize < 0 {
		g.totalSize = 0
	}

	return -1 * int64(oldSize)
}

func (g *generation) keys() []string {
	keys := make([]string, len(g.sizesByKey))
	i := 0
	for k := range g.sizesByKey {
		keys[i] = k
		i++
	}
	return keys
}

type Eviction struct {
	NoopObserver // make sure that we don't need to implement unnecessary observer methods

	blockNumber uint64

	generations *generations
}

func NewEviction() *Eviction {
	return &Eviction{
		generations: newGenerations(),
	}
}

func (tp *Eviction) SetBlockNumber(blockNumber uint64) {
	tp.blockNumber = blockNumber
}

func (tp *Eviction) BlockNumber() uint64 {
	return tp.blockNumber
}

func (tp *Eviction) BranchNodeCreated(hex []byte) {
	key := hex
	tp.generations.add(tp.blockNumber, key, 1)
}

func (tp *Eviction) BranchNodeDeleted(hex []byte) {
	key := hex
	tp.generations.remove(key)
}

func (tp *Eviction) BranchNodeTouched(hex []byte) {
	key := hex
	tp.generations.touch(tp.blockNumber, key)
}

func (tp *Eviction) CodeNodeCreated(hex []byte, size uint) {
	key := hex
	tp.generations.add(tp.blockNumber, CodeKeyFromAddrHash(key), size)
}

func (tp *Eviction) CodeNodeDeleted(hex []byte) {
	key := hex
	tp.generations.remove(CodeKeyFromAddrHash(key))
}

func (tp *Eviction) CodeNodeTouched(hex []byte) {
	key := hex
	tp.generations.touch(tp.blockNumber, CodeKeyFromAddrHash(key))
}

func (tp *Eviction) CodeNodeSizeChanged(hex []byte, newSize uint) {
	key := hex
	tp.generations.updateSize(tp.blockNumber, CodeKeyFromAddrHash(key), newSize)
}

func evictList(evicter AccountEvicter, hexes []string) bool {
	var empty = false
	sort.Strings(hexes)

	// from long to short -- a naive way to first clean up nodes and then accounts
	// FIXME: optimize to avoid the same paths
	for i := len(hexes) - 1; i >= 0; i-- {
		evicter.EvictNode([]byte(hexes[i]))
	}
	return empty
}

// EvictToFitSize evicts mininum number of generations necessary so that the total
// size of accounts left is fits into the provided threshold
func (tp *Eviction) EvictToFitSize(
	evicter AccountEvicter,
	threshold uint64,
) bool {

	if uint64(tp.generations.totalSize) <= threshold {
		return false
	}

	keys := tp.generations.popKeysToEvict(threshold)

	return evictList(evicter, keys)
}

func (tp *Eviction) TotalSize() uint64 {
	return uint64(tp.generations.totalSize)
}

func (tp *Eviction) NumberOf() uint64 {
	total := uint64(0)
	for _, gen := range tp.generations.blockNumToGeneration {
		if gen == nil {
			continue
		}
		total += uint64(len(gen.sizesByKey))
	}
	return total
}

func (tp *Eviction) DebugDump() string {
	var sb strings.Builder

	for block, gen := range tp.generations.blockNumToGeneration {
		if gen.empty() {
			continue
		}
		sb.WriteString(fmt.Sprintf("Block: %v\n", block))
		for key, size := range gen.sizesByKey {
			sb.WriteString(fmt.Sprintf("    %x->%v\n", key, size))
		}
	}

	return sb.String()
}
