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

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
)

type TrieEviction struct {
	accountTimestamps map[string]uint64

	// Maps timestamp (uint64) to set of prefixes of nodes (string)
	accounts map[uint64]map[string]struct{}

	// For each timestamp, keeps number of branch nodes belonging to it
	generationCounts map[uint64]int

	// Keeps total number of branch nodes
	nodeCount int

	// The oldest timestamp of all branch nodes
	oldestGeneration uint64

	// Current timestamp
	blockNr uint64

	createNodeFunc func(prefixAsNibbles []byte)
	unloadNodeFunc func(prefix []byte, nodeHash []byte) // called when fullNode or dualNode unloaded
}

func NewTrieEviction(oldestGeneration uint64) *TrieEviction {
	return &TrieEviction{
		oldestGeneration:  oldestGeneration,
		blockNr:           oldestGeneration,
		accountTimestamps: make(map[string]uint64),
		accounts:          make(map[uint64]map[string]struct{}),
		generationCounts:  make(map[uint64]int),
		createNodeFunc:    func([]byte) {},
	}
}

func (tp *TrieEviction) SetBlockNr(blockNr uint64) {
	tp.blockNr = blockNr
}

func (tp *TrieEviction) BlockNr() uint64 {
	return tp.blockNr
}

func (tp *TrieEviction) SetCreateNodeFunc(f func(prefixAsNibbles []byte)) {
	tp.createNodeFunc = f
}

func (tp *TrieEviction) SetEvictNodeFunc(f func(prefix []byte, nodeHash []byte)) {
	tp.unloadNodeFunc = f
}

// Updates a node to the current timestamp
// contract is effectively address of the smart contract
// hex is the prefix of the key
// parent is the node that needs to be modified to unload the touched node
// exists is true when the node existed before, and false if it is a new one
// prevTimestamp is the timestamp the node current has
func (tp *TrieEviction) touch(hexS string, exists bool, prevTimestamp uint64, del bool, newTimestamp uint64) {
	//fmt.Printf("TouchFrom %x, exists: %t, prevTimestamp %d, del %t, newTimestamp %d\n", hexS, exists, prevTimestamp, del, newTimestamp)
	if exists && !del && prevTimestamp == newTimestamp {
		return
	}
	if !del {
		var newMap map[string]struct{}
		if m, ok := tp.accounts[newTimestamp]; ok {
			newMap = m
		} else {
			newMap = make(map[string]struct{})
			tp.accounts[newTimestamp] = newMap
		}

		newMap[hexS] = struct{}{}
	}
	if exists {
		if m, ok := tp.accounts[prevTimestamp]; ok {
			delete(m, hexS)
			if len(m) == 0 {
				delete(tp.accounts, prevTimestamp)
			}
		}
	}
	// Update generation count
	if !del {
		tp.generationCounts[newTimestamp]++
		tp.nodeCount++
	}
	if exists {
		tp.generationCounts[prevTimestamp]--
		if tp.generationCounts[prevTimestamp] == 0 {
			delete(tp.generationCounts, prevTimestamp)
		}
		tp.nodeCount--
	}
}

func (tp *TrieEviction) Timestamp(hex []byte) uint64 {
	ts := tp.accountTimestamps[string(hex)]
	return ts
}

// Updates a node to the current timestamp
// contract is effectively address of the smart contract
// hex is the prefix of the key
// parent is the node that needs to be modified to unload the touched node
func (tp *TrieEviction) Touch(hex []byte, del bool) {
	var exists = false
	var prevTimestamp uint64
	hexS := string(common.CopyBytes(hex))

	if m, ok := tp.accountTimestamps[hexS]; ok {
		prevTimestamp = m
		exists = true
		if del {
			delete(tp.accountTimestamps, hexS)
		}
	}
	if !del {
		tp.accountTimestamps[hexS] = tp.blockNr
	}
	if !exists {
		tp.createNodeFunc([]byte(hexS))
	}

	tp.touch(hexS, exists, prevTimestamp, del, tp.blockNr)
}

func evictMap(t *Trie, m map[string]struct{}) bool {
	hexes := make([]string, len(m))
	i := 0
	for hexS := range m {
		hexes[i] = hexS
		i++
	}
	var empty = false
	sort.Strings(hexes)

	for i, hex := range hexes {
		if i == 0 || len(hex) == 0 || !strings.HasPrefix(hex, hexes[i-1]) { // If the parent nodes pruned, there is no need to prune descendants
			t.unload([]byte(hex))
			if len(hex) == 0 {
				empty = true
			}
		}
	}
	return empty
}

// EvictToTimestamp evicts all nodes that are older than given timestamp
func (tp *TrieEviction) EvictToTimestamp(
	accountsTrie *Trie,
	targetTimestamp uint64,
) {
	// Remove (unload) nodes from storage tries and account trie
	aggregateAccounts := make(map[string]struct{})
	for gen := tp.oldestGeneration; gen < targetTimestamp; gen++ {
		tp.nodeCount -= tp.generationCounts[gen]
		if m, ok := tp.accounts[gen]; ok {
			for hexS := range m {
				aggregateAccounts[hexS] = struct{}{}
			}
		}
		delete(tp.accounts, gen)
	}

	// intermediate hashes
	key := pool.GetBuffer(64)
	defer pool.PutBuffer(key)
	for prefix := range aggregateAccounts {
		if len(prefix) == 0 || len(prefix)%2 == 1 {
			continue
		}

		nd, parent, ok := accountsTrie.getNode([]byte(prefix), false)
		if !ok {
			continue
		}
		switch nd.(type) {
		case *duoNode, *fullNode:
			// will work only with these types of nodes
		default:
			continue
		}
		switch parent.(type) { // without this condition - doesn't work. Need investigate why.
		case *duoNode, *fullNode:
			// will work only with these types of nodes
			CompressNibbles([]byte(prefix), &key.B)
			tp.unloadNodeFunc(key.B, nd.reference())
		default:
			continue
		}
	}

	evictMap(accountsTrie, aggregateAccounts)

	// Remove fom the timestamp structure
	for hexS := range aggregateAccounts {
		delete(tp.accountTimestamps, hexS)
	}
	tp.oldestGeneration = targetTimestamp
}

// EvictTo evicts mininum number of generations necessary so that the total
// number of prunable nodes is at most `targetNodeCount`
func (tp *TrieEviction) EvictTo(
	accountsTrie *Trie,
	targetNodeCount int,
) bool {
	if tp.nodeCount <= targetNodeCount {
		return false
	}
	excess := tp.nodeCount - targetNodeCount
	prunable := 0
	pruneGeneration := tp.oldestGeneration
	for prunable < excess && pruneGeneration < tp.blockNr {
		prunable += tp.generationCounts[pruneGeneration]
		pruneGeneration++
	}
	//fmt.Printf("Will prune to generation %d, nodes to prune: %d, excess %d\n", pruneGeneration, prunable, excess)
	tp.EvictToTimestamp(accountsTrie, pruneGeneration)
	return true
}

func (tp *TrieEviction) NodeCount() int {
	return tp.nodeCount
}

func (tp *TrieEviction) GenCounts() map[uint64]int {
	return tp.generationCounts
}

// DebugDump is used in the tests to ensure that there are no prunable entries (in such case, this function returns empty string)
func (tp *TrieEviction) DebugDump() string {
	var sb strings.Builder
	for timestamp, m := range tp.accounts {
		for account := range m {
			sb.WriteString(fmt.Sprintf("%d %x\n", timestamp, account))
		}
	}
	return sb.String()
}
