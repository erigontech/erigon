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
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
)

type TriePruning struct {
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

	createNodeFunc func(prefix []byte)
}

func NewTriePruning(oldestGeneration uint64) *TriePruning {
	return &TriePruning{
		oldestGeneration:  oldestGeneration,
		blockNr:           oldestGeneration,
		accountTimestamps: make(map[string]uint64),
		accounts:          make(map[uint64]map[string]struct{}),
		generationCounts:  make(map[uint64]int),
		createNodeFunc:    func([]byte) {},
	}
}

func (tp *TriePruning) SetBlockNr(blockNr uint64) {
	tp.blockNr = blockNr
}

func (tp *TriePruning) BlockNr() uint64 {
	return tp.blockNr
}

func (tp *TriePruning) SetCreateNodeFunc(f func(prefix []byte)) {
	tp.createNodeFunc = f
}

// Updates a node to the current timestamp
// contract is effectively address of the smart contract
// hex is the prefix of the key
// parent is the node that needs to be modified to unload the touched node
// exists is true when the node existed before, and false if it is a new one
// prevTimestamp is the timestamp the node current has
func (tp *TriePruning) touch(hexS string, exists bool, prevTimestamp uint64, del bool, newTimestamp uint64) {
	defer func(t time.Time) { fmt.Println("trie_pruning.go:74", time.Since(t)) }(time.Now())

	//fmt.Printf("TouchFrom %x, exists: %t, prevTimestamp %d, del %t, newTimestamp %d\n", hex, exists, prevTimestamp, del, newTimestamp)
	if exists && !del && prevTimestamp == newTimestamp {
		return
	}
	if !del {
		if !exists { // Created New node
			tp.createNodeFunc(keyNibblesToBytes([]byte(hexS)))
		}

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

func (tp *TriePruning) Timestamp(hex []byte) uint64 {
	ts := tp.accountTimestamps[string(hex)]
	return ts
}

// Updates a node to the current timestamp
// contract is effectively address of the smart contract
// hex is the prefix of the key
// parent is the node that needs to be modified to unload the touched node
func (tp *TriePruning) Touch(hex []byte, del bool) error {
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

	tp.touch(hexS, exists, prevTimestamp, del, tp.blockNr)
	return nil
}

func pruneMap(t *Trie, m map[string]struct{}, h *hasher) bool {
	hexes := make([]string, len(m))
	i := 0
	for hexS := range m {
		hexes[i] = hexS
		i++
	}
	var empty = false
	sort.Strings(hexes)
	for i, hex := range hexes {
		if i == 0 || len(hex) == 0 || !strings.HasPrefix(hex, hexes[i-1]) { // If the parent nodes are pruned, there is no need to prune descendants
			t.unload([]byte(hex), h)
			if len(hex) == 0 {
				empty = true
			}
		}
	}
	return empty
}

// Prunes all nodes that are older than given timestamp
func (tp *TriePruning) PruneToTimestamp(
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
	h := newHasher(false)
	defer returnHasherToPool(h)
	pruneMap(accountsTrie, aggregateAccounts, h)
	// Remove fom the timestamp structure
	for hexS := range aggregateAccounts {
		delete(tp.accountTimestamps, hexS)
	}
	tp.oldestGeneration = targetTimestamp
}

// Prunes mininum number of generations necessary so that the total
// number of prunable nodes is at most `targetNodeCount`
func (tp *TriePruning) PruneTo(
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
	tp.PruneToTimestamp(accountsTrie, pruneGeneration)
	return true
}

func (tp *TriePruning) NodeCount() int {
	return tp.nodeCount
}

func (tp *TriePruning) GenCounts() map[uint64]int {
	return tp.generationCounts
}

// DebugDump is used in the tests to ensure that there are no prunable entries (in such case, this function returns empty string)
func (tp *TriePruning) DebugDump() string {
	var sb strings.Builder
	for timestamp, m := range tp.accounts {
		for account := range m {
			sb.WriteString(fmt.Sprintf("%d %x\n", timestamp, account))
		}
	}
	return sb.String()
}
