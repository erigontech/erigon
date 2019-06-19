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
	"sort"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
)

type TriePruning struct {
	storageTimestamps map[common.Address]map[string]uint64
	accountTimestamps map[string]uint64

	// Maps timestamp (uint64) to address of the contract to set of prefixes of nodes (string)
	storage map[uint64]map[common.Address]map[string]struct{}

	// Maps timestamp (uint64) to set of prefixes of nodees (string)
	accounts map[uint64]map[string]struct{}

	// For each timestamp, keeps number of branch nodes belonging to it
	generationCounts map[uint64]int

	// Keeps total number of branch nodes
	nodeCount int

	// The oldest timestamp of all branch nodes
	oldestGeneration uint64

	// Current timestamp
	blockNr uint64
}

func NewTriePruning(oldestGeneration uint64) (*TriePruning, error) {
	return &TriePruning{
		oldestGeneration:  oldestGeneration,
		blockNr:           oldestGeneration,
		storageTimestamps: make(map[common.Address]map[string]uint64),
		accountTimestamps: make(map[string]uint64),
		storage:           make(map[uint64]map[common.Address]map[string]struct{}),
		accounts:          make(map[uint64]map[string]struct{}),
		generationCounts:  make(map[uint64]int),
	}, nil
}

func (tp *TriePruning) SetBlockNr(blockNr uint64) {
	tp.blockNr = blockNr
}

func (tp *TriePruning) BlockNr() uint64 {
	return tp.blockNr
}

// Updates a node to the current timestamp
// contract is effectively address of the smart contract
// hex is the prefix of the key
// parent is the node that needs to be modified to unload the touched node
// exists is true when the node existed before, and false if it is a new one
// prevTimestamp is the timestamp the node current has
func (tp *TriePruning) touchContract(contract common.Address, hexS string, exists bool, prevTimestamp uint64, del bool, newTimestamp uint64) {
	if exists && !del && prevTimestamp == newTimestamp {
		return
	}
	if !del {
		var newMap map[string]struct{}
		if m, ok := tp.storage[newTimestamp]; ok {
			if m1, ok1 := m[contract]; ok1 {
				newMap = m1
			} else {
				newMap = make(map[string]struct{})
				m[contract] = newMap
			}
		} else {
			m = make(map[common.Address]map[string]struct{})
			newMap = make(map[string]struct{})
			m[contract] = newMap
			tp.storage[newTimestamp] = m
		}
		newMap[hexS] = struct{}{}
	}
	if exists {
		if m, ok := tp.storage[prevTimestamp]; ok {
			if m1, ok1 := m[contract]; ok1 {
				delete(m1, hexS)
				if len(m1) == 0 {
					delete(m, contract)
					if len(m) == 0 {
						delete(tp.storage, prevTimestamp)
					}
				}
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

// Updates a node to the current timestamp
// contract is effectively address of the smart contract
// hex is the prefix of the key
// parent is the node that needs to be modified to unload the touched node
// exists is true when the node existed before, and false if it is a new one
// prevTimestamp is the timestamp the node current has
func (tp *TriePruning) touch(hexS string, exists bool, prevTimestamp uint64, del bool, newTimestamp uint64) {
	//fmt.Printf("TouchFrom %x, exists: %t, prevTimestamp %d, del %t, newTimestamp %d\n", hex, exists, prevTimestamp, del, newTimestamp)
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

func (tp *TriePruning) Timestamp(hex []byte) uint64 {
	return tp.accountTimestamps[string(hex)]
}

// Returns timestamp for the given prunable node
func (tp *TriePruning) TimestampContract(contract common.Address, hex []byte) uint64 {
	if m, ok := tp.storageTimestamps[contract]; ok {
		return m[string(hex)]
	}
	return 0
}

func (tp *TriePruning) TouchContract(contract common.Address, hex []byte, del bool) {
	var exists = false
	var prevTimestamp uint64
	hexS := string(common.CopyBytes(hex))
	if m, ok := tp.storageTimestamps[contract]; ok {
		if m1, ok1 := m[hexS]; ok1 {
			prevTimestamp = m1
			exists = true
			if del {
				delete(m, hexS)
				if len(m) == 0 {
					delete(tp.storageTimestamps, contract)
				}
			}
		}
		if !del {
			m[hexS] = tp.blockNr
		}
	} else if !del {
		m = make(map[string]uint64)
		tp.storageTimestamps[contract] = m
		m[hexS] = tp.blockNr
	}
	tp.touchContract(contract, hexS, exists, prevTimestamp, del, tp.blockNr)
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
	mainTrie *Trie,
	targetTimestamp uint64,
	storageTrieFunc func(contract common.Address) (*Trie, error),
) ([]common.Address, error) {
	// Remove (unload) nodes from storage tries and account trie
	aggregateStorage := make(map[common.Address]map[string]struct{})
	aggregateAccounts := make(map[string]struct{})
	for gen := tp.oldestGeneration; gen < targetTimestamp; gen++ {
		tp.nodeCount -= tp.generationCounts[gen]
		delete(tp.generationCounts, gen)
		if m, ok := tp.storage[gen]; ok {
			for address, m1 := range m {
				var aggregateM map[string]struct{}
				if m2, ok2 := aggregateStorage[address]; ok2 {
					aggregateM = m2
				} else {
					aggregateM = make(map[string]struct{})
					aggregateStorage[address] = aggregateM
				}
				for hexS := range m1 {
					aggregateM[hexS] = struct{}{}
				}
			}
		}
		delete(tp.storage, gen)
		if m, ok := tp.accounts[gen]; ok {
			for hexS := range m {
				aggregateAccounts[hexS] = struct{}{}
			}
		}
		delete(tp.accounts, gen)
	}
	var emptyAddresses []common.Address
	h := newHasher(true) // Create hasher appropriate for storage tries first
	defer returnHasherToPool(h)
	for address, m := range aggregateStorage {
		storageTrie, err := storageTrieFunc(address)
		if err != nil {
			return nil, err
		}
		empty := pruneMap(storageTrie, m, h)
		if empty {
			emptyAddresses = append(emptyAddresses, address)
		}
	}
	// Change hasher to be appropriate for the main trie
	h.encodeToBytes = false
	pruneMap(mainTrie, aggregateAccounts, h)
	// Remove fom the timestamp structure
	for hexS := range aggregateAccounts {
		delete(tp.accountTimestamps, hexS)
	}
	for address, m := range aggregateStorage {
		if m1, ok := tp.storageTimestamps[address]; ok {
			for hexS := range m {
				delete(m1, hexS)
			}
			if len(m1) == 0 {
				delete(tp.storageTimestamps, address)
			}
		}
	}
	tp.oldestGeneration = targetTimestamp
	return emptyAddresses, nil
}

// Prunes mininum number of generations necessary so that the total
// number of prunable nodes is at most `targetNodeCount`
func (tp *TriePruning) PruneTo(
	mainTrie *Trie,
	targetNodeCount int,
	storageTrieFunc func(contract common.Address) (*Trie, error),
) (bool, []common.Address, error) {
	if tp.nodeCount <= targetNodeCount {
		return false, nil, nil
	}
	excess := tp.nodeCount - targetNodeCount
	prunable := 0
	pruneGeneration := tp.oldestGeneration
	for prunable < excess {
		prunable += tp.generationCounts[pruneGeneration]
		pruneGeneration++
	}
	//fmt.Printf("Will prune to generation %d, nodes to prune: %d, excess %d\n", pruneGeneration, prunable, excess)
	emptyAddresses, err := tp.PruneToTimestamp(mainTrie, pruneGeneration, storageTrieFunc)
	if err != nil {
		return false, nil, err
	}
	return true, emptyAddresses, nil
}

func (tp *TriePruning) NodeCount() int {
	return tp.nodeCount
}

func (tp *TriePruning) GenCounts() map[uint64]int {
	return tp.generationCounts
}
