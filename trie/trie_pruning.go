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
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
)

type TriePruning struct {
	// boltDB is used here for its B+tree implementation with prefix-compression.
	// It maps prefixes to their corresponding timestamps (uint64)
	timestamps *bolt.DB

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
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		return nil, err
	}
	// Pre-create the bucket so we can assume it is there
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket(abucket, false); err != nil {
			return err
		}
		if _, err := tx.CreateBucket(sbucket, false); err != nil {
			return err
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &TriePruning{
		oldestGeneration: oldestGeneration,
		blockNr:          oldestGeneration,
		timestamps:       db,
		storage:          make(map[uint64]map[common.Address]map[string]struct{}),
		accounts:         make(map[uint64]map[string]struct{}),
		generationCounts: make(map[uint64]int),
	}, nil
}

func (tp *TriePruning) SetBlockNr(blockNr uint64) {
	tp.blockNr = blockNr
}

var abucket = []byte("a")
var sbucket = []byte("s")

// Updates a node to the current timestamp
// contract is effectively address of the smart contract
// hex is the prefix of the key
// parent is the node that needs to be modified to unload the touched node
// exists is true when the node existed before, and false if it is a new one
// prevTimestamp is the timestamp the node current has
func (tp *TriePruning) TouchFrom(contract []byte, hex []byte, exists bool, prevTimestamp uint64, del bool, newTimestamp uint64) {
	//fmt.Printf("TouchFrom %x, exists: %t, prevTimestamp %d, del %t, newTimestamp %d\n", hex, exists, prevTimestamp, del, newTimestamp)
	if exists && !del && prevTimestamp == newTimestamp {
		return
	}
	if !del {
		hexS := string(common.CopyBytes(hex))
		var newMap map[string]struct{}
		if contract == nil {
			if m, ok := tp.accounts[newTimestamp]; ok {
				newMap = m
			} else {
				newMap = make(map[string]struct{})
				tp.accounts[newTimestamp] = newMap
			}
		} else {
			contractAddress := common.BytesToAddress(contract)
			if m, ok := tp.storage[newTimestamp]; ok {
				if m1, ok1 := m[contractAddress]; ok1 {
					newMap = m1
				} else {
					newMap = make(map[string]struct{})
					m[contractAddress] = newMap
				}
			} else {
				m = make(map[common.Address]map[string]struct{})
				newMap = make(map[string]struct{})
				m[contractAddress] = newMap
				tp.storage[newTimestamp] = m
			}
		}
		newMap[hexS] = struct{}{}
	}
	if exists {
		if contract == nil {
			if m, ok := tp.accounts[prevTimestamp]; ok {
				delete(m, string(hex))
				if len(m) == 0 {
					delete(tp.accounts, prevTimestamp)
				}
			}
		} else {
			contractAddress := common.BytesToAddress(contract)
			if m, ok := tp.storage[prevTimestamp]; ok {
				if m1, ok1 := m[contractAddress]; ok1 {
					delete(m1, string(hex))
					if len(m1) == 0 {
						delete(m, contractAddress)
						if len(m) == 0 {
							delete(tp.storage, prevTimestamp)
						}
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
func (tp *TriePruning) Touch(contract []byte, hex []byte, del bool) error {
	var exists = false
	var timestampInput [8]byte
	var timestampOutput [8]byte
	// Now it is the current timestamp, but after the transaction, it will be replaced
	// by the previously existing (if it existed)
	binary.BigEndian.PutUint64(timestampInput[:], tp.blockNr)
	var cKey []byte
	var bucket []byte
	if contract == nil {
		cKey = make([]byte, len(hex)+1)
		cKey[0] = 0xff
		copy(cKey[1:], hex)
		bucket = abucket
	} else {
		cKey = make([]byte, len(contract)+len(hex))
		copy(cKey, contract)
		copy(cKey[len(contract):], hex)
		bucket = sbucket
	}
	if err := tp.timestamps.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("timestamp bucket %s did not exist", bucket)
		}
		if v, _ := b.Get(cKey); v != nil {
			if del {
				if err := b.Delete(cKey); err != nil {
					return err
				}
			} else if !bytes.Equal(v, timestampInput[:]) {
				if err := b.Put(cKey, timestampInput[:]); err != nil {
					return err
				}
			}
			copy(timestampOutput[:], v)
			exists = true
		} else {
			if !del {
				if err := b.Put(cKey, timestampInput[:]); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	var prevTimestamp uint64
	if exists {
		prevTimestamp = binary.BigEndian.Uint64(timestampOutput[:])
	}
	tp.TouchFrom(contract, hex, exists, prevTimestamp, del, tp.blockNr)
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

func (tp *TriePruning) PruneTo(
	t *Trie,
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
		delete(tp.generationCounts, pruneGeneration)
		pruneGeneration++
	}
	//fmt.Printf("Will prune to generation %d, nodes to prune: %d, excess %d\n", pruneGeneration, prunable, excess)
	// Remove (unload) nodes from storage tries and account trie
	aggregateStorage := make(map[common.Address]map[string]struct{})
	aggregateAccounts := make(map[string]struct{})
	for gen := tp.oldestGeneration; gen < pruneGeneration; gen++ {
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
			return false, nil, err
		}
		empty := pruneMap(storageTrie, m, h)
		if empty {
			emptyAddresses = append(emptyAddresses, address)
		}
	}
	// Change hasher to be appropriate for the main trie
	h.encodeToBytes = false
	pruneMap(t, aggregateAccounts, h)
	// Remove fom the timestamp structure
	if err := tp.timestamps.Update(func(tx *bolt.Tx) error {
		ab := tx.Bucket(abucket)
		if ab == nil {
			return fmt.Errorf("timestamp bucket %s did not exist", abucket)
		}
		for hexS := range aggregateAccounts {
			cKey := make([]byte, 1+len(hexS))
			cKey[0] = 0xff
			copy(cKey[1:], []byte(hexS))
			if err := ab.Delete(cKey); err != nil {
				return err
			}
		}
		sb := tx.Bucket(sbucket)
		if sb == nil {
			return fmt.Errorf("timestamp bucket %s did not exist", sbucket)
		}
		for address, m := range aggregateStorage {
			for hexS := range m {
				cKey := make([]byte, len(address)+len(hexS))
				copy(cKey, address[:])
				copy(cKey[len(address):], []byte(hexS))
				if err := sb.Delete(cKey); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return false, nil, err
	}
	tp.oldestGeneration = pruneGeneration
	tp.nodeCount -= prunable
	return true, emptyAddresses, nil
}

func (tp *TriePruning) NodeCount() int {
	return tp.nodeCount
}

func (tp *TriePruning) GenCounts() map[uint64]int {
	return tp.generationCounts
}
