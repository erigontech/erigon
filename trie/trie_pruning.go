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
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
)

type TriePruning struct {
	// boltDB is used here for its B+tree implementation with prefix-compression.
	// It maps prefixes to their corresponding timestamps (uint64)
	timestamps *bolt.DB

	// Maps timestamp (uint64) to address of the contract to prefix of node (string) to parent node
	storage map[uint64]map[common.Address]map[string]node

	// Maps timestamp (uint64) to prefix of node (string) to parent node
	accounts map[uint64]map[string]node

	// For each timestamp, keeps number of branch nodes belonging to it
	generationCounts map[uint64]int

	// Keeps total number of branch nodes
	nodeCount int

	// The oldest timestamp of all branch nodes
	oldestGeneration uint64

	// Current timestamp
	blockNr uint64
}

func NewTriePruning() (*TriePruning, error) {
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		return nil, err
	}
	return &TriePruning{
		timestamps: db,
		storage: make(map[uint64]map[common.Address]map[string]node),
		accounts: make(map[uint64]map[string]node),
		generationCounts: make(map[uint64]int),
	}, nil
}

// Updates a node to the current timestamp
// contract is effectively address of the smart contract
// hex is the prefix of the key 
func (tp *TriePruning) touch(contract []byte, hex []byte, parent, n node) {

}