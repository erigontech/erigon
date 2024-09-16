// Copyright 2024 The Erigon Authors
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

package trie

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
)

var emptyHash [32]byte

// SubTrie is a result of loading sub-trie from either flat db
// or witness db. It encapsulates sub-trie root (which is of the un-exported type `node`)
// If the loading is done for verification and testing purposes, then usually only
// sub-tree root hash would be queried
type SubTries struct {
	Hashes []libcommon.Hash // Root hashes of the sub-tries
	roots  []Node           // Sub-tries
}

func (st SubTries) Roots() []Node {
	return st.roots
}

type LoadFunc func(*SubTrieLoader, *RetainList, [][]byte, []int, [][]byte) (SubTries, error)

// Resolver looks up (resolves) some keys and corresponding values from a database.
// One resolver per trie (prefix).
// See also ResolveRequest in trie.go
type SubTrieLoader struct {
	blockNr      uint64
	codeRequests []*LoadRequestForCode
}

func NewSubTrieLoader(blockNr uint64) *SubTrieLoader {
	tr := SubTrieLoader{
		codeRequests: []*LoadRequestForCode{},
		blockNr:      blockNr,
	}
	return &tr
}

func (stl *SubTrieLoader) Reset(blockNr uint64) {
	stl.blockNr = blockNr
	stl.codeRequests = stl.codeRequests[:0]
}

// AddCodeRequest add a request for code loading
func (stl *SubTrieLoader) AddCodeRequest(req *LoadRequestForCode) {
	stl.codeRequests = append(stl.codeRequests, req)
}

func (stl *SubTrieLoader) CodeRequests() []*LoadRequestForCode {
	return stl.codeRequests
}

// Various values of the account field set
const (
	AccountFieldNonceOnly     uint32 = 0x01
	AccountFieldBalanceOnly   uint32 = 0x02
	AccountFieldStorageOnly   uint32 = 0x04
	AccountFieldCodeOnly      uint32 = 0x08
	AccountFieldSSizeOnly     uint32 = 0x10
	AccountFieldSetNotAccount uint32 = 0x00
)

// LoadFromDb loads subtries from a state database.
func (stl *SubTrieLoader) LoadSubTries(db kv.Tx, blockNr uint64, rl RetainDecider, hc HashCollector, dbPrefixes [][]byte, fixedbits []int, trace bool) (SubTries, error) {
	return stl.LoadFromFlatDB(db, rl, hc, dbPrefixes, fixedbits, trace)
}

func (stl *SubTrieLoader) LoadFromFlatDB(db kv.Tx, rl RetainDecider, hc HashCollector, dbPrefixes [][]byte, fixedbits []int, trace bool) (SubTries, error) {
	loader := NewFlatDbSubTrieLoader()
	if err1 := loader.Reset(db, rl, rl, hc, dbPrefixes, fixedbits, trace); err1 != nil {
		return SubTries{}, err1
	}
	subTries, err := loader.LoadSubTries()
	if err != nil {
		return subTries, err
	}
	if err = AttachRequestedCode(db, stl.codeRequests); err != nil {
		return subTries, err
	}
	return subTries, nil
}

// LoadFromWitnessDb loads subtries from a witnesses database instead of a state DB.
func (stl *SubTrieLoader) LoadFromWitnessDb(db WitnessStorage, blockNr uint64, trieLimit uint32, startPos int64, count int) (SubTries, int64, error) {
	loader := NewWitnessDbSubTrieLoader()
	// we expect CodeNodes to be already attached to the trie when loading from witness db
	return loader.LoadSubTries(db, blockNr, trieLimit, startPos, count)
}
