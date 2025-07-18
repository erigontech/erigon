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

import "github.com/erigontech/erigon-lib/common"

var emptyHash [32]byte

// SubTrie is a result of loading sub-trie from either flat db
// or witness db. It encapsulates sub-trie root (which is of the un-exported type `node`)
// If the loading is done for verification and testing purposes, then usually only
// sub-tree root hash would be queried
type SubTries struct {
	Hashes []common.Hash // Root hashes of the sub-tries
	roots  []Node        // Sub-tries
}

type LoadFunc func(*SubTrieLoader, *RetainList, [][]byte, []int) (SubTries, error)

// Resolver looks up (resolves) some keys and corresponding values from a database.
// One resolver per trie (prefix).
// See also ResolveRequest in trie.go
type SubTrieLoader struct {
	codeRequests []*LoadRequestForCode
}

func NewSubTrieLoader() *SubTrieLoader {
	tr := SubTrieLoader{
		codeRequests: []*LoadRequestForCode{},
	}
	return &tr
}

func (stl *SubTrieLoader) Reset() {
	stl.codeRequests = stl.codeRequests[:0]
}

// AddCodeRequest add a request for code loading
func (stl *SubTrieLoader) AddCodeRequest(req *LoadRequestForCode) {
	stl.codeRequests = append(stl.codeRequests, req)
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
