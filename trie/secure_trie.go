// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"

	"github.com/hashicorp/golang-lru"
)

// SecureTrie wraps a trie with key hashing. In a secure trie, all
// access operations hash the key using keccak256. This prevents
// calling code from creating long chains of nodes that
// increase the access time.
//
// Contrary to a regular trie, a SecureTrie can only be created with
// New and must have an attached database. The database also stores
// the preimage of each key.
//
// SecureTrie is not safe for concurrent use.
type SecureTrie struct {
	trie         Trie
	hashKeyBuf   [common.HashLength]byte
	hashKeyCache *lru.Cache
}

// NewSecure creates a trie with an existing root node from a backing database
// and optional intermediate in-memory node pool.
//
// If root is the zero hash or the sha3 hash of an empty string, the
// trie is initially empty. Otherwise, New will panic if db is nil
// and returns MissingNodeError if the root node cannot be found.
//
// Accessing the trie loads nodes from the database or node pool on demand.
// Loaded nodes are kept around until their 'cache generation' expires.
// A new cache generation is created by each call to Commit.
// cachelimit sets the number of past cache generations to keep.
func NewSecure(root common.Hash, bucket []byte, prefix []byte, encodeToBytes bool) (*SecureTrie, error) {
	trie := New(root, bucket, prefix, encodeToBytes)
	hashKeyCache, err := lru.New(1024 * 1024)
	if err != nil {
		return nil, err
	}
	return &SecureTrie{trie: *trie, hashKeyCache: hashKeyCache}, nil
}

// Get returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
func (t *SecureTrie) Get(db ethdb.Database, key []byte, blockNr uint64) []byte {
	res, err := t.TryGet(db, key, blockNr)
	if err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
	return res
}

// TryGet returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *SecureTrie) TryGet(db ethdb.Database, key []byte, blockNr uint64) ([]byte, error) {
	value, err := t.trie.TryGet(db, t.HashKey(key), blockNr)
	return value, err
}

// Update associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
func (t *SecureTrie) Update(db ethdb.Database, key, value []byte, blockNr uint64) {
	if err := t.TryUpdate(db, key, value, blockNr); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryUpdate associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
//
// If a node was not found in the database, a MissingNodeError is returned.
func (t *SecureTrie) TryUpdate(db ethdb.Database, key, value []byte, blockNr uint64) error {
	hk := t.HashKey(key)
	err := t.trie.TryUpdate(db, hk, value, blockNr)
	if err != nil {
		return err
	}
	if err = db.Put(SecureKeyPrefix, hk, key); err != nil {
		return err
	}
	return nil
}

// Delete removes any existing value for key from the trie.
func (t *SecureTrie) Delete(db ethdb.Database, key []byte, blockNr uint64) {
	if err := t.TryDelete(db, key, blockNr); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryDelete removes any existing value for key from the trie.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *SecureTrie) TryDelete(db ethdb.Database, key []byte, blockNr uint64) error {
	hk := t.HashKey(key)
	return t.trie.TryDelete(db, hk, blockNr)
}

// GetKey returns the sha3 preimage of a hashed key that was
// previously used to store a value.
func (t *SecureTrie) GetKey(dbr DatabaseReader, shaKey []byte) []byte {
	key, _ := dbr.Get(SecureKeyPrefix, shaKey)
	return key
}

func (t *SecureTrie) Hash() common.Hash {
	return t.trie.Hash()
}

// Root returns the root hash of SecureTrie.
// Deprecated: use Hash instead.
func (t *SecureTrie) Root() []byte {
	return t.trie.Root()
}

// Copy returns a copy of SecureTrie.
func (t *SecureTrie) Copy() *SecureTrie {
	cpy := *t
	return &cpy
}

// NodeIterator returns an iterator that returns nodes of the underlying trie. Iteration
// starts at the key after the given start key.
func (t *SecureTrie) NodeIterator(db ethdb.Database, start []byte, blockNr uint64) NodeIterator {
	return t.trie.NodeIterator(db, start, blockNr)
}

// hashKey returns the hash of key as an ephemeral buffer.
// The caller must not hold onto the return value because it will become
// invalid on the next call to hashKey or secKey.
func (t *SecureTrie) HashKey(key []byte) []byte {
	keyStr := string(key)
	if cached, ok := t.hashKeyCache.Get(keyStr); ok {
		return cached.([]byte)
	}
	h := newHasher(false)
	h.sha.Reset()
	h.sha.Write(key)
	buf := h.sha.Sum(t.hashKeyBuf[:0])
	returnHasherToPool(h)
	cachedVal := make([]byte, len(buf))
	copy(cachedVal, buf)
	t.hashKeyCache.Add(keyStr, cachedVal)
	return buf
}

func (t *SecureTrie) GetTrie() *Trie {
	return &t.trie
}
