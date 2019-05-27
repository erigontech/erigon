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

package light

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

func NewState(ctx context.Context, head *types.Header, odr OdrBackend) *state.StateDB {
	tds, _ := state.NewTrieDbState(head.Root, NewStateDatabase(ctx, head, odr).TrieDB(), head.Number.Uint64())
	state := state.New(tds)
	return state
}

func NewStateDatabase(ctx context.Context, head *types.Header, odr OdrBackend) state.Database {
	return &odrDatabase{ctx, StateTrieID(head), odr}
}

type odrDatabase struct {
	ctx     context.Context
	id      *TrieID
	backend OdrBackend
}

func (db *odrDatabase) OpenTrie(root common.Hash) (state.Trie, error) {
	return &odrTrie{db: db, id: db.id, prefix: state.AccountsBucket}, nil
}

func (db *odrDatabase) OpenStorageTrie(addrHash common.Hash, root common.Hash) (state.Trie, error) {
	return &odrTrie{db: db, id: StorageTrieID(db.id, addrHash, root), prefix: addrHash[:]}, nil
}

func (db *odrDatabase) CopyTrie(t state.Trie) state.Trie {
	switch t := t.(type) {
	case *odrTrie:
		cpy := &odrTrie{db: t.db, id: t.id}
		if t.trie != nil {
			cpytrie := *t.trie
			cpy.trie = &cpytrie
		}
		return cpy
	default:
		panic(fmt.Errorf("unknown trie type %T", t))
	}
}

func (db *odrDatabase) ContractCode(addrHash, codeHash common.Hash) ([]byte, error) {
	if codeHash == sha3_nil {
		return nil, nil
	}
	if code, err := db.backend.Database().Get(state.CodeBucket, codeHash[:]); err == nil {
		return code, nil
	}
	id := *db.id
	id.AccKey = addrHash[:]
	req := &CodeRequest{Id: &id, Hash: codeHash}
	err := db.backend.Retrieve(db.ctx, req)
	return req.Data, err
}

func (db *odrDatabase) ContractCodeSize(addrHash, codeHash common.Hash) (int, error) {
	code, err := db.ContractCode(addrHash, codeHash)
	return len(code), err
}

func (db *odrDatabase) TrieDB() ethdb.Database {
	return nil
}

type odrTrie struct {
	db   *odrDatabase
	id   *TrieID
	trie *trie.Trie
	// Prefix to form the database key
	prefix []byte
}

func (t *odrTrie) TryGet(db ethdb.Database, key []byte, blockNr uint64) ([]byte, error) {
	key = crypto.Keccak256(key)
	var res []byte
	err := t.do(key, func() (err error) {
		res, err = t.trie.TryGet(db, key, blockNr)
		return err
	})
	return res, err
}

func (t *odrTrie) TryUpdate(db ethdb.Database, key, value []byte, blockNr uint64) error {
	key = crypto.Keccak256(key)
	return t.do(key, func() error {
		return t.trie.TryUpdate(db, key, value, blockNr)
	})
}

func (t *odrTrie) TryDelete(db ethdb.Database, key []byte, blockNr uint64) error {
	key = crypto.Keccak256(key)
	return t.do(key, func() error {
		return t.trie.TryDelete(db, key, blockNr)
	})
}

func (t *odrTrie) CommitPreimages(dbw ethdb.Putter) error {
	return nil
}

func (t *odrTrie) Hash() common.Hash {
	if t.trie == nil {
		return t.id.Root
	}
	return t.trie.Hash()
}

func (t *odrTrie) NodeIterator(db ethdb.Database, startkey []byte, blockNr uint64) trie.NodeIterator {
	return newNodeIterator(t, startkey, blockNr)
}

func (t *odrTrie) GetKey(dbr trie.DatabaseReader, sha []byte) []byte {
	return nil
}

func (t *odrTrie) Prove(db ethdb.Database, key []byte, fromLevel uint, proofDb ethdb.Putter, blockNr uint64) error {
	return errors.New("not implemented, needs client/server interface split")
}

func (t *odrTrie) HashKey(key []byte) []byte {
	return nil
}

// do tries and retries to execute a function until it returns with no error or
// an error type other than MissingNodeError
func (t *odrTrie) do(key []byte, fn func() error) error {
	for {
		if t.trie == nil {
			t.trie = trie.New(t.id.Root, t.prefix, nil, false)
		}
		err := fn()
		if _, ok := err.(*trie.MissingNodeError); !ok {
			return err
		}
		r := &TrieRequest{Id: t.id, Key: key}
		if err := t.db.backend.Retrieve(t.db.ctx, r); err != nil {
			return err
		}
	}
}

type nodeIterator struct {
	trie.NodeIterator
	t   *odrTrie
	err error
}

func newNodeIterator(t *odrTrie, startkey []byte, blockNr uint64) trie.NodeIterator {
	it := &nodeIterator{t: t}
	// Open the actual non-ODR trie if that hasn't happened yet.
	if t.trie == nil {
		it.do(func() error {
			it.t.trie = trie.New(t.id.Root, t.prefix, nil, false)
			return nil
		})
	}
	it.do(func() error {
		it.NodeIterator = it.t.trie.NodeIterator(t.db.backend.Database(), startkey, blockNr)
		return it.NodeIterator.Error()
	})
	return it
}

func (it *nodeIterator) Next(descend bool) bool {
	var ok bool
	it.do(func() error {
		ok = it.NodeIterator.Next(descend)
		return it.NodeIterator.Error()
	})
	return ok
}

// do runs fn and attempts to fill in missing nodes by retrieving.
func (it *nodeIterator) do(fn func() error) {
	var lasthash common.Hash
	for {
		it.err = fn()
		missing, ok := it.err.(*trie.MissingNodeError)
		if !ok {
			return
		}
		if missing.NodeHash == lasthash {
			it.err = fmt.Errorf("retrieve loop for trie node %x", missing.NodeHash)
			return
		}
		lasthash = missing.NodeHash
		r := &TrieRequest{Id: it.t.id, Key: nibblesToKey(missing.Path)}
		if it.err = it.t.db.backend.Retrieve(it.t.db.ctx, r); it.err != nil {
			return
		}
	}
}

func (it *nodeIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.NodeIterator.Error()
}

func nibblesToKey(nib []byte) []byte {
	if len(nib) > 0 && nib[len(nib)-1] == 0x10 {
		nib = nib[:len(nib)-1] // drop terminator
	}
	if len(nib)&1 == 1 {
		nib = append(nib, 0) // make even
	}
	key := make([]byte, len(nib)/2)
	for bi, ni := 0, 0; ni < len(nib); bi, ni = bi+1, ni+2 {
		key[bi] = nib[ni]<<4 | nib[ni+1]
	}
	return key
}
