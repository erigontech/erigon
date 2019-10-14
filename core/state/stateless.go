// Copyright 2019 The go-ethereum Authors
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

package state

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/trie"
)

// Stateless is the inter-block cache for stateless client prototype, iteration 2
// It creates the initial state trie during the construction, and then updates it
// during the execution of block(s)
type Stateless struct {
	t              *trie.Trie             // State trie
	codeMap        map[common.Hash][]byte // Lookup index from code hashes to corresponding bytecode
	blockNr        uint64                 // Current block number
	storageUpdates map[common.Hash]map[common.Hash][]byte
	accountUpdates map[common.Hash]*accounts.Account
	deleted        map[common.Hash]struct{}
	created        map[common.Hash]struct{}
	trace          bool
}

// NewStateless creates a new instance of Stateless
// It deserialises the block witness and creates the state trie out of it, checking that the root of the constructed
// state trie matches the value of `stateRoot` parameter
func NewStateless(stateRoot common.Hash, witness []byte, blockNr uint64, trace bool) (*Stateless, error) {
	t, codeMap, err := trie.BlockWitnessToTrie(witness, trace)
	if err != nil {
		return nil, err
	}
	if t.Hash() != stateRoot {
		filename := fmt.Sprintf("root_%d.txt", blockNr)
		f, err := os.Create(filename)
		if err == nil {
			defer f.Close()
			t.Print(f)
		}
		return nil, fmt.Errorf("state root mistmatch when creating Stateless2, got %x, expected %x", t.Hash(), stateRoot)
	}
	return &Stateless{
		t:              t,
		codeMap:        codeMap,
		storageUpdates: make(map[common.Hash]map[common.Hash][]byte),
		accountUpdates: make(map[common.Hash]*accounts.Account),
		deleted:        make(map[common.Hash]struct{}),
		created:        make(map[common.Hash]struct{}),
		blockNr:        blockNr,
		trace:          trace,
	}, nil
}

// SetBlockNr changes the block number associated with this
func (s *Stateless) SetBlockNr(blockNr uint64) {
	s.blockNr = blockNr
}

// ReadAccountData is a part of the StateReader interface
// This implementation attempts to look up account data in the state trie, and fails if it is not found
func (s *Stateless) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	acc, ok := s.t.GetAccount(addrHash[:])
	if ok {
		return acc, nil
	}
	return nil, fmt.Errorf("could not find account with address %x", address)
}

// ReadAccountStorage is a part of the StateReader interface
// This implementation attempts to look up the storage in the state trie, and fails if it is not found
func (s *Stateless) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	seckey, err := common.HashData(key[:])
	if err != nil {
		return nil, err
	}

	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	enc, ok := s.t.Get(dbutils.GenerateCompositeTrieKey(addrHash, seckey))
	if ok {
		// Unwrap one RLP level
		if len(enc) > 1 {
			enc = enc[1:]
		}
		return enc, nil
	}
	return nil, fmt.Errorf("could not find storage item %x in account with address %x", key, address)
}

// ReadAccountCode is a part of the StateReader interface
// This implementation looks the code up in the codeMap, failing if the code is not found.
func (s *Stateless) ReadAccountCode(address common.Address, codeHash common.Hash) (code []byte, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if s.trace {
		fmt.Printf("Getting code for %x\n", codeHash)
	}
	if code, ok := s.codeMap[codeHash]; ok {
		return code, nil
	}
	return nil, fmt.Errorf("could not find bytecode for hash %x", codeHash)
}

// ReadAccountCodeSize is a part of the StateReader interface
// This implementation looks the code up in the codeMap, and returns its size
// It fails if the code is not found in the map
func (s *Stateless) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (codeSize int, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return 0, nil
	}
	if code, ok := s.codeMap[codeHash]; ok {
		return len(code), nil
	}
	return 0, fmt.Errorf("could not find bytecode for hash %x", codeHash)
}

// UpdateAccountData is a part of the StateWriter interface
// This implementation registers the account update in the `accountUpdates` map
func (s *Stateless) UpdateAccountData(_ context.Context, address common.Address, original, account *accounts.Account) error {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	if s.trace {
		fmt.Printf("UpdateAccountData for addrHash %x\n", addrHash)
	}
	s.accountUpdates[addrHash] = account
	return nil
}

// DeleteAccount is a part of the StateWriter interface
// This implementation registers the deletion of the account in two internal maps
func (s *Stateless) DeleteAccount(_ context.Context, address common.Address, original *accounts.Account) error {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	s.accountUpdates[addrHash] = nil
	s.deleted[addrHash] = struct{}{}
	return nil
}

// UpdateAccountCode is a part of the StateWriter interface
// This implementation adds the code to the codeMap to make it available for further accesses
func (s *Stateless) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	if _, ok := s.codeMap[codeHash]; !ok {
		s.codeMap[codeHash] = code
	}
	return nil
}

// WriteAccountStorage is a part of the StateWriter interface
// This implementation registeres the change of the account's storage in the internal double map `storageUpdates`
func (s *Stateless) WriteAccountStorage(_ context.Context, address common.Address, incarnation uint64, key, original, value *common.Hash) error {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}

	v := bytes.TrimLeft(value[:], "\x00")
	m, ok := s.storageUpdates[addrHash]
	if !ok {
		m = make(map[common.Hash][]byte)
		s.storageUpdates[addrHash] = m
	}
	seckey, err := common.HashData(key[:])
	if err != nil {
		return err
	}
	if len(v) > 0 {
		// Write into 1 extra RLP level
		m[seckey] = AddExtraRLPLevel(v)
	} else {
		m[seckey] = nil
	}
	return nil
}

// CreateContract is a part of StateWriter interface
// This implementation registers given address in the internal map `created`
func (s *Stateless) CreateContract(address common.Address) error {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	s.created[addrHash] = struct{}{}
	return nil
}

// CheckRoot finalises the execution of a block and computes the resulting state root
func (s *Stateless) CheckRoot(expected common.Hash) error {
	// New contracts are being created at these addresses. Therefore, we need to clear the storage items
	// that might be remaining in the trie and figure out the next incarnations
	for addrHash := range s.created {
		if account, ok := s.accountUpdates[addrHash]; ok && account != nil {
			account.Root = trie.EmptyRoot
		}
		// The only difference between Delete and DeleteSubtree is that Delete would delete accountNode too,
		// wherewas DeleteSubtree will keep the accountNode, but will make the storage sub-trie empty
		s.t.DeleteSubtree(addrHash[:], s.blockNr)
	}
	for addrHash, account := range s.accountUpdates {
		if account != nil {
			s.t.UpdateAccount(addrHash[:], account)
		} else {
			s.t.Delete(addrHash[:], s.blockNr)
		}
	}
	for addrHash, m := range s.storageUpdates {
		if _, ok := s.deleted[addrHash]; ok {
			// Deleted contracts will be dealth with later, in the next loop
			continue
		}

		for keyHash, v := range m {
			cKey := dbutils.GenerateCompositeTrieKey(addrHash, keyHash)
			if len(v) > 0 {
				s.t.Update(cKey, v, s.blockNr)
			} else {
				s.t.Delete(cKey, s.blockNr)
			}
		}
		if account, ok := s.accountUpdates[addrHash]; ok && account != nil {
			ok, root := s.t.DeepHash(addrHash[:])
			if ok {
				account.Root = root
			} else {
				account.Root = trie.EmptyRoot
			}
		}
	}
	// For the contracts that got deleted
	for addrHash := range s.deleted {
		if account, ok := s.accountUpdates[addrHash]; ok && account != nil {
			account.Root = trie.EmptyRoot
		}
		s.t.DeleteSubtree(addrHash[:], s.blockNr)
	}
	myRoot := s.t.Hash()
	if myRoot != expected {
		filename := fmt.Sprintf("root_%d.txt", s.blockNr)
		f, err := os.Create(filename)
		if err == nil {
			defer f.Close()
			s.t.Print(f)
		}
		return fmt.Errorf("final root: %x, expected: %x", myRoot, expected)
	}
	s.storageUpdates = make(map[common.Hash]map[common.Hash][]byte)
	s.accountUpdates = make(map[common.Hash]*accounts.Account)
	s.deleted = make(map[common.Hash]struct{})
	s.created = make(map[common.Hash]struct{})
	return nil
}
