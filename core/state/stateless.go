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
	"fmt"
	"os"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv/dbutils"
	"github.com/erigontech/erigon/core/types/accounts"
	"github.com/erigontech/erigon/turbo/trie"
)

var (
	_ StateReader = (*Stateless)(nil)
	_ StateWriter = (*Stateless)(nil)
)

// Stateless is the inter-block cache for stateless client prototype, iteration 2
// It creates the initial state trie during the construction, and then updates it
// during the execution of block(s)
type Stateless struct {
	t              *trie.Trie             // State trie
	codeUpdates    map[common.Hash][]byte // Lookup index from code hashes to corresponding bytecode
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
func NewStateless(stateRoot common.Hash, blockWitness *trie.Witness, blockNr uint64, trace bool, isBinary bool) (*Stateless, error) {
	t, err := trie.BuildTrieFromWitness(blockWitness, trace)
	if err != nil {
		return nil, err
	}

	if !isBinary {
		if t.Hash() != stateRoot {
			filename := fmt.Sprintf("root_%d.txt", blockNr)
			f, err := os.Create(filename)
			if err == nil {
				defer f.Close()
				t.Print(f)
			}
			return nil, fmt.Errorf("state root mistmatch when creating Stateless2, got %x, expected %x", t.Hash(), stateRoot)
		}
	}
	return &Stateless{
		t:              t,
		codeUpdates:    make(map[common.Hash][]byte),
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

func (s *Stateless) SetStrictHash(strict bool) {
	s.t.SetStrictHash(strict)
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
	return nil, nil
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

	if enc, ok := s.t.Get(dbutils.GenerateCompositeTrieKey(addrHash, seckey)); ok {
		return enc, nil
	}
	return nil, nil
}

// ReadAccountCode is a part of the StateReader interface
func (s *Stateless) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) (code []byte, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if s.trace {
		fmt.Printf("Getting code for %x\n", codeHash)
	}

	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	if code, ok := s.codeUpdates[addrHash]; ok {
		return code, nil
	}

	if code, ok := s.t.GetAccountCode(addrHash[:]); ok {
		return code, nil
	}
	return nil, nil
}

// ReadAccountCodeSize is a part of the StateReader interface
// This implementation looks the code up in the codeMap, and returns its size
// It fails if the code is not found in the map
func (s *Stateless) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (codeSize int, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return 0, nil
	}

	addrHash, err := common.HashData(address[:])
	if err != nil {
		return 0, err
	}

	if code, ok := s.codeUpdates[addrHash]; ok {
		return len(code), nil
	}

	if code, ok := s.t.GetAccountCode(addrHash[:]); ok {
		return len(code), nil
	}

	if codeSize, ok := s.t.GetAccountCodeSize(addrHash[:]); ok {
		return codeSize, nil
	}

	return 0, nil
}

func (s *Stateless) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

// UpdateAccountData is a part of the StateWriter interface
// This implementation registers the account update in the `accountUpdates` map
func (s *Stateless) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	if s.trace {
		fmt.Printf("UpdateAccountData for address %x, addrHash %x\n", address, addrHash)
	}
	s.accountUpdates[addrHash] = account
	return nil
}

// DeleteAccount is a part of the StateWriter interface
// This implementation registers the deletion of the account in two internal maps
func (s *Stateless) DeleteAccount(address common.Address, original *accounts.Account) error {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	s.accountUpdates[addrHash] = nil
	s.deleted[addrHash] = struct{}{}
	if s.trace {
		fmt.Printf("Stateless: DeleteAccount %x hash %x\n", address, addrHash)
	}
	return nil
}

// UpdateAccountCode is a part of the StateWriter interface
// This implementation adds the code to the codeMap to make it available for further accesses
func (s *Stateless) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	s.codeUpdates[codeHash] = code

	if s.trace {
		fmt.Printf("Stateless: UpdateAccountCode %x codeHash %x\n", address, codeHash)
	}
	return nil
}

// WriteAccountStorage is a part of the StateWriter interface
// This implementation registeres the change of the account's storage in the internal double map `storageUpdates`
func (s *Stateless) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}

	v := value.Bytes()
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
		m[seckey] = v
	} else {
		m[seckey] = nil
	}
	if s.trace {
		fmt.Printf("Stateless: WriteAccountStorage %x key %x val %x\n", address, *key, *value)
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
	if s.trace {
		fmt.Printf("Stateless: CreateContract %x hash %x\n", address, addrHash)
	}
	s.created[addrHash] = struct{}{}
	return nil
}

func (s *Stateless) WriteChangeSets() error { return nil }

func (s *Stateless) WriteHistory() error { return nil }

// CheckRoot finalises the execution of a block and computes the resulting state root
func (s *Stateless) CheckRoot(expected common.Hash) error {
	h := s.Finalize()
	fmt.Printf("trie root after stateless exec : %x  ,  expected trie root: %x\n", h, expected)
	if h != expected {
		filename := fmt.Sprintf("root_%d.txt", s.blockNr)
		f, err := os.Create(filename)
		if err == nil {
			defer f.Close()
			s.t.Print(f)
		}
		return fmt.Errorf("final root: %x, expected: %x", h, expected)
	}

	return nil
}

// Finalize the execution of a block and computes the resulting state root
func (s *Stateless) Finalize() common.Hash {
	// The following map is to prevent repeated clearouts of the storage
	alreadyCreated := make(map[common.Hash]struct{})
	// New contracts are being created at these addresses. Therefore, we need to clear the storage items
	// that might be remaining in the trie and figure out the next incarnations
	for addrHash := range s.created {
		// Prevent repeated storage clearouts
		if _, ok := alreadyCreated[addrHash]; ok {
			continue
		}
		alreadyCreated[addrHash] = struct{}{}
		if account, ok := s.accountUpdates[addrHash]; ok && account != nil {
			account.Root = trie.EmptyRoot
		}
		// The only difference between Delete and DeleteSubtree is that Delete would delete accountNode too,
		// wherewas DeleteSubtree will keep the accountNode, but will make the storage sub-trie empty
		s.t.DeleteSubtree(addrHash[:])
	}
	for addrHash, account := range s.accountUpdates {
		if account != nil {
			s.t.UpdateAccount(addrHash[:], account)
		} else {
			s.t.Delete(addrHash[:])
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
				s.t.Update(cKey, v)
			} else {
				s.t.Delete(cKey)
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
		if _, ok := s.created[addrHash]; ok {
			// In some rather artificial circumstances, an account can be recreated after having been self-destructed
			// in the same block. It can only happen when contract is introduced in the genesis state with nonce 0
			// rather than created by a transaction (in that case, its starting nonce is 1). The self-destructed
			// contract actually gets removed from the state only at the end of the block, so if its nonce is not 0,
			// it will prevent any re-creation within the same block. However, if the contract is introduced in
			// the genesis state, its nonce is 0, and that means it can be self-destructed, and then re-created,
			// all in the same block. In such cases, we must preserve storage modifications happening after the
			// self-destruction
			continue
		}
		if account, ok := s.accountUpdates[addrHash]; ok && account != nil {
			account.Root = trie.EmptyRoot
		}
		s.t.DeleteSubtree(addrHash[:])
	}

	s.storageUpdates = make(map[common.Hash]map[common.Hash][]byte)
	s.accountUpdates = make(map[common.Hash]*accounts.Account)
	s.deleted = make(map[common.Hash]struct{})
	s.created = make(map[common.Hash]struct{})

	return s.t.Hash()
}

func (s *Stateless) GetTrie() *trie.Trie {
	return s.t
}
