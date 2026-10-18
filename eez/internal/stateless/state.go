// Copyright 2026 The Erigon Authors
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
package stateless

import (
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	// ErrMissingParentState reports a witness without the parent state root.
	ErrMissingParentState = errors.New("witness lacks the parent state root")
	// ErrIncompleteWitness reports a read the witness does not cover.
	ErrIncompleteWitness = errors.New("incomplete witness")
)

// witnessState serves reads from the trie a witness proves and buffers
// writes until flush applies them to it.
type witnessState struct {
	t     *trie.Trie
	codes map[common.Hash][]byte

	accounts map[common.Address]*accounts.Account // nil for a deleted account
	storage  map[common.Address]map[common.Hash]uint256.Int
	created  map[common.Address]struct{}
	deleted  map[common.Address]struct{}
}

var (
	_ state.StateReader = (*witnessState)(nil)
	_ state.StateWriter = (*witnessState)(nil)
)

// newWitnessState builds the state whose root is root from witness trie
// nodes in any order and the bytecodes they reference.
func newWitnessState(nodes, codes [][]byte, root common.Hash) (*witnessState, error) {
	s := &witnessState{codes: make(map[common.Hash][]byte, len(codes))}
	for _, code := range codes {
		s.codes[crypto.Keccak256Hash(code)] = code
	}
	s.reset()
	if root == empty.RootHash {
		s.t = trie.New(common.Hash{})
		return s, nil
	}
	ordered := make([][]byte, 0, len(nodes))
	for _, n := range nodes {
		if crypto.Keccak256Hash(n) == root {
			ordered = append(ordered, n)
			break
		}
	}
	if len(ordered) == 0 {
		return nil, fmt.Errorf("%w %x", ErrMissingParentState, root)
	}
	ordered = append(ordered, nodes...)
	t, err := trie.RLPDecode(ordered)
	if err != nil {
		return nil, err
	}
	s.t = t
	return s, nil
}

func (s *witnessState) reset() {
	s.accounts = map[common.Address]*accounts.Account{}
	s.storage = map[common.Address]map[common.Hash]uint256.Int{}
	s.created = map[common.Address]struct{}{}
	s.deleted = map[common.Address]struct{}{}
}

func (s *witnessState) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addr := address.Value()
	if acc, ok := s.accounts[addr]; ok {
		if acc == nil {
			return nil, nil
		}
		cp := new(accounts.Account)
		cp.Copy(acc)
		return cp, nil
	}
	addrHash := crypto.Keccak256Hash(addr[:])
	acc, ok := s.t.GetAccount(addrHash[:])
	if !ok {
		// reth leaves the EIP-4788 system caller out of its witnesses.
		if address == params.SystemAddress {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: account %x", ErrIncompleteWitness, addr)
	}
	return acc, nil
}

func (s *witnessState) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return s.ReadAccountData(address)
}

func (s *witnessState) ReadAccountByPlainAddr(address common.Address) (*accounts.Account, error) {
	return s.ReadAccountData(accounts.InternAddress(address))
}

func (s *witnessState) HasAccount(address common.Address) (bool, error) {
	acc, err := s.ReadAccountByPlainAddr(address)
	return acc != nil, err
}

func (s *witnessState) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addr, k := address.Value(), key.Value()
	if slots, ok := s.storage[addr]; ok {
		if v, ok := slots[k]; ok {
			return v, !v.IsZero(), nil
		}
	}
	if _, ok := s.created[addr]; ok {
		return uint256.Int{}, false, nil
	}
	if _, ok := s.deleted[addr]; ok {
		return uint256.Int{}, false, nil
	}
	enc, ok := s.t.Get(dbutils.GenerateCompositeTrieKey(crypto.Keccak256Hash(addr[:]), crypto.Keccak256Hash(k[:])))
	if !ok {
		return uint256.Int{}, false, fmt.Errorf("%w: slot %x of %x", ErrIncompleteWitness, k, addr)
	}
	var v uint256.Int
	v.SetBytes(enc)
	return v, len(enc) > 0, nil
}

func (s *witnessState) ReadAccountCode(address accounts.Address) ([]byte, error) {
	acc, err := s.ReadAccountData(address)
	if err != nil || acc == nil || acc.IsEmptyCodeHash() {
		return nil, err
	}
	code, ok := s.codes[acc.CodeHash.Value()]
	if !ok {
		return nil, fmt.Errorf("%w: code %x of %x", ErrIncompleteWitness, acc.CodeHash.Value(), address.Value())
	}
	return code, nil
}

func (s *witnessState) ReadAccountCodeSize(address accounts.Address) (int, error) {
	code, err := s.ReadAccountCode(address)
	return len(code), err
}

func (s *witnessState) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }

func (s *witnessState) SetTrace(bool, string) {}
func (s *witnessState) Trace() bool           { return false }
func (s *witnessState) TracePrefix() string   { return "" }

func (s *witnessState) UpdateAccountData(address accounts.Address, _, account *accounts.Account) error {
	acc := new(accounts.Account)
	acc.Copy(account)
	s.accounts[address.Value()] = acc
	return nil
}

func (s *witnessState) DeleteAccount(address accounts.Address, _ *accounts.Account) error {
	addr := address.Value()
	s.accounts[addr] = nil
	delete(s.storage, addr)
	s.deleted[addr] = struct{}{}
	return nil
}

func (s *witnessState) UpdateAccountCode(_ accounts.Address, _ uint64, codeHash accounts.CodeHash, code []byte) error {
	s.codes[codeHash.Value()] = code
	return nil
}

func (s *witnessState) WriteAccountStorage(address accounts.Address, _ uint64, key accounts.StorageKey, _, value uint256.Int) error {
	addr := address.Value()
	if s.storage[addr] == nil {
		s.storage[addr] = map[common.Hash]uint256.Int{}
	}
	s.storage[addr][key.Value()] = value
	return nil
}

func (s *witnessState) CreateContract(address accounts.Address) error {
	addr := address.Value()
	s.created[addr] = struct{}{}
	delete(s.deleted, addr)
	return nil
}

// flush applies the buffered writes to the trie and returns its root.
func (s *witnessState) flush() (common.Hash, error) {
	for addr := range s.created {
		if acc := s.accounts[addr]; acc != nil {
			acc.Root = empty.RootHash
		}
		addrHash := crypto.Keccak256Hash(addr[:])
		s.t.DeleteSubtree(addrHash[:])
	}
	for addr, acc := range s.accounts {
		addrHash := crypto.Keccak256Hash(addr[:])
		if acc == nil {
			s.t.Delete(addrHash[:])
			continue
		}
		s.t.UpdateAccount(addrHash[:], acc)
	}
	for addr, slots := range s.storage {
		acc := s.accounts[addr]
		if acc == nil {
			return common.Hash{}, fmt.Errorf("storage of %x written without its account", addr)
		}
		addrHash := crypto.Keccak256Hash(addr[:])
		for k, v := range slots {
			key := dbutils.GenerateCompositeTrieKey(addrHash, crypto.Keccak256Hash(k[:]))
			if v.IsZero() {
				s.t.Delete(key)
			} else {
				s.t.Update(key, v.Bytes())
			}
		}
		ok, root, err := s.t.DeepHash(addrHash[:])
		if err != nil {
			return common.Hash{}, err
		}
		if !ok {
			return common.Hash{}, fmt.Errorf("%w: storage of %x", ErrIncompleteWitness, addr)
		}
		acc.Root = root
		s.t.UpdateAccount(addrHash[:], acc)
	}
	s.reset()
	return s.t.Hash(), nil
}
