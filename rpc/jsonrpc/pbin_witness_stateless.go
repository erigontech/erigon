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

package jsonrpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Re-executing a block against a binary witness alone. This is the bin analogue
// of witnessStateless: same StateReader/StateWriter seams, resolving leaves by
// tree key instead of by MPT path, and finalizing through PBinPatriciaHashed
// rather than an in-memory MPT.
//
// Strict resolution is the only mode. Hex makes it a WITNESS_STRICT_VERIFY
// opt-in because an unresolved MPT node is not always a defect; under bin an
// unresolved hash is unambiguous, so a missing node is always an error and never
// an empty read.
//
// Code has one owner: the witness's own chunk leaves, reassembled and checked
// against the CODE_HASH leaf (commitment.PBinWitnessState.Code). result.Codes is
// not read. The leaves are committed by the root and the fold re-chunks every
// account it touches, so the pruned witness carries them wherever the post-state
// pass needs code; a blob list is keyed by code reads, a strictly narrower set.
// Code a block deploys has no pre-state leaves and arrives through
// UpdateAccountCode, as it does under hex.

var errPBinWitnessNoRemoval = errors.New("binary trie defines no removal")

type pbinWitnessStateless struct {
	state *commitment.PBinWitnessState

	codeUpdates    map[common.Address][]byte
	accountUpdates map[common.Address]*accounts.Account
	storageWrites  map[common.Address]map[common.Hash]uint256.Int
	deleted        map[common.Address]struct{}

	// preimages the witness supplied during re-exec; keys[] must cover these
	usedTrieAddrs map[common.Address]struct{}
	usedTrieSlots map[common.Hash]struct{}

	trace bool
}

var (
	_ state.StateReader = (*pbinWitnessStateless)(nil)
	_ state.StateWriter = (*pbinWitnessStateless)(nil)
)

func newPBinWitnessStateless(result *ExecutionWitnessResult, parentRoot common.Hash) (*pbinWitnessStateless, error) {
	nodes := make([][]byte, len(result.State))
	for i, node := range result.State {
		nodes[i] = node
	}
	witnessState, err := commitment.PBinNewWitnessState(nodes, parentRoot[:])
	if err != nil {
		return nil, fmt.Errorf("failed to decode binary witness: %w", err)
	}
	return &pbinWitnessStateless{
		state:          witnessState,
		codeUpdates:    make(map[common.Address][]byte),
		accountUpdates: make(map[common.Address]*accounts.Account),
		storageWrites:  make(map[common.Address]map[common.Hash]uint256.Int),
		deleted:        make(map[common.Address]struct{}),
		usedTrieAddrs:  make(map[common.Address]struct{}),
		usedTrieSlots:  make(map[common.Hash]struct{}),
	}, nil
}

func (s *pbinWitnessStateless) SetTrace(trace bool, tracePrefix string) { s.trace = trace }
func (s *pbinWitnessStateless) Trace() bool                             { return s.trace }
func (s *pbinWitnessStateless) TracePrefix() string                     { return "" }

func (s *pbinWitnessStateless) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return s.ReadAccountData(address)
}

func (s *pbinWitnessStateless) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addr := address.Value()
	if acc, ok := s.accountUpdates[addr]; ok {
		return acc, nil
	}
	if _, ok := s.deleted[addr]; ok {
		return nil, nil
	}
	return s.preStateAccount(addr)
}

func (s *pbinWitnessStateless) preStateAccount(addr common.Address) (*accounts.Account, error) {
	witnessAcc, ok, err := s.state.Account(addr[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	s.usedTrieAddrs[addr] = struct{}{}
	// The binary tree commits no per-account storage root, so Root stays empty; it
	// is read only by HasStorage, which reports what the witness can see.
	acc := &accounts.Account{
		Nonce:    witnessAcc.Nonce,
		Balance:  witnessAcc.Balance,
		Root:     empty.RootHash,
		CodeHash: accounts.InternCodeHash(witnessAcc.CodeHash),
	}
	return acc, nil
}

func (s *pbinWitnessStateless) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addr, slot := address.Value(), key.Value()
	if m, ok := s.storageWrites[addr]; ok {
		if v, ok := m[slot]; ok {
			return v, true, nil
		}
	}
	if _, ok := s.deleted[addr]; ok {
		return uint256.Int{}, false, nil
	}
	value, ok, err := s.state.Storage(addr[:], slot[:])
	if err != nil || !ok {
		return uint256.Int{}, false, err
	}
	s.usedTrieSlots[slot] = struct{}{}
	var v uint256.Int
	v.SetBytes(value[:])
	return v, !v.IsZero(), nil
}

func (s *pbinWitnessStateless) ReadAccountCode(address accounts.Address) ([]byte, error) {
	addr := address.Value()
	if code, ok := s.codeUpdates[addr]; ok {
		return code, nil
	}
	if _, ok := s.deleted[addr]; ok {
		return nil, nil
	}
	code, _, err := s.state.Code(addr[:])
	return code, err
}

func (s *pbinWitnessStateless) ReadAccountCodeSize(address accounts.Address) (int, error) {
	code, err := s.ReadAccountCode(address)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (s *pbinWitnessStateless) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return 0, nil
}

// HasStorage reports the storage this verifier can see. The binary tree commits
// no per-account storage root, so a pre-state slot outside the witness is
// invisible here; CREATE-collision detection then rests on the nonce and code
// the witness does carry.
func (s *pbinWitnessStateless) HasStorage(address accounts.Address) (bool, error) {
	addr := address.Value()
	if _, ok := s.deleted[addr]; ok {
		return false, nil
	}
	for _, v := range s.storageWrites[addr] {
		if !v.IsZero() {
			return true, nil
		}
	}
	return false, nil
}

func (s *pbinWitnessStateless) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	addr := address.Value()
	if account == nil {
		s.accountUpdates[addr] = nil
		return nil
	}
	accCopy := new(accounts.Account)
	accCopy.Copy(account)
	s.accountUpdates[addr] = accCopy
	return nil
}

// DeleteAccount is where a block that cannot be committed under bin surfaces.
// An account the witness proves absent was created and dropped inside the block,
// leaving no leaf behind; anything else would have to remove one.
func (s *pbinWitnessStateless) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	addr := address.Value()
	acc, err := s.preStateAccount(addr)
	if err != nil {
		return err
	}
	if acc != nil {
		return fmt.Errorf("%w: account %x", errPBinWitnessNoRemoval, addr)
	}
	delete(s.accountUpdates, addr)
	delete(s.storageWrites, addr)
	s.deleted[addr] = struct{}{}
	return nil
}

func (s *pbinWitnessStateless) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	addr := address.Value()
	s.codeUpdates[addr] = code
	if acc, ok := s.accountUpdates[addr]; ok && acc != nil {
		acc.CodeHash = codeHash
	}
	return nil
}

func (s *pbinWitnessStateless) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	addr, slot := address.Value(), key.Value()
	m, ok := s.storageWrites[addr]
	if !ok {
		m = make(map[common.Hash]uint256.Int)
		s.storageWrites[addr] = m
	}
	m[slot] = value
	return nil
}

// CreateContract un-deletes the address: a create over an account dropped
// earlier in the block puts its leaves back. There is no subtree to clear —
// EIP-7610 forbids creating over an account that already has storage.
func (s *pbinWitnessStateless) CreateContract(address accounts.Address) error {
	delete(s.deleted, address.Value())
	return nil
}

// Finalize turns the block's writes into the plain-key updates the commitment
// layer takes and recomputes the root over the witness.
func (s *pbinWitnessStateless) Finalize(ctx context.Context) (common.Hash, error) {
	plainKeys, updates, err := s.pendingUpdates()
	if err != nil {
		return common.Hash{}, err
	}
	root, err := s.state.Root(ctx, plainKeys, updates)
	if err != nil {
		return common.Hash{}, err
	}
	return common.BytesToHash(root), nil
}

func (s *pbinWitnessStateless) pendingUpdates() (plainKeys [][]byte, updates []commitment.Update, err error) {
	for addr, acc := range s.accountUpdates {
		if acc == nil {
			continue
		}
		if _, gone := s.deleted[addr]; gone {
			continue
		}
		update, err := s.accountUpdate(addr, acc)
		if err != nil {
			return nil, nil, err
		}
		plainKeys = append(plainKeys, addr[:])
		updates = append(updates, update)
	}
	for addr, written := range s.storageWrites {
		if _, gone := s.deleted[addr]; gone {
			continue
		}
		for slot, value := range written {
			update, keep, err := s.storageUpdate(addr, slot, value)
			if err != nil {
				return nil, nil, err
			}
			if !keep {
				continue
			}
			key := make([]byte, 0, len(addr)+len(slot))
			key = append(append(key, addr[:]...), slot[:]...)
			plainKeys = append(plainKeys, key)
			updates = append(updates, update)
		}
	}
	return plainKeys, updates, nil
}

// accountUpdate carries the code size the BASIC_DATA leaf packs, which the
// account itself does not hold: code deployed in-block comes from the write, and
// unchanged code from the witness.
func (s *pbinWitnessStateless) accountUpdate(addr common.Address, acc *accounts.Account) (commitment.Update, error) {
	update := commitment.Update{
		Flags:    commitment.NonceUpdate | commitment.BalanceUpdate | commitment.CodeUpdate,
		Nonce:    acc.Nonce,
		Balance:  acc.Balance,
		CodeHash: acc.CodeHash.Value(),
	}
	if update.CodeHash == empty.CodeHash {
		return update, nil
	}
	if code, ok := s.codeUpdates[addr]; ok {
		update.CodeSize = uint64(len(code))
		s.state.SetCode(addr[:], code)
		return update, nil
	}
	witnessAcc, ok, err := s.state.Account(addr[:])
	if err != nil {
		return update, err
	}
	if !ok || witnessAcc.CodeHash != update.CodeHash {
		return update, fmt.Errorf("witness holds no code for account %x with code hash %x", addr, update.CodeHash)
	}
	update.CodeSize = witnessAcc.CodeSize
	return update, nil
}

// storageUpdate keeps a zeroed slot as a present zero when the witness holds its
// leaf: EIP-8297 has no removal, so the leaf stays and commits 32 zero bytes. A
// slot with no leaf to begin with is dropped — writing zero to it would commit a
// leaf the chain never had.
func (s *pbinWitnessStateless) storageUpdate(addr common.Address, slot common.Hash, value uint256.Int) (commitment.Update, bool, error) {
	update := commitment.Update{Flags: commitment.StorageUpdate}
	if value.IsZero() {
		_, ok, err := s.state.Storage(addr[:], slot[:])
		if err != nil || !ok {
			return update, false, err
		}
		return update, true, nil
	}
	trimmed := value.Bytes()
	update.StorageLen = int8(len(trimmed))
	copy(update.Storage[:], trimmed)
	return update, true, nil
}
