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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
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
// Code has one owner: the witness's own leaves — chunks reassembled and checked
// against the CODE_HASH leaf, or the delegation indicator read from its header
// leaf (commitment.PBinWitnessState.Code). result.Codes is not read. The leaves
// are committed by the root and the fold re-chunks every account it touches, so
// the pruned witness carries them wherever the post-state pass needs code; a
// blob list is keyed by code reads, a strictly narrower set. Code a block
// deploys has no pre-state leaves and arrives through UpdateAccountCode, as it
// does under hex.

// pbinExecBlockStatelessly re-executes the block against the binary witness alone
// and returns the post-state root it reaches. It is the bin arm of the gate
// debug_executionWitness applies before returning a witness; the replay itself is
// shared with hex. parentRoot roots the decode: the node set is not self-rooting.
func pbinExecBlockStatelessly(
	ctx context.Context,
	result *ExecutionWitnessResult,
	block *types.Block,
	parentRoot common.Hash,
	chainConfig *chain.Config,
	engine rules.Engine,
) (postStateRoot common.Hash, stateless *pbinWitnessStateless, err error) {
	// Genesis has no transactions but does have pre-allocated accounts, which no
	// witness covers.
	if block.NumberU64() == 0 {
		return block.Root(), nil, nil
	}
	if len(result.State) == 0 {
		return common.Hash{}, nil, errors.New("empty State field in witness")
	}

	stateless, err = newPBinWitnessStateless(result, parentRoot)
	if err != nil {
		return common.Hash{}, nil, err
	}
	if err := replayBlockOverWitness(result, block, chainConfig, engine, stateless); err != nil {
		return common.Hash{}, stateless, err
	}

	root, err := stateless.Finalize(ctx)
	if err != nil {
		return common.Hash{}, stateless, fmt.Errorf("[statelessExec] pbin post-state root failed: %w", err)
	}
	return root, stateless, nil
}

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
	// The binary tree commits no per-account storage root, so Root stays empty.
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

// HasStorage answers EIP-7610's CREATE-collision predicate. The binary tree
// commits no per-account storage root, so the witness's own leaves are the
// source: the header slots resolve off the proof path the account's leaves sit
// on, and the storage zone off the probe the builder touches for it (see
// accessedState.pbinStorageProbes).
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
	return s.state.HasStorage(addr[:]), nil
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

// DeleteAccount records the removal. The pre-state read is what makes it strict:
// an account whose leaves the witness cannot resolve errors here rather than
// being dropped on a guess.
func (s *pbinWitnessStateless) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	addr := address.Value()
	if _, err := s.preStateAccount(addr); err != nil {
		return err
	}
	delete(s.accountUpdates, addr)
	delete(s.storageWrites, addr)
	delete(s.codeUpdates, addr)
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
// earlier in the block puts its leaves back. Pre-state storage under it is the
// one case this cannot express — the chain drops the whole storage prefix here,
// and no plain-key update reaches that subtree without also dropping the header
// the create rewrites. EIP-7610 keeps a create off such an account, so the case
// is refused rather than answered with a root that keeps the leaves.
func (s *pbinWitnessStateless) CreateContract(address accounts.Address) error {
	addr := address.Value()
	if s.state.HasStorage(addr[:]) {
		return fmt.Errorf("create over account %x whose pre-state storage the witness proves", addr)
	}
	delete(s.deleted, addr)
	return nil
}

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
	// A removal is one update on the address: the engine drops the account's
	// header stem and its storage subtree, neither of which the writes enumerate.
	// Removals go first so that a write the block made after one merges over it,
	// as the same pair merges when the domain layer collects a block's updates:
	// DeleteAccount clears the maps below, so anything left in them is later.
	for addr := range s.deleted {
		plainKeys = append(plainKeys, addr[:])
		updates = append(updates, commitment.Update{Flags: commitment.DeleteUpdate})
	}
	for addr, acc := range s.accountUpdates {
		if acc == nil {
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

// storageUpdate writes a zeroed slot the witness holds, which the fold reads as
// a removal of its leaf. A slot with no leaf to begin with is dropped instead:
// there is nothing to remove, and the walk would prove a key the block never
// reached.
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
