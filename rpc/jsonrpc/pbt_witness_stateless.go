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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	eipWitness "github.com/erigontech/erigon/execution/commitment/eip8297/witness"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func pbinExecBlockStatelessly(ctx context.Context, result *ExecutionWitnessResult, block *types.Block, parentRoot common.Hash, chainConfig *chain.Config, engine rules.Engine) (common.Hash, *pbinWitnessStateless, error) {
	if block.NumberU64() == 0 {
		return block.Root(), nil, nil
	}
	if len(result.State) == 0 && parentRoot != eip8297.EmptyTreeHash {
		return common.Hash{}, nil, errors.New("empty State field in witness")
	}
	stateless, err := newPBinWitnessStateless(result, parentRoot)
	if err != nil {
		return common.Hash{}, nil, err
	}
	if err := replayBlockOverWitness(result, block, chainConfig, engine, stateless); err != nil {
		return common.Hash{}, stateless, err
	}
	root, err := stateless.Finalize(ctx)
	if err != nil {
		return common.Hash{}, stateless, fmt.Errorf("pbin post-state root: %w", err)
	}
	return root, stateless, nil
}

func verifyPBinWitnessAgainstBlock(ctx context.Context, result *ExecutionWitnessResult, block *types.Block, parentRoot, expectedRoot common.Hash, chainConfig *chain.Config, engine rules.Engine) error {
	root, _, err := pbinExecBlockStatelessly(ctx, result, block, parentRoot, chainConfig, engine)
	if err != nil {
		return fmt.Errorf("pbin stateless execution: %w", err)
	}
	if root != expectedRoot {
		return fmt.Errorf("pbin state root mismatch after stateless execution: got %x, expected %x", root, expectedRoot)
	}
	return nil
}

type pbinWitnessStateless struct {
	tree  *eipWitness.PBinTree
	codes map[common.Hash][]byte

	systemCallScope bool

	codeUpdates      map[common.Address][]byte
	accountUpdates   map[common.Address]*accounts.Account
	preStateAccounts map[common.Address]bool
	storageWrites    map[common.Address]map[common.Hash]uint256.Int
	deleted          map[common.Address]struct{}

	trace       bool
	tracePrefix string
}

func (s *pbinWitnessStateless) setPBinSystemCallScope(active bool) {
	s.systemCallScope = active
}

var (
	_ state.StateReader = (*pbinWitnessStateless)(nil)
	_ state.StateWriter = (*pbinWitnessStateless)(nil)
)

func newPBinWitnessStateless(result *ExecutionWitnessResult, parentRoot common.Hash) (*pbinWitnessStateless, error) {
	if len(result.Keys) != len(result.State) {
		return nil, fmt.Errorf("pbin witness: keys and state lengths differ: %d and %d", len(result.Keys), len(result.State))
	}
	blobs := make(map[string][]byte, len(result.State))
	for index, key := range result.Keys {
		path := bytes.Clone(key)
		if len(result.State[index]) == 0 {
			return nil, fmt.Errorf("pbin witness: empty node at path %x", path)
		}
		if _, exists := blobs[string(path)]; exists {
			return nil, fmt.Errorf("pbin witness: duplicate node path %x", path)
		}
		blobs[string(path)] = bytes.Clone(result.State[index])
	}
	resolve := func(path []byte) ([]byte, error) {
		blob, ok := blobs[string(path)]
		if !ok {
			return nil, fmt.Errorf("%w: missing node at path %x", commitment.ErrPBinWitnessBlinded, path)
		}
		return bytes.Clone(blob), nil
	}
	tree, err := eipWitness.NewPBinTree(parentRoot, resolve)
	if err != nil {
		return nil, err
	}
	codes := make(map[common.Hash][]byte, len(result.Codes))
	for _, code := range result.Codes {
		hash := crypto.Keccak256Hash(code)
		if old, exists := codes[hash]; exists && !bytes.Equal(old, code) {
			return nil, fmt.Errorf("pbin witness: code hash collision for %x", hash)
		}
		codes[hash] = bytes.Clone(code)
	}
	return &pbinWitnessStateless{
		tree:             tree,
		codes:            codes,
		codeUpdates:      make(map[common.Address][]byte),
		accountUpdates:   make(map[common.Address]*accounts.Account),
		preStateAccounts: make(map[common.Address]bool),
		storageWrites:    make(map[common.Address]map[common.Hash]uint256.Int),
		deleted:          make(map[common.Address]struct{}),
	}, nil
}

func (s *pbinWitnessStateless) SetTrace(trace bool, tracePrefix string) {
	s.trace, s.tracePrefix = trace, tracePrefix
}

func (s *pbinWitnessStateless) Trace() bool { return s.trace }

func (s *pbinWitnessStateless) TracePrefix() string { return s.tracePrefix }

func (s *pbinWitnessStateless) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return s.ReadAccountData(address)
}

func (s *pbinWitnessStateless) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addr := address.Value()
	if account, ok := s.accountUpdates[addr]; ok {
		return account, nil
	}
	if _, deleted := s.deleted[addr]; deleted {
		return nil, nil
	}
	value, present, err := s.tree.Read(eip8297.TreeKeyAccount(addr[:], eip8297.BasicDataLeafKey))
	if err != nil {
		if s.systemCallScope && isPBinSystemAddress(addr) && errors.Is(err, commitment.ErrPBinWitnessBlinded) {
			return nil, nil
		}
		return nil, err
	}
	s.preStateAccounts[addr] = present
	if !present {
		return nil, nil
	}
	if len(value) != eip8297.ValueLength {
		return nil, fmt.Errorf("pbin witness: basic data for %x has length %d", addr, len(value))
	}
	account := &accounts.Account{Root: empty.RootHash}
	account.Nonce = binary.BigEndian.Uint64(value[eip8297.BasicDataNonceOffset:])
	account.Balance.SetBytes(value[eip8297.BasicDataBalanceOffset : eip8297.BasicDataBalanceOffset+16])
	delegation, delegationPresent, err := s.readDelegation(addr)
	if err != nil {
		return nil, err
	}
	if delegationPresent {
		account.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(delegation))
		return account, nil
	}
	codeHash, codeHashPresent, err := s.readCodeHash(addr)
	if err != nil {
		return nil, err
	}
	if !codeHashPresent {
		return nil, fmt.Errorf("pbin witness: account %x has no code-hash or delegation leaf", addr)
	}
	account.CodeHash = accounts.InternCodeHash(codeHash)
	return account, nil
}

func (s *pbinWitnessStateless) readCodeHash(addr common.Address) (common.Hash, bool, error) {
	value, present, err := s.tree.Read(eip8297.TreeKeyAccount(addr[:], eip8297.CodeHashLeafKey))
	if err != nil {
		return common.Hash{}, false, err
	}
	if !present {
		return common.Hash{}, false, nil
	}
	if len(value) != eip8297.ValueLength {
		return common.Hash{}, false, fmt.Errorf("pbin witness: code hash for %x has length %d", addr, len(value))
	}
	return common.BytesToHash(value), true, nil
}

func (s *pbinWitnessStateless) readDelegation(addr common.Address) ([]byte, bool, error) {
	value, present, err := s.tree.Read(eip8297.TreeKeyAccount(addr[:], eip8297.DelegationLeafKey))
	if err != nil {
		return nil, false, err
	}
	if !present {
		return nil, false, nil
	}
	if len(value) != eip8297.ValueLength || !eip8297.IsDelegation(value[:eip8297.DelegationCodeLength]) {
		return nil, false, fmt.Errorf("pbin witness: invalid delegation leaf for %x", addr)
	}
	return bytes.Clone(value[:eip8297.DelegationCodeLength]), true, nil
}

func (s *pbinWitnessStateless) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addr, slot := address.Value(), key.Value()
	if writes, ok := s.storageWrites[addr]; ok {
		if value, ok := writes[slot]; ok {
			return value, !value.IsZero(), nil
		}
	}
	if _, deleted := s.deleted[addr]; deleted {
		return uint256.Int{}, false, nil
	}
	value, present, err := s.tree.Read(eip8297.TreeKeyStorage(addr[:], slot[:]))
	if err != nil {
		if s.systemCallScope && isPBinSystemAddress(addr) && errors.Is(err, commitment.ErrPBinWitnessBlinded) {
			return uint256.Int{}, false, nil
		}
		return uint256.Int{}, false, err
	}
	if !present {
		return uint256.Int{}, false, nil
	}
	if len(value) != eip8297.ValueLength {
		return uint256.Int{}, false, fmt.Errorf("pbin witness: storage value for %x/%x has length %d", addr, slot, len(value))
	}
	var result uint256.Int
	result.SetBytes(value)
	return result, !result.IsZero(), nil
}

func (s *pbinWitnessStateless) ReadAccountCode(address accounts.Address) ([]byte, error) {
	addr := address.Value()
	if code, ok := s.codeUpdates[addr]; ok {
		return bytes.Clone(code), nil
	}
	if _, deleted := s.deleted[addr]; deleted {
		return nil, nil
	}
	account, err := s.ReadAccountData(address)
	if err != nil || account == nil {
		return nil, err
	}
	if delegation, present, err := s.readDelegation(addr); err != nil {
		return nil, err
	} else if present {
		return delegation, nil
	}
	codeHash := account.CodeHash.Value()
	if eip8297.IsEmptyCodeHash(codeHash) {
		return nil, nil
	}
	code, ok := s.codes[codeHash]
	if !ok {
		return nil, fmt.Errorf("pbin witness: missing code for account %x with code hash %x", addr, codeHash)
	}
	return bytes.Clone(code), nil
}

func (s *pbinWitnessStateless) ReadAccountCodeSize(address accounts.Address) (int, error) {
	code, err := s.ReadAccountCode(address)
	return len(code), err
}

func (*pbinWitnessStateless) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }

func (s *pbinWitnessStateless) UpdateAccountData(address accounts.Address, _, account *accounts.Account) error {
	addr := address.Value()
	if account == nil {
		s.accountUpdates[addr] = nil
		return nil
	}
	copyAccount := new(accounts.Account)
	copyAccount.Copy(account)
	s.accountUpdates[addr] = copyAccount
	delete(s.deleted, addr)
	return nil
}

func (s *pbinWitnessStateless) DeleteAccount(address accounts.Address, _ *accounts.Account) error {
	addr := address.Value()
	if _, checked := s.preStateAccounts[addr]; !checked {
		if _, err := s.ReadAccountData(address); err != nil {
			return err
		}
	}
	if !s.preStateAccounts[addr] {
		return nil
	}
	if err := s.tree.DeleteAccount(addr[:]); err != nil {
		return err
	}
	delete(s.accountUpdates, addr)
	delete(s.codeUpdates, addr)
	delete(s.storageWrites, addr)
	s.deleted[addr] = struct{}{}
	return nil
}

func (s *pbinWitnessStateless) UpdateAccountCode(address accounts.Address, _ uint64, codeHash accounts.CodeHash, code []byte) error {
	addr := address.Value()
	s.codeUpdates[addr] = bytes.Clone(code)
	if account, ok := s.accountUpdates[addr]; ok && account != nil {
		account.CodeHash = codeHash
	}
	return nil
}

func (s *pbinWitnessStateless) WriteAccountStorage(address accounts.Address, _ uint64, key accounts.StorageKey, _, value uint256.Int) error {
	addr, slot := address.Value(), key.Value()
	writes := s.storageWrites[addr]
	if writes == nil {
		writes = make(map[common.Hash]uint256.Int)
		s.storageWrites[addr] = writes
	}
	writes[slot] = value
	return nil
}

func (s *pbinWitnessStateless) CreateContract(address accounts.Address) error {
	addr := address.Value()
	if s.preStateAccounts[addr] {
		if err := s.tree.DeleteAccount(addr[:]); err != nil {
			return err
		}
	}
	delete(s.deleted, addr)
	delete(s.storageWrites, addr)
	return nil
}

func (s *pbinWitnessStateless) Finalize(ctx context.Context) (common.Hash, error) {
	if ctx == nil {
		return common.Hash{}, errors.New("pbin witness: nil context")
	}
	if err := ctx.Err(); err != nil {
		return common.Hash{}, err
	}
	input := eipWitness.PBinDriverInput{}
	for addr, writes := range s.storageWrites {
		for slot, value := range writes {
			encoded := value.Bytes32()
			input.Storage = append(input.Storage, eipWitness.PBinStorageWrite{Address: bytes.Clone(addr[:]), Slot: bytes.Clone(slot[:]), Value: bytes.Clone(encoded[:])})
		}
	}
	for addr, account := range s.accountUpdates {
		if account == nil {
			continue
		}
		codeSize, err := s.codeSize(addr)
		if err != nil {
			return common.Hash{}, err
		}
		basic, err := eip8297.EncodeBasicData(account.Nonce, &account.Balance, codeSize)
		if err != nil {
			return common.Hash{}, err
		}
		update := eipWitness.PBinAccountUpdate{Address: bytes.Clone(addr[:]), Values: map[byte][]byte{eip8297.BasicDataLeafKey: basic[:]}}
		if code, changed := s.codeUpdates[addr]; changed {
			if eip8297.IsDelegation(code) {
				update.Delegation = bytes.Clone(code)
			} else {
				update.Code = bytes.Clone(code)
			}
		} else if delegation, present, err := s.readDelegation(addr); err != nil {
			return common.Hash{}, err
		} else if present {
			update.Delegation = delegation
		} else {
			codeHash := eip8297.CodeHashValue(account.CodeHash.Value())
			update.Values[eip8297.CodeHashLeafKey] = codeHash[:]
		}
		input.Accounts = append(input.Accounts, update)
	}
	root, _, err := s.tree.Apply(input)
	return root, err
}

func (s *pbinWitnessStateless) codeSize(addr common.Address) (uint64, error) {
	if code, changed := s.codeUpdates[addr]; changed {
		return uint64(len(code)), nil
	}
	if account, updated := s.accountUpdates[addr]; updated && account != nil && eip8297.IsEmptyCodeHash(account.CodeHash.Value()) {
		return 0, nil
	}
	value, present, err := s.tree.Read(eip8297.TreeKeyAccount(addr[:], eip8297.BasicDataLeafKey))
	if err != nil {
		return 0, err
	}
	if !present {
		return 0, fmt.Errorf("pbin witness: missing basic data for account %x", addr)
	}
	if len(value) != eip8297.ValueLength {
		return 0, fmt.Errorf("pbin witness: basic data for %x has length %d", addr, len(value))
	}
	return uint64(binary.BigEndian.Uint32(value[eip8297.BasicDataCodeSizeOffset:])), nil
}

func isPBinSystemAddress(addr common.Address) bool {
	return addr == common.Address(params.SystemAddress.Value())
}
