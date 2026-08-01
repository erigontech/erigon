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
	"maps"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// pbinStatelessState is the plain-state seam the binary engine reads while a
// witness is being built. It stands in for the domain layer: an absent key reads
// as deleted, exactly as a domain read does.
type pbinStatelessState struct {
	branches map[string][]byte
	accounts map[common.Address]commitment.Update
	storage  map[string]commitment.Update
	code     map[common.Address][]byte
}

func newPBinStatelessState() *pbinStatelessState {
	return &pbinStatelessState{
		branches: make(map[string][]byte),
		accounts: make(map[common.Address]commitment.Update),
		storage:  make(map[string]commitment.Update),
		code:     make(map[common.Address][]byte),
	}
}

func (s *pbinStatelessState) clone() *pbinStatelessState {
	c := newPBinStatelessState()
	for k, v := range s.branches {
		c.branches[k] = bytes.Clone(v)
	}
	maps.Copy(c.accounts, s.accounts)
	maps.Copy(c.storage, s.storage)
	for k, v := range s.code {
		c.code[k] = bytes.Clone(v)
	}
	return c
}

func (s *pbinStatelessState) Branch(prefix []byte) ([]byte, kv.Step, error) {
	return s.branches[string(prefix)], 0, nil
}

func (s *pbinStatelessState) PutBranch(prefix, data, prevData []byte) error {
	s.branches[string(prefix)] = bytes.Clone(data)
	return nil
}

func (s *pbinStatelessState) Account(plainKey []byte) (*commitment.Update, error) {
	update, ok := s.accounts[common.BytesToAddress(plainKey)]
	if !ok {
		return &commitment.Update{Flags: commitment.DeleteUpdate}, nil
	}
	return &update, nil
}

func (s *pbinStatelessState) Storage(plainKey []byte) (*commitment.Update, error) {
	update, ok := s.storage[string(plainKey)]
	if !ok {
		return &commitment.Update{Flags: commitment.DeleteUpdate}, nil
	}
	return &update, nil
}

func (s *pbinStatelessState) Code(plainKey []byte) ([]byte, error) {
	return s.code[common.BytesToAddress(plainKey)], nil
}

func (s *pbinStatelessState) setAccount(addr common.Address, nonce, balance uint64, code []byte) {
	update := commitment.Update{
		Flags:    commitment.NonceUpdate | commitment.BalanceUpdate | commitment.CodeUpdate,
		Nonce:    nonce,
		CodeHash: crypto.Keccak256Hash(code),
		CodeSize: uint64(len(code)),
	}
	update.Balance.SetUint64(balance)
	s.accounts[addr] = update
	if len(code) > 0 {
		s.code[addr] = bytes.Clone(code)
	}
}

func (s *pbinStatelessState) setStorage(addr common.Address, slot common.Hash, value uint64) {
	key := string(append(bytes.Clone(addr[:]), slot[:]...))
	if value == 0 {
		delete(s.storage, key)
		return
	}
	var v uint256.Int
	v.SetUint64(value)
	trimmed := v.Bytes()
	update := commitment.Update{Flags: commitment.StorageUpdate, StorageLen: int8(len(trimmed))}
	copy(update.Storage[:], trimmed)
	s.storage[key] = update
}

// pbinStatelessProcess folds the state the way the domain layer does — ModeDirect,
// so every value comes back through the context rather than the touch.
func pbinStatelessProcess(t *testing.T, state *pbinStatelessState, plainKeys [][]byte) []byte {
	t.Helper()
	trie, updates := commitment.InitializeTrieAndUpdates(commitment.ModeDirect, t.TempDir(),
		commitment.TrieConfig{Variant: commitment.VariantBinPatriciaTrie})
	defer trie.Release()
	trie.ResetContext(state)
	for _, key := range plainKeys {
		updates.TouchPlainKeyDirect(string(key), &commitment.Update{})
	}
	root, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	return bytes.Clone(root)
}

// pbinStatelessWitness captures the witness of the accessed keys and prunes it to
// their proof paths, which is the node set debug_executionWitness returns.
func pbinStatelessWitness(t *testing.T, state *pbinStatelessState, accessed [][]byte) ([][]byte, []byte) {
	t.Helper()
	trie, updates := commitment.InitializeTrieAndUpdates(commitment.ModeDirect, t.TempDir(),
		commitment.TrieConfig{Variant: commitment.VariantBinPatriciaTrie})
	defer trie.Release()
	trie.ResetContext(state)
	for _, key := range accessed {
		updates.TouchPlainKeyDirect(string(key), &commitment.Update{})
	}
	capturer, ok := trie.(interface {
		Witnesses(ctx context.Context, updates *commitment.Updates, produceExclusionProofs bool, logPrefix string) ([][]byte, [][]byte, []byte, error)
	})
	require.True(t, ok, "the binary trie captures no witness")

	full, provedKeys, root, err := capturer.Witnesses(context.Background(), updates, false, "")
	require.NoError(t, err)
	lean, err := commitment.PBinWitnessNodesForKeys(full, root, provedKeys)
	require.NoError(t, err)
	return lean, bytes.Clone(root)
}

func pbinStatelessAddr(b byte) common.Address {
	var addr common.Address
	addr[0], addr[19] = b, b
	return addr
}

func pbinStatelessSlot(n uint64) common.Hash {
	var v uint256.Int
	v.SetUint64(n)
	return common.Hash(v.Bytes32())
}

func pbinStatelessSlotBytes(n uint64) []byte {
	slot := pbinStatelessSlot(n)
	return slot[:]
}

// pbinStatelessCorpus is the pre-state every test in this file reads: an EOA, a
// contract whose code spans several header chunks, a contract whose code spills
// into the code zone, and storage in both the account header and the storage
// zone.
type pbinStatelessCorpus struct {
	state    *pbinStatelessState
	eoa      common.Address
	contract common.Address
	big      common.Address
	fresh    common.Address
	code     []byte
	bigCode  []byte
}

func pbinStatelessNewCorpus() *pbinStatelessCorpus {
	c := &pbinStatelessCorpus{
		state:    newPBinStatelessState(),
		eoa:      pbinStatelessAddr(0x11),
		contract: pbinStatelessAddr(0x22),
		big:      pbinStatelessAddr(0x33),
		fresh:    pbinStatelessAddr(0x44),
		code:     bytes.Repeat([]byte{0x60, 0x01}, 100),
		bigCode:  bytes.Repeat([]byte{0x5b}, 5000),
	}
	c.state.setAccount(c.eoa, 7, 1_000_000, nil)
	c.state.setAccount(c.contract, 1, 500, c.code)
	c.state.setAccount(c.big, 1, 900, c.bigCode)
	for _, slot := range []uint64{1, 63, 64, 1 << 20} {
		c.state.setStorage(c.contract, pbinStatelessSlot(slot), slot+1)
	}
	return c
}

// accessed is the key set the block touches: reads and writes both, which is what
// buildWitnessTrie folds over.
func (c *pbinStatelessCorpus) accessed() [][]byte {
	keys := [][]byte{c.eoa[:], c.contract[:], c.big[:], c.fresh[:]}
	for _, slot := range []uint64{1, 63, 64, 1 << 20, 999} {
		s := pbinStatelessSlot(slot)
		keys = append(keys, append(bytes.Clone(c.contract[:]), s[:]...))
	}
	return keys
}

func (c *pbinStatelessCorpus) verifier(t *testing.T) (*pbinWitnessStateless, [][]byte, common.Hash) {
	t.Helper()
	pbinStatelessProcess(t, c.state, c.accessed())
	nodes, root := pbinStatelessWitness(t, c.state, c.accessed())

	// Codes stays empty on purpose: under bin the chunk leaves are the code
	// source, so every code read here has to come out of State alone.
	result := &ExecutionWitnessResult{State: make([]hexutil.Bytes, len(nodes))}
	for i, node := range nodes {
		result.State[i] = node
	}
	stateless, err := newPBinWitnessStateless(result, common.BytesToHash(root))
	require.NoError(t, err)
	return stateless, nodes, common.BytesToHash(root)
}

// TestPBinWitnessStatelessResolvesAccessedState: the witness alone answers every
// account, slot and code read the block made.
func TestPBinWitnessStatelessResolvesAccessedState(t *testing.T) {
	t.Parallel()

	c := pbinStatelessNewCorpus()
	stateless, _, _ := c.verifier(t)

	eoa, err := stateless.ReadAccountData(accounts.InternAddress(c.eoa))
	require.NoError(t, err)
	require.NotNil(t, eoa)
	require.Equal(t, uint64(7), eoa.Nonce)
	require.Equal(t, uint64(1_000_000), eoa.Balance.Uint64())
	require.Equal(t, crypto.Keccak256Hash(nil), eoa.CodeHash.Value())

	contract, err := stateless.ReadAccountData(accounts.InternAddress(c.contract))
	require.NoError(t, err)
	require.NotNil(t, contract)
	require.Equal(t, crypto.Keccak256Hash(c.code), contract.CodeHash.Value())

	for _, tc := range []struct {
		addr common.Address
		want []byte
	}{
		{c.eoa, []byte{}},
		{c.contract, c.code},
		{c.big, c.bigCode},
	} {
		code, err := stateless.ReadAccountCode(accounts.InternAddress(tc.addr))
		require.NoError(t, err)
		require.Equal(t, tc.want, code, "code of %x", tc.addr)
		size, err := stateless.ReadAccountCodeSize(accounts.InternAddress(tc.addr))
		require.NoError(t, err)
		require.Equal(t, len(tc.want), size)
	}

	for _, slot := range []uint64{1, 63, 64, 1 << 20} {
		value, ok, err := stateless.ReadAccountStorage(accounts.InternAddress(c.contract),
			accounts.InternKey(pbinStatelessSlot(slot)))
		require.NoError(t, err)
		require.True(t, ok, "slot %d is absent", slot)
		require.Equal(t, slot+1, value.Uint64())
	}
}

// TestPBinWitnessStatelessAbsentResolvesWithoutError: absence is proved by the
// nodes on the way, so it resolves rather than erroring — and an absent read is
// not the same answer as an unresolved one.
func TestPBinWitnessStatelessAbsentResolvesWithoutError(t *testing.T) {
	t.Parallel()

	c := pbinStatelessNewCorpus()
	stateless, _, _ := c.verifier(t)

	acc, err := stateless.ReadAccountData(accounts.InternAddress(c.fresh))
	require.NoError(t, err)
	require.Nil(t, acc)

	value, ok, err := stateless.ReadAccountStorage(accounts.InternAddress(c.contract),
		accounts.InternKey(pbinStatelessSlot(999)))
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, value.IsZero())

	code, err := stateless.ReadAccountCode(accounts.InternAddress(c.fresh))
	require.NoError(t, err)
	require.Empty(t, code)
}

// TestPBinWitnessStatelessMissingNodeErrors: dropping a node has to make the read
// that needs it fail. An empty read there would hash a wrong subtree into the
// post-state root and report success.
func TestPBinWitnessStatelessMissingNodeErrors(t *testing.T) {
	t.Parallel()

	c := pbinStatelessNewCorpus()
	_, nodes, root := c.verifier(t)

	require.NotEmpty(t, nodes)
	broke := 0
	for drop := range nodes {
		trimmed := make([]hexutil.Bytes, 0, len(nodes)-1)
		for i, node := range nodes {
			if i != drop {
				trimmed = append(trimmed, node)
			}
		}
		stateless, err := newPBinWitnessStateless(&ExecutionWitnessResult{State: trimmed}, root)
		if err != nil {
			broke++ // the root node itself: the decode refuses before any read
			continue
		}
		if pbinStatelessReadsAll(t, stateless, c) != nil {
			broke++
		}
	}
	require.Equal(t, len(nodes), broke, "a node can be dropped without any read noticing")
}

// pbinStatelessReadsAll replays every read the corpus makes and returns the first
// failure.
func pbinStatelessReadsAll(t *testing.T, s *pbinWitnessStateless, c *pbinStatelessCorpus) error {
	t.Helper()
	for _, addr := range []common.Address{c.eoa, c.contract, c.big, c.fresh} {
		if _, err := s.ReadAccountData(accounts.InternAddress(addr)); err != nil {
			return err
		}
		if _, err := s.ReadAccountCode(accounts.InternAddress(addr)); err != nil {
			return err
		}
	}
	for _, slot := range []uint64{1, 63, 64, 1 << 20, 999} {
		if _, _, err := s.ReadAccountStorage(accounts.InternAddress(c.contract),
			accounts.InternKey(pbinStatelessSlot(slot))); err != nil {
			return err
		}
	}
	return nil
}

// TestPBinWitnessStatelessPostStateRoot is the gate the whole verifier exists
// for: the block's writes replayed over the witness reach the root the same
// writes reach over full state.
func TestPBinWitnessStatelessPostStateRoot(t *testing.T) {
	t.Parallel()

	c := pbinStatelessNewCorpus()
	stateless, _, parentRoot := c.verifier(t)

	deployed := bytes.Repeat([]byte{0x60, 0x02}, 40)
	writes := func(t *testing.T, s *pbinWitnessStateless) {
		t.Helper()
		eoa := accounts.InternAddress(c.eoa)
		acc, err := s.ReadAccountData(eoa)
		require.NoError(t, err)
		acc.Nonce, acc.Balance = 8, *uint256.NewInt(900_000)
		require.NoError(t, s.UpdateAccountData(eoa, nil, acc))

		contract := accounts.InternAddress(c.contract)
		contractAcc, err := s.ReadAccountData(contract)
		require.NoError(t, err)
		contractAcc.Balance = *uint256.NewInt(600)
		require.NoError(t, s.UpdateAccountData(contract, nil, contractAcc))
		require.NoError(t, s.WriteAccountStorage(contract, 0, accounts.InternKey(pbinStatelessSlot(1)),
			uint256.Int{}, *uint256.NewInt(0xAB)))
		// A slot the witness holds keeps its leaf as a present zero; one it proves
		// absent must not gain a leaf.
		require.NoError(t, s.WriteAccountStorage(contract, 0, accounts.InternKey(pbinStatelessSlot(64)),
			uint256.Int{}, uint256.Int{}))
		require.NoError(t, s.WriteAccountStorage(contract, 0, accounts.InternKey(pbinStatelessSlot(999)),
			uint256.Int{}, uint256.Int{}))

		fresh := accounts.InternAddress(c.fresh)
		require.NoError(t, s.CreateContract(fresh))
		require.NoError(t, s.UpdateAccountCode(fresh, 0, accounts.InternCodeHash(crypto.Keccak256Hash(deployed)), deployed))
		freshAcc := &accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(deployed))}
		freshAcc.Balance.SetUint64(42)
		require.NoError(t, s.UpdateAccountData(fresh, nil, freshAcc))
	}
	writes(t, stateless)

	got, err := stateless.Finalize(context.Background())
	require.NoError(t, err)

	full := c.state.clone()
	full.setAccount(c.eoa, 8, 900_000, nil)
	full.setAccount(c.contract, 1, 600, c.code)
	full.setStorage(c.contract, pbinStatelessSlot(1), 0xAB)
	full.setStorage(c.contract, pbinStatelessSlot(64), 0)
	full.setAccount(c.fresh, 1, 42, deployed)
	want := pbinStatelessProcess(t, full, [][]byte{
		c.eoa[:], c.contract[:], c.fresh[:],
		append(bytes.Clone(c.contract[:]), pbinStatelessSlotBytes(1)...),
		append(bytes.Clone(c.contract[:]), pbinStatelessSlotBytes(64)...),
		append(bytes.Clone(c.contract[:]), pbinStatelessSlotBytes(999)...),
	})

	require.Equal(t, common.BytesToHash(want), got)
	require.NotEqual(t, parentRoot, got, "the writes do not move the root, so the test proves nothing")
}

// TestPBinWitnessStatelessRefusesRemoval: the binary trie has no removal, so a
// block that drops an on-tree account cannot be verified — and must say so
// rather than produce a root.
func TestPBinWitnessStatelessRefusesRemoval(t *testing.T) {
	t.Parallel()

	c := pbinStatelessNewCorpus()
	stateless, _, _ := c.verifier(t)

	require.ErrorIs(t, stateless.DeleteAccount(accounts.InternAddress(c.eoa), nil), errPBinWitnessNoRemoval)
	// An account the witness proves absent was created and dropped inside the
	// block, so it leaves no leaf to remove.
	require.NoError(t, stateless.DeleteAccount(accounts.InternAddress(c.fresh), nil))
}
