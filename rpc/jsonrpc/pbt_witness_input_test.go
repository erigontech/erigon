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
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	pbtengine "github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestPBinWitnessInputExcludesOverlayReads(t *testing.T) {
	address := common.HexToAddress("0x4600000000000000000000000000000000000000")
	original := &accounts.Account{Nonce: 1}
	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{address: original}}
	rs := NewRecordingState(inner)
	_, err := rs.ReadAccountData(accounts.InternAddress(address))
	require.NoError(t, err)
	updated := *original
	updated.Nonce++
	require.NoError(t, rs.UpdateAccountData(accounts.InternAddress(address), original, &updated))
	_, err = rs.ReadAccountData(accounts.InternAddress(address))
	require.NoError(t, err)

	got, err := buildPBinWitnessInput(rs)
	require.NoError(t, err)
	wantKey := eip8297.TreeKeyAccount(address[:], eip8297.BasicDataLeafKey)
	require.Equal(t, [][]byte{wantKey}, got.Reads, "the pre-state read must be retained exactly once")

	overlayOnly := NewRecordingState(inner)
	require.NoError(t, overlayOnly.UpdateAccountData(accounts.InternAddress(address), original, &updated))
	_, err = overlayOnly.ReadAccountData(accounts.InternAddress(address))
	require.NoError(t, err)
	got, err = buildPBinWitnessInput(overlayOnly)
	require.NoError(t, err)
	require.NotContains(t, got.Reads, wantKey, "overlay read must not become a pre-state load")
}

func TestPBinWitnessInputKeepsRevertedCallReads(t *testing.T) {
	address := common.HexToAddress("0x4700000000000000000000000000000000000000")
	slot := common.HexToHash("0x80")
	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{address: {Nonce: 1}}}
	rs := NewRecordingState(inner)
	func() {
		_, _, err := rs.ReadAccountStorage(accounts.InternAddress(address), accounts.InternKey(slot))
		require.NoError(t, err)
	}()
	value := uint256.NewInt(1)
	require.NoError(t, rs.WriteAccountStorage(accounts.InternAddress(address), 0, accounts.InternKey(slot), uint256.Int{}, *value))

	got, err := buildPBinWitnessInput(rs)
	require.NoError(t, err)
	require.Contains(t, got.Reads, eip8297.TreeKeyStorage(address[:], slot[:]), "reverted-call read must remain a pre-state load")
}

func TestPBinWitnessInputCodesAreContentKeyed(t *testing.T) {
	address := common.HexToAddress("0x4800000000000000000000000000000000000000")
	first := append([]byte(nil), eip8297.DelegationMarker[:]...)
	first = append(first, bytes.Repeat([]byte{1}, eip8297.DelegationCodeLength-len(first))...)
	second := []byte{0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	reader := &pbinCodeReader{fakeStateReader: &fakeStateReader{accounts: map[common.Address]*accounts.Account{address: {Nonce: 1}}}, codes: map[common.Address][]byte{address: first}}
	rs := NewRecordingState(reader)
	size, err := rs.ReadAccountCodeSize(accounts.InternAddress(address))
	require.NoError(t, err)
	require.Equal(t, len(first), size)
	require.NoError(t, rs.UpdateAccountCode(accounts.InternAddress(address), 0, accounts.InternCodeHash(common.BytesToHash(second)), second))
	size, err = rs.ReadAccountCodeSize(accounts.InternAddress(address))
	require.NoError(t, err)
	require.Equal(t, len(second), size)

	got, err := buildPBinWitnessInput(rs)
	require.NoError(t, err)
	require.Contains(t, got.Codes, first, "the first code version must remain in the pbt code set")
	require.Contains(t, got.Codes, second, "the second code version must remain in the pbt code set")
	mpt := collectAccessedState(rs, witnessModeLegacy)
	require.Contains(t, mpt.SortedCodes, hexutil.Bytes(first), "the MPT code set must retain the first code version")
	require.Contains(t, mpt.SortedCodes, hexutil.Bytes(second), "the MPT code set must retain the second code version")
}

func TestPBinWitnessInputCollectsNetWrites(t *testing.T) {
	address := common.HexToAddress("0x4900000000000000000000000000000000000000")
	slot := common.HexToHash("0x80")
	original := &accounts.Account{Nonce: 1, Balance: *uint256.NewInt(4)}
	inner := &pbinCodeReader{
		fakeStateReader: &fakeStateReader{accounts: map[common.Address]*accounts.Account{address: original}},
		codes:           map[common.Address][]byte{address: {0x60, 0x00}},
	}
	rs := NewRecordingState(inner)
	updated := *original
	updated.Nonce = 2
	require.NoError(t, rs.UpdateAccountData(accounts.InternAddress(address), original, &updated))
	zero := uint256.Int{}
	require.NoError(t, rs.WriteAccountStorage(accounts.InternAddress(address), 0, accounts.InternKey(slot), *uint256.NewInt(7), zero))
	unchanged := common.HexToHash("0x81")
	require.NoError(t, rs.WriteAccountStorage(accounts.InternAddress(address), 0, accounts.InternKey(unchanged), uint256.Int{}, uint256.Int{}))
	deployed := []byte{0x60, 0x01, 0x00}
	require.NoError(t, rs.UpdateAccountCode(accounts.InternAddress(address), 0, accounts.InternCodeHash(common.HexToHash("0x1234")), deployed))

	got, err := buildPBinWitnessInput(rs)
	require.NoError(t, err)
	require.Len(t, got.Storage, 1, "only the changed storage slot is a pbt write")
	require.Equal(t, slot[:], got.Storage[0].Slot)
	require.Equal(t, make([]byte, eip8297.ValueLength), got.Storage[0].Value)
	require.Len(t, got.Accounts, 1)
	require.Equal(t, deployed, got.Accounts[0].Code)
	require.NotContains(t, got.Codes, deployed, "unread deployed code must stay out of pbt codes")
}

func TestPBinWitnessInputHandlesCreationDeletionAndDelegation(t *testing.T) {
	created := common.HexToAddress("0x4a00000000000000000000000000000000000000")
	delegated := common.HexToAddress("0x4b00000000000000000000000000000000000000")
	designator := append([]byte{}, eip8297.DelegationMarker[:]...)
	designator = append(designator, bytes.Repeat([]byte{2}, eip8297.DelegationCodeLength-len(designator))...)
	inner := &pbinCodeReader{fakeStateReader: &fakeStateReader{accounts: map[common.Address]*accounts.Account{delegated: {Nonce: 1}}}, codes: map[common.Address][]byte{delegated: nil}}
	rs := NewRecordingState(inner)
	require.NoError(t, rs.CreateContract(accounts.InternAddress(created)))
	require.NoError(t, rs.DeleteAccount(accounts.InternAddress(created), nil))
	require.NoError(t, rs.UpdateAccountCode(accounts.InternAddress(delegated), 0, accounts.InternCodeHash(common.Hash{}), designator))

	got, err := buildPBinWitnessInput(rs)
	require.NoError(t, err)
	require.NotContains(t, got.Deletes, created[:], "an account created and destroyed in one transaction has no pre-state deletion")
	require.Len(t, got.Accounts, 1)
	require.Equal(t, designator, got.Accounts[0].Delegation)

	rs = NewRecordingState(&pbinCodeReader{fakeStateReader: &fakeStateReader{accounts: map[common.Address]*accounts.Account{delegated: {Nonce: 1}}}, codes: map[common.Address][]byte{delegated: designator}})
	require.NoError(t, rs.UpdateAccountCode(accounts.InternAddress(delegated), 0, accounts.InternCodeHash(common.Hash{}), []byte{}))
	got, err = buildPBinWitnessInput(rs)
	require.NoError(t, err)
	require.Len(t, got.Accounts, 1)
	require.NotNil(t, got.Accounts[0].Code, "clearing delegation must be an explicit empty code update")
	require.Empty(t, got.Accounts[0].Code)
}

func TestPBinWitnessInputNewAccountMatchesEngineRoot(t *testing.T) {
	withBinCommitmentDatadir(t)
	sender := common.HexToAddress("0x5000000000000000000000000000000000000000")
	receiver := common.HexToAddress("0x5100000000000000000000000000000000000000")
	balance := uint256.NewInt(100)
	basic, err := eip8297.EncodeBasicData(0, balance, 0)
	require.NoError(t, err)
	emptyCodeHash := eip8297.CodeHashValue(common.Hash{})
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKeyAccount(sender[:], eip8297.BasicDataLeafKey), Value: basic[:]},
		{Key: eip8297.TreeKeyAccount(sender[:], eip8297.CodeHashLeafKey), Value: emptyCodeHash[:]},
	}
	preContext := newPBinWitnessInputContext()
	preRoot, err := pbtengine.NewTrie(preContext).Process(pbinWitnessInputEntriesToOps(entries))
	require.NoError(t, err)

	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{sender: {Balance: *balance, CodeHash: accounts.EmptyCodeHash}}}
	rs := NewRecordingState(inner)
	newAccount := &accounts.Account{Balance: *uint256.NewInt(10), CodeHash: accounts.EmptyCodeHash}
	require.NoError(t, rs.UpdateAccountData(accounts.InternAddress(receiver), nil, newAccount))
	input, err := buildPBinWitnessInput(rs)
	require.NoError(t, err)
	receiverUpdate := input.Accounts[0]
	require.Equal(t, emptyCodeHash[:], receiverUpdate.Values[eip8297.CodeHashLeafKey], "new codeless accounts need a code-hash leaf")

	_, _, resolverRoot, err := pbtengine.NewTrie(preContext).Witness(context.Background(), preRoot, input.PBinDriverInput)
	require.NoError(t, err)

	postContext := newPBinWitnessInputContext()
	postContext.records = clonePBinWitnessInputRecords(preContext.records)
	postBasic, err := eip8297.EncodeBasicData(0, &newAccount.Balance, 0)
	require.NoError(t, err)
	postOps := []pbtengine.Op{{Key: eip8297.TreeKeyAccount(receiver[:], eip8297.BasicDataLeafKey), Value: postBasic}, {Key: eip8297.TreeKeyAccount(receiver[:], eip8297.CodeHashLeafKey), Value: emptyCodeHash}}
	engineRoot, err := pbtengine.NewTrie(postContext).Process(postOps)
	require.NoError(t, err)
	require.Equal(t, engineRoot, resolverRoot, "the witness driver must match the engine root for a funded new account")
}

type pbinWitnessInputContext struct {
	records map[string][]byte
}

func newPBinWitnessInputContext() *pbinWitnessInputContext {
	return &pbinWitnessInputContext{records: make(map[string][]byte)}
}

func (c *pbinWitnessInputContext) Branch(key []byte) ([]byte, kv.Step, error) {
	return bytes.Clone(c.records[string(key)]), 0, nil
}

func (c *pbinWitnessInputContext) PutBranch(key, data, prev []byte) error {
	old := c.records[string(key)]
	if !bytes.Equal(old, prev) {
		return fmt.Errorf("pbin witness input: previous record mismatch for %x", key)
	}
	if len(data) == 0 {
		delete(c.records, string(key))
	} else {
		c.records[string(key)] = bytes.Clone(data)
	}
	return nil
}

func (*pbinWitnessInputContext) Account([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected account read")
}

func (*pbinWitnessInputContext) Storage([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected storage read")
}

func pbinWitnessInputEntriesToOps(entries []eip8297.Entry) []pbtengine.Op {
	ops := make([]pbtengine.Op, len(entries))
	for i, entry := range entries {
		var value [eip8297.ValueLength]byte
		copy(value[:], entry.Value)
		ops[i] = pbtengine.Op{Key: entry.Key, Value: value}
	}
	return ops
}

func clonePBinWitnessInputRecords(records map[string][]byte) map[string][]byte {
	clone := make(map[string][]byte, len(records))
	for key, value := range records {
		clone[key] = bytes.Clone(value)
	}
	return clone
}

type pbinCodeReader struct {
	*fakeStateReader
	codes map[common.Address][]byte
}

func (r *pbinCodeReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	return r.codes[address.Value()], nil
}

func (r *pbinCodeReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	return len(r.codes[address.Value()]), nil
}
