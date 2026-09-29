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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
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
	require.Contains(t, got.Deletes, created[:])
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
