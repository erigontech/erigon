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

package commitmentdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type pbinFeedReader struct {
	values map[string][]byte
}

func (r *pbinFeedReader) WithHistory() bool { return false }

func (r *pbinFeedReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *pbinFeedReader) Read(domain kv.Domain, key []byte, _ uint64) ([]byte, kv.Step, error) {
	return append([]byte(nil), r.values[fmt.Sprintf("%d:%x", domain, key)]...), 0, nil
}

func (r *pbinFeedReader) Clone(kv.TemporalTx) StateReader { return r }

func (r *pbinFeedReader) CloneForWorker(context.Context, kv.TemporalTx) StateReader { return r }

func pbinReaderValue(domain kv.Domain, key []byte) string {
	return fmt.Sprintf("%d:%x", domain, key)
}

func TestBinFeedFromStateReadsFinalAccountCodeAndSlots(t *testing.T) {
	address := common.HexToAddress("0x1234")
	code := []byte{1, 2, 3}
	codeHash := crypto.Keccak256Hash(code)
	account := accounts.Account{Nonce: 7, Balance: *uint256.NewInt(9), CodeHash: accounts.InternCodeHash(codeHash)}
	accountKey := address[:]
	storageKey := append(append([]byte(nil), accountKey...), make([]byte, 32)...)
	storageKey[len(accountKey)+31] = 1
	values := map[string][]byte{
		pbinReaderValue(kv.AccountsDomain, accountKey): accounts.SerialiseV3(&account),
		pbinReaderValue(kv.CodeDomain, accountKey):     code,
	}
	values[pbinReaderValue(kv.StorageDomain, storageKey)] = []byte{8}
	reader := &pbinFeedReader{values: values}
	keys := map[string]struct{}{string(accountKey): {}, string(storageKey): {}}
	codeKeys := map[string]struct{}{string(accountKey): {}}
	feed, err := BinFeedFromState(keys, codeKeys, map[string]struct{}{string(accountKey): {}}, reader)
	require.NoError(t, err)
	require.Len(t, feed.Accounts, 1)
	got := feed.Accounts[0]
	require.Equal(t, accountKey, got.Address)
	require.True(t, got.Exists)
	require.Equal(t, uint64(7), got.Nonce)
	require.True(t, got.Balance.Eq(uint256.NewInt(9)))
	require.Equal(t, common.Hash(codeHash), got.CodeHash)
	require.True(t, got.Wiped)
	require.True(t, got.CodeWritten)
	require.Equal(t, code, got.Code)
	require.Equal(t, []commitment.PBinFeedSlot{{Key: append(make([]byte, 31), 1), Value: []byte{8}}}, got.Slots)
}

func TestBinFeedFromStateRejectsCodeHashMismatch(t *testing.T) {
	address := common.HexToAddress("0x1234")
	account := accounts.Account{CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash([]byte{9}))}
	values := map[string][]byte{pbinReaderValue(kv.AccountsDomain, address[:]): accounts.SerialiseV3(&account)}
	values[pbinReaderValue(kv.CodeDomain, address[:])] = []byte{8}
	reader := &pbinFeedReader{values: values}
	_, err := BinFeedFromState(map[string]struct{}{string(address[:]): {}}, map[string]struct{}{string(address[:]): {}}, nil, reader)
	require.ErrorContains(t, err, "code hash mismatch")
}

func TestBinFeedFromStateDoesNotUseClearedCodeResidue(t *testing.T) {
	address := common.HexToAddress("0x1234")
	account := accounts.Account{CodeHash: accounts.EmptyCodeHash}
	residue := append(append([]byte(nil), eip8297.DelegationMarker[:]...), address[:]...)
	values := map[string][]byte{pbinReaderValue(kv.AccountsDomain, address[:]): accounts.SerialiseV3(&account)}
	values[pbinReaderValue(kv.CodeDomain, address[:])] = residue
	reader := &pbinFeedReader{values: values}
	feed, err := BinFeedFromState(map[string]struct{}{string(address[:]): {}}, map[string]struct{}{string(address[:]): {}}, nil, reader)
	require.NoError(t, err)
	require.Len(t, feed.Accounts, 1)
	require.True(t, feed.Accounts[0].CodeWritten)
	require.Equal(t, empty.CodeHash, feed.Accounts[0].CodeHash)
	require.Empty(t, feed.Accounts[0].Code)
}

func TestBinFeedFromStateKeepsFinalDelegationCode(t *testing.T) {
	address := common.HexToAddress("0x1234")
	delegation := append(append([]byte(nil), eip8297.DelegationMarker[:]...), address[:]...)
	account := accounts.Account{CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(delegation))}
	values := map[string][]byte{pbinReaderValue(kv.AccountsDomain, address[:]): accounts.SerialiseV3(&account)}
	values[pbinReaderValue(kv.CodeDomain, address[:])] = delegation
	reader := &pbinFeedReader{values: values}
	feed, err := BinFeedFromState(map[string]struct{}{string(address[:]): {}}, map[string]struct{}{string(address[:]): {}}, nil, reader)
	require.NoError(t, err)
	require.Equal(t, delegation, feed.Accounts[0].Code)
}

func TestBinFeedFromStateRejectsDelegationHashMismatch(t *testing.T) {
	address := common.HexToAddress("0x1234")
	delegation := append(append([]byte(nil), eip8297.DelegationMarker[:]...), address[:]...)
	account := accounts.Account{CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash([]byte{9}))}
	values := map[string][]byte{
		pbinReaderValue(kv.AccountsDomain, address[:]): accounts.SerialiseV3(&account),
		pbinReaderValue(kv.CodeDomain, address[:]):     delegation,
	}
	reader := &pbinFeedReader{values: values}
	_, err := BinFeedFromState(map[string]struct{}{string(address[:]): {}}, map[string]struct{}{string(address[:]): {}}, nil, reader)
	require.ErrorContains(t, err, "code hash mismatch")
}

func TestBinFeedFromStateRejectsMissingCode(t *testing.T) {
	address := common.HexToAddress("0x1234")
	codeHash := common.Hash{1}
	account := accounts.Account{CodeHash: accounts.InternCodeHash(codeHash)}
	reader := &pbinFeedReader{values: map[string][]byte{
		pbinReaderValue(kv.AccountsDomain, address[:]): accounts.SerialiseV3(&account),
	}}
	_, err := BinFeedFromState(map[string]struct{}{string(address[:]): {}}, map[string]struct{}{string(address[:]): {}}, nil, reader)
	require.ErrorContains(t, err, "code hash mismatch")
	require.ErrorContains(t, err, address.Hex()[2:])
}
