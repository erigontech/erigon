// Copyright 2024 The Erigon Authors
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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// The reader treats AddressPath >= destructTx as revival too; the validator doesn't. Production's CodeHash co-write at the revival tx is what keeps the two definitions from diverging.

func recreatedAccount(inc uint64) *accounts.Account {
	return &accounts.Account{
		Nonce:       1,
		Incarnation: inc,
		CodeHash:    accounts.InternCodeHash(common.HexToHash("0xdeadbeefcafebabe1111111111111111111111111111111111111111111111ff")),
	}
}

// Same-tx SD+CREATE2: the re-create rewrites the read's own path at the same
// tx, so checkVersion catches staleness before the revival arm is reached.
func TestRevivalConsistency_SameTxMetamorphic_ReaderAndValidatorAgree(t *testing.T) {
	t.Parallel()
	addr := getAddress(78)

	vm := NewVersionMap(nil)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0}, *uint256.NewInt(1_000), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 3}, true, true)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 3}, uint256.Int{}, true)
	writeFor(vm, addr, IncarnationPath, accounts.NilKey, Version{TxIndex: 3}, uint64(2), true)
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 3}, recreatedAccount(2).CodeHash, true)
	vm.WriteAddress(addr, Version{TxIndex: 3}, recreatedAccount(2), true)

	reader := newAccountStateReader(addr)
	ibs := New(NewVersionedStateReader(4, ReadSet{}, vm, reader))
	ibs.SetTxContext(0, 4)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	account, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, account, "reader must surface the re-created account")
	require.Equal(t, uint64(2), account.Incarnation)

	io := NewVersionedIO(5)
	rs := ReadSet{}
	rs.SetBalance(addr, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 0}},
		Val:        *uint256.NewInt(1_000),
	})
	io.RecordReads(Version{TxIndex: 4}, rs)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(4, io, validateEqualVersion, true, false, false, ""),
		"pre-destruct field read is stale after same-tx re-create — reader and validator agree the old value is gone")
}

// AddressPath-only revival — no field rewritten at the same tx — makes the
// reader report revived while the validator doesn't; createObject never produces this shape.
func TestRevivalConsistency_AddressPathOnly_ReaderAndValidatorDiverge(t *testing.T) {
	t.Parallel()
	addr := getAddress(79)

	vm := NewVersionMap(nil)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0}, *uint256.NewInt(1_000), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
	vm.WriteAddress(addr, Version{TxIndex: 2}, recreatedAccount(2), true)

	reader := newAccountStateReader(addr)
	ibs := New(NewVersionedStateReader(5, ReadSet{}, vm, reader))
	ibs.SetTxContext(0, 5)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	account, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, account, "reader's AddressPath >= arm reports the account revived")

	io := NewVersionedIO(6)
	rs := ReadSet{}
	rs.SetBalance(addr, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 0}},
		Val:        *uint256.NewInt(1_000),
	})
	io.RecordReads(Version{TxIndex: 5}, rs)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(5, io, validateEqualVersion, true, false, false, ""),
		"validator lacks the AddressPath >= revival arm, so it diverges from the reader on an AddressPath-only revival")
}
