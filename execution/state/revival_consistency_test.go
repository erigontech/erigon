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

// Revival is decided at three sites with two different definitions:
//
//   - the two readers — getVersionedAccount and versionedStateReader.ReadAccountData —
//     agree exactly: revived = AddressPath >= destructTx OR {Balance,Nonce,CodeHash} > destructTx
//   - the validator — validateReadImpl — omits the AddressPath >= arm:
//     revived = {Balance,Nonce,CodeHash} > destructTx
//
// These tests pin whether that omission is observably divergent. The invariant
// they lock: production revival (createObject) always co-writes CodeHash at the
// revival tx, so the two definitions never disagree on a production-shaped
// history — but the validator's verdict *does* diverge from the readers in the
// synthetic AddressPath-only revival, which is why the co-write is load-bearing.

func recreatedAccount(inc uint64) *accounts.Account {
	return &accounts.Account{
		Nonce:       1,
		Incarnation: inc,
		CodeHash:    accounts.InternCodeHash(common.HexToHash("0xdeadbeefcafebabe1111111111111111111111111111111111111111111111ff")),
	}
}

// Production-shaped same-tx metamorphic SD+CREATE2: the reader surfaces the
// re-created account, and the validator invalidates a pre-destruct field read —
// consistent, because the re-create re-writes the read's own path at the same
// tx, so checkVersion catches the staleness before the revival arm is reached.
func TestRevivalConsistency_SameTxMetamorphic_ReaderAndValidatorAgree(t *testing.T) {
	t.Parallel()
	addr := getAddress(78)

	vm := NewVersionMap(nil)
	// Pre-destruct account established at tx0.
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0}, *uint256.NewInt(1_000), true)
	// Tx3: SD + CREATE2, all writes at (3,0) — what createObject/newObject emit.
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 3}, true, true)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 3}, uint256.Int{}, true)
	writeFor(vm, addr, IncarnationPath, accounts.NilKey, Version{TxIndex: 3}, uint64(2), true)
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 3}, recreatedAccount(2).CodeHash, true)
	vm.WriteAddress(addr, Version{TxIndex: 3}, recreatedAccount(2), true)

	// Reader: tx4 sees the re-created account (AddressPath >= destructTx arm).
	reader := newAccountStateReader(addr)
	ibs := New(NewVersionedStateReader(4, ReadSet{}, vm, reader))
	ibs.SetTxContext(0, 4)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	account, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, account, "reader must surface the re-created account")
	require.Equal(t, uint64(2), account.Incarnation)

	// Validator: tx4 recorded the pre-destruct balance (1000 @ {0,0}). The
	// re-create re-wrote balance at tx3, so this read is genuinely stale and
	// must invalidate — caught by version mismatch, not the revival arm.
	io := NewVersionedIO(5)
	rs := ReadSet{}
	rs.SetBalance(addr, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 0}},
		Val:        *uint256.NewInt(1_000),
	})
	io.RecordReads(Version{TxIndex: 4}, rs)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(4, io, validateEqualVersion, false, ""),
		"pre-destruct field read is stale after same-tx re-create — reader and validator agree the old value is gone")
}

// Synthetic AddressPath-only revival: no field is re-written at the revival tx.
// Here the reader reports revived (AddressPath >= arm) while the validator does
// not (it lacks that arm) — the divergence. Production never produces this
// history because createObject always co-writes CodeHash; this test documents
// that the co-write is what keeps the two revival definitions consistent.
func TestRevivalConsistency_AddressPathOnly_ReaderAndValidatorDiverge(t *testing.T) {
	t.Parallel()
	addr := getAddress(79)

	vm := NewVersionMap(nil)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0}, *uint256.NewInt(1_000), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
	// Revival writes ONLY AddressPath at the same tx as the destruct — no
	// Balance/Nonce/CodeHash re-write. createObject never does this.
	vm.WriteAddress(addr, Version{TxIndex: 2}, recreatedAccount(2), true)

	reader := newAccountStateReader(addr)
	ibs := New(NewVersionedStateReader(5, ReadSet{}, vm, reader))
	ibs.SetTxContext(0, 5)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	account, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, account, "reader's AddressPath >= arm reports the account revived")

	// The pre-destruct balance read still matches the latest balance write
	// ({0,0}=1000, since revival wrote no balance), so checkVersion passes and
	// the SD-staleness revival arm is actually reached — and the validator,
	// lacking the AddressPath arm, invalidates where the reader revived.
	io := NewVersionedIO(6)
	rs := ReadSet{}
	rs.SetBalance(addr, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 0}},
		Val:        *uint256.NewInt(1_000),
	})
	io.RecordReads(Version{TxIndex: 5}, rs)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(5, io, validateEqualVersion, false, ""),
		"validator lacks the AddressPath >= revival arm, so it diverges from the reader on an AddressPath-only revival")
}
