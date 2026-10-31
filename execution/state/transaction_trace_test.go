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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestTransactionTraceStorageWipe(t *testing.T) {
	for _, mode := range []string{"serial", "parallel", "prior-transaction"} {
		t.Run(mode, func(t *testing.T) {
			address := accounts.InternAddress(common.HexToAddress("0x1000"))
			written := accounts.InternKey(common.HexToHash("0x01"))
			untouched := accounts.InternKey(common.HexToHash("0x02"))
			account := accounts.NewAccount()
			account.Balance.SetUint64(1)
			reader := &storageReader{addr: address, account: &account, storage: map[accounts.StorageKey]uint256.Int{
				written: *uint256.NewInt(7), untouched: *uint256.NewInt(11),
			}}
			ibs := New(reader)
			t.Cleanup(ibs.Close)
			if mode != "serial" {
				versions := NewVersionMap(nil)
				if mode == "prior-transaction" {
					versions.WriteStorage(address, written, Version{TxIndex: 0}, *uint256.NewInt(7), true)
					versions.WriteStorage(address, untouched, Version{TxIndex: 0}, *uint256.NewInt(11), true)
					reader.storage = nil
				}
				ibs.SetVersionMap(versions)
			}
			ibs.SetTxContext(1, 1)
			ctx := ibs.NewFrameContext(&types.FrameTransaction{}, uint256.Int{})
			require.NoError(t, ibs.CreateAccount(address, true))
			require.NoError(t, ibs.SetState(address, written, *uint256.NewInt(22)))
			trace, err := ctx.TransactionTrace(ibs)
			require.NoError(t, err)
			before, err := trace.Storage(ibs, address, written, true)
			require.NoError(t, err)
			require.Equal(t, *uint256.NewInt(7), before)
			after, err := trace.Storage(ibs, address, written, false)
			require.NoError(t, err)
			require.Equal(t, *uint256.NewInt(22), after)
			before, err = trace.Storage(ibs, address, untouched, true)
			require.NoError(t, err)
			require.Equal(t, *uint256.NewInt(11), before)
			after, err = trace.Storage(ibs, address, untouched, false)
			require.NoError(t, err)
			require.True(t, after.IsZero())
			require.Len(t, trace.Slots, 1, "unwritten wiped slots are not enumerated")
			require.Equal(t, *uint256.NewInt(7), trace.Slots[0].Before)
			live, err := ibs.GetState(address, untouched)
			require.NoError(t, err)
			require.True(t, live.IsZero(), "the prestate read must not change the live view")
		})
	}
}

func TestCreatedStorageRevertPreservesOnlyBALRead(t *testing.T) {
	address := accounts.InternAddress(common.HexToAddress("0x1000"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	account := accounts.NewAccount()
	account.Balance.SetUint64(1)
	reader := &storageReader{addr: address, account: &account, storage: map[accounts.StorageKey]uint256.Int{key: {7}}}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)
	ibs.StartAccessRecording()
	require.NoError(t, ibs.CreateAccount(address, true))
	snapshot := ibs.PushSnapshot()
	require.NoError(t, ibs.SetState(address, key, uint256.Int{22}))
	ibs.RevertToSnapshot(snapshot, nil)
	ibs.PopSnapshot(snapshot)
	io := NewVersionedIO(1)
	io.RecordReads(Version{TxIndex: 0}, ibs.VersionedReads())
	io.RecordWrites(Version{TxIndex: 0}, ibs.VersionedWrites())
	bal := io.AsBlockAccessList()
	require.Len(t, bal, 1)
	require.Empty(t, bal[0].StorageChanges)
	require.Equal(t, []common.Hash{key.Value()}, bal[0].StorageReads)
}
