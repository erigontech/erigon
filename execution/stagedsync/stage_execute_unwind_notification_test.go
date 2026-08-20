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

package stagedsync

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// diffKey builds a DomainEntryDiff key: the entity key followed by the 8-byte
// inverted step suffix that GetDiffset produces. The unwind notifier keys the
// account collect on the leading address bytes, so the suffix content is inert.
func diffKey(entity []byte) string {
	k := make([]byte, len(entity)+8)
	copy(k, entity)
	for i := len(entity); i < len(k); i++ {
		k[i] = 0xff // ^step for step 0
	}
	return string(k)
}

func changesByAddress(acc *notifications.Accumulator) map[common.Address]notifications.AccountChange {
	streamed := acc.Changes()
	out := make(map[common.Address]notifications.AccountChange)
	if len(streamed) == 0 {
		return out
	}
	for _, ch := range streamed[len(streamed)-1].Changes {
		out[ch.Address] = ch
	}
	return out
}

// A DomainEntryDiff with a nil Value means "different step": on unwind the account
// reverts to a value stored at another step, which is not a deletion (see the
// DomainEntryDiff.Value semantics in db/state/domain.go unwind). The notifier must
// not collapse it with an empty ([]byte{}) tombstone and broadcast ActionRemove,
// or the RPC state cache is told an account was deleted when it still exists.
func TestStateChangesStreamAtUnwind_NilAccountDiffIsNotDelete(t *testing.T) {
	t.Parallel()

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, 16)

	ctx := context.Background()
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	nilAddr := common.HexToAddress("0x00000000000000000000000000000000000000a1")
	delAddr := common.HexToAddress("0x00000000000000000000000000000000000000a2")
	chgAddr := common.HexToAddress("0x00000000000000000000000000000000000000a3")

	chgAccount := accounts.Account{Nonce: 7, Balance: *uint256.NewInt(1_000)}
	chgVal := accounts.SerialiseV3(&chgAccount)

	var cs [kv.DomainLen][]kv.DomainEntryDiff
	cs[kv.AccountsDomain] = []kv.DomainEntryDiff{
		{Key: diffKey(nilAddr[:]), Value: nil},      // different step -> no change
		{Key: diffKey(delAddr[:]), Value: []byte{}}, // absent before -> ActionRemove
		{Key: diffKey(chgAddr[:]), Value: chgVal},   // restore value -> ActionUpsert
	}

	acc := notifications.NewAccumulator()
	acc.StartChange(makeHeader(50, common.Hash{}), nil, true)

	require.NoError(t, stateChangesStreamAtUnwind(ctx, tx, 50, 500, acc, &cs, logger))

	changes := changesByAddress(acc)

	_, nilEmitted := changes[nilAddr]
	require.False(t, nilEmitted,
		"nil diff means 'different step', not a deletion — no state change must be streamed for it")

	del, ok := changes[delAddr]
	require.True(t, ok, "empty ([]byte{}) diff is a real removal and must be streamed")
	require.Equal(t, notifications.ActionRemove, del.Action)

	chg, ok := changes[chgAddr]
	require.True(t, ok, "non-empty diff must be streamed as an account change")
	require.Equal(t, notifications.ActionUpsert, chg.Action)
}

// The storage branch has the same nil-vs-empty hazard: a nil ("different step")
// storage diff must be skipped, not streamed as a ChangeStorage to an empty value
// (which would tell the state cache the slot was zeroed when it still holds a value
// restored at another step). A non-nil diff must still be streamed.
func TestStateChangesStreamAtUnwind_NilStorageDiffIsSkipped(t *testing.T) {
	t.Parallel()

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, 16)

	ctx := context.Background()
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	skipAddr := common.HexToAddress("0x00000000000000000000000000000000000000b1")
	skipLoc := common.Hash{31: 0x01}
	keepAddr := common.HexToAddress("0x00000000000000000000000000000000000000b2")
	keepLoc := common.Hash{31: 0x02}
	keepVal := []byte{0xbe, 0xef}

	var cs [kv.DomainLen][]kv.DomainEntryDiff
	cs[kv.StorageDomain] = []kv.DomainEntryDiff{
		{Key: diffKey(storageEntity(skipAddr, skipLoc)), Value: nil},
		{Key: diffKey(storageEntity(keepAddr, keepLoc)), Value: keepVal},
	}

	acc := notifications.NewAccumulator()
	acc.StartChange(makeHeader(50, common.Hash{}), nil, true)

	require.NoError(t, stateChangesStreamAtUnwind(ctx, tx, 50, 500, acc, &cs, logger))

	changes := changesByAddress(acc)

	_, skipEmitted := changes[skipAddr]
	require.False(t, skipEmitted,
		"nil storage diff means 'different step', not a slot change — nothing must be streamed for it")

	keep, ok := changes[keepAddr]
	require.True(t, ok, "non-nil storage diff must be streamed")
	require.Len(t, keep.StorageChanges, 1)
	require.Equal(t, keepLoc, keep.StorageChanges[0].Location)
	require.Equal(t, keepVal, keep.StorageChanges[0].Data)
}

func storageEntity(addr common.Address, loc common.Hash) []byte {
	k := make([]byte, 0, length.Addr+length.Hash)
	k = append(k, addr[:]...)
	k = append(k, loc[:]...)
	return k
}
