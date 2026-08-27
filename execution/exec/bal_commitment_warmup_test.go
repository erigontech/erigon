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

package exec

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type commitmentRecordingTx struct {
	kv.TemporalTx
	mu      sync.Mutex
	domains []kv.Domain
}

func (tx *commitmentRecordingTx) GetLatest(domain kv.Domain, _ []byte, _ kv.GetLatestOptions) ([]byte, kv.Step, error) {
	tx.mu.Lock()
	tx.domains = append(tx.domains, domain)
	tx.mu.Unlock()
	return nil, 0, nil
}

func (*commitmentRecordingTx) Rollback() {}

type commitmentBeginErrorDB struct {
	kv.RoDB
	errs []error
	next atomic.Uint64
}

func (db *commitmentBeginErrorDB) BeginRo(context.Context) (kv.Tx, error) {
	return nil, db.errs[db.next.Add(1)-1]
}

func TestBALCommitmentWarmupKeysUseChangesOnly(t *testing.T) {
	readOnlyAddress := accounts.InternAddress(common.Address{19: 1})
	accountAddress := accounts.InternAddress(common.Address{19: 2})
	storageAddress := accounts.InternAddress(common.Address{19: 3})
	readSlot := accounts.InternKey(common.Hash{31: 4})
	changedSlot := accounts.InternKey(common.Hash{31: 5})
	bal := types.BlockAccessList{
		{Address: readOnlyAddress, StorageReads: []accounts.StorageKey{readSlot}},
		{Address: accountAddress, BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}}},
		{Address: storageAddress, StorageChanges: []*types.SlotChanges{{
			Slot: changedSlot, Changes: []*types.StorageChange{{Value: *uint256.NewInt(2)}},
		}}},
	}

	accountValue := accountAddress.Value()
	storageValue := storageAddress.Value()
	slotValue := changedSlot.Value()
	storageKey := make([]byte, len(storageValue)+len(slotValue))
	copy(storageKey, storageValue[:])
	copy(storageKey[len(storageValue):], slotValue[:])
	want := [][]byte{
		commitment.KeyToHexNibbleHash(accountValue[:]),
		commitment.KeyToHexNibbleHash(storageKey),
	}
	slices.SortFunc(want, bytes.Compare)

	got := balCommitmentWarmupKeys(bal)
	require.Equal(t, want, got)
	require.True(t, slices.IsSortedFunc(got, bytes.Compare))
}

func TestWarmBALCommitmentReadsCommitmentDomain(t *testing.T) {
	tx := new(commitmentRecordingTx)
	db := &singleTxRoDB{tx: tx}
	bal := types.BlockAccessList{{
		Address:        accounts.InternAddress(common.Address{19: 2}),
		BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}},
	}}

	require.NoError(t, warmBALCommitment(t.Context(), db, bal, 2))

	tx.mu.Lock()
	defer tx.mu.Unlock()
	require.NotEmpty(t, tx.domains)
	for _, domain := range tx.domains {
		require.Equal(t, kv.CommitmentDomain, domain)
	}
}

func TestWarmBALCommitmentCollectsWorkerFactoryErrors(t *testing.T) {
	firstErr := errors.New("first factory error")
	secondErr := errors.New("second factory error")
	db := &commitmentBeginErrorDB{errs: []error{firstErr, secondErr}}
	bal := types.BlockAccessList{
		{
			Address:        accounts.InternAddress(common.Address{19: 1}),
			BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}},
		},
		{
			Address:        accounts.InternAddress(common.Address{19: 2}),
			BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(2)}},
		},
	}

	err := warmBALCommitment(t.Context(), db, bal, 2)
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
}
