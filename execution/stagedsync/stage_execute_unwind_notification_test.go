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
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func diffKey(entity []byte, step uint64) string {
	return string(binary.BigEndian.AppendUint64(bytes.Clone(entity), ^step))
}

func TestStateChangesStreamAtUnwind(t *testing.T) {
	t.Parallel()

	oldAccount := accounts.SerialiseV3(&accounts.Account{Nonce: 6, Balance: *uint256.NewInt(2_000)})
	newAccount := accounts.SerialiseV3(&accounts.Account{Nonce: 7, Balance: *uint256.NewInt(1_000)})
	for _, tc := range []struct {
		name     string
		domain   kv.Domain
		previous []byte
		current  []byte
		diff     []byte
	}{
		{"nil account restores value", kv.AccountsDomain, oldAccount, newAccount, nil},
		{"nil account restores absence", kv.AccountsDomain, nil, newAccount, nil},
		{"account tombstone", kv.AccountsDomain, nil, newAccount, []byte{}},
		{"account value", kv.AccountsDomain, oldAccount, newAccount, oldAccount},
		{"nil storage restores value", kv.StorageDomain, []byte{1}, []byte{2}, nil},
		{"nil storage restores absence", kv.StorageDomain, nil, []byte{2}, nil},
		{"storage tombstone", kv.StorageDomain, nil, []byte{2}, []byte{}},
		{"storage value", kv.StorageDomain, []byte{1}, []byte{2}, []byte{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			logger := log.New()
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
			tx, err := db.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			writer, err := execctx.NewSharedDomains(ctx, tx, logger)
			require.NoError(t, err)
			defer writer.Close()

			addr := common.Address{19: 1}
			loc := common.Hash{31: 2}
			key, cacheKey := addr[:], addr[:]
			if tc.domain == kv.StorageDomain {
				key = dbutils.GenerateStoragePlainKey(addr, loc)
				cacheKey = append(append(bytes.Clone(addr[:]), make([]byte, 8)...), loc[:]...)
			}
			if tc.previous != nil {
				require.NoError(t, writer.DomainPut(tc.domain, tx, key, tc.previous, 5, nil))
			}
			require.NoError(t, writer.DomainPut(tc.domain, tx, key, tc.current, 20, tc.previous))
			require.NoError(t, writer.Flush(ctx, tx))

			doms, err := execctx.NewSharedDomains(ctx, tx, logger)
			require.NoError(t, err)
			defer doms.Close()
			require.NoError(t, doms.InitBlockOverlay(tx))

			cache := kvcache.New(kvcache.DefaultCoherentConfig)
			acc := notifications.NewAccumulator()
			acc.StartChange(makeHeader(2, common.Hash{}), nil, false)
			if tc.domain == kv.AccountsDomain {
				acc.ChangeAccount(addr, 0, tc.current)
			} else {
				acc.ChangeStorage(addr, 0, loc, tc.current)
			}
			cache.OnNewBlock((&notifications.BlockBatchNotification{StateVersionID: 1, Changes: acc.Changes()}).ToProtoBatch())

			var diffs [kv.DomainLen][]kv.DomainEntryDiff
			diffs[tc.domain] = []kv.DomainEntryDiff{{Key: diffKey(key, 1), Value: tc.diff}}
			doms.Unwind(16, &diffs)
			got, found, err := doms.GetAsOf(tc.domain, key, 16)
			require.NoError(t, err)
			require.Equal(t, tc.diff != nil, found, "nil diffs require a backing history lookup")
			require.True(t, bytes.Equal(tc.diff, got))
			got, _, err = tx.GetLatest(tc.domain, key, kv.GetLatestOptions{})
			require.NoError(t, err)
			require.Equal(t, tc.current, got, "database unwind is still pending")

			acc.Reset(2)
			acc.StartChange(makeHeader(1, common.Hash{}), nil, true)
			require.NoError(t, stateChangesStreamAtUnwind(ctx, doms.BlockOverlay(), 1, 16, acc, &diffs, logger))
			cache.OnNewBlock((&notifications.BlockBatchNotification{StateVersionID: 2, Changes: acc.Changes()}).ToProtoBatch())
			got, err = cache.Get(cacheKey, tx, 2)
			require.NoError(t, err)
			require.True(t, bytes.Equal(tc.previous, got), "cache: want %x, got %x", tc.previous, got)

			require.NoError(t, doms.Flush(ctx, tx))
			got, _, err = tx.GetLatest(tc.domain, key, kv.GetLatestOptions{})
			require.NoError(t, err)
			require.True(t, bytes.Equal(tc.previous, got), "database: want %x, got %x", tc.previous, got)
		})
	}
}

func TestStateChangesStreamAtUnwind_StorageIncarnation(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	writer, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer writer.Close()

	loc := common.Hash{31: 2}
	contracts := []struct {
		name           string
		address        common.Address
		incarnation    uint64
		accountChanged bool
	}{
		{"storage only at zero address", common.Address{}, 2, false},
		{"account and storage", common.Address{19: 1}, 1, true},
		{"storage only after account", common.Address{19: 2}, 3, false},
	}
	acc := notifications.NewAccumulator()
	acc.StartChange(makeHeader(2, common.Hash{}), nil, false)
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	for _, contract := range contracts {
		key := dbutils.GenerateStoragePlainKey(contract.address, loc)
		oldAccount := accounts.SerialiseV3(&accounts.Account{Nonce: 1, Incarnation: contract.incarnation})
		newAccount := oldAccount
		require.NoError(t, writer.DomainPut(kv.AccountsDomain, tx, contract.address[:], oldAccount, 5, nil))
		if contract.accountChanged {
			newAccount = accounts.SerialiseV3(&accounts.Account{Nonce: 2, Incarnation: contract.incarnation})
			require.NoError(t, writer.DomainPut(kv.AccountsDomain, tx, contract.address[:], newAccount, 20, oldAccount))
			diffs[kv.AccountsDomain] = append(diffs[kv.AccountsDomain], kv.DomainEntryDiff{Key: diffKey(contract.address[:], 1), Value: oldAccount})
		}
		require.NoError(t, writer.DomainPut(kv.StorageDomain, tx, key, []byte{1}, 5, nil))
		require.NoError(t, writer.DomainPut(kv.StorageDomain, tx, key, []byte{2}, 20, []byte{1}))
		diffs[kv.StorageDomain] = append(diffs[kv.StorageDomain], kv.DomainEntryDiff{Key: diffKey(key, 1), Value: []byte{1}})
		acc.ChangeAccount(contract.address, contract.incarnation, newAccount)
		acc.ChangeStorage(contract.address, contract.incarnation, loc, []byte{2})
	}
	require.NoError(t, writer.Flush(ctx, tx))

	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer doms.Close()
	require.NoError(t, doms.InitBlockOverlay(tx))

	cache := kvcache.New(kvcache.DefaultCoherentConfig)
	cache.OnNewBlock((&notifications.BlockBatchNotification{StateVersionID: 1, Changes: acc.Changes()}).ToProtoBatch())

	doms.Unwind(16, &diffs)
	acc.Reset(2)
	acc.StartChange(makeHeader(1, common.Hash{}), nil, true)
	require.NoError(t, stateChangesStreamAtUnwind(ctx, doms.BlockOverlay(), 1, 16, acc, &diffs, logger))
	require.Len(t, acc.Changes(), 1)
	require.Len(t, acc.Changes()[0].Changes, len(contracts))
	cache.OnNewBlock((&notifications.BlockBatchNotification{StateVersionID: 2, Changes: acc.Changes()}).ToProtoBatch())
	for i, contract := range contracts {
		t.Run(contract.name, func(t *testing.T) {
			change := acc.Changes()[0].Changes[i]
			require.Equal(t, contract.address, change.Address)
			require.Equal(t, contract.incarnation, change.Incarnation)
			if contract.accountChanged {
				oldAccount := accounts.SerialiseV3(&accounts.Account{Nonce: 1, Incarnation: contract.incarnation})
				require.Equal(t, oldAccount, change.Data)
			}
			cacheKey := append(binary.BigEndian.AppendUint64(bytes.Clone(contract.address[:]), contract.incarnation), loc[:]...)
			got, err := cache.Get(cacheKey, tx, 2)
			require.NoError(t, err)
			require.Equal(t, []byte{1}, got)
		})
	}
}

func TestStateChangesStreamAtUnwind_AcrossSteps(t *testing.T) {
	t.Parallel()

	oldVal := accounts.SerialiseV3(&accounts.Account{Nonce: 1})
	middleVal := accounts.SerialiseV3(&accounts.Account{Nonce: 2})
	newVal := accounts.SerialiseV3(&accounts.Account{Nonce: 3})
	for _, tc := range []struct {
		name       string
		domain     kv.Domain
		previous   []byte
		middle     []byte
		current    []byte
		oldestDiff []byte
	}{
		{"account", kv.AccountsDomain, oldVal, middleVal, newVal, oldVal},
		{"legacy account", kv.AccountsDomain, oldVal, middleVal, newVal, nil},
		{"storage", kv.StorageDomain, []byte{1}, []byte{2}, []byte{3}, []byte{1}},
		{"legacy storage", kv.StorageDomain, []byte{1}, []byte{2}, []byte{3}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			logger := log.New()
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
			tx, err := db.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			writer, err := execctx.NewSharedDomains(ctx, tx, logger)
			require.NoError(t, err)
			defer writer.Close()

			addr := common.Address{19: 1}
			loc := common.Hash{31: 2}
			key := addr[:]
			if tc.domain == kv.StorageDomain {
				key = dbutils.GenerateStoragePlainKey(addr, loc)
			}
			require.NoError(t, writer.DomainPut(tc.domain, tx, key, tc.previous, 5, nil))
			require.NoError(t, writer.DomainPut(tc.domain, tx, key, tc.middle, 20, tc.previous))
			require.NoError(t, writer.DomainPut(tc.domain, tx, key, tc.current, 40, tc.middle))
			require.NoError(t, writer.Flush(ctx, tx))

			doms, err := execctx.NewSharedDomains(ctx, tx, logger)
			require.NoError(t, err)
			defer doms.Close()
			require.NoError(t, doms.InitBlockOverlay(tx))
			var diffs [kv.DomainLen][]kv.DomainEntryDiff
			diffs[tc.domain] = []kv.DomainEntryDiff{
				{Key: diffKey(key, 2), Value: tc.middle},
				{Key: diffKey(key, 1), Value: tc.oldestDiff},
			}
			doms.Unwind(16, &diffs)

			acc := notifications.NewAccumulator()
			acc.StartChange(makeHeader(1, common.Hash{}), nil, true)
			require.NoError(t, stateChangesStreamAtUnwind(ctx, doms.BlockOverlay(), 1, 16, acc, &diffs, logger))
			require.Len(t, acc.Changes(), 1)
			require.Len(t, acc.Changes()[0].Changes, 1)
			change := acc.Changes()[0].Changes[0]
			if tc.domain == kv.AccountsDomain {
				require.Equal(t, tc.previous, change.Data)
			} else {
				require.Len(t, change.StorageChanges, 1)
				require.Equal(t, loc, change.StorageChanges[0].Location)
				require.Equal(t, tc.previous, change.StorageChanges[0].Data)
			}
		})
	}
}

type unwindHistoryErrorTx struct {
	kv.TemporalRwTx
	err error
}

func (tx unwindHistoryErrorTx) GetAsOf(kv.Domain, []byte, uint64) ([]byte, bool, error) {
	return nil, false, tx.err
}

func TestStateChangesStreamAtUnwind_WithoutAccumulator(t *testing.T) {
	t.Parallel()

	addr := common.Address{19: 1}
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	diffs[kv.AccountsDomain] = []kv.DomainEntryDiff{{Key: diffKey(addr[:], 1)}}
	diffs[kv.StorageDomain] = []kv.DomainEntryDiff{{Key: diffKey(dbutils.GenerateStoragePlainKey(addr, common.Hash{}), 1)}}
	tx := unwindHistoryErrorTx{err: errors.New("history unavailable")}
	require.NoError(t, stateChangesStreamAtUnwind(t.Context(), tx, 1, 16, nil, &diffs, log.New()))
}
