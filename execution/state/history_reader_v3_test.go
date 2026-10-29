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
	"errors"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type historyGateDebugTx struct {
	kv.TemporalDebugTx
	coverageStart uint64
	filesEnd      uint64
	recorded      bool
	lookupErr     error
}

func (d historyGateDebugTx) HistoryCoverageStart(kv.Domain) uint64 { return d.coverageStart }

func (d historyGateDebugTx) TxNumsInFiles(...kv.Domain) uint64 { return d.filesEnd }

func (d historyGateDebugTx) HistoryRecordedInDB(kv.Domain, uint64, uint64) (bool, error) {
	return d.recorded, d.lookupErr
}

// TestRequireStateHistory pins when state history may serve reads at a past
// txNum: history files prove their coverage, while history held only in the DB
// must show a record in the range, because an empty history table reports no
// start and a history read then answers from the latest state.
func TestRequireStateHistory(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		dbg  historyGateDebugTx
		ok   bool
	}{
		{"history starts after the range", historyGateDebugTx{coverageStart: 10, filesEnd: 20, recorded: true}, false},
		{"history files cover the range", historyGateDebugTx{filesEnd: 20}, true},
		{"DB history holds a record", historyGateDebugTx{recorded: true}, true},
		{"DB history holds no record", historyGateDebugTx{}, false},
		{"DB history lookup fails", historyGateDebugTx{recorded: true, lookupErr: errors.New("history read failed")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := RequireStateHistory(tc.dbg, 5, 6)
			if tc.ok {
				require.NoError(t, err)
				return
			}
			if tc.dbg.lookupErr != nil {
				require.ErrorIs(t, err, tc.dbg.lookupErr)
				return
			}
			require.ErrorIs(t, err, ErrPruned)
		})
	}
}

type countingAsOfTx struct {
	kv.TemporalTx
	reads int
	err   error
}

func (tx *countingAsOfTx) GetAsOf(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	tx.reads++
	if tx.err != nil {
		return nil, false, tx.err
	}
	return tx.TemporalTx.GetAsOf(domain, key, ts)
}

// An account committed before the read's txNum is served from the shared state cache, without a history lookup.
func TestHistoryReaderV3ReadsUnchangedAccountFromStateCache(t *testing.T) {
	ctx := t.Context()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
	b := 1 * datasize.MB
	stateCache := cache.NewStateCache(b, b, b, b)
	t.Cleanup(stateCache.Close)
	addr := common.Address{0xcc}
	enc := accounts.SerialiseV3(&accounts.Account{Nonce: 1})

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	writer, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer writer.Close()
	writer.BindStateCache(stateCache)
	writer.SetTxNum(5)
	require.NoError(t, writer.DomainPut(kv.AccountsDomain, rwTx, addr[:], enc, 5, nil))
	require.NoError(t, writer.Commit(ctx, rwTx))
	require.NoError(t, rwTx.Commit())

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(stateCache)

	tx := &countingAsOfTx{TemporalTx: roTx}
	acc, err := NewHistoryReaderV3WithSharedDomains(tx, sd, 10).ReadAccountData(accounts.InternAddress(addr))
	require.NoError(t, err)
	require.NotNil(t, acc)
	require.Equal(t, uint64(1), acc.Nonce)
	require.Zero(t, tx.reads)
}

// SetTx rebinds the in-batch read, so a later read is served by the new tx; a tx read error is not retried.
func TestHistoryReaderV3SetTxRebindsTheAsOfRead(t *testing.T) {
	ctx := t.Context()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	first, second := &countingAsOfTx{TemporalTx: roTx}, &countingAsOfTx{TemporalTx: roTx}
	r := NewHistoryReaderV3WithSharedDomains(first, sd, 10)
	_, err = r.ReadAccountData(accounts.InternAddress(common.Address{0xdd}))
	require.NoError(t, err)
	r.SetTx(second)
	_, err = r.ReadAccountData(accounts.InternAddress(common.Address{0xdd}))
	require.NoError(t, err)
	require.Equal(t, 1, first.reads)
	require.Equal(t, 1, second.reads)

	second.err = errors.New("pruned")
	_, err = r.ReadAccountData(accounts.InternAddress(common.Address{0xee}))
	require.ErrorIs(t, err, second.err)
	require.Equal(t, 2, second.reads, "a tx read error must not be retried")
}
