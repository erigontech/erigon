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

package execctx_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
)

type countingTxReads struct {
	kv.TemporalTx
	reads int
}

func (tx *countingTxReads) GetAsOf(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	tx.reads++
	return tx.TemporalTx.GetAsOf(domain, key, ts)
}

func (tx *countingTxReads) HistorySeek(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	tx.reads++
	return tx.TemporalTx.HistorySeek(domain, key, ts)
}

func (tx *countingTxReads) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	tx.reads++
	return tx.TemporalTx.GetLatest(domain, key, opts)
}

func TestAsOfGetterRepeatReadOfLatestOnlyKeyHitsTheCache(t *testing.T) {
	db := newTestDb(t, 16)
	stateCache := newSmallStateCache()
	defer stateCache.Close()
	key := bytes.Repeat([]byte{0xcc}, 20)
	commitAccount(t, db, nil, key, encAccount(1), 5)
	indexBlock1(t, db, 1)
	require.NoError(t, execctx.InitStateCacheVersion(t.Context(), db, stateCache))

	read := func() int {
		roTx, err := db.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer roTx.Rollback()
		sd, err := execctx.NewSharedDomains(t.Context(), roTx, log.New())
		require.NoError(t, err)
		defer sd.Close()
		sd.BindStateCache(stateCache)
		tx := &countingTxReads{TemporalTx: roTx}
		v, ok, err := sd.AsOfGetter(tx).GetAsOf(kv.AccountsDomain, key, 10)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, encAccount(1), v)
		return tx.reads
	}
	require.Positive(t, read())
	require.Zero(t, read(), "the first read fills the cache with a value older than ts")
}

// The as-of getter must answer exactly as tx.GetAsOf, before and after its reads fill the cache.
func TestAsOfGetterMatchesTxGetAsOf(t *testing.T) {
	const stepSize = 4
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	stateCache := newSmallStateCache()
	defer stateCache.Close()

	type write struct {
		txNum uint64
		v     []byte // nil deletes
	}
	histories := map[string][]write{
		"history":   {{2, []byte{1}}, {9, []byte{2}}, {13, []byte{3}}},
		"latest":    {{3, []byte{1}}},
		"deleted":   {{1, []byte{1}}, {6, nil}},
		"recreated": {{5, []byte{1}}, {10, nil}, {14, []byte{2}}},
		"edges":     {{4, []byte{1}}, {7, []byte{2}}, {8, []byte{3}}, {11, []byte{4}}, {12, []byte{5}}},
		"absent":    nil,
	}
	keyOf := func(domain kv.Domain, name string) []byte {
		k := bytes.Repeat([]byte{byte(len(name))}, 20)
		copy(k, name)
		if domain == kv.StorageDomain {
			k = append(k, bytes.Repeat([]byte{0x5a}, 32)...)
		}
		return k
	}
	valueOf := func(domain kv.Domain, v []byte) []byte {
		if domain == kv.AccountsDomain {
			return encAccount(uint64(v[0]))
		}
		return bytes.Repeat(v, 8)
	}
	domains := []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain}

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	for txNum := uint64(0); txNum <= 20; txNum++ {
		sd.SetTxNum(txNum)
		for _, domain := range domains {
			for name, writes := range histories {
				var prev []byte
				for _, w := range writes {
					if w.txNum == txNum {
						key := keyOf(domain, name)
						if w.v == nil {
							require.NoError(t, sd.DomainDel(domain, rwTx, key, txNum, prev))
						} else {
							require.NoError(t, sd.DomainPut(domain, rwTx, key, valueOf(domain, w.v), txNum, prev))
						}
					}
					if w.txNum <= txNum {
						prev = nil
						if w.v != nil {
							prev = valueOf(domain, w.v)
						}
					}
				}
			}
		}
	}
	sd.SetTxNum(20)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, []byte{0x01}, encAccount(1), 20, nil))
	_, err = sd.ComputeCommitment(ctx, rwTx, true, 1, 20, "", nil)
	require.NoError(t, err)
	require.NoError(t, sd.Commit(ctx, rwTx))
	sd.Close()
	require.NoError(t, rwTx.Commit())
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(db, 2*stepSize, unboundedFinalityCtx))
	indexBlock1(t, db, 1)
	require.NoError(t, execctx.InitStateCacheVersion(ctx, db, stateCache))

	pass := func(tsOrder []uint64, withCache bool, domains ...kv.Domain) int {
		t.Helper()
		roTx, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		require.NotEmpty(t, roTx.Debug().DomainFiles(kv.StorageDomain))
		sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
		require.NoError(t, err)
		defer sd.Close()
		if withCache {
			sd.BindStateCache(stateCache)
		}
		tx := &countingTxReads{TemporalTx: roTx}
		asOf := sd.AsOfGetter(tx)
		for _, ts := range tsOrder {
			for _, domain := range domains {
				for name := range histories {
					key := keyOf(domain, name)
					want, wantOk, err := roTx.GetAsOf(domain, key, ts)
					require.NoError(t, err)
					got, gotOk, err := asOf.GetAsOf(domain, key, ts)
					require.NoError(t, err)
					require.Truef(t, bytes.Equal(want, got), "%s %s ts=%d: want %x, got %x", domain, name, ts, want, got)
					require.Equalf(t, wantOk, gotOk, "%s %s ts=%d", domain, name, ts)
				}
			}
		}
		return tx.reads
	}
	var ascending, descending []uint64
	for ts := uint64(0); ts <= 22; ts++ {
		ascending = append(ascending, ts)
		descending = append(descending, 22-ts)
	}
	pass(ascending, false, domains...)
	pass(ascending, true, domains...)
	pass(descending, true, domains...)
	// Code absences are not cached by address.
	require.Zero(t, pass([]uint64{21, 22}, true, kv.AccountsDomain, kv.StorageDomain), "reads after the visible end are served from the fills")
}
