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

package commitmentdb_test

import (
	"fmt"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/kvmetrics"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

type pbinStubSharedDomains struct{ sharedCache bool }

func (s *pbinStubSharedDomains) SetTxNum(uint64)                                         {}
func (s *pbinStubSharedDomains) AsGetter(kv.TemporalTx) kv.TemporalGetter                { return nil }
func (s *pbinStubSharedDomains) AsPutDel(kv.TemporalTx) kv.TemporalPutDel                { return nil }
func (s *pbinStubSharedDomains) MergeMetrics(kvmetrics.Source, *kvmetrics.DomainMetrics) {}
func (s *pbinStubSharedDomains) StepSize() uint64                                        { return 1 }
func (s *pbinStubSharedDomains) Metrics() *kvmetrics.DomainMetrics                       { return nil }
func (s *pbinStubSharedDomains) HasSharedBranchCache() bool                              { return s.sharedCache }

func pbinRecoverMessage(t *testing.T, fn func()) (msg string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()
	fn()
	return ""
}

// TestPBinCtorRefusesSharedBranchCache pins the structural assert for H1: a
// bin-variant commitment context over a SharedDomains that shares the branch
// cache must be refused at construction, by name, before anything else runs.
func TestPBinCtorRefusesSharedBranchCache(t *testing.T) {
	t.Parallel()

	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	msg := pbinRecoverMessage(t, func() {
		commitmentdb.NewSharedDomainsCommitmentContext(&pbinStubSharedDomains{sharedCache: true}, commitment.ModeDirect, t.TempDir(), cfg)
	})
	require.Contains(t, msg, "branch cache")
}

// TestPBinBranchCacheTrunkSlotCollision demonstrates H1, the reason the bin
// variant must not share the BranchCache. The trunk-slot index reads a prefix
// as a hex compact path, which is injective for hex keys; a pbin bit-path key
// is packed MSB-first bits plus a trailing bitLen%8 byte, so distinct short
// paths land on one slot and the cache serves another node's record as a
// well-formed hit.
func TestPBinBranchCacheTrunkSlotCollision(t *testing.T) {
	t.Parallel()

	cache := commitment.NewBranchCache(commitment.DefaultBranchCacheTailCapacity)
	defer cache.Close()

	// 3-bit path 000 and 3-bit path 001: both index depth-2 slot d2[0x03].
	keyA := []byte{0x00, 0x03}
	keyB := []byte{0x20, 0x03}
	dataA := []byte{0xde, 0xad, 0xbe, 0xef}

	cache.Put(keyA, dataA, 1, 1)
	got, _, ok := cache.Get(keyB)
	require.True(t, ok, "distinct bit-path key no longer collides — revisit whether the bin variant may share the BranchCache")
	require.Equal(t, dataA, got)
}

func pbinNewTestDb(tb testing.TB) kv.TemporalRwDB {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(tb, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	tb.Cleanup(db.Close)

	agg := state.NewTest(dirs).StepSize(16).Logger(logger).MustOpen(tb.Context(), db)
	tb.Cleanup(agg.Close)
	require.NoError(tb, agg.OpenFolder())
	tdb, err := temporal.New(db, agg, nil)
	require.NoError(tb, err)
	return tdb
}

// TestPBinSharedDomainsHasNoSharedBranchCache checks the execctx wiring: a
// bin-variant SharedDomains over an aggregator whose AggTx provides the shared
// BranchCache must reach the commitment-context ctor without it, and must open.
func TestPBinSharedDomainsHasNoSharedBranchCache(t *testing.T) {
	t.Parallel()

	db := pbinNewTestDb(t)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie

	var sd *execctx.SharedDomains
	msg := pbinRecoverMessage(t, func() {
		var sdErr error
		sd, sdErr = execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg))
		require.NoError(t, sdErr)
	})
	require.Empty(t, msg)
	defer sd.Close()
	require.False(t, sd.HasSharedBranchCache())
}
