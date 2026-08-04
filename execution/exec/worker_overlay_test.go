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
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

// Pins that a Worker's chain reader (rw.chain) resolves an overlay-staged
// header via GetHeaderByHash — the exact path the wedge takes:
// AuRa.Initialize → verifyGasLimitOverride → chain.GetHeaderByHash(parentHash).
// GetHeaderByHash goes hash → HeaderNumber → Headers, so both overlay
// entries must be visible through the worker's chainTx.
//
// Red-first verified locally: removing the BlockOverlay wrap in
// Worker.resetTx makes the require.NotNil here fail (HeaderNumber lookup
// misses → parent header invisible → consensus reports ErrUnknownAncestor).
func TestWorker_ChainReader_SeesOverlayHeader(t *testing.T) {
	t.Parallel()

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	rawDB := mdbx.New(dbcfg.ChainDB, logger).
		InMem(t, dirs.Chaindata).
		GrowthStep(32 * datasize.MB).
		MapSize(2 * datasize.GB).
		MustOpen()
	t.Cleanup(rawDB.Close)

	agg := dbstate.NewTest(dirs).StepSize(16).Logger(logger).MustOpen(t.Context(), rawDB)
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder())

	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)

	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	doms, err := execctx.NewSharedDomains(t.Context(), roTx, logger)
	require.NoError(t, err)
	t.Cleanup(doms.Close)
	require.NoError(t, doms.InitBlockOverlay(roTx, dirs.Tmp))

	header := &types.Header{
		Number:     *uint256.NewInt(2_713_395),
		ParentHash: empty.RootHash,
		Difficulty: *uint256.NewInt(0),
	}
	hash, number := header.Hash(), header.Number.Uint64()

	require.NoError(t, rawdb.WriteHeader(doms.BlockOverlay(), header))

	rs := state.NewStateV3Buffered(state.NewStateV3(doms, false, logger))

	// nil blockReader → consensuschain.Reader uses rawdb.ReadHeader directly,
	// keeping the test focused on the tx + overlay path without any
	// snapshot-file plumbing.
	rw := NewWorker(t.Context(), true, nil, db, nil, nil,
		&chain.Config{ChainID: uint256.NewInt(1)}, nil, nil, nil, dirs, logger)
	rw.rs = rs

	// workerRoTx ownership transfers to rw on ResetTx; rolling it back
	// directly here would double-Rollback against rw.chainRoTx.
	workerRoTx, err := db.BeginTemporalRo(t.Context()) //nolint:gocritic
	require.NoError(t, err)
	t.Cleanup(func() {
		if rw.chainRoTx != nil {
			rw.chainRoTx.Rollback()
			rw.chainRoTx = nil
		}
	})
	require.NoError(t, rw.ResetTx(workerRoTx))

	got := rw.chain.GetHeaderByHash(hash)
	require.NotNil(t, got,
		"chain reader must resolve an overlay-staged header by hash; AuRa.verifyGasLimitOverride takes this exact path")
	require.Equal(t, number, got.Number.Uint64())
	require.Equal(t, hash, got.Hash())
}

// Pins that when no BlockOverlay is active, the chain reader still works —
// a no-overlay worker (e.g. initial sync from snapshots) must not regress.
func TestWorker_ChainReader_NoOverlayStillWorks(t *testing.T) {
	t.Parallel()

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	rawDB := mdbx.New(dbcfg.ChainDB, logger).
		InMem(t, dirs.Chaindata).
		GrowthStep(32 * datasize.MB).
		MapSize(2 * datasize.GB).
		MustOpen()
	t.Cleanup(rawDB.Close)

	agg := dbstate.NewTest(dirs).StepSize(16).Logger(logger).MustOpen(t.Context(), rawDB)
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder())

	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)

	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	header := &types.Header{
		Number:     *uint256.NewInt(42),
		ParentHash: empty.RootHash,
		Difficulty: *uint256.NewInt(0),
	}
	hash, number := header.Hash(), header.Number.Uint64()
	require.NoError(t, rawdb.WriteHeader(rwTx, header))
	require.NoError(t, rwTx.Commit())

	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	doms, err := execctx.NewSharedDomains(t.Context(), roTx, logger)
	require.NoError(t, err)
	t.Cleanup(doms.Close)
	// Deliberately do NOT InitBlockOverlay — chain reads must still resolve
	// through the worker's raw roTx for committed MDBX data.
	require.Nil(t, doms.BlockOverlay())

	rs := state.NewStateV3Buffered(state.NewStateV3(doms, false, logger))

	// nil blockReader → consensuschain.Reader uses rawdb.ReadHeader directly,
	// keeping the test focused on the tx + overlay path without any
	// snapshot-file plumbing.
	rw := NewWorker(t.Context(), true, nil, db, nil, nil,
		&chain.Config{ChainID: uint256.NewInt(1)}, nil, nil, nil, dirs, logger)
	rw.rs = rs

	// workerRoTx ownership transfers to rw on ResetTx; rolling it back
	// directly here would double-Rollback against rw.chainRoTx.
	workerRoTx, err := db.BeginTemporalRo(t.Context()) //nolint:gocritic
	require.NoError(t, err)
	t.Cleanup(func() {
		if rw.chainRoTx != nil {
			rw.chainRoTx.Rollback()
			rw.chainRoTx = nil
		}
	})
	require.NoError(t, rw.ResetTx(workerRoTx))

	got := rw.chain.GetHeaderByHash(hash)
	require.NotNil(t, got, "committed-MDBX header must still resolve when no overlay is active")
	require.Equal(t, number, got.Number.Uint64())
}
