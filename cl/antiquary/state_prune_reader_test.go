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

package antiquary

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Segment file names floor slot ranges to 1000-slot units, so every frozen
// range in these tests is a multiple of 1000.
const statePruneTestFileSlots = uint64(1_000)

type historicalReadEnv struct {
	db      kv.RwDB
	reader  *tests.MockBlockReader
	genesis *state.CachingBeaconState
	stateSn *snapshotsync.CaplinStateSnapshots
	sd      synced_data.SyncedData
}

func (e *historicalReadEnv) readRoot(t *testing.T, ctx context.Context, slot uint64) common.Hash {
	t.Helper()
	tx, err := e.db.BeginRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	vt := state_accessors.NewStaticValidatorTable()
	require.NoError(t, state_accessors.ReadValidatorsTable(tx, vt))
	hr := historical_states_reader.NewHistoricalStatesReader(&clparams.MainnetBeaconConfig, e.reader, vt, e.genesis, e.stateSn, e.sd)
	s, err := hr.ReadHistoricalState(ctx, tx, slot)
	require.NoError(t, err)
	require.NotNil(t, s)
	root, err := s.HashSSZ()
	require.NoError(t, err)
	return common.Hash(root)
}

func runStateAntiquaryWithSnapshots(t *testing.T, ctx context.Context, blocks []*cltypes.SignedBeaconBlock, preState, postState *state.CachingBeaconState) (*historicalReadEnv, datadir.Dirs) {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	reader := tests.LoadChain(blocks, postState, db, t)
	sd := synced_data.NewSyncedDataManager(cfg, true)
	sd.OnHeadState(postState)
	vt := state_accessors.NewStaticValidatorTable()
	dirs := datadir.New(t.TempDir())
	stateSn := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{}, cfg, dirs, snapshotsync.MakeCaplinStateSnapshotsTypes(db), log.New())
	a := NewAntiquary(ctx, nil, preState, vt, cfg, dirs, nil, db, stateSn, nil, reader, sd, log.New(), true, true, true, false, nil)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
	return &historicalReadEnv{db: db, reader: reader, genesis: preState, stateSn: stateSn, sd: sd}, dirs
}

func stateRootOf(t *testing.T, blocks []*cltypes.SignedBeaconBlock, slot uint64) common.Hash {
	t.Helper()
	for _, b := range blocks {
		if b.Block.Slot == slot {
			return b.Block.StateRoot
		}
	}
	t.Fatalf("no block at slot %d", slot)
	return common.Hash{}
}

// A historical state at a slot below the prune boundary must reconstruct from
// segments exactly as it did from the DB rows the prune deleted.
func TestPruneStateHistoricalReadsServedFromSegments(t *testing.T) {
	blocks, preState, postState := tests.GetBellatrixRandom()
	ctx := context.Background()
	env, dirs := runStateAntiquaryWithSnapshots(t, ctx, blocks, preState, postState)
	db, stateSn := env.db, env.stateSn

	boundary := statePruneTestFileSlots
	midSlot, headSlot := uint64(100), blocks[len(blocks)-1].Block.Slot

	midRootBefore := env.readRoot(t, ctx, midSlot)
	require.Equal(t, stateRootOf(t, blocks, headSlot), env.readRoot(t, ctx, headSlot))

	// The fixture chain starts past slot 0, so the dense root tables have no
	// rows outside the transitioned window and would refuse to freeze. Backfill
	// synthetic roots (never read back: history reads stay within the window)
	// to make the whole [0, boundary) range freezable, as it is in production.
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		for _, table := range []string{kv.BlockRoot, kv.StateRoot} {
			for slot := uint64(0); slot < boundary; slot++ {
				key := base_encoding.Encode64ToBytes4(slot)
				v, err := tx.GetOne(table, key)
				if err != nil {
					return err
				}
				if len(v) != 0 {
					continue
				}
				pad := common.Hash{0xba, 0xdd, byte(slot), byte(slot >> 8)}
				if err := tx.Put(table, key, pad[:]); err != nil {
					return err
				}
			}
		}
		return nil
	}))

	require.NoError(t, stateSn.DumpCaplinState(ctx, boundary, statePruneTestFileSlots, 0, dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, stateSn.OpenFolder())
	for _, table := range []string{kv.SlotData, kv.BalancesDump, kv.BlockRoot} {
		require.Equal(t, boundary, stateSn.ContiguousCoverageEnd(table))
	}

	next, err := pruneStateTables(ctx, db, stateSn.TypeNames(), stateSn.ContiguousCoverageEnd, 0, 1000, log.New())
	require.NoError(t, err)
	require.Equal(t, 0, next)

	// the whole chain sits below the boundary: every state table must be empty
	for _, table := range []string{kv.SlotData, kv.BalancesDump, kv.ValidatorBalance, kv.BlockRoot, kv.EpochData} {
		require.Empty(t, tableSlots(t, db, table), "table %s", table)
		require.Equal(t, boundary, pruneMarker(t, db, table))
	}

	require.Equal(t, midRootBefore, env.readRoot(t, ctx, midSlot))
	require.Equal(t, stateRootOf(t, blocks, headSlot), env.readRoot(t, ctx, headSlot))
}

// A historical state at a tail slot above coverage keeps reading its own rows
// from the DB while its below-boundary dependencies (the balance dumps rounded
// down to a pruned slot) are served from segments.
func TestPruneStateTailReadsAboveCoverage(t *testing.T) {
	blocks, preState, postState := tests.GetCapellaRandom()
	ctx := context.Background()
	env, dirs := runStateAntiquaryWithSnapshots(t, ctx, blocks, preState, postState)
	db, stateSn := env.db, env.stateSn

	boundary := uint64(8_000)
	dumpSlot := preState.Slot() - preState.Slot()%clparams.SlotsPerDump
	require.Less(t, dumpSlot, boundary)
	firstSlot, headSlot := blocks[0].Block.Slot, blocks[len(blocks)-1].Block.Slot

	firstRootBefore := env.readRoot(t, ctx, firstSlot)
	require.Equal(t, stateRootOf(t, blocks, headSlot), env.readRoot(t, ctx, headSlot))

	require.NoError(t, stateSn.DumpCaplinState(ctx, boundary, statePruneTestFileSlots, 0, dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, stateSn.OpenFolder())
	require.Equal(t, boundary, stateSn.ContiguousCoverageEnd(kv.BalancesDump))
	// dense root tables have no rows below the chain start, refuse to freeze,
	// and must stay unprunable
	require.Zero(t, stateSn.ContiguousCoverageEnd(kv.BlockRoot))

	next, err := pruneStateTables(ctx, db, stateSn.TypeNames(), stateSn.ContiguousCoverageEnd, 0, 1000, log.New())
	require.NoError(t, err)
	require.Equal(t, 0, next)

	// the genesis-rounded dumps were the only rows below the boundary: gone
	// from the DB, still visible through the segment
	require.NotContains(t, tableSlots(t, db, kv.BalancesDump), dumpSlot)
	require.NotContains(t, tableSlots(t, db, kv.EffectiveBalancesDump), dumpSlot)
	fromSeg, err := stateSn.Get(kv.BalancesDump, dumpSlot)
	require.NoError(t, err)
	require.NotEmpty(t, fromSeg)
	// tail rows above coverage are untouched
	require.Contains(t, tableSlots(t, db, kv.SlotData), headSlot)
	require.Contains(t, tableSlots(t, db, kv.BlockRoot), firstSlot)

	require.Equal(t, firstRootBefore, env.readRoot(t, ctx, firstSlot))
	require.Equal(t, stateRootOf(t, blocks, headSlot), env.readRoot(t, ctx, headSlot))
}

// Pins both balance-reconstruction paths against a pruned DB: the forward path
// must fetch its base dump from the segment once the DB row is pruned, and the
// reverse (next-dump) path must combine the above-boundary dump from the DB
// with below-boundary diffs from the segment. Each path's epoch-diff chain is
// seeded only for that path, so taking the wrong branch cannot pass.
func TestPruneStateBalancesForwardAndReverseDumpPaths(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	ctx := context.Background()
	const valCount = uint64(4)
	boundary := statePruneTestFileSlots
	const forwardSlot, reverseSlot = uint64(500), uint64(900)

	balancesAt := func(slot uint64) []byte {
		raw := make([]byte, valCount*8)
		for v := uint64(0); v < valCount; v++ {
			binary.LittleEndian.PutUint64(raw[v*8:], 32_000_000_000+v*1_000+slot*13)
		}
		return raw
	}
	epochDiff := func(oldSlot, newSlot uint64) []byte {
		var b bytes.Buffer
		require.NoError(t, base_encoding.ComputeCompressedSerializedUint64ListDiff(&b, balancesAt(oldSlot), balancesAt(newSlot)))
		return common.Copy(b.Bytes())
	}
	compressor, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	dump := func(slot uint64) []byte { return compressor.EncodeAll(balancesAt(slot), nil) }
	slotData := func() []byte {
		var b bytes.Buffer
		sdata := &state_accessors.SlotData{Version: clparams.BellatrixVersion, ValidatorLength: valCount, Eth1Data: &cltypes.Eth1Data{}, Fork: &cltypes.Fork{}}
		require.NoError(t, sdata.WriteTo(&b))
		return common.Copy(b.Bytes())
	}

	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		put := func(table string, slot uint64, v []byte) {
			require.NoError(t, tx.Put(table, base_encoding.Encode64ToBytes4(slot), v))
		}
		put(kv.BalancesDump, 0, dump(0))
		put(kv.BalancesDump, clparams.SlotsPerDump, dump(clparams.SlotsPerDump))
		for i := cfg.SlotsPerEpoch; i <= cfg.RoundSlotToEpoch(forwardSlot); i += cfg.SlotsPerEpoch {
			put(kv.ValidatorBalance, i, epochDiff(i-cfg.SlotsPerEpoch, i))
		}
		put(kv.ValidatorBalance, forwardSlot, epochDiff(cfg.RoundSlotToEpoch(forwardSlot), forwardSlot))
		for i := cfg.RoundSlotToEpoch(reverseSlot) + cfg.SlotsPerEpoch; i <= clparams.SlotsPerDump; i += cfg.SlotsPerEpoch {
			put(kv.ValidatorBalance, i, epochDiff(i-cfg.SlotsPerEpoch, i))
		}
		put(kv.ValidatorBalance, reverseSlot, epochDiff(cfg.RoundSlotToEpoch(reverseSlot), reverseSlot))
		put(kv.SlotData, forwardSlot, slotData())
		put(kv.SlotData, reverseSlot, slotData())
		// progress past the next dump is what selects the reverse path for
		// slots deep in the dump window
		return state_accessors.SetStateProcessingProgress(tx, clparams.SlotsPerDump+forwardSlot)
	}))

	dirs := datadir.New(t.TempDir())
	stateSn := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{}, cfg, dirs, snapshotsync.MakeCaplinStateSnapshotsTypes(db), log.New())
	require.NoError(t, stateSn.DumpCaplinState(ctx, boundary, statePruneTestFileSlots, 0, dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, stateSn.OpenFolder())
	require.Equal(t, boundary, stateSn.ContiguousCoverageEnd(kv.ValidatorBalance))
	require.Equal(t, boundary, stateSn.ContiguousCoverageEnd(kv.BalancesDump))

	next, err := pruneStateTables(ctx, db, stateSn.TypeNames(), stateSn.ContiguousCoverageEnd, 0, 1000, log.New())
	require.NoError(t, err)
	require.Equal(t, 0, next)

	require.Equal(t, []uint64{clparams.SlotsPerDump}, tableSlots(t, db, kv.BalancesDump))
	for _, s := range tableSlots(t, db, kv.ValidatorBalance) {
		require.GreaterOrEqual(t, s, boundary)
	}

	checkBalances := func(slot uint64) {
		tx, err := db.BeginRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		view := stateSn.View()
		defer view.Close()

		hr := historical_states_reader.NewHistoricalStatesReader(cfg, nil, nil, nil, stateSn, nil)
		balances, err := hr.ReadValidatorsBalances(tx, state_accessors.GetValFnTxAndSnapshot(tx, view), slot)
		require.NoError(t, err)
		require.NotNil(t, balances)
		expected := balancesAt(slot)
		require.Equal(t, int(valCount), balances.Length())
		for v := uint64(0); v < valCount; v++ {
			require.Equal(t, binary.LittleEndian.Uint64(expected[v*8:]), balances.Get(int(v)))
		}
	}
	checkBalances(forwardSlot)
	checkBalances(reverseSlot)
}
