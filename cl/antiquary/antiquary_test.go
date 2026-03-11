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

package antiquary

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func newTestCollector(t *testing.T) *etl.Collector {
	t.Helper()
	return etl.NewCollector("test", t.TempDir(), etl.NewSortableBuffer(etl.BufferOptimalSize), log.New())
}

func newTestCompressor(buf *bytes.Buffer) *zstd.Encoder {
	c, err := zstd.NewWriter(buf)
	if err != nil {
		panic(err)
	}
	return c
}

// collectAll drains the collector into a map of key→value by loading into an
// in-memory callback.
func collectAll(t *testing.T, c *etl.Collector) map[string][]byte {
	t.Helper()
	result := make(map[string][]byte)
	c.Load(nil, "", func(k, v []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error { //nolint:gocritic
		result[string(k)] = common.Copy(v)
		return next(nil, nil, nil)
	}, etl.TransformArgs{})
	return result
}

func TestAntiquateFullUint64List(t *testing.T) {
	buf := &bytes.Buffer{}
	compressor := newTestCompressor(buf)
	collector := newTestCollector(t)
	defer collector.Close()

	// Build a uint64 list: [10, 20, 30]
	raw := make([]byte, 3*8)
	binary.LittleEndian.PutUint64(raw[0:], 10)
	binary.LittleEndian.PutUint64(raw[8:], 20)
	binary.LittleEndian.PutUint64(raw[16:], 30)

	slot := uint64(100)
	require.NoError(t, antiquateFullUint64List(collector, slot, raw, buf, compressor))

	collected := collectAll(t, collector)
	compressed, ok := collected[string(base_encoding.Encode64ToBytes4(slot))]
	require.True(t, ok, "expected key for slot %d", slot)

	// Decompress and verify round-trip
	dec, err := zstd.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, raw, decompressed)
}

func TestAntiquateFullUint64List_Empty(t *testing.T) {
	buf := &bytes.Buffer{}
	compressor := newTestCompressor(buf)
	collector := newTestCollector(t)
	defer collector.Close()

	require.NoError(t, antiquateFullUint64List(collector, 0, nil, buf, compressor))

	collected := collectAll(t, collector)
	compressed := collected[string(base_encoding.Encode64ToBytes4(0))]

	dec, err := zstd.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Empty(t, decompressed)
}

func TestAntiquateField(t *testing.T) {
	buf := &bytes.Buffer{}
	compressor := newTestCompressor(buf)
	collector := newTestCollector(t)
	defer collector.Close()

	data := []byte("hello world antiquary field data")
	slot := uint64(clparams.SlotsPerDump*3 + 42) // not aligned to SlotsPerDump
	require.NoError(t, antiquateField(context.Background(), slot, data, buf, compressor, collector))

	collected := collectAll(t, collector)
	// antiquateField rounds the slot down to SlotsPerDump boundary
	roundedSlot := slot - (slot % clparams.SlotsPerDump)
	compressed, ok := collected[string(base_encoding.Encode64ToBytes4(roundedSlot))]
	require.True(t, ok, "expected key for rounded slot %d", roundedSlot)

	dec, err := zstd.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, data, decompressed)
}

func TestAntiquateField_SlotAligned(t *testing.T) {
	buf := &bytes.Buffer{}
	compressor := newTestCompressor(buf)
	collector := newTestCollector(t)
	defer collector.Close()

	data := []byte("aligned slot data")
	slot := uint64(clparams.SlotsPerDump * 5) // exactly aligned
	require.NoError(t, antiquateField(context.Background(), slot, data, buf, compressor, collector))

	collected := collectAll(t, collector)
	_, ok := collected[string(base_encoding.Encode64ToBytes4(slot))]
	require.True(t, ok, "expected key for aligned slot %d", slot)
}

func TestAntiquateBytesListDiff(t *testing.T) {
	collector := newTestCollector(t)
	defer collector.Close()

	old := make([]byte, 3*8)
	binary.LittleEndian.PutUint64(old[0:], 100)
	binary.LittleEndian.PutUint64(old[8:], 200)
	binary.LittleEndian.PutUint64(old[16:], 300)

	// new has same length but different values
	newData := make([]byte, 3*8)
	binary.LittleEndian.PutUint64(newData[0:], 100)
	binary.LittleEndian.PutUint64(newData[8:], 250) // changed
	binary.LittleEndian.PutUint64(newData[16:], 300)

	key := base_encoding.Encode64ToBytes4(42)
	// Use a simple diff function that just writes the new data
	simpleDiff := func(w io.Writer, old, new []byte) error {
		_, err := w.Write(new)
		return err
	}

	require.NoError(t, antiquateBytesListDiff(context.Background(), key, old, newData, collector, simpleDiff))

	collected := collectAll(t, collector)
	result, ok := collected[string(key)]
	require.True(t, ok)
	require.Equal(t, newData, result)
}

func TestAntiquateBytesListDiff_WithRealDiffFn(t *testing.T) {
	collector := newTestCollector(t)
	defer collector.Close()

	old := make([]byte, 3*8)
	binary.LittleEndian.PutUint64(old[0:], 100)
	binary.LittleEndian.PutUint64(old[8:], 200)
	binary.LittleEndian.PutUint64(old[16:], 300)

	newData := make([]byte, 4*8) // one extra element
	binary.LittleEndian.PutUint64(newData[0:], 100)
	binary.LittleEndian.PutUint64(newData[8:], 200)
	binary.LittleEndian.PutUint64(newData[16:], 300)
	binary.LittleEndian.PutUint64(newData[24:], 400)

	key := base_encoding.Encode64ToBytes4(99)
	require.NoError(t, antiquateBytesListDiff(
		context.Background(), key, old, newData, collector,
		base_encoding.ComputeCompressedSerializedUint64ListDiff,
	))

	collected := collectAll(t, collector)
	_, ok := collected[string(key)]
	require.True(t, ok, "expected diff output")
}

func TestFindNearestSlotBackwards(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	cfg := &clparams.MainnetBeaconConfig

	// Write canonical roots at slots 0, 32, 64 (epoch boundaries with SlotsPerEpoch=32)
	for _, slot := range []uint64{0, 32, 64} {
		root := common.Hash{byte(slot)}
		require.NoError(t, beacon_indicies.MarkRootCanonical(context.Background(), tx, slot, root))
	}

	// From slot 64 (epoch boundary with canonical root) → should return 64
	result, err := findNearestSlotBackwards(tx, cfg, 64)
	require.NoError(t, err)
	require.Equal(t, uint64(64), result)

	// From slot 70 (not epoch boundary, no canonical root) → should walk back to 64
	result, err = findNearestSlotBackwards(tx, cfg, 70)
	require.NoError(t, err)
	require.Equal(t, uint64(64), result)

	// From slot 50 (between 32 and 64) → should walk back to 32
	result, err = findNearestSlotBackwards(tx, cfg, 50)
	require.NoError(t, err)
	require.Equal(t, uint64(32), result)

	// From slot 32 → should return 32
	result, err = findNearestSlotBackwards(tx, cfg, 32)
	require.NoError(t, err)
	require.Equal(t, uint64(32), result)
}

func TestFindNearestSlotBackwards_NoRoots(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	cfg := &clparams.MainnetBeaconConfig

	// No canonical roots at all → should walk all the way back to 0
	result, err := findNearestSlotBackwards(tx, cfg, 10)
	require.NoError(t, err)
	require.Equal(t, uint64(0), result)
}

func TestComputeSlotToBeRequested(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	cfg := &clparams.MainnetBeaconConfig

	// Write canonical roots at epoch boundaries
	for slot := uint64(0); slot <= 200000; slot += cfg.SlotsPerEpoch {
		root := common.Hash{}
		binary.LittleEndian.PutUint64(root[:], slot)
		require.NoError(t, beacon_indicies.MarkRootCanonical(context.Background(), tx, slot, root))
	}

	// If targetSlot > SlotsPerDump * backoffStep, we get findNearestSlotBackwards(targetSlot - SlotsPerDump*backoffStep)
	targetSlot := uint64(100000)
	backoffStep := uint64(10)
	result, err := computeSlotToBeRequested(tx, cfg, 0, targetSlot, backoffStep)
	require.NoError(t, err)
	// expected: findNearestSlotBackwards(100000 - 1536*10 = 84640)
	// 84640 / 32 * 32 = 84640 (already epoch-aligned), but need canonical root → nearest epoch boundary
	expected := (targetSlot - clparams.SlotsPerDump*backoffStep)
	expected = (expected / cfg.SlotsPerEpoch) * cfg.SlotsPerEpoch
	require.Equal(t, expected, result)

	// If targetSlot is small (less than SlotsPerDump * backoffStep), return genesis
	result, err = computeSlotToBeRequested(tx, cfg, 0, 1000, 10)
	require.NoError(t, err)
	require.Equal(t, uint64(0), result)
}

func TestComputeSlotToBeRequested_ReturnsGenesis(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	cfg := &clparams.MainnetBeaconConfig

	genesisSlot := uint64(100)
	// targetSlot < SlotsPerDump * backoffStep → returns genesisSlot
	result, err := computeSlotToBeRequested(tx, cfg, genesisSlot, 500, 10)
	require.NoError(t, err)
	require.Equal(t, genesisSlot, result)
}

func TestNewAntiquary(t *testing.T) {
	ctx := context.Background()
	cfg := &clparams.MainnetBeaconConfig

	a := NewAntiquary(ctx, nil, nil, nil, cfg, datadir.Dirs{}, nil, nil, nil, nil, nil, nil, log.New(), true, true, true, false, nil)
	require.NotNil(t, a)
	require.True(t, a.states)
	require.True(t, a.blocks)
	require.True(t, a.blobs)
	require.False(t, a.snapgen)
	require.False(t, a.backfilled.Load())
	require.False(t, a.blobBackfilled.Load())
}

func TestNotifyBackfilled(t *testing.T) {
	ctx := context.Background()
	a := NewAntiquary(ctx, nil, nil, nil, &clparams.MainnetBeaconConfig, datadir.Dirs{}, nil, nil, nil, nil, nil, nil, log.New(), true, true, true, false, nil)

	require.False(t, a.backfilled.Load())
	a.NotifyBackfilled()
	require.True(t, a.backfilled.Load())
}

func TestNotifyBlobBackfilled(t *testing.T) {
	ctx := context.Background()
	a := NewAntiquary(ctx, nil, nil, nil, &clparams.MainnetBeaconConfig, datadir.Dirs{}, nil, nil, nil, nil, nil, nil, log.New(), true, true, true, false, nil)

	require.False(t, a.blobBackfilled.Load())
	a.NotifyBlobBackfilled()
	require.True(t, a.blobBackfilled.Load())
}

func TestBeaconStatesCollector_CollectStateRoot(t *testing.T) {
	c := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer c.close()

	root := common.HexToHash("0xdeadbeef")
	require.NoError(t, c.collectStateRoot(42, root))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	// Verify the state root was written
	val, err := tx.GetOne("StateRoot", base_encoding.Encode64ToBytes4(42))
	require.NoError(t, err)
	require.Equal(t, root[:], val)
}

func TestBeaconStatesCollector_CollectBlockRoot(t *testing.T) {
	c := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer c.close()

	root := common.HexToHash("0xcafebabe")
	require.NoError(t, c.collectBlockRoot(100, root))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	val, err := tx.GetOne("BlockRoot", base_encoding.Encode64ToBytes4(100))
	require.NoError(t, err)
	require.Equal(t, root[:], val)
}

func TestBeaconStatesCollector_CollectEpochRandaoMix(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	c := newBeaconStatesCollector(cfg, t.TempDir(), log.New())
	defer c.close()

	mix := common.HexToHash("0xabcdef1234567890")
	epoch := uint64(5)
	require.NoError(t, c.collectEpochRandaoMix(epoch, mix))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	// collectEpochRandaoMix stores at slot = epoch * SlotsPerEpoch
	expectedSlot := epoch * cfg.SlotsPerEpoch
	val, err := tx.GetOne("RandaoMixes", base_encoding.Encode64ToBytes4(expectedSlot))
	require.NoError(t, err)
	require.Equal(t, mix[:], val)
}

func TestBeaconStatesCollector_CollectIntraEpochRandaoMix(t *testing.T) {
	c := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer c.close()

	mix := common.HexToHash("0x1111222233334444")
	require.NoError(t, c.collectIntraEpochRandaoMix(77, mix))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	val, err := tx.GetOne("IntraRandaoMixes", base_encoding.Encode64ToBytes4(77))
	require.NoError(t, err)
	require.Equal(t, mix[:], val)
}

func TestBeaconStatesCollector_CollectSlashings(t *testing.T) {
	c := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer c.close()

	// Build raw slashings data (3 uint64s)
	slashings := make([]byte, 3*8)
	binary.LittleEndian.PutUint64(slashings[0:], 1000)
	binary.LittleEndian.PutUint64(slashings[8:], 2000)
	binary.LittleEndian.PutUint64(slashings[16:], 3000)

	require.NoError(t, c.collectSlashings(200, slashings))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	val, err := tx.GetOne("ValidatorSlashings", base_encoding.Encode64ToBytes4(200))
	require.NoError(t, err)
	require.NotEmpty(t, val)

	// Decompress and verify
	dec, err := zstd.NewReader(bytes.NewReader(val))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, slashings, decompressed)
}

func TestBeaconStatesCollector_CollectBalancesDiffs(t *testing.T) {
	c := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer c.close()

	old := make([]byte, 2*8)
	binary.LittleEndian.PutUint64(old[0:], 100)
	binary.LittleEndian.PutUint64(old[8:], 200)

	newBal := make([]byte, 3*8)
	binary.LittleEndian.PutUint64(newBal[0:], 100)
	binary.LittleEndian.PutUint64(newBal[8:], 250)
	binary.LittleEndian.PutUint64(newBal[16:], 300)

	require.NoError(t, c.collectBalancesDiffs(context.Background(), 500, old, newBal))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	val, err := tx.GetOne("ValidatorBalance", base_encoding.Encode64ToBytes4(500))
	require.NoError(t, err)
	require.NotEmpty(t, val)

	// Verify we can apply the diff to reconstruct
	reconstructed, err := base_encoding.ApplyCompressedSerializedUint64ListDiff(old, nil, val, false)
	require.NoError(t, err)
	require.Equal(t, newBal, reconstructed)
}

func TestBeaconStatesCollector_CollectBalancesDump(t *testing.T) {
	c := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer c.close()

	balances := make([]byte, 4*8)
	binary.LittleEndian.PutUint64(balances[0:], 32_000_000_000)
	binary.LittleEndian.PutUint64(balances[8:], 32_100_000_000)
	binary.LittleEndian.PutUint64(balances[16:], 31_900_000_000)
	binary.LittleEndian.PutUint64(balances[24:], 32_050_000_000)

	slot := uint64(clparams.SlotsPerDump * 2) // aligned to dump boundary
	require.NoError(t, c.collectBalancesDump(slot, balances))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	roundedSlot := slot - (slot % clparams.SlotsPerDump)
	val, err := tx.GetOne("BalancesDump", base_encoding.Encode64ToBytes4(roundedSlot))
	require.NoError(t, err)
	require.NotEmpty(t, val)

	// Decompress
	dec, err := zstd.NewReader(bytes.NewReader(val))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, balances, decompressed)
}

func TestBeaconStatesCollector_CollectActiveIndices(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	c := newBeaconStatesCollector(cfg, t.TempDir(), log.New())
	defer c.close()

	indices := []uint64{0, 1, 5, 10, 100, 999}
	epoch := uint64(7)
	require.NoError(t, c.collectActiveIndices(epoch, indices))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	expectedSlot := epoch * cfg.SlotsPerEpoch
	val, err := tx.GetOne("ActiveValidatorIndicies", base_encoding.Encode64ToBytes4(expectedSlot))
	require.NoError(t, err)
	require.NotEmpty(t, val)

	// Decode back
	decoded, err := base_encoding.ReadRabbits(indices[:0], bytes.NewReader(val))
	require.NoError(t, err)
	require.Equal(t, indices, decoded)
}

func TestBeaconStatesCollector_FlushMultipleCollections(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	c := newBeaconStatesCollector(cfg, t.TempDir(), log.New())
	defer c.close()

	// Collect several different types of data
	root1 := common.HexToHash("0xaaa")
	root2 := common.HexToHash("0xbbb")
	mix := common.HexToHash("0xccc")

	require.NoError(t, c.collectStateRoot(10, root1))
	require.NoError(t, c.collectBlockRoot(10, root2))
	require.NoError(t, c.collectIntraEpochRandaoMix(10, mix))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	// Verify all three are present
	val, err := tx.GetOne("StateRoot", base_encoding.Encode64ToBytes4(10))
	require.NoError(t, err)
	require.Equal(t, root1[:], val)

	val, err = tx.GetOne("BlockRoot", base_encoding.Encode64ToBytes4(10))
	require.NoError(t, err)
	require.Equal(t, root2[:], val)

	val, err = tx.GetOne("IntraRandaoMixes", base_encoding.Encode64ToBytes4(10))
	require.NoError(t, err)
	require.Equal(t, mix[:], val)
}

func TestBeaconStatesCollector_CollectInactivityScores(t *testing.T) {
	c := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer c.close()

	// Build inactivity scores: 3 uint64s
	scores := make([]byte, 3*8)
	binary.LittleEndian.PutUint64(scores[0:], 0)
	binary.LittleEndian.PutUint64(scores[8:], 5)
	binary.LittleEndian.PutUint64(scores[16:], 10)

	require.NoError(t, c.collectInactivityScores(300, scores))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	val, err := tx.GetOne("InactivityScores", base_encoding.Encode64ToBytes4(300))
	require.NoError(t, err)
	require.NotEmpty(t, val)

	dec, err := zstd.NewReader(bytes.NewReader(val))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, scores, decompressed)
}

func TestBeaconStatesCollector_CollectEffectiveBalancesDump(t *testing.T) {
	c := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer c.close()

	// Build a fake validator set (each validator is 121 bytes, effective balance at offset 80:88)
	validatorSetSize := 121
	numValidators := 3
	raw := make([]byte, validatorSetSize*numValidators)
	// Set effective balances
	for i := 0; i < numValidators; i++ {
		binary.LittleEndian.PutUint64(raw[i*validatorSetSize+80:], uint64((i+1)*32_000_000_000))
	}

	slot := uint64(clparams.SlotsPerDump * 4)
	require.NoError(t, c.collectEffectiveBalancesDump(slot, raw))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, c.flush(context.Background(), tx))

	roundedSlot := slot - (slot % clparams.SlotsPerDump)
	val, err := tx.GetOne("EffectiveBalancesDump", base_encoding.Encode64ToBytes4(roundedSlot))
	require.NoError(t, err)
	require.NotEmpty(t, val)

	// Decompress and verify effective balances were extracted
	dec, err := zstd.NewReader(bytes.NewReader(val))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, numValidators*8, len(decompressed))

	for i := 0; i < numValidators; i++ {
		eb := binary.LittleEndian.Uint64(decompressed[i*8:])
		require.Equal(t, uint64((i+1)*32_000_000_000), eb)
	}
}
