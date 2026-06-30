package execctx_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/decodedstate"
)

func decodedDomainFileWordCount(t *testing.T, dirs datadir.Dirs, rng decodedFileRange) int {
	t.Helper()

	pattern := filepath.Join(dirs.SnapDomain, fmt.Sprintf("*decodedstorage.%d-%d.kv", rng.fromStep, rng.toStep))
	matches, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Lenf(t, matches, 1, "expected exactly one decodedstorage .kv for steps %d-%d, got %v", rng.fromStep, rng.toStep, matches)

	d, err := seg.NewDecompressor(matches[0])
	require.NoError(t, err)
	defer d.Close()
	return d.Count()
}

// writeDecodedDomainSteps writes decoded values straight through the decoded
// domain writer across [fromBlock, toBlock), mirroring what WriteEntriesToDomains
// does to the decoded domain: per-block puts plus the synthetic progress marker
// one step ahead.
func writeDecodedDomainSteps(t *testing.T, ctx context.Context, db kv.TemporalRwDB, stepSize, fromBlock, toBlock uint64) {
	t.Helper()

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	w := state.AggTx(tx).DbgDomain(kv.DecodedStorageDomain).NewWriter()
	defer w.Close()

	var lastTxNum uint64
	for blockNum := fromBlock; blockNum < toBlock; blockNum++ {
		key := make([]byte, 53)
		binary.BigEndian.PutUint64(key[45:], blockNum)
		val := make([]byte, 32)
		binary.BigEndian.PutUint64(val[24:], blockNum*10)
		require.NoError(t, w.PutWithPrev(key, val, blockNum, nil))
		lastTxNum = blockNum
	}

	nextStepTxNum := ((lastTxNum / stepSize) + 1) * stepSize
	var progressVal [8]byte
	binary.BigEndian.PutUint64(progressVal[:], nextStepTxNum)
	require.NoError(t, w.PutWithPrev([]byte("__decoded_progress__"), progressVal[:], nextStepTxNum, nil))

	require.NoError(t, w.Flush(ctx, tx))
	require.NoError(t, decodedstate.AdvanceLatestTxNumTx(tx, lastTxNum))
	require.NoError(t, tx.Commit())
}

func buildDecodedSnapshotSteps(t *testing.T, ctx context.Context, db kv.TemporalRwDB, agg *state.Aggregator) {
	t.Helper()

	var fromStep, toStep kv.Step
	require.NoError(t, db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		fromStep = state.AggTx(tx).DbgDomain(kv.DecodedStorageDomain).FirstStepNotInFiles()
		latestTxNum, err := decodedstate.LatestTxNumTx(tx)
		if err != nil {
			return err
		}
		toStep = kv.Step(latestTxNum / agg.StepSize())
		return nil
	}))
	if toStep >= fromStep {
		require.NoError(t, agg.BuildFiles2(ctx, fromStep, toStep, true))
	}
}

// TestS_SNAP_20_ReplayPruneDoesNotDropUnfrozenDecodedSteps reproduces the
// decoded-replay snapshot-loss bug: the per-domain prune boundary removed decoded
// DB rows for steps the decoded domain had not frozen yet, so those steps later
// collated into EMPTY .kv files.
//
// Setup matches the real archive replay: core state domains are already frozen far
// ahead, so EndTxNumMinimax (the prune frontier) sits well beyond the decoded
// frozen extent. Decoded data is written across many steps while the decoded
// snapshot build still lags behind, then PruneSmallBatches runs — exactly the
// stage's build/prune ordering when the async BuildFiles2 is skipped because a
// prior build/merge is still in flight. After the lagging build finally runs,
// every populated step must freeze with real data, not collate empty.
func TestS_SNAP_20_ReplayPruneDoesNotDropUnfrozenDecodedSteps(t *testing.T) {
	ctx := context.Background()
	stepSize := uint64(4)
	db, dirs := newDecodedStateExecCtxDB(t, stepSize)
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)

	const lastBlock = uint64(48)
	appendCanonicalTxNumsThrough(t, db, lastBlock)
	primeCoreStateFilesAheadOfDecoded(t, dirs, stepSize, 13)
	require.NoError(t, agg.OpenFolder())

	writeDecodedDomainSteps(t, ctx, db, stepSize, 1, lastBlock+1)

	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		_, err := tx.(kv.TemporalRwTx).PruneSmallBatches(ctx, 10*time.Hour)
		return err
	}))

	buildDecodedSnapshotSteps(t, ctx, db, agg)
	<-agg.WaitForBuildAndMerge(ctx)

	ranges := decodedStorageDomainFileRanges(t, dirs)
	require.NotEmpty(t, ranges, "decoded replay must produce decodedstorage snapshot files")

	for _, rng := range ranges {
		require.Positivef(
			t,
			decodedDomainFileWordCount(t, dirs, rng),
			"decodedstorage file for steps %d-%d froze EMPTY; prune must not remove decoded rows for steps that are not frozen yet, so every populated step freezes with real data",
			rng.fromStep, rng.toStep,
		)
	}
}
