package stagedsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockreplay"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

// singleBlockSource serves one captured block to the parallel executor with no
// DB walk behind it; header answers only the parent.
type singleBlockSource struct {
	block  *types.Block
	num    uint64
	parent *types.Header
	done   bool
}

func (s *singleBlockSource) next(ctx context.Context) (*types.Block, types.BlockAccessList, uint64, bool, error) {
	if s.done {
		return nil, nil, 0, false, nil
	}
	s.done = true
	return s.block, nil, s.num, true, nil
}

func (s *singleBlockSource) header(ctx context.Context, hash common.Hash, number uint64) (*types.Header, error) {
	if s.parent != nil && number == s.parent.Number.Uint64() {
		return s.parent, nil
	}
	return &types.Header{}, nil
}

func fixturePath(tb testing.TB) string {
	tb.Helper()
	if p := os.Getenv("BLOCKREPLAY_FIXTURE"); p != "" {
		return p
	}
	return filepath.Join("..", "tests", "blockreplay", "testdata", "block-25604144.gob")
}

// ephemeralReplayParallel runs a captured block through the real parallel ExecV3
// with no MDBX behind the state: SharedDomains is backed by a witness mem batch
// (the flat pre-state) and blocks/headers come from an in-memory block reader.
// Commitment is skipped (DISCARD_COMMITMENT) since the witness read-set is not a
// full trie; receipts/gas/bloom are validated inside the apply loop, so a nil
// return means the parallel path reproduced the block.
func ephemeralReplayParallel(tb testing.TB, fx *blockreplay.Fixture, expected *blockreplay.Outputs) error {
	tb.Helper()
	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := temporaltest.NewTestDB(tb, dirs)

	block, err := fx.Block()
	require.NoError(tb, err)
	parent, err := fx.ParentHeader()
	require.NoError(tb, err)
	num := block.NumberU64()

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(tb, err)
	defer tx.Rollback()

	const seedTxNum = uint64(1) << 20
	doms, err := blockreplay.NewWitnessDomains(ctx, tx, fx, seedTxNum, logger)
	require.NoError(tb, err)
	defer doms.Close()

	br, err := blockreplay.NewMemBlockReader(fx)
	require.NoError(tb, err)

	engine := merge.New(ethash.NewFaker())
	defer engine.Close()

	syncCfg := ethconfig.Defaults.Sync
	if syncCfg.ExecWorkerCount < 2 {
		syncCfg.ExecWorkerCount = 4
	}
	if v := os.Getenv("EPHEMERAL_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			syncCfg.ExecWorkerCount = n
		}
	}

	cfg := StageExecuteBlocksCfg(db, prune.DefaultMode, 512*datasize.MB,
		chainspec.Mainnet.Config, engine, &vm.Config{}, nil, false, false,
		dirs, br, chainspec.Mainnet.Genesis, syncCfg, false, nil)

	inputTxNum := seedTxNum + 1
	doms.SetTxNum(inputTxNum)

	rng := execRange{
		blockNum:                 num,
		initialTxNum:             seedTxNum,
		inputTxNum:               inputTxNum,
		offsetFromBlockBeginning: 0,
		maxBlockNum:              num,
	}
	src := &singleBlockSource{block: block, num: num, parent: parent}

	if _, execErr := ExecV3(ctx, cfg, doms, tx, stages.ModeApplyingBlocks, false, "replay", rng, src, logger); execErr != nil {
		return execErr
	}

	// Flush -> outputs: post-state is received through the standard reader over
	// the post-execution domains. Check the data (accounts/storage/code, not the
	// trie root) against the serial reference so we know exec was correct.
	if expected != nil {
		got, err := blockreplay.CollectOutputs(state.NewReaderV3(doms.AsGetter(tx)), expected)
		if err != nil {
			return err
		}
		if diffs := expected.Diff(got); len(diffs) > 0 {
			return fmt.Errorf("post-state mismatch (%d differences), first: %s", len(diffs), diffs[0])
		}
	}
	return nil
}

func BenchmarkEphemeralParallelReplay(b *testing.B) {
	if !dbg.DiscardCommitment() {
		b.Fatal("set DISCARD_COMMITMENT=true: the witness carries no commitment trie")
	}
	fx, err := blockreplay.Load(fixturePath(b))
	require.NoError(b, err)

	// The reference post-state is the block's authoritative canonical output,
	// captured from the node into the fixture — not re-derived by any executor.
	// Each parallel replay is checked against it, so we profile verified-correct
	// execution.
	require.NotNil(b, fx.Outputs, "fixture missing captured outputs; recapture with `integration capture_block`")
	expected := fx.Outputs

	b.ResetTimer()
	for range b.N {
		if err := ephemeralReplayParallel(b, fx, expected); err != nil {
			b.Fatal(err)
		}
	}
}
