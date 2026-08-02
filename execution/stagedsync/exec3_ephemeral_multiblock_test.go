package stagedsync

import (
	"context"
	"fmt"
	"os"
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

// multiBlockSource streams a contiguous ascending run of blocks to ExecV3.
type multiBlockSource struct {
	blocks  []*types.Block
	headers map[uint64]*types.Header
	i       int
}

func (s *multiBlockSource) next(ctx context.Context) (*types.Block, types.BlockAccessList, uint64, bool, error) {
	if s.i >= len(s.blocks) {
		return nil, nil, 0, false, nil
	}
	b := s.blocks[s.i]
	s.i++
	return b, nil, b.NumberU64(), true, nil
}

func (s *multiBlockSource) header(ctx context.Context, hash common.Hash, number uint64) (*types.Header, error) {
	if h, ok := s.headers[number]; ok {
		return h, nil
	}
	return &types.Header{}, nil
}

// TestEphemeralMultiBlockReplay streams a captured block RANGE through the real
// parallel executor across one accumulating SharedDomains, verifying the
// range-final post-state each iteration. Run many iterations at N workers to
// surface non-deterministic parallel-execution hazards a single-block replay
// cannot (per-block state resets hide cross-block/intra-block base races).
//
//	RANGE_FIXTURE=<path> EPHEMERAL_WORKERS=8 MULTIBLOCK_ITERS=200 \
//	  DISCARD_COMMITMENT=true <gates...> go test -run TestEphemeralMultiBlockReplay
func TestEphemeralMultiBlockReplay(t *testing.T) {
	path := os.Getenv("RANGE_FIXTURE")
	if path == "" {
		t.Skip("set RANGE_FIXTURE=<range.gob> to run the multi-block replay")
	}
	if !dbg.DiscardCommitment() {
		t.Fatal("set DISCARD_COMMITMENT=true: the witness carries no commitment trie")
	}
	rf, err := blockreplay.LoadRange(path)
	require.NoError(t, err)
	require.NotNil(t, rf.Outputs, "range fixture missing captured outputs")

	iters := 100
	if v := os.Getenv("MULTIBLOCK_ITERS"); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n > 0 {
			iters = n
		}
	}

	merged := rf.MergedWitness()
	blocks := make([]*types.Block, len(rf.Blocks))
	headers := map[uint64]*types.Header{}
	for i, b := range rf.Blocks {
		blk, e := b.Block()
		require.NoError(t, e)
		blocks[i] = blk
		headers[blk.NumberU64()] = blk.HeaderNoCopy()
	}
	parent, err := rf.Blocks[0].ParentHeader()
	require.NoError(t, err)
	headers[parent.Number.Uint64()] = parent
	lo, hi := blocks[0].NumberU64(), blocks[len(blocks)-1].NumberU64()

	ctx := context.Background()
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	br, err := blockreplay.NewMemBlockReaderRange(rf)
	require.NoError(t, err)
	engine := merge.New(ethash.NewFaker())
	defer engine.Close()

	syncCfg := ethconfig.Defaults.Sync
	syncCfg.ExecWorkerCount = 8
	if v := os.Getenv("EPHEMERAL_WORKERS"); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n > 0 {
			syncCfg.ExecWorkerCount = n
		}
	}
	cfg := StageExecuteBlocksCfg(db, prune.DefaultMode, 512*datasize.MB,
		chainspec.Mainnet.Config, engine, &vm.Config{}, nil, false, false,
		dirs, br, chainspec.Mainnet.Genesis, syncCfg, false, nil)

	const seedTxNum = uint64(1) << 20
	rng := execRange{
		blockNum:     lo,
		initialTxNum: seedTxNum,
		inputTxNum:   seedTxNum + 1,
		maxBlockNum:  hi,
	}

	diagContinue := os.Getenv("DIAG_CONTINUE") != ""
	fails := 0
	for it := 0; it < iters; it++ {
		if os.Getenv("CB_TRACE") == "true" {
			fmt.Printf("[ITER %d]\n", it)
		}
		tx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		doms, err := blockreplay.NewWitnessDomains(ctx, tx, merged, seedTxNum, logger)
		require.NoError(t, err)
		doms.SetTxNum(rng.inputTxNum)

		src := &multiBlockSource{blocks: blocks, headers: headers}
		_, execErr := ExecV3(ctx, cfg, doms, tx, stages.ModeApplyingBlocks, false, "mb-replay", rng, src, logger)
		if execErr != nil {
			doms.Close()
			tx.Rollback()
			if diagContinue {
				fails++
				fmt.Printf("[DIAG-FAIL] iter %d exec: %v\n", it, execErr)
				continue
			}
			t.Fatalf("iter %d: exec range [%d..%d]: %v", it, lo, hi, execErr)
		}

		got, err := blockreplay.CollectOutputs(state.NewReaderV3(doms.AsGetter(tx)), rf.Outputs)
		doms.Close()
		tx.Rollback()
		require.NoError(t, err)
		if diffs := rf.Outputs.Diff(got); len(diffs) > 0 {
			if diagContinue {
				fails++
				fmt.Printf("[DIAG-FAIL] iter %d: %v\n", it, diffs)
				continue
			}
			t.Fatalf("iter %d: post-state mismatch (%d diffs): %v", it, len(diffs), diffs)
		}
	}
	if diagContinue {
		fmt.Printf("[DIAG-TOTAL] %d/%d iters failed\n", fails, iters)
	}
}

// TestEphemeralMultiBlockPerBlock drives the captured range one block at a time
// over a SINGLE accumulating SharedDomains (a fresh ExecV3 call per block), and
// after each block verifies the accumulating post-state against THAT block's own
// captured Outputs (as-of its own block end). Because each block is fully applied
// and flushed before the next runs, the domains hold a clean 0..N snapshot with no
// exec-ahead pollution, so a per-block diff localizes a mismatch to the exact
// block — unlike the whole-range TestEphemeralMultiBlockReplay, whose merged
// range-final check masks intra-range block-end bugs.
//
//	RANGE_FIXTURE=<path> EPHEMERAL_WORKERS=8 DISCARD_COMMITMENT=true <gates...> \
//	  go test -run TestEphemeralMultiBlockPerBlock
func TestEphemeralMultiBlockPerBlock(t *testing.T) {
	path := os.Getenv("RANGE_FIXTURE")
	if path == "" {
		t.Skip("set RANGE_FIXTURE=<range.gob> to run the per-block multi-block replay")
	}
	if !dbg.DiscardCommitment() {
		t.Fatal("set DISCARD_COMMITMENT=true: the witness carries no commitment trie")
	}
	rf, err := blockreplay.LoadRange(path)
	require.NoError(t, err)

	// Generate mode backfills each block's own post-state (projected onto the
	// range-final key set) from THIS run and saves an enriched fixture; check mode
	// asserts each block against those per-block oracles. Generate once from a
	// trusted (canonical-at-range-end) run, then check under the candidate path.
	genPath := os.Getenv("PERBLOCK_GEN")

	merged := rf.MergedWitness()
	blocks := make([]*types.Block, len(rf.Blocks))
	headers := map[uint64]*types.Header{}
	for i, b := range rf.Blocks {
		blk, e := b.Block()
		require.NoError(t, e)
		blocks[i] = blk
		headers[blk.NumberU64()] = blk.HeaderNoCopy()
	}
	parent, err := rf.Blocks[0].ParentHeader()
	require.NoError(t, err)
	headers[parent.Number.Uint64()] = parent

	if genPath == "" {
		perBlockOutputs := 0
		for _, b := range rf.Blocks {
			if b.Outputs != nil {
				perBlockOutputs++
			}
		}
		require.NotZero(t, perBlockOutputs, "range fixture carries no per-block Outputs; regenerate with PERBLOCK_GEN=<out.gob> on a trusted run")
	}

	ctx := context.Background()
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	br, err := blockreplay.NewMemBlockReaderRange(rf)
	require.NoError(t, err)
	engine := merge.New(ethash.NewFaker())
	defer engine.Close()

	syncCfg := ethconfig.Defaults.Sync
	syncCfg.ExecWorkerCount = 8
	if v := os.Getenv("EPHEMERAL_WORKERS"); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n > 0 {
			syncCfg.ExecWorkerCount = n
		}
	}
	cfg := StageExecuteBlocksCfg(db, prune.DefaultMode, 512*datasize.MB,
		chainspec.Mainnet.Config, engine, &vm.Config{}, nil, false, false,
		dirs, br, chainspec.Mainnet.Genesis, syncCfg, false, nil)

	const seedTxNum = uint64(1) << 20
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	doms, err := blockreplay.NewWitnessDomains(ctx, tx, merged, seedTxNum, logger)
	require.NoError(t, err)
	defer doms.Close()

	cursor := seedTxNum
	for i, blk := range blocks {
		num := blk.NumberU64()
		rng := execRange{
			blockNum:     num,
			initialTxNum: cursor,
			inputTxNum:   cursor + 1,
			maxBlockNum:  num,
		}
		doms.SetTxNum(cursor + 1)
		src := &multiBlockSource{blocks: blocks[i : i+1], headers: headers}
		_, execErr := ExecV3(ctx, cfg, doms, tx, stages.ModeApplyingBlocks, false, "mb-perblock", rng, src, logger)
		require.NoErrorf(t, execErr, "block %d (idx %d): exec", num, i)
		cursor = doms.TxNum()

		snapshot, err := blockreplay.CollectOutputs(state.NewReaderV3(doms.AsGetter(tx)), rf.Outputs)
		require.NoError(t, err)
		if genPath != "" {
			rf.Blocks[i].Outputs = snapshot
			continue
		}
		want := rf.Blocks[i].Outputs
		if want == nil {
			continue
		}
		if diffs := want.Diff(snapshot); len(diffs) > 0 {
			t.Fatalf("block %d (idx %d): post-state mismatch (%d diffs): %v", num, i, len(diffs), diffs)
		}
	}
	if genPath != "" {
		require.NoError(t, rf.Save(genPath))
		t.Logf("wrote per-block oracle fixture %s (%d blocks)", genPath, len(rf.Blocks))
	}
}
