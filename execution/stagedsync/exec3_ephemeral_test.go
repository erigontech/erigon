package stagedsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockreplay"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

// singleBlockSource serves one captured block to the parallel executor with no
// DB walk behind it; header answers only the parent.
type singleBlockSource struct {
	block  *types.Block
	num    uint64
	parent *types.Header
	bal    types.BlockAccessList // seed BAL for pre-seeding the versionMap (nil = none)
	done   bool
}

func (s *singleBlockSource) next(ctx context.Context) (*types.Block, types.BlockAccessList, uint64, bool, error) {
	if s.done {
		return nil, nil, 0, false, nil
	}
	s.done = true
	return s.block, s.bal, s.num, true, nil
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

const ephemeralSeedTxNum = uint64(1) << 20

// ephemeralReplay holds the one-time setup for replaying a fixture's block
// through parallel ExecV3 with no MDBX behind the state. Only the per-run
// SharedDomains is rebuilt each iteration (its witness mem batch accumulates the
// block's writes), so a benchmark can time newDomains/verify separately from the
// ExecV3 call itself.
type ephemeralReplay struct {
	ctx        context.Context
	logger     log.Logger
	db         kv.TemporalRwDB
	cfg        ExecuteBlockCfg
	rng        execRange
	block      *types.Block
	parent     *types.Header
	num        uint64
	inputTxNum uint64
}

// setupEphemeralReplay builds everything reusable across runs: the disposable
// temporal DB (page-cache resident, state served from the witness mem batch),
// the in-memory block reader, engine, and exec config. Returns a close func for
// the engine.
func setupEphemeralReplay(tb testing.TB, fx *blockreplay.Fixture) (*ephemeralReplay, func()) {
	tb.Helper()
	ctx := context.Background()
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	dirs := datadir.New(tb.TempDir())
	db := temporaltest.NewTestDB(tb, dirs)

	block, err := fx.Block()
	require.NoError(tb, err)
	parent, err := fx.ParentHeader()
	require.NoError(tb, err)

	br, err := blockreplay.NewMemBlockReader(fx)
	require.NoError(tb, err)

	engine := merge.New(ethash.NewFaker())

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

	num := block.NumberU64()
	inputTxNum := ephemeralSeedTxNum + 1
	r := &ephemeralReplay{
		ctx: ctx, logger: logger, db: db, cfg: cfg,
		block: block, parent: parent, num: num, inputTxNum: inputTxNum,
		rng: execRange{
			blockNum:     num,
			initialTxNum: ephemeralSeedTxNum,
			inputTxNum:   inputTxNum,
			maxBlockNum:  num,
		},
	}
	return r, func() { engine.Close() }
}

// newDomains builds a fresh witness-backed SharedDomains for one run. Not part
// of the ExecV3 measurement.
func (r *ephemeralReplay) newDomains(tb testing.TB, fx *blockreplay.Fixture) (kv.TemporalRwTx, *execctx.SharedDomains) {
	tb.Helper()
	tx, err := r.db.BeginTemporalRw(r.ctx) //nolint:gocritic
	require.NoError(tb, err)
	doms, err := blockreplay.NewWitnessDomains(r.ctx, tx, fx, ephemeralSeedTxNum, r.logger)
	require.NoError(tb, err)
	doms.SetTxNum(r.inputTxNum)
	return tx, doms
}

// exec runs the block once through parallel ExecV3. This is the only thing a
// benchmark should time. Receipts/gas/bloom are validated inside the apply loop.
func (r *ephemeralReplay) exec(tx kv.TemporalRwTx, doms *execctx.SharedDomains) error {
	return r.execSeeded(tx, doms, nil)
}

// execSeeded runs the block once, optionally pre-seeding the versionMap from
// seedBAL (the BAL round-trip's run 2).
func (r *ephemeralReplay) execSeeded(tx kv.TemporalRwTx, doms *execctx.SharedDomains, seedBAL types.BlockAccessList) error {
	src := &singleBlockSource{block: r.block, num: r.num, parent: r.parent, bal: seedBAL}
	_, err := ExecV3(r.ctx, r.cfg, doms, tx, stages.ModeApplyingBlocks, false, "replay", r.rng, src, r.logger)
	return err
}

// verify checks the post-state (Flush -> outputs read via the domains) against
// the authoritative canonical outputs — the data, not the trie root.
func (r *ephemeralReplay) verify(tx kv.TemporalRwTx, doms *execctx.SharedDomains, expected *blockreplay.Outputs) error {
	got, err := blockreplay.CollectOutputs(state.NewReaderV3(doms.AsGetter(tx)), expected)
	if err != nil {
		return err
	}
	if diffs := expected.Diff(got); len(diffs) > 0 {
		return fmt.Errorf("post-state mismatch (%d differences): %s", len(diffs), strings.Join(diffs, " | "))
	}
	return nil
}

// BenchmarkEphemeralParallelReplay times ONLY the parallel ExecV3 call: setup,
// the per-run witness SharedDomains, and the post-state check are excluded via
// StopTimer/StartTimer. Each run is checked against the fixture's authoritative
// canonical outputs, so the measurement is of verified-correct execution.
func BenchmarkEphemeralParallelReplay(b *testing.B) {
	if !dbg.DiscardCommitment() {
		b.Fatal("set DISCARD_COMMITMENT=true: the witness carries no commitment trie")
	}
	fx, err := blockreplay.Load(fixturePath(b))
	require.NoError(b, err)
	require.NotNil(b, fx.Outputs, "fixture missing captured outputs; recapture with `integration capture_block`")
	expected := fx.Outputs

	r, closeFn := setupEphemeralReplay(b, fx)
	defer closeFn()

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		tx, doms := r.newDomains(b, fx)
		b.StartTimer()

		execErr := r.exec(tx, doms)

		b.StopTimer()
		require.NoError(b, execErr)
		require.NoError(b, r.verify(tx, doms, expected))
		doms.Close()
		tx.Rollback()
	}
}

// BenchmarkEphemeralBALRoundTrip exercises the pre-seed==post-output invariant:
// a BAL derived from a block's execution must be reproduced when the versionMap
// is pre-seeded from it and the block re-executed. The reference BAL is derived
// here rather than read from the header, since the header may carry no BAL hash.
// Same recipe as BenchmarkEphemeralParallelReplay (DISCARD_COMMITMENT=true).
func BenchmarkEphemeralBALRoundTrip(b *testing.B) {
	if !dbg.DiscardCommitment() {
		b.Fatal("set DISCARD_COMMITMENT=true: the witness carries no commitment trie")
	}
	fx, err := blockreplay.Load(fixturePath(b))
	require.NoError(b, err)
	require.NotNil(b, fx.Outputs, "fixture missing captured outputs; recapture with `integration capture_block`")
	expected := fx.Outputs

	r, closeFn := setupEphemeralReplay(b, fx)
	defer closeFn()

	// experimentalBAL is the pre-Amsterdam BAL-production debug option: it makes
	// ProcessBAL derive a BAL for this block even though the header has none.
	r.cfg.experimentalBAL = true
	var captured types.BlockAccessList
	r.cfg.SetBALSink(func(_ uint64, bal types.BlockAccessList) { captured = bal })

	derive := func(seed types.BlockAccessList) types.BlockAccessList {
		captured = nil
		tx, doms := r.newDomains(b, fx)
		require.NoError(b, r.execSeeded(tx, doms, seed))
		require.NoError(b, r.verify(tx, doms, expected))
		doms.Close()
		tx.Rollback()
		require.NotNil(b, captured, "no BAL derived (experimentalBAL not honored?)")
		return captured
	}

	balOut := derive(nil)     // run 1: reference, no seed
	balOut2 := derive(balOut) // run 2: pre-seed from run 1's BAL
	require.Equal(b, balOut.Hash(), balOut2.Hash(),
		"pre-seed != post-output: derived BAL changed when the versionMap was seeded from it")
}

// TestSelfLoopEvaluateBlockerTaskSpaceOnPartialBlock pins that selfLoopEvaluate
// returns a park target in dense task-list-index space, not versionMap
// (block-TxIndex) space. For a resumed (partial) block whose leading committed
// txs were skipped, task 0 starts at a non-zero block TxIndex, so the two spaces
// diverge; a dependency's block-TxIndex maps to a much larger number than any
// task-list index. Before the taskIndexOf fix the blocker was `wv.TxIndex+1`
// (block-TxIndex space, here 250) — beyond the commit frontier's reach, so the
// self-loop worker parked forever and the whole block deadlocked. It must instead
// be `wv.TxIndex - startTxIndex` (task-list space, here 49). Full blocks keep the
// two aligned (startTxIndex -1 → +1), which is why only resumed blocks hit this.
func TestSelfLoopEvaluateBlockerTaskSpaceOnPartialBlock(t *testing.T) {
	const startTxIndex = 200 // resumed/partial block: task 0 is not the block-init sys tx
	const writerTxIndex = 249
	const readerTxIndex = 250
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000deadbeef"))

	vm := state.NewVersionMap(nil)
	// A committed write below the reader, at a high block-TxIndex.
	vm.WriteBalance(addr, state.Version{TxIndex: writerTxIndex}, *uint256.NewInt(7), true)

	// The read recorded a stale value, so revalidating it against the current
	// versionMap write is invalid — driving the blocker branch.
	var rs state.ReadSet
	rs.SetBalance(addr, state.VersionedRead[uint256.Int]{
		ReadHeader: state.ReadHeader{Source: state.MapRead, Version: state.Version{TxIndex: 205}},
		Val:        *uint256.NewInt(3),
	})

	be := &blockExecutor{
		versionMap: vm,
		tasks:      []*execTask{{Task: &exec.TxTask{TxIndex: startTxIndex}, index: 0}},
	}
	// The reader is task (readerTxIndex - startTxIndex) in dense task-list space;
	// selfLoopEvaluate reads tv.index (the embedded execTask) for the forward-dep guard.
	tv := &taskVersion{
		execTask: &execTask{index: readerTxIndex - startTxIndex},
		version:  state.Version{TxIndex: readerTxIndex},
	}

	valid, _, blocker := be.selfLoopEvaluate(tv, &exec.TxResult{TxIn: rs})
	require.False(t, valid, "a stale read must revalidate as invalid")
	require.Equal(t, writerTxIndex-startTxIndex, blocker,
		"blocker must be in dense task-list space, not versionMap block-TxIndex space")
}
