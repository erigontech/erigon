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
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/blockreplay"
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
	bal    types.BlockAccessList
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
	return filepath.Join("..", "blockreplay", "testdata", "block-25604144.gob")
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
	bal        types.BlockAccessList
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
	dirs := datadir.New(tb.TempDir())
	db := temporaltest.NewTestDB(tb, dirs)

	block, err := fx.Block()
	require.NoError(tb, err)
	parent, err := fx.ParentHeader()
	require.NoError(tb, err)
	bal, err := fx.BAL()
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
	// The witness carries a flat post-state, no commitment trie — always run
	// exec-only regardless of the env (the flag is read only at package init).
	cfg.discardCommitment = true

	num := block.NumberU64()
	inputTxNum := ephemeralSeedTxNum + 1
	r := &ephemeralReplay{
		ctx: ctx, logger: logger, db: db, cfg: cfg,
		block: block, parent: parent, bal: bal, num: num, inputTxNum: inputTxNum,
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
func (r *ephemeralReplay) newDomains(tb testing.TB, fx *blockreplay.Fixture) (kv.TemporalRwTx, *execctx.SharedDomains, *blockreplay.WitnessWriteSet) {
	tb.Helper()
	tx, err := r.db.BeginTemporalRw(r.ctx) //nolint:gocritic
	require.NoError(tb, err)
	doms, writeSet, err := blockreplay.NewWitnessDomains(r.ctx, tx, fx, ephemeralSeedTxNum, r.logger)
	require.NoError(tb, err)
	doms.SetTxNum(r.inputTxNum)
	return tx, doms, writeSet
}

// exec runs the block once through parallel ExecV3. This is the only thing a
// benchmark should time. Receipts/gas/bloom are validated inside the apply loop.
func (r *ephemeralReplay) exec(tx kv.TemporalRwTx, doms *execctx.SharedDomains) error {
	src := &singleBlockSource{block: r.block, num: r.num, parent: r.parent, bal: r.bal}
	_, err := execV3(r.ctx, r.cfg, doms, tx, stages.ModeApplyingBlocks, false, "replay", r.rng, src, r.logger)
	return err
}

// verify checks the post-state (Flush -> outputs read via the domains) against
// the authoritative canonical outputs — the data, not the trie root. It first
// flags any state-changing write the replay made outside the reference set (a
// commitment-off replay can't otherwise catch an extra write), then compares
// values — which also catches reference keys the replay failed to write.
func (r *ephemeralReplay) verify(tx kv.TemporalRwTx, doms *execctx.SharedDomains, writeSet *blockreplay.WitnessWriteSet, expected *blockreplay.Outputs) error {
	if diffs := writeSet.Diff(expected); len(diffs) > 0 {
		return fmt.Errorf("write-set has extra writes (%d): %s", len(diffs), strings.Join(diffs, " | "))
	}
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
	// Exec-only is set on cfg by setupEphemeralReplay; no env needed.
	fx, err := blockreplay.Load(fixturePath(b))
	require.NoError(b, err)
	require.NotNil(b, fx.Outputs, "fixture missing captured outputs; recapture with `integration capture_block`")
	expected := fx.Outputs

	r, closeFn := setupEphemeralReplay(b, fx)
	defer closeFn()

	b.ResetTimer()
	for range b.N {
		func() {
			b.StopTimer()
			tx, doms, writeSet := r.newDomains(b, fx)
			// Deferred so a require failure (which Goexits) still closes the tx
			// before the test-DB cleanup, which otherwise blocks on the open tx.
			defer tx.Rollback()
			defer doms.Close()
			b.StartTimer()

			execErr := r.exec(tx, doms)

			b.StopTimer()
			require.NoError(b, execErr)
			require.NoError(b, r.verify(tx, doms, writeSet, expected))
		}()
	}
}

// TestEphemeralParallelReplay runs one exec-only parallel replay so a normal
// `go test` covers the nil-commitResults topology (exec-only: no commitment
// consumer) that otherwise only BenchmarkEphemeralParallelReplay exercises.
func TestEphemeralParallelReplay(t *testing.T) {
	fx, err := blockreplay.Load(fixturePath(t))
	require.NoError(t, err)
	require.NotNil(t, fx.Outputs, "fixture missing captured outputs; recapture with `integration capture_block`")

	r, closeFn := setupEphemeralReplay(t, fx)
	defer closeFn()

	tx, doms, writeSet := r.newDomains(t, fx)
	defer tx.Rollback()
	defer doms.Close()

	require.NoError(t, r.exec(tx, doms))
	require.NoError(t, r.verify(tx, doms, writeSet, fx.Outputs))
}
