// Reproducer for the parallel-exec from-0 failure surfaced by
// qa-stage-exec (from-0, parallel) on #21017's CI: after `stage_exec
// --reset` wipes the domain state and `stage_exec --batchSize=10mb`
// runs from block 0, a genesis-allocated address that no subsequent
// block touches is read back as balance=0 instead of its allocation.
// On mainnet this hits at block 46147 against
// 0xA1E4380A3B1f749673E270229993eE55F35663b4.
//
// The CI failure does NOT go through the engine API path
// (InsertBlocks + UpdateForkChoice); it goes through the integration
// tool's direct SpawnExecuteBlocksStage loop in cmd/integration/
// commands/stages.go:802. This test mirrors that path.
//
// Internal-package test so it can reach the unexported cfg /
// posStagedSync / sentriesClient fields execmoduletester builds.

package execmoduletester

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// Verifies the bug is fixed under both serial and parallel exec — the
// underlying ResetExec → SeekCommitment → ExecV3 skip-block-0 path is
// shared, so the failure surfaces in either mode. Driver flips
// dbg.Exec3Parallel for each sub-test; not safe to t.Parallel.
func TestFromZero_GenesisAllocPreservedAfterResetReExec(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	for _, mode := range []struct {
		name     string
		parallel bool
	}{
		{"serial", false},
		{"parallel", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			prev := dbg.Exec3Parallel
			dbg.Exec3Parallel = mode.parallel
			t.Cleanup(func() { dbg.Exec3Parallel = prev })
			runFromZeroGenesisAllocPreservedAfterResetReExec(t)
		})
	}
}

func runFromZeroGenesisAllocPreservedAfterResetReExec(t *testing.T) {

	// Untouched-after-genesis address mirroring 0xA1E4380A's role on mainnet.
	dormantAddr := accounts.InternAddress(common.HexToAddress("0xA1E4380A3B1f749673E270229993eE55F35663b4"))
	dormantFunds := new(big.Int).Mul(big.NewInt(2000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)) // 2000 ETH

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	keyAddr := crypto.PubkeyToAddress(key.PublicKey)
	keyFunds := new(big.Int).Mul(big.NewInt(10), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)) // 10 ETH

	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			dormantAddr.Value(): types.GenesisAccount{Balance: dormantFunds},
			keyAddr:             types.GenesisAccount{Balance: keyFunds},
		},
	}

	emt := New(t, WithGenesisSpec(gspec), WithKey(key))

	// Build a chain of empty blocks that NEVER touch the dormant address.
	gen, err := blockgen.GenerateChain(emt.ChainConfig, emt.Genesis, emt.Engine, emt.DB, 5, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)

	require.NoError(t, emt.InsertChain(gen))

	ctx := context.Background()
	logger := log.New()

	checkBalance := func(label string) uint256.Int {
		t.Helper()
		var got uint256.Int
		require.NoError(t, emt.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
			rTx, err := emt.DB.BeginTemporalRo(ctx)
			if err != nil {
				return err
			}
			defer rTx.Rollback()
			doms, err := execctx.NewSharedDomains(ctx, rTx, logger)
			if err != nil {
				return err
			}
			defer doms.Close()
			r := state.NewReaderV3(doms.AsGetter(rTx))
			st := state.New(r)
			b, err := st.GetBalance(dormantAddr)
			if err != nil {
				return err
			}
			got = b
			return nil
		}))
		t.Logf("[%s] dormant addr balance = %s wei (want %s wei)", label, got.String(), dormantFunds.String())
		return got
	}

	postInitial := checkBalance("after-initial-sync")
	require.Equal(t, dormantFunds.String(), postInitial.ToBig().String(), "after engine-API InsertChain, alloc'd balance must survive")

	// Mimic `stage_exec --reset`: wipe domain tables and reset stage progress.
	require.NoError(t, rawdbreset.ResetExec(ctx, emt.DB))

	// Now drive execution the SAME way cmd/integration/commands/stages.go:802
	// does — direct SpawnExecuteBlocksStage in a loop, with Flush/ClearRam/
	// Commit between iterations. This is the path that fails in CI; the
	// engine-API InsertChain path above succeeds.
	require.NoError(t, reExecViaIntegrationPath(t, ctx, emt, gen.TopBlock.NumberU64(), emt.cfg.BatchSize, false /*badBlockHalt*/, logger))

	postReExec := checkBalance("after-reset-and-integration-reexec")
	require.Equal(t, dormantFunds.String(), postReExec.ToBig().String(),
		"BUG #21138: after reset + integration-path re-exec, genesis-allocated balance dropped (mainnet block 46147)")
}

// reExecViaIntegrationPath drives execution the way cmd/integration/commands/
// stages.go does: SpawnExecuteBlocksStage one batch per rwtx, committing each
// with doms.Commit + ClearRam and reusing the SharedDomains. This bypasses the
// engine API. doms.Commit (not Flush) is load-bearing: it refreshes the
// aggregator BranchCache to match committed state — Flush leaves it stale and
// corrupts the next batch's trie root.
func reExecViaIntegrationPath(t *testing.T, ctx context.Context, emt *ExecModuleTester, toBlock uint64, batchSize datasize.ByteSize, badBlockHalt bool, logger log.Logger) error {
	t.Helper()

	// The exec stage cfg execmoduletester built sits inside emt.Sync; we
	// rebuild it with the same arguments so we can invoke
	// SpawnExecuteBlocksStage directly without coupling to internal stage
	// indices.
	cfg := stagedsync.StageExecuteBlocksCfg(
		emt.DB,
		emt.cfg.Prune,
		batchSize,
		emt.ChainConfig,
		emt.Engine,
		&vm.Config{},
		emt.Notifications,
		emt.cfg.StateStream,
		badBlockHalt,
		emt.Dirs,
		emt.BlockReader,
		emt.cfg.Genesis,
		emt.cfg.Sync,
		false, /*experimentalBAL*/
		exec.NewBlockReadAheader(),
	)

	// Lock the offline-execution writers like the integration tool does so
	// the periodic snapshot retiring doesn't fight us.
	if agg, ok := emt.DB.(dbstate.HasAgg); ok {
		if aggT, okT := agg.Agg().(*dbstate.Aggregator); okT {
			aggT.PresetOfflineExecution()
		}
	}

	doms, err := newReusedDomains(ctx, emt, logger)
	if err != nil {
		return err
	}
	defer doms.Close()

	for {
		progress, err := execOneBatch(ctx, emt, doms, cfg, toBlock, logger)
		if err != nil {
			return err
		}
		if progress >= toBlock {
			return nil
		}
	}
}

// newReusedDomains opens a SharedDomains seeded from committed state. The seeding
// tx is rolled back right away: each batch re-seeks commitment under its own tx,
// and the SharedDomains keeps no reference to the tx it was built from.
func newReusedDomains(ctx context.Context, emt *ExecModuleTester, logger log.Logger) (*execctx.SharedDomains, error) {
	tx, err := emt.DB.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	if err != nil {
		return nil, err
	}
	doms.SetInMemHistoryReads(false)
	return doms, nil
}

// execOneBatch runs a single batch in its own rwtx (begin/rollback-on-error/
// commit), reusing doms. doms.Commit commits the tx and refreshes the BranchCache;
// ClearRam drops the flushed batch so doms is clean for the next call. Returns the
// Execution stage progress after the batch.
func execOneBatch(ctx context.Context, emt *ExecModuleTester, doms *execctx.SharedDomains, cfg stagedsync.ExecuteBlockCfg, toBlock uint64, logger log.Logger) (uint64, error) {
	tx, err := emt.DB.BeginTemporalRw(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	s, err := emt.Sync.StageState(stages.Execution, tx, true, false)
	if err != nil {
		return 0, err
	}

	err = stagedsync.SpawnExecuteBlocksStage(s, emt.Sync, doms, tx, toBlock, ctx, cfg, logger)
	if err != nil && !errors.Is(err, &stagedsync.ErrLoopExhausted{}) {
		return 0, err
	}

	progress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, err
	}
	if err := doms.Commit(ctx, tx); err != nil {
		return 0, err
	}
	doms.ClearRam(true)
	return progress, nil
}

// TestFromZero_BranchCacheCoherentAcrossBatches pins the fix for the chiado
// from-0 wrong-trie-root: the integration-tool loop reused a single
// SharedDomains and persisted each batch with Flush, which never refreshed the
// aggregator BranchCache. With a tiny batchSize (one block per batch) the stale
// cache fed an outdated commitment branch into the next block's trie compute and
// produced a wrong root at block 2. Driving the same loop with doms.Commit +
// fresh SD keeps the cache coherent. badBlockHalt=true turns a wrong root into a
// hard error, so a regression fails here instead of silently warning.
func TestFromZero_BranchCacheCoherentAcrossBatches(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	for _, mode := range []struct {
		name     string
		parallel bool
	}{
		{"serial", false},
		{"parallel", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			prev := dbg.Exec3Parallel
			dbg.Exec3Parallel = mode.parallel
			t.Cleanup(func() { dbg.Exec3Parallel = prev })
			runBranchCacheCoherentAcrossBatches(t)
		})
	}
}

func runBranchCacheCoherentAcrossBatches(t *testing.T) {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	keyAddr := crypto.PubkeyToAddress(key.PublicKey)
	keyFunds := new(big.Int).Mul(big.NewInt(1_000_000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc:  types.GenesisAlloc{keyAddr: types.GenesisAccount{Balance: keyFunds}},
	}

	emt := New(t, WithGenesisSpec(gspec), WithKey(key))

	// A chain whose state mutates every block: each block sends transfers to a
	// fresh recipient, so new account leaves and the branch nodes above them
	// change block to block — exactly what the BranchCache must track.
	signer := types.LatestSignerForChainID(emt.ChainConfig.ChainID)
	gen, err := blockgen.GenerateChain(emt.ChainConfig, emt.Genesis, emt.Engine, emt.DB, 12, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		for j := 0; j < 3; j++ {
			to := common.BytesToAddress([]byte{byte(i + 1), byte(j + 1), 0xab})
			tx, txErr := types.SignTx(
				types.NewTransaction(b.TxNonce(keyAddr), to, uint256.NewInt(1_000_000), params.TxGas, uint256.NewInt(1), nil),
				*signer, key)
			require.NoError(t, txErr)
			b.AddTx(tx)
		}
	})
	require.NoError(t, err)
	require.NoError(t, emt.InsertChain(gen))

	ctx := context.Background()
	logger := log.New()

	// Wipe domain state and stage progress, then re-execute from 0 driving the
	// same loop the integration tool uses, with a 1KB batch so every block is
	// its own batch+commit — the condition that exposed the stale cache.
	require.NoError(t, rawdbreset.ResetExec(ctx, emt.DB))
	require.NoError(t,
		reExecViaIntegrationPath(t, ctx, emt, gen.TopBlock.NumberU64(), 1*datasize.KB, true /*badBlockHalt*/, logger),
		"re-exec from 0 in tiny batches must reproduce each block's trie root; a stale BranchCache corrupts it")
}

// TestExec_RestoresCommitmentStateReader checks that an execution batch leaves
// the commitment state reader as it found it. The exec mode is whatever the
// suite selects — the serial/parallel CI matrix (and the EXEC3_PARALLEL default)
// cover both. It bites in parallel: the commitment calculator installs a
// GetAsOf-based asOfStateReader on the shared context, and leaving it there
// makes a later foreground SeekCommitment in the offline re-exec path (in-mem
// history reads disabled) fail with "GetAsOf called on TemporalMemBatch with
// inMemHistoryReads disabled". Serial installs no custom reader.
func TestExec_RestoresCommitmentStateReader(t *testing.T) {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	keyAddr := crypto.PubkeyToAddress(key.PublicKey)
	keyFunds := new(big.Int).Mul(big.NewInt(1_000_000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc:  types.GenesisAlloc{keyAddr: types.GenesisAccount{Balance: keyFunds}},
	}
	emt := New(t, WithGenesisSpec(gspec), WithKey(key))

	signer := types.LatestSignerForChainID(emt.ChainConfig.ChainID)
	gen, err := blockgen.GenerateChain(emt.ChainConfig, emt.Genesis, emt.Engine, emt.DB, 4, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		to := common.BytesToAddress([]byte{byte(i + 1), 0xab})
		tx, txErr := types.SignTx(
			types.NewTransaction(b.TxNonce(keyAddr), to, uint256.NewInt(1_000_000), params.TxGas, uint256.NewInt(1), nil),
			*signer, key)
		require.NoError(t, txErr)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, emt.InsertChain(gen))

	ctx := context.Background()
	logger := log.New()
	require.NoError(t, rawdbreset.ResetExec(ctx, emt.DB))

	cfg := stagedsync.StageExecuteBlocksCfg(
		emt.DB, emt.cfg.Prune, emt.cfg.BatchSize, emt.ChainConfig, emt.Engine,
		&vm.Config{}, emt.Notifications, emt.cfg.StateStream, false /*badBlockHalt*/, emt.Dirs,
		emt.BlockReader, emt.cfg.Genesis, emt.cfg.Sync, false /*experimentalBAL*/, exec.NewBlockReadAheader())
	if agg, ok := emt.DB.(dbstate.HasAgg); ok {
		if aggT, okT := agg.Agg().(*dbstate.Aggregator); okT {
			aggT.PresetOfflineExecution()
		}
	}

	doms, err := newReusedDomains(ctx, emt, logger)
	require.NoError(t, err)
	defer doms.Close()

	readerBefore := doms.GetCommitmentContext().StateReader()
	_, err = execOneBatch(ctx, emt, doms, cfg, gen.TopBlock.NumberU64(), logger)
	require.NoError(t, err)

	require.Equal(t, readerBefore, doms.GetCommitmentContext().StateReader(),
		"exec must restore the commitment state reader it found; leaving the parallel calculator's asOfStateReader installed breaks a later foreground SeekCommitment with in-mem history reads disabled")
}
