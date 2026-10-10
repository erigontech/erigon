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

package engineapi_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"path/filepath"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

const catchupCrashBlockLimit = 8

type crashRecoveryBlock struct {
	RLP []byte
	BAL []byte
}

func configureCatchupCrashRecovery(config *ethconfig.Config) {
	configureCrashRecovery(config)
	config.Sync.LoopBlockLimit = catchupCrashBlockLimit
}

func TestEngineApiCatchupCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess crash recovery integration test")
	}
	ctx := t.Context()
	genesis, key, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	baseArgs := engineapitester.EngineApiTesterInitArgs{
		Genesis:          genesis,
		CoinbaseKey:      key,
		NoEmptyBlock1:    true,
		EthConfigTweaker: configureCatchupCrashRecovery,
	}
	const prefixPokes = 20
	const prefixBlocks = prefixPokes + 2
	// Reference runs execute one block per forkchoice update, so the expected
	// state does not depend on the catch-up batching being tested.
	buildReference := func(replacement bool) (chain crashRecoveryChain, checkpoints []crashRecoveryState, downloaded []crashRecoveryBlock, addr common.Address) {
		args := baseArgs
		args.Logger, args.DataDir = testlog.Logger(t, log.LvlError), newSmallStepDataDir(t)
		eat, initErr := engineapitester.InitialiseEngineApiTester(ctx, args)
		require.NoError(t, initErr)
		t.Cleanup(func() { require.NoError(t, eat.Close()) })
		empty, buildErr := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, buildErr)
		prefix, addr, churn, _ := buildChurnChain(ctx, t, eat, prefixPokes, func(k int) int64 { return int64(k) })
		chain.payloads = append([]*engineapitester.MockClPayload{empty}, prefix...)
		chunks, chunkSize := 1, 12
		if replacement {
			chunks, chunkSize = 5, catchupCrashBlockLimit
		}
		for chunk := range chunks {
			suffix, sums := buildCrashRecoverySuffix(ctx, t, eat, churn, prefixPokes+chunk*chunkSize, chunkSize, replacement)
			chain.payloads = append(chain.payloads, suffix...)
			chain.sum = sums[len(sums)-1]
			chain.state = readCrashRecoveryState(t, eat.ChainDB)
			assertCrashRecoveryReference(t, chain)
			require.GreaterOrEqual(t, chain.state.CommitmentTx, eat.ChainDB.StepSize(), "the batch limit needs at least one domain step")
			checkpoints = append(checkpoints, chain.state)
		}
		if replacement {
			downloaded = readCrashRecoveryBlocks(t, eat.ChainDB, chain.payloads)
			chain.continuation, chain.continuationSums = churnAndAssert(ctx, t, eat, churn, 3, func(k int) int64 { return int64(2_000 + k) })
		}
		require.NoError(t, eat.Close())
		return chain, checkpoints, downloaded, addr
	}
	canonical, _, _, addr := buildReference(false)
	replacement, checkpoints, downloaded, replacementAddr := buildReference(true)
	require.Equal(t, addr, replacementAddr)
	require.Equal(t, canonical.payloads[prefixBlocks-1].ExecutionPayload.BlockHash, replacement.payloads[prefixBlocks-1].ExecutionPayload.BlockHash)
	require.NotEqual(t, canonical.payloads[prefixBlocks].ExecutionPayload.BlockHash, replacement.payloads[prefixBlocks].ExecutionPayload.BlockHash)
	// Checkpoints below and above the old head cover both partial re-execution
	// of the unwound range and execution beyond it. Keep them before the
	// replacement tip so the restart still has blocks to execute.
	require.Less(t, checkpoints[0].CommitmentBlock, canonical.state.CommitmentBlock)
	require.Greater(t, checkpoints[1].CommitmentBlock, canonical.state.CommitmentBlock)
	require.NotEqual(t, canonical.state.Domains[kv.CodeDomain], checkpoints[0].Domains[kv.CodeDomain])
	require.NotEqual(t, checkpoints[0].Domains[kv.CodeDomain], checkpoints[1].Domains[kv.CodeDomain])
	for cycle := 1; cycle <= 2; cycle++ {
		checkpoint := checkpoints[cycle-1].CommitmentBlock
		require.Equal(t, uint64(prefixBlocks+cycle*catchupCrashBlockLimit), checkpoint, "reference catch-up checkpoint %d", cycle)
		require.Less(t, checkpoint, replacement.state.CommitmentBlock, "catch-up checkpoint %d must be intermediate", cycle)
	}

	for cycle := 1; cycle <= 2; cycle++ {
		t.Run(fmt.Sprintf("cycle_%d", cycle), func(t *testing.T) {
			for _, window := range []struct {
				name      string
				point     execmodule.StateTransitionPoint
				committed bool
			}{
				{"commit_ready", execmodule.StateTransitionFCUCatchupCommitReady, false},
				{"commit_complete", execmodule.StateTransitionFCUCatchupCommitComplete, true},
			} {
				t.Run(window.name, func(t *testing.T) {
					request := crashRecoveryRequest{
						Genesis:      genesis,
						CoinbaseKey:  crypto.FromECDSA(key),
						DataDir:      newSmallStepDataDir(t),
						Point:        window.point,
						Canonical:    canonical.payloads,
						CatchupCycle: cycle,
						Downloaded:   downloaded,
					}
					killAtUnwindBoundary(t, request)
					committedCycles := cycle - 1
					if window.committed {
						committedCycles++
					}
					want := canonical.state
					if committedCycles > 0 {
						want = catchupRecoveryCheckpoint(checkpoints[committedCycles-1], canonical.state, replacement.state)
					}
					// Inspect persisted state before node initialization can change it.
					checkpoint, blocks := readCrashRecoveryDataDir(t, request.DataDir, replacement.payloads)
					assertCatchupRecoveryState(t, want, checkpoint)
					require.Equal(t, downloaded, blocks, "bulk-imported blocks and BALs must survive every crash")

					args := baseArgs
					// Without the artificial batch cap, startup can finish catch-up
					// before the CL repeats the interrupted forkchoice request.
					args.EthConfigTweaker = configureCrashRecovery
					args.Logger, args.DataDir = testlog.Logger(t, log.LvlError), request.DataDir
					eat, initErr := engineapitester.InitialiseEngineApiTester(t.Context(), args)
					require.NoError(t, initErr)
					t.Cleanup(func() { require.NoError(t, eat.Close()) })
					if committedCycles > 0 {
						require.NoError(t, waitCrashRecoveryExecution(t.Context(), eat.ChainDB, replacement.state.CommitmentBlock))
					}
					// Bulk import is durable before the FCU; recovery must not re-import.
					// Re-importing would hide missing headers, bodies, or BALs after the crash.
					require.NoError(t, eat.MockCl.UpdateForkChoice(t.Context(), replacement.payloads[len(replacement.payloads)-1]))
					assertCatchupRecoveryState(t, replacement.state, readCrashRecoveryState(t, eat.ChainDB))
					churn, bindErr := contracts.NewStateChurn(addr, eat.ContractBackend)
					require.NoError(t, bindErr)
					assertChurnState(t.Context(), t, eat, churn, replacement.payloads[len(replacement.payloads)-1], replacement.sum)
					for _, target := range []crashRecoveryChain{canonical, replacement} {
						insertCrashRecoveryPayloads(t.Context(), t, eat, target.payloads)
						require.NoError(t, eat.MockCl.UpdateForkChoice(t.Context(), target.payloads[len(target.payloads)-1]))
						assertCatchupRecoveryState(t, target.state, readCrashRecoveryState(t, eat.ChainDB))
						assertChurnState(t.Context(), t, eat, churn, target.payloads[len(target.payloads)-1], target.sum)
					}
					assertCrashRecoveryContinuation(t, eat, churn, replacement)
				})
			}
		})
	}
}

func readCrashRecoveryDataDir(t *testing.T, dataDir string, payloads []*engineapitester.MockClPayload) (crashRecoveryState, []crashRecoveryBlock) {
	t.Helper()
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	dirs := datadir.New(dataDir)
	require.FileExists(t, filepath.Join(dirs.Chaindata, "mdbx.dat"))
	require.FileExists(t, filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE))
	// MDBX needs a writable open for crash recovery. The inspection itself
	// uses only read transactions, without node initialization or execution.
	rawDB, err := mdbx.New(dbcfg.ChainDB, logger).Path(dirs.Chaindata).MapSize(datasize.GB).Open(ctx)
	require.NoError(t, err)
	defer rawDB.Close()

	settings, err := state.ResolveErigonDBSettings(dirs, logger, false)
	require.NoError(t, err)
	agg, err := state.New(dirs).Logger(logger).WithErigonDBSettings(settings).Open(ctx)
	require.NoError(t, err)
	defer agg.Close()
	require.NoError(t, agg.OpenFolder(rawDB))

	blockSnapshots := blocksnapshots.NewRoSnapshots(ethconfig.BlocksFreezing{}, dirs.Snap, logger)
	defer blockSnapshots.Close()
	require.NoError(t, blockSnapshots.OpenFolder())
	db, err := temporal.New(rawDB, agg, blockSnapshots)
	require.NoError(t, err)
	return readCrashRecoveryCheckpoint(t, db), readCrashRecoveryBlocks(t, db, payloads)
}

func waitCrashRecoveryExecution(ctx context.Context, db kv.TemporalRoDB, head uint64) error {
	ctx, cancel := context.WithTimeout(ctx, rpcTransitionTimeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var progress uint64
		err := db.View(ctx, func(tx kv.Tx) error {
			var err error
			progress, err = stages.GetStageProgress(tx, stages.Finish)
			return err
		})
		if err != nil {
			return fmt.Errorf("read startup execution progress: %w", err)
		}
		if progress == head {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("startup execution at block %d, want %d: %w", progress, head, ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertCatchupRecoveryState(t *testing.T, want, got crashRecoveryState) {
	t.Helper()
	require.Equal(t, want.CommitmentBlock, got.CommitmentBlock, "persisted catch-up checkpoint: got block %d, want %d", got.CommitmentBlock, want.CommitmentBlock)
	require.Equal(t, want.CommitmentTx, got.CommitmentTx, "persisted catch-up checkpoint: got transaction %d, want %d", got.CommitmentTx, want.CommitmentTx)
	require.Equal(t, want.StageProgress, got.StageProgress, "persisted catch-up stage progress")
	// Different flush boundaries can keep or remove empty branch records.
	// Compare every live branch, including its child hashes and plain keys.
	for _, state := range []*crashRecoveryState{&want, &got} {
		domains := maps.Clone(state.Domains)
		branches := make(map[string]string)
		for key, value := range state.Domains[kv.CommitmentDomain] {
			if key != hex.EncodeToString(commitmentdb.KeyCommitmentState) {
				data, err := hex.DecodeString(value)
				require.NoError(t, err)
				require.GreaterOrEqual(t, len(data), 4)
				if commitment.BranchData(data).ChildCount() == 0 {
					require.Len(t, data, 4, "an empty branch must not contain cell data")
					continue
				}
			}
			branches[key] = value
		}
		domains[kv.CommitmentDomain] = branches
		state.Domains = domains
	}
	assertCrashRecoveryState(t, want, got)
}

// An intermediate commit persists execution only through its batch, but block
// metadata can describe the full downloaded chain. Forkchoice markers still
// describe the previously accepted FCU until the replacement FCU completes.
func catchupRecoveryCheckpoint(executed, previous, downloaded crashRecoveryState) crashRecoveryState {
	state := executed
	state.HeadBlock, state.HeadHeader = downloaded.HeadBlock, downloaded.HeadHeader
	state.Canonical, state.TxNums = downloaded.Canonical, downloaded.TxNums
	state.ForkchoiceHead, state.ForkchoiceSafe, state.Finalized = previous.ForkchoiceHead, previous.ForkchoiceSafe, previous.Finalized
	state.StageProgress = maps.Clone(executed.StageProgress)
	for _, stage := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders} {
		state.StageProgress[stage] = downloaded.StageProgress[stage]
	}
	return state
}

func readCrashRecoveryBlocks(t *testing.T, db kv.TemporalRoDB, payloads []*engineapitester.MockClPayload) []crashRecoveryBlock {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	blocks := make([]crashRecoveryBlock, len(payloads))
	for i, payload := range payloads {
		hash, number := payload.ExecutionPayload.BlockHash, uint64(payload.ExecutionPayload.BlockNumber)
		blocks[i] = readCrashRecoveryBlock(t, tx, hash, number)
	}
	return blocks
}

func readCrashRecoveryBlock(t *testing.T, tx kv.Tx, hash common.Hash, number uint64) crashRecoveryBlock {
	t.Helper()
	block := rawdb.ReadBlock(tx, hash, number)
	require.NotNilf(t, block, "missing durable block %d (%s)", number, hash)
	require.NotNilf(t, block.BlockAccessListHash(), "block %d must use Amsterdam", number)
	encoded, err := rlp.EncodeToBytes(block)
	require.NoError(t, err)
	bal, err := rawdb.ReadBlockAccessListBytes(tx, hash, number)
	require.NoError(t, err)
	require.NotEmptyf(t, bal, "missing durable block access list for block %d", number)
	return crashRecoveryBlock{RLP: encoded, BAL: bytes.Clone(bal)}
}

func decodeCrashRecoveryBlocks(downloaded []crashRecoveryBlock) ([]*types.Block, error) {
	blocks := make([]*types.Block, len(downloaded))
	for i, data := range downloaded {
		var block types.Block
		if err := rlp.DecodeBytes(data.RLP, &block); err != nil {
			return nil, err
		}
		if len(data.BAL) == 0 {
			return nil, fmt.Errorf("block %d: missing block access list", block.NumberU64())
		}
		sidecar, err := types.DecodeBlockAccessListSidecar(data.BAL)
		if err != nil {
			return nil, err
		}
		blocks[i] = block.WithBlockAccessListSidecar(sidecar)
	}
	return blocks, nil
}
