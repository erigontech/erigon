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
	"context"
	"fmt"
	"math/big"
	"math/rand/v2"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

type reorgReference struct {
	payloads []*engineapitester.MockClPayload
	blocks   []*types.Block
	sums     []*big.Int
	states   map[common.Hash]crashRecoveryState
}

func generatedReorgSchedule(seed uint64) []byte {
	random := rand.New(rand.NewPCG(seed, 0))
	schedule := make([]byte, 128)
	for i := range schedule {
		schedule[i] = byte(random.Uint32N(256))
	}
	return schedule
}

// Each pair selects an operation and its target. Reference branches are built by
// forward execution with snapshot production disabled. A random case can be
// replayed with ERIGON_REORG_SEED.
func TestEngineApiReorgSchedules(t *testing.T) {
	if testing.Short() {
		t.Skip("node restart and snapshot reorg integration test")
	}
	const (
		opExtend = iota
		opSelectBranch
		opBounce
		opRestart
		opPrune
		opInvalidPayload
		opCount
	)
	opNames := [opCount]string{
		opExtend:         "extend",
		opSelectBranch:   "select_branch",
		opBounce:         "bounce",
		opRestart:        "restart",
		opPrune:          "prune",
		opInvalidPayload: "invalid_payload",
	}
	type scheduleCase struct {
		name     string
		schedule []byte
	}
	cases := []scheduleCase{
		{"bounce_restart", []byte{
			opExtend, 23,
			opSelectBranch, 47,
			opBounce, 7,
			opRestart, 0,
			opPrune, 0,
			opInvalidPayload, 0,
			opSelectBranch, 0,
			opRestart, 0,
			opExtend, 19,
		}},
		{"prune_invalid", []byte{
			opPrune, 0,
			opSelectBranch, 71,
			opInvalidPayload, 0,
			opRestart, 0,
			opBounce, 0,
			opExtend, 23,
			opPrune, 0,
			opSelectBranch, 24,
		}},
		{"repeated_unwind", []byte{
			opSelectBranch, 98,
			opExtend, 89,
			opSelectBranch, 179,
			opBounce, 16,
		}},
		{"restored_versions", generatedReorgSchedule(3)},
	}
	if value, ok := os.LookupEnv("ERIGON_REORG_SEED"); ok {
		seed, err := strconv.ParseUint(value, 10, 64)
		require.NoError(t, err)
		count := 1
		if value, ok := os.LookupEnv("ERIGON_REORG_CASES"); ok {
			count, err = strconv.Atoi(value)
			require.NoError(t, err)
			require.Positive(t, count)
		}
		for i := range count {
			caseSeed := seed + uint64(i)
			cases = append(cases, scheduleCase{fmt.Sprintf("seed_%d", caseSeed), generatedReorgSchedule(caseSeed)})
		}
	}
	var branches []reorgReference
	var address common.Address
	const prefixPokes, suffixPokes = 48, 24
	const forkIndex = prefixPokes + 1 // payload index: block 1, deployment, then pokes
	require.Greater(t, prefixPokes, snapshotCrashReorgDepth, "the shared prefix must be eligible for snapshots")
	require.Less(t, suffixPokes, snapshotCrashReorgDepth, "every generated reorg must fit inside the retained window")
	genesis, key, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	base := engineapitester.EngineApiTesterInitArgs{
		Genesis: genesis, CoinbaseKey: key, NoEmptyBlock1: true,
		EthConfigTweaker: configureCrashRecovery,
	}
	for branch := range 3 {
		args := base
		args.Logger, args.DataDir = testlog.Logger(t, log.LvlError), newSmallStepDataDir(t)
		eat, err := engineapitester.InitialiseEngineApiTester(t.Context(), args)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, eat.Close()) })
		empty, err := eat.MockCl.BuildCanonicalBlock(t.Context())
		require.NoError(t, err)
		payloads, addr, churn, sums := buildChurnChain(t.Context(), t, eat, prefixPokes, func(k int) int64 { return int64(k) })
		if branch == 0 {
			address = addr
		} else {
			require.Equal(t, address, addr)
		}
		ref := reorgReference{
			payloads: append([]*engineapitester.MockClPayload{empty}, payloads...),
			sums:     append([]*big.Int{nil}, sums...),
			states:   make(map[common.Hash]crashRecoveryState),
		}
		if branch > 0 {
			require.Equal(t, branches[0].payloads[forkIndex].ExecutionPayload.BlockHash, ref.payloads[forkIndex].ExecutionPayload.BlockHash)
		}
		for k := 0; k <= suffixPokes; k++ {
			eat.ExecutionModule.Drain()
			head := ref.payloads[len(ref.payloads)-1]
			ref.states[head.ExecutionPayload.BlockHash] = readCrashRecoveryState(t, eat.ChainDB)
			if k == suffixPokes {
				break
			}
			next, nextSums := churnAndAssert(t.Context(), t, eat, churn, 1, func(int) int64 { return int64(branch*1_000 + prefixPokes + k) })
			ref.payloads = append(ref.payloads, next...)
			ref.sums = append(ref.sums, nextSums...)
		}
		ref.blocks, err = decodeCrashRecoveryBlocks(readCrashRecoveryBlocks(t, eat.ChainDB, ref.payloads))
		require.NoError(t, err)
		require.NoError(t, eat.Close())
		branches = append(branches, ref)
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			schedule := test.schedule

			ctx := t.Context()
			args := base
			args.Logger, args.DataDir = testlog.Logger(t, log.LvlError), newSmallStepDataDir(t)
			args.EthConfigTweaker = func(config *ethconfig.Config) {
				configureSnapshotCrashRecovery(config)
				config.Sync.LoopBlockLimit = catchupCrashBlockLimit
			}
			var catchupCommits atomic.Uint64
			args.StateTransitionObserver = func(_ context.Context, point execmodule.StateTransitionPoint) {
				if point == execmodule.StateTransitionFCUCatchupCommitComplete {
					catchupCommits.Add(1)
				}
			}
			eat, err := engineapitester.InitialiseEngineApiTester(ctx, args)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, eat.Close()) })
			branch, headIndex := 0, forkIndex
			selectHead := func(nextBranch, nextHeadIndex int) {
				ref := branches[nextBranch]
				status, err := eat.ExecutionModule.InsertBlocks(ctx, ref.blocks[:nextHeadIndex+1])
				require.NoError(t, err)
				require.Equal(t, execmodule.ExecutionStatusSuccess, status)
				payload := ref.payloads[nextHeadIndex]
				require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payload))
				eat.ExecutionModule.Drain()
				if slot := payload.ExecutionPayload.SlotNumber; slot != nil {
					eat.MockCl.State().SlotNumber = max(eat.MockCl.State().SlotNumber, uint64(*slot)+1)
				}
				branch, headIndex = nextBranch, nextHeadIndex
			}
			check := func() {
				t.Helper()
				ref := branches[branch]
				churn, err := contracts.NewStateChurn(address, eat.ContractBackend)
				require.NoError(t, err)
				assertChurnState(ctx, t, eat, churn, ref.payloads[headIndex], ref.sums[headIndex])
				require.NoError(t, eat.ChainDB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
					assertEquivalentCrashRecoveryState(t, ref.states[ref.payloads[headIndex].ExecutionPayload.BlockHash], readCrashRecoveryTx(t, tx))
					return nil
				}))
			}
			selectHead(branch, headIndex)
			require.Positive(t, catchupCommits.Load(), "the mode matrix must exercise multi-block catch-up")
			check()
			for i := 0; i+1 < len(schedule); i += 2 {
				op, target := schedule[i]%opCount, int(schedule[i+1])
				t.Logf("step=%d op=%s target=%d branch=%d height=%d schedule=%x", i/2, opNames[op], target, branch, headIndex+1, schedule)
				switch op {
				case opExtend:
					selectHead(branch, min(headIndex+1+target%suffixPokes, len(branches[branch].payloads)-1))
				case opSelectBranch:
					selectHead(target%len(branches), forkIndex+(target/len(branches))%(suffixPokes+1))
				case opBounce:
					previousBranch, previousHeadIndex := branch, headIndex
					selectHead((branch+1)%len(branches), forkIndex+target%(suffixPokes+1))
					check()
					selectHead(previousBranch, previousHeadIndex)
					assertCanonicalHead(ctx, t, eat, branches[previousBranch].payloads[previousHeadIndex])
				case opRestart:
					args.MockClState = eat.MockCl.State()
					require.NoError(t, eat.Close())
					eat, err = engineapitester.InitialiseEngineApiTester(ctx, args)
					require.NoError(t, err)
				case opPrune:
					// A pass may build files; the next can prune their MDBX copies.
					for range 2 {
						finished, err := eat.ExecutionModule.Prune(ctx)
						require.NoError(t, err)
						select {
						case <-finished:
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						waitForDomainFilesSettled(ctx, t, eat.StateAgg)
					}
					require.NoError(t, eat.ChainDB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
						require.Positive(t, tx.Debug().DomainFiles(kv.AccountsDomain).EndRootNum(), "pruning must exercise snapshot-backed state")
						return nil
					}))
				case opInvalidPayload:
					payload, err := eat.MockCl.BuildNewPayload(ctx)
					require.NoError(t, err)
					bad := engineapitester.TamperMockClPayloadStateRoot(payload, common.Hash{1})
					status, err := eat.MockCl.InsertNewPayload(ctx, bad)
					require.NoError(t, err)
					require.Equal(t, enginetypes.InvalidStatus, status.Status)
					require.Equal(t, &payload.ExecutionPayload.ParentHash, status.LatestValidHash, "the payload must reach execution validation")
					require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, branches[branch].payloads[headIndex]))
					eat.ExecutionModule.Drain()
				}
				check()
			}
			churn, err := contracts.NewStateChurn(address, eat.ContractBackend)
			require.NoError(t, err)
			churnAndAssert(ctx, t, eat, churn, 3, func(k int) int64 { return int64(9_000 + k) })
		})
	}
}
