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
	"encoding/json"
	"math/big"
	"net"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

const unwindCrashChild = "ERIGON_UNWIND_CRASH_CHILD"

type crashRecoveryRequest struct {
	Genesis        *types.Genesis
	CoinbaseKey    []byte
	DataDir        string
	ControlAddress string
	Point          execmodule.StateTransitionPoint
	Canonical      []*engineapitester.MockClPayload
	Replacement    []*engineapitester.MockClPayload
}

type crashRecoveryState struct {
	HeadBlock       common.Hash
	HeadHeader      common.Hash
	ForkchoiceHead  common.Hash
	ForkchoiceSafe  common.Hash
	Finalized       common.Hash
	Canonical       []common.Hash
	TxNums          []uint64
	StageProgress   map[stages.SyncStage]uint64
	CommitmentBlock uint64
	CommitmentTx    uint64
	StateRoot       common.Hash
	Domains         map[kv.Domain]map[string]string
}

type crashRecoveryChain struct {
	payloads         []*engineapitester.MockClPayload
	state            crashRecoveryState
	sum              *big.Int
	continuation     []*engineapitester.MockClPayload
	continuationSums []*big.Int
}

func TestEngineApiCrashRecovery(t *testing.T) {
	if os.Getenv(unwindCrashChild) == "1" {
		runUnwindCrashChild(t)
		return
	}
	if testing.Short() {
		t.Skip("subprocess crash recovery integration test")
	}

	ctx := t.Context()
	genesis, key, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	baseArgs := engineapitester.EngineApiTesterInitArgs{
		Genesis:     genesis,
		CoinbaseKey: key,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = stateChurnReorgDepthBudget
			config.FcuBackgroundPrune = true
		},
	}
	const prefixPokes, suffixPokes = 20, 12
	buildReference := func(side bool) (prefix, tip crashRecoveryChain, addr common.Address) {
		args := baseArgs
		args.Logger, args.DataDir = testlog.Logger(t, log.LvlError), t.TempDir()
		eat, initErr := engineapitester.InitialiseEngineApiTester(ctx, args)
		require.NoError(t, initErr)
		t.Cleanup(func() { require.NoError(t, eat.Close()) })
		payloads, addr, churn, sums := buildChurnChain(ctx, t, eat, prefixPokes, func(k int) int64 { return int64(k) })
		prefix = crashRecoveryChain{
			payloads: payloads,
			state:    readCrashRecoveryState(t, eat.ChainDB),
			sum:      sums[len(sums)-1],
		}
		suffix, sums := churnAndAssert(ctx, t, eat, churn, suffixPokes, func(k int) int64 {
			seed := int64(prefixPokes + k)
			if side {
				seed += 1_000_000
			}
			return seed
		})
		tip = crashRecoveryChain{
			payloads: append(payloads, suffix...),
			state:    readCrashRecoveryState(t, eat.ChainDB),
			sum:      sums[len(sums)-1],
		}
		prefix.continuation, prefix.continuationSums = suffix[:3], sums[:3]
		tip.continuation, tip.continuationSums = churnAndAssert(ctx, t, eat, churn, 3, func(k int) int64 { return int64(2_000 + k) })
		require.NoError(t, eat.Close())
		return prefix, tip, addr
	}
	prefix, canonical, addr := buildReference(false)
	sidePrefix, side, sideAddr := buildReference(true)
	require.Equal(t, addr, sideAddr)
	assertCrashRecoveryState(t, prefix.state, sidePrefix.state)
	require.NotEqual(t, canonical.state.Domains[kv.StorageDomain], side.state.Domains[kv.StorageDomain])

	for _, scenario := range []struct {
		name        string
		replacement crashRecoveryChain
	}{
		{"ancestor", prefix},
		{"side_chain", side},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			for _, window := range []struct {
				name      string
				point     execmodule.StateTransitionPoint
				committed bool
			}{
				{"unwind_complete", execmodule.StateTransitionUnwindComplete, false},
				{"overlay_published", execmodule.StateTransitionOverlayPublished, false},
				{"commit_ready", execmodule.StateTransitionCommitReady, false},
				{"commit_complete", execmodule.StateTransitionCommitComplete, true},
			} {
				t.Run(window.name, func(t *testing.T) {
					request := crashRecoveryRequest{
						Genesis:     genesis,
						CoinbaseKey: crypto.FromECDSA(key),
						DataDir:     t.TempDir(),
						Point:       window.point,
						Canonical:   canonical.payloads,
						Replacement: scenario.replacement.payloads,
					}
					killAtUnwindBoundary(t, request)

					args := baseArgs
					args.Logger, args.DataDir = testlog.Logger(t, log.LvlError), request.DataDir
					args.NoEmptyBlock1 = true
					eat, initErr := engineapitester.InitialiseEngineApiTester(t.Context(), args)
					require.NoError(t, initErr)
					t.Cleanup(func() { require.NoError(t, eat.Close()) })
					churn, bindErr := contracts.NewStateChurn(addr, eat.ContractBackend)
					require.NoError(t, bindErr)
					want := canonical
					if window.committed {
						want = scenario.replacement
					}
					// Check before any FCU or newPayload can repair inconsistent disk state.
					assertCrashRecoveryState(t, want.state, readCrashRecoveryState(t, eat.ChainDB))
					assertChurnState(t.Context(), t, eat, churn, want.payloads[len(want.payloads)-1], want.sum)

					for _, target := range []crashRecoveryChain{scenario.replacement, canonical, scenario.replacement} {
						insertCrashRecoveryPayloads(t, eat, target.payloads)
						tip := target.payloads[len(target.payloads)-1]
						require.NoError(t, eat.MockCl.UpdateForkChoice(t.Context(), tip))
						assertChurnState(t.Context(), t, eat, churn, tip, target.sum)
						assertCrashRecoveryState(t, target.state, readCrashRecoveryState(t, eat.ChainDB))
					}
					for i, payload := range scenario.replacement.continuation {
						insertCrashRecoveryPayloads(t, eat, []*engineapitester.MockClPayload{payload})
						require.NoError(t, eat.MockCl.UpdateForkChoice(t.Context(), payload))
						assertChurnState(t.Context(), t, eat, churn, payload, scenario.replacement.continuationSums[i])
					}
					built, buildErr := eat.MockCl.BuildCanonicalBlock(t.Context())
					require.NoError(t, buildErr)
					assertCanonicalHead(t.Context(), t, eat, built)
					_, _, _, consistent := readChurn(t.Context(), t, churn)
					require.True(t, consistent, "state must remain consistent after block production resumes")
				})
			}
		})
	}
}

func readCrashRecoveryState(t *testing.T, db kv.TemporalRoDB) crashRecoveryState {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	lastBlock, _, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	state := crashRecoveryState{
		HeadBlock:      rawdb.ReadHeadBlockHash(tx),
		HeadHeader:     rawdb.ReadHeadHeaderHash(tx),
		ForkchoiceHead: rawdb.ReadForkchoiceHead(tx),
		ForkchoiceSafe: rawdb.ReadForkchoiceSafe(tx),
		Finalized:      rawdb.ReadForkchoiceFinalized(tx),
		Canonical:      make([]common.Hash, lastBlock+1),
		TxNums:         make([]uint64, lastBlock+1),
		StageProgress:  make(map[stages.SyncStage]uint64),
		Domains:        make(map[kv.Domain]map[string]string),
	}
	for block := uint64(0); block <= lastBlock; block++ {
		state.Canonical[block], err = rawdb.ReadCanonicalHash(tx, block)
		require.NoError(t, err)
		state.TxNums[block], err = rawdbv3.TxNums.Max(t.Context(), tx, block)
		require.NoError(t, err)
	}
	nextHash, err := rawdb.ReadCanonicalHash(tx, lastBlock+1)
	require.NoError(t, err)
	require.Zero(t, nextHash, "canonical hashes must not extend beyond TxNums")
	for _, stage := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Execution, stages.Finish} {
		state.StageProgress[stage], err = stages.GetStageProgress(tx, stage)
		require.NoError(t, err)
	}
	encoded, _, err := tx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, kv.GetLatestOptions{})
	require.NoError(t, err)
	root, blockNum, txNum, err := commitment.HexTrieExtractStateRoot(encoded)
	require.NoError(t, err)
	state.StateRoot, state.CommitmentBlock, state.CommitmentTx = common.BytesToHash(root), blockNum, txNum
	require.Equal(t, lastBlock, blockNum, "commitment and canonical TxNums must describe the same block")
	header, err := rawdb.ReadHeaderByHash(tx, state.HeadBlock)
	require.NoError(t, err)
	require.NotNil(t, header)
	require.Equal(t, header.Root, state.StateRoot, "persisted commitment root must match the head header")
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		state.Domains[domain] = readCrashRecoveryDomain(t, tx, domain)
	}
	return state
}

func readCrashRecoveryDomain(t *testing.T, tx kv.TemporalTx, domain kv.Domain) map[string]string {
	t.Helper()
	entries, err := tx.Debug().RangeLatest(domain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer entries.Close()
	state := make(map[string]string)
	for entries.HasNext() {
		key, value, nextErr := entries.Next()
		require.NoError(t, nextErr)
		if len(value) != 0 {
			state[hex.EncodeToString(key)] = hex.EncodeToString(value)
		}
	}
	entries.Close()
	return state
}

func assertCrashRecoveryState(t *testing.T, want, got crashRecoveryState) {
	t.Helper()
	for domain, entries := range want.Domains {
		require.Equalf(t, entries, got.Domains[domain], "persisted %s state", domain)
	}
	want.Domains, got.Domains = nil, nil
	require.Equal(t, want, got, "persisted canonical metadata and commitment")
}

func insertCrashRecoveryPayloads(t *testing.T, eat engineapitester.EngineApiTester, payloads []*engineapitester.MockClPayload) {
	t.Helper()
	for _, payload := range payloads {
		status, err := eat.MockCl.InsertNewPayload(t.Context(), payload)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, status.Status)
	}
}

func runUnwindCrashChild(t *testing.T) {
	var request crashRecoveryRequest
	require.NoError(t, json.NewDecoder(os.Stdin).Decode(&request))
	key, err := crypto.ToECDSA(request.CoinbaseKey)
	require.NoError(t, err)
	settled := make(chan struct{}, 1)
	var armed atomic.Bool
	eat, err := engineapitester.InitialiseEngineApiTester(t.Context(), engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, log.LvlError),
		DataDir:     request.DataDir,
		Genesis:     request.Genesis,
		CoinbaseKey: key,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = stateChurnReorgDepthBudget
			config.FcuBackgroundPrune = true
			config.Sync.ParallelStateFlushing = true
		},
		StateTransitionObserver: func(ctx context.Context, point execmodule.StateTransitionPoint) {
			if armed.Load() && point == request.Point {
				var dialer net.Dialer
				conn, dialErr := dialer.DialContext(ctx, "tcp", request.ControlAddress)
				if dialErr != nil {
					panic(dialErr)
				}
				defer conn.Close()
				if encodeErr := json.NewEncoder(conn).Encode(point); encodeErr != nil {
					panic(encodeErr)
				}
				<-ctx.Done()
			}
			if point == execmodule.StateTransitionOverlayCleared {
				select {
				case settled <- struct{}{}:
				case <-ctx.Done():
				}
			}
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	<-settled
	insertCrashRecoveryPayloads(t, eat, request.Canonical)
	require.NoError(t, eat.MockCl.UpdateForkChoice(t.Context(), request.Canonical[len(request.Canonical)-1]))
	<-settled
	insertCrashRecoveryPayloads(t, eat, request.Replacement)
	armed.Store(true)
	require.NoError(t, eat.MockCl.UpdateForkChoice(t.Context(), request.Replacement[len(request.Replacement)-1]))
	// A VALID response can precede the commit; keep the process alive for the hook.
	<-t.Context().Done()
	t.Fatal("crash boundary was not reached")
}

func killAtUnwindBoundary(t *testing.T, request crashRecoveryRequest) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	request.ControlAddress = listener.Addr().String()
	input, err := json.Marshal(request)
	require.NoError(t, err)
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestEngineApiCrashRecovery$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), unwindCrashChild+"=1")
	cmd.Stdin = bytes.NewReader(input)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	require.NoError(t, cmd.Start())
	exit := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		_ = listener.Close()
		exit <- err
	}()
	waited := false
	defer func() {
		cancel()
		if !waited {
			<-exit
		}
		if t.Failed() {
			t.Logf("crash child output:\n%s", output.String())
		}
	}()
	conn, err := listener.Accept()
	require.NoError(t, err, "child exited or timed out before the crash boundary")
	defer conn.Close()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.NoError(t, conn.SetReadDeadline(deadline))
	var reached execmodule.StateTransitionPoint
	require.NoError(t, json.NewDecoder(conn).Decode(&reached))
	require.Equal(t, request.Point, reached)
	require.NoError(t, ctx.Err(), "only a boundary-triggered kill counts as a crash test")
	require.NoError(t, cmd.Process.Kill())
	err = <-exit
	waited = true
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.NotContains(t, output.String(), "WARNING: DATA RACE", "a killed child must not hide a race report")
}
