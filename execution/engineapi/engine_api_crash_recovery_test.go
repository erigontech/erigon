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
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/rlp"
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
	Deadline       time.Time
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
	TxLookup        map[string]string
	ReceiptHistory  map[kv.Domain][]crashRecoveryHistoryEntry
}

type crashRecoveryHistoryEntry struct {
	Key    string
	TxNum  uint64
	Before string
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
		Genesis:       genesis,
		CoinbaseKey:   key,
		NoEmptyBlock1: true,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = stateChurnReorgDepthBudget
			config.FcuBackgroundPrune = true
			config.PersistReceiptsCacheV2 = true
		},
	}
	const prefixPokes, suffixPokes = 20, 12
	buildReference := func(side bool) (prefix, tip crashRecoveryChain, addr common.Address) {
		args := baseArgs
		args.Logger, args.DataDir = testlog.Logger(t, log.LvlError), t.TempDir()
		eat, initErr := engineapitester.InitialiseEngineApiTester(ctx, args)
		require.NoError(t, initErr)
		t.Cleanup(func() { require.NoError(t, eat.Close()) })
		emptyBlock, buildErr := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, buildErr)
		payloads, addr, churn, sums := buildChurnChain(ctx, t, eat, prefixPokes, func(k int) int64 { return int64(k) })
		payloads = append([]*engineapitester.MockClPayload{emptyBlock}, payloads...)
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
		assertCrashRecoveryReference(t, prefix)
		assertCrashRecoveryReference(t, tip)
		prefix.continuation, prefix.continuationSums = suffix[:3], sums[:3]
		if side {
			tip.continuation, tip.continuationSums = churnAndAssert(ctx, t, eat, churn, 3, func(k int) int64 { return int64(2_000 + k) })
		}
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
						insertCrashRecoveryPayloads(t.Context(), t, eat, target.payloads)
						tip := target.payloads[len(target.payloads)-1]
						require.NoError(t, eat.MockCl.UpdateForkChoice(t.Context(), tip))
						assertChurnState(t.Context(), t, eat, churn, tip, target.sum)
						assertCrashRecoveryState(t, target.state, readCrashRecoveryState(t, eat.ChainDB))
					}
					for i, payload := range scenario.replacement.continuation {
						insertCrashRecoveryPayloads(t.Context(), t, eat, []*engineapitester.MockClPayload{payload})
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

func assertCrashRecoveryReference(t *testing.T, chain crashRecoveryChain) {
	t.Helper()
	head := uint64(chain.payloads[len(chain.payloads)-1].ExecutionPayload.BlockNumber)
	for _, stage := range []stages.SyncStage{stages.Senders, stages.TxLookup} {
		require.Equalf(t, head, chain.state.StageProgress[stage], "reference %s progress", stage)
	}
	require.NotEmpty(t, chain.state.TxLookup)
	for _, key := range [][]byte{
		rawtemporaldb.CumulativeGasUsedInBlockKey,
		rawtemporaldb.CumulativeBlobGasUsedInBlockKey,
		rawtemporaldb.LogIndexAfterTxKey,
	} {
		require.NotEmptyf(t, chain.state.Domains[kv.ReceiptDomain][hex.EncodeToString(key)], "reference receipt metadata key %x", key)
	}
	// Block-end transactions clear the latest cached receipt; its data remains in history.
	require.Contains(t, chain.state.Domains, kv.RCacheDomain)
	for _, domain := range []kv.Domain{kv.ReceiptDomain, kv.RCacheDomain} {
		require.NotEmpty(t, chain.state.ReceiptHistory[domain], "receipt history must be part of the oracle")
	}
	hasCachedReceipt := false
	for _, entry := range chain.state.ReceiptHistory[kv.RCacheDomain] {
		if entry.Before == "" {
			continue
		}
		encoded, err := hex.DecodeString(entry.Before)
		require.NoError(t, err)
		var receipt types.ReceiptForStorage
		require.NoErrorf(t, rlp.DecodeBytes(encoded, &receipt), "reference cached receipt before txNum %d", entry.TxNum)
		hasCachedReceipt = true
	}
	require.True(t, hasCachedReceipt, "reference receipt cache must contain encoded receipts")
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
		TxLookup:       make(map[string]string),
		ReceiptHistory: make(map[kv.Domain][]crashRecoveryHistoryEntry),
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
	for _, stage := range []stages.SyncStage{
		stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders,
		stages.Execution, stages.TxLookup, stages.Finish,
	} {
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
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.ReceiptDomain, kv.RCacheDomain} {
		state.Domains[domain] = readCrashRecoveryDomain(t, tx, domain)
	}
	require.NoError(t, tx.ForEach(kv.TxLookup, nil, func(key, value []byte) error {
		state.TxLookup[hex.EncodeToString(key)] = hex.EncodeToString(value)
		return nil
	}))
	for _, domain := range []kv.Domain{kv.ReceiptDomain, kv.RCacheDomain} {
		state.ReceiptHistory[domain] = readCrashRecoveryHistory(t, tx, domain)
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

func readCrashRecoveryHistory(t *testing.T, tx kv.TemporalTx, domain kv.Domain) []crashRecoveryHistoryEntry {
	t.Helper()
	// An unbounded scan also catches history left above the recovered head.
	entries, err := tx.Debug().HistoryKeyTxNumRange(domain, -1, -1, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer entries.Close()
	var history []crashRecoveryHistoryEntry
	for entries.HasNext() {
		key, txNum, nextErr := entries.Next()
		require.NoError(t, nextErr)
		before, ok, readErr := tx.HistorySeek(domain, key, txNum)
		require.NoError(t, readErr)
		require.True(t, ok, "history index must have a matching value")
		history = append(history, crashRecoveryHistoryEntry{
			Key:    hex.EncodeToString(key),
			TxNum:  txNum,
			Before: hex.EncodeToString(before),
		})
	}
	entries.Close()
	return history
}

func assertCrashRecoveryState(t *testing.T, want, got crashRecoveryState) {
	t.Helper()
	for domain, entries := range want.Domains {
		require.Equalf(t, entries, got.Domains[domain], "persisted %s state", domain)
	}
	want.Domains, got.Domains = nil, nil
	for domain, history := range want.ReceiptHistory {
		require.Equalf(t, history, got.ReceiptHistory[domain], "persisted %s history", domain)
	}
	want.ReceiptHistory, got.ReceiptHistory = nil, nil
	require.Equal(t, want.TxLookup, got.TxLookup, "persisted transaction lookup index")
	want.TxLookup, got.TxLookup = nil, nil
	require.Equal(t, want, got, "persisted canonical metadata and commitment")
}

func insertCrashRecoveryPayloads(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, payloads []*engineapitester.MockClPayload) {
	t.Helper()
	for _, payload := range payloads {
		status, err := eat.MockCl.InsertNewPayload(ctx, payload)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, status.Status)
	}
}

func waitCrashRecoveryTransition(ctx context.Context, hold *stateTransitionHold, response <-chan error) error {
	for {
		select {
		case <-hold.reached:
			return ctx.Err()
		case err := <-response:
			if err != nil {
				return fmt.Errorf("forkchoice before transition %d: %w", hold.point, err)
			}
			response = nil
		case <-ctx.Done():
			return fmt.Errorf("waiting for transition %d: %w", hold.point, ctx.Err())
		}
	}
}

func runUnwindCrashChild(t *testing.T) {
	t.Cleanup(func() { os.Exit(1) })
	var request crashRecoveryRequest
	require.NoError(t, json.NewDecoder(os.Stdin).Decode(&request))
	ctx, cancel := context.WithDeadline(t.Context(), request.Deadline)
	defer cancel()
	key, err := crypto.ToECDSA(request.CoinbaseKey)
	require.NoError(t, err)
	transitions := newStateTransitionController()
	clientTimeout := time.Until(request.Deadline)
	require.Positive(t, clientTimeout)
	// Node lifetime belongs to the process. Test cancellation must not release
	// a pre-commit barrier and let shutdown persist the parked FCU.
	eat, err := engineapitester.InitialiseEngineApiTester(context.Background(), engineapitester.EngineApiTesterInitArgs{
		Logger:                  testlog.Logger(t, log.LvlError),
		DataDir:                 request.DataDir,
		Genesis:                 request.Genesis,
		CoinbaseKey:             key,
		NoEmptyBlock1:           true,
		EngineApiClientTimeout:  &clientTimeout,
		StateTransitionObserver: transitions.observe,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = stateChurnReorgDepthBudget
			config.FcuBackgroundPrune = true
			config.Sync.ParallelStateFlushing = true
			config.PersistReceiptsCacheV2 = true
		},
	})
	require.NoError(t, err)
	startForkchoice := func(payload *engineapitester.MockClPayload) <-chan error {
		response := make(chan error, 1)
		go func() { response <- eat.MockCl.UpdateForkChoice(ctx, payload) }()
		return response
	}
	insertCrashRecoveryPayloads(ctx, t, eat, request.Canonical)
	canonicalPublished := transitions.hold(t, execmodule.StateTransitionOverlayPublished, 1)
	canonicalCleared := transitions.hold(t, execmodule.StateTransitionOverlayCleared, 1)
	canonicalResponse := startForkchoice(request.Canonical[len(request.Canonical)-1])
	// Force the early response to arrive while persistence is still blocked.
	// Only this FCU's clear event allows the replacement barrier to be armed.
	select {
	case err := <-canonicalResponse:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatalf("canonical FCU did not respond before commit: %v", ctx.Err())
	}
	require.NoError(t, waitCrashRecoveryTransition(ctx, canonicalPublished, nil))
	canonicalPublished.release()
	require.NoError(t, waitCrashRecoveryTransition(ctx, canonicalCleared, nil))
	canonicalCleared.release()

	insertCrashRecoveryPayloads(ctx, t, eat, request.Replacement)
	boundary := transitions.hold(t, request.Point, 1)
	// Abort before cleanup can release the crash barrier, even on failure.
	t.Cleanup(func() { os.Exit(1) })
	response := startForkchoice(request.Replacement[len(request.Replacement)-1])
	require.NoError(t, waitCrashRecoveryTransition(ctx, boundary, response))
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", request.ControlAddress)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetWriteDeadline(request.Deadline))
	require.NoError(t, json.NewEncoder(conn).Encode(boundary.point))
	<-ctx.Done()
	t.Fatalf("parent did not kill the child at transition %d: %v", boundary.point, ctx.Err())
}

func killAtUnwindBoundary(t *testing.T, request crashRecoveryRequest) {
	t.Helper()
	deadline := time.Now().Add(rpcClientTimeout)
	if testDeadline, ok := t.Deadline(); ok {
		cleanupBudget := min(time.Minute, time.Until(testDeadline)/10)
		if latest := testDeadline.Add(-cleanupBudget); latest.Before(deadline) {
			deadline = latest
		}
	}
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	stopClosing := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stopClosing()
	request.ControlAddress = listener.Addr().String()
	request.Deadline = deadline
	input, err := json.Marshal(request)
	require.NoError(t, err)
	executable, err := os.Executable()
	require.NoError(t, err)
	// The parent owns timeout and boundary kills; CommandContext would race them.
	cmd := exec.Command(executable, "-test.run=^TestEngineApiCrashRecovery$", "-test.v", "-test.timeout="+(time.Until(deadline)+time.Minute).String()) //nolint:noctx
	cmd.Env = append(os.Environ(), unwindCrashChild+"=1")
	cmd.Stdin = bytes.NewReader(input)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	require.NoError(t, cmd.Start())
	done := make(chan struct{})
	var exitErr error
	go func() {
		exitErr = cmd.Wait()
		_ = listener.Close()
		close(done)
	}()
	defer func() {
		_ = cmd.Process.Kill()
		<-done
		if t.Failed() {
			t.Logf("crash child output:\n%s", output.String())
		}
	}()
	conn, err := listener.Accept()
	require.NoError(t, err, "child exited or timed out before the crash boundary")
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(deadline))
	var reached execmodule.StateTransitionPoint
	require.NoError(t, json.NewDecoder(conn).Decode(&reached))
	require.Equal(t, request.Point, reached)
	require.NoError(t, ctx.Err(), "only a boundary-triggered kill counts as a crash test")
	require.NoError(t, cmd.Process.Kill())
	<-done
	var killed *exec.ExitError
	require.ErrorAs(t, exitErr, &killed)
	require.NotContains(t, output.String(), "WARNING: DATA RACE", "a killed child must not hide a race report")
}
