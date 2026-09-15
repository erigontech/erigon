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
	"encoding/binary"
	"math/big"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
)

const (
	rpcTransitionTimeout       = time.Minute
	rpcClientTimeout           = 10 * rpcTransitionTimeout
	stateChurnSeed       int64 = 0
	delayedRPCUserAgent        = "erigon-rpc-unwind-delayed"
)

func TestEngineApiRPCStateAcrossUnwindPhases(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)

	transitions := newStateTransitionController()
	engineAPIClientTimeout := rpcClientTimeout
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:                  logger,
		DataDir:                 t.TempDir(),
		Genesis:                 genesis,
		CoinbaseKey:             coinbaseKey,
		EngineApiClientTimeout:  &engineAPIClientTimeout,
		StateTransitionObserver: transitions.observe,
		EnableTestingAPI:        true,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = stateChurnReorgDepthBudget
		},
	})
	require.NoError(t, err)
	var asyncCalls sync.WaitGroup
	t.Cleanup(func() {
		asyncCalls.Wait()
		require.NoError(t, eat.Close())
	})

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		seed := stateChurnSeed
		// Each boundary gets its own contract because an RPC assertion may fill
		// StateCache; sharing a key could let one phase mask the next.
		_, storageRefillAddress, storageRefill, _ := buildChurnChain(ctx, t, eat, 0, nil)
		_, publicationBoundaryAddress, publicationBoundary, _ := buildChurnChain(ctx, t, eat, 0, nil)
		_, commitPhaseAddress, commitPhase, _ := buildChurnChain(ctx, t, eat, 0, nil)
		_, databasePhaseAddress, databasePhase, _ := buildChurnChain(ctx, t, eat, 0, nil)
		reorgTarget, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
		require.NoError(t, err)
		transactOpts.GasLimit = params.MaxTxnGasLimit
		for _, churn := range []*contracts.StateChurn{storageRefill, publicationBoundary, commitPhase, databasePhase} {
			applyStateChurnPoke(ctx, t, eat, churn, transactOpts, seed)
		}
		// This post-target deployment makes account and code removal part of the
		// unwind, while retained contracts lose their post-target storage writes.
		_, removedAccountAddress, _, _ := buildChurnChain(ctx, t, eat, 0, nil)
		storageSlot := stateChurnStorageSlot(0)
		expectedStorage := common.BigToHash(new(big.Int).SetUint64(stateChurnPokeValue(uint64(seed), 0)))
		require.NotZero(t, expectedStorage)

		transport := http.DefaultTransport
		if defaultTransport, ok := transport.(*http.Transport); ok {
			transport = defaultTransport.Clone()
		}
		httpClient := &http.Client{Transport: transport, Timeout: rpcClientTimeout}
		t.Cleanup(httpClient.CloseIdleConnections)
		rpcClient, err := rpc.DialHTTPWithClient(
			eat.JsonRpcUrl,
			httpClient,
			logger,
		)
		require.NoError(t, err)
		delayedRPCClient, err := rpc.DialHTTPWithClient(eat.JsonRpcUrl, httpClient, logger)
		require.NoError(t, err)
		delayedRPCClient.SetHeader("User-Agent", delayedRPCUserAgent)

		assertContractRPCStatePresentWithStorage(t, readContractRPCState(ctx, t, rpcClient, storageRefillAddress, storageSlot), expectedStorage)
		assertContractRPCStatePresentWithStorage(t, readContractRPCState(ctx, t, rpcClient, publicationBoundaryAddress, storageSlot), expectedStorage)
		assertContractRPCStatePresentWithStorage(t, readContractRPCState(ctx, t, rpcClient, commitPhaseAddress, storageSlot), expectedStorage)
		assertContractRPCStatePresentWithStorage(t, readContractRPCState(ctx, t, rpcClient, databasePhaseAddress, storageSlot), expectedStorage)
		assertContractRPCStatePresentWithZeroStorage(t, readContractRPCState(ctx, t, rpcClient, removedAccountAddress, storageSlot))

		oldHead, err := eat.MockCl.BuildNewPayload(ctx)
		require.NoError(t, err)
		status, err := eat.MockCl.InsertNewPayload(ctx, oldHead)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, status.Status)

		oldHeadOverlay := transitions.hold(t, execmodule.StateTransitionOverlayPublished, 1)
		// An FCU response does not imply teardown completion. Consume this
		// lifecycle's clear event before reusing the transition point.
		oldHeadCleared := transitions.hold(t, execmodule.StateTransitionOverlayCleared, 1)
		advanceToOldHead := startAsync(&asyncCalls, func() (struct{}, error) {
			return struct{}{}, eat.MockCl.UpdateForkChoice(ctx, oldHead)
		})
		oldHeadOverlay.wait(t)

		// These requests stay bound to the old head across the reorg. Their MVCC
		// reads may finish later, but they must not refill canonical cache entries.
		preUnwindViews := transitions.holdMatching(t, execmodule.StateTransitionRPCViewBound, 3, func(ctx context.Context) bool {
			return rpc.PeerInfoFromContext(ctx).HTTP.UserAgent == delayedRPCUserAgent
		})
		delayedStorage := startAsync(&asyncCalls, func() (common.Hash, error) {
			return readRPCStorage(ctx, delayedRPCClient, storageRefillAddress, storageSlot)
		})
		delayedCode := startAsync(&asyncCalls, func() (hexutil.Bytes, error) {
			return readRPCCode(ctx, delayedRPCClient, removedAccountAddress)
		})
		delayedNonce := startAsync(&asyncCalls, func() (hexutil.Uint64, error) {
			return readRPCNonce(ctx, delayedRPCClient, removedAccountAddress)
		})
		preUnwindViews.wait(t)
		oldHeadOverlay.release()
		oldHeadCleared.wait(t)
		oldHeadCleared.release()
		awaitAsync(t, advanceToOldHead)
		assertCanonicalHead(ctx, t, eat, oldHead)

		unwindComplete := transitions.hold(t, execmodule.StateTransitionUnwindComplete, 1)
		replacementOverlay := transitions.hold(t, execmodule.StateTransitionOverlayPublished, 1)
		replacementCommitted := transitions.hold(t, execmodule.StateTransitionCommitComplete, 1)
		replacementCleared := transitions.hold(t, execmodule.StateTransitionOverlayCleared, 1)
		reorgToTarget := startAsync(&asyncCalls, func() (struct{}, error) {
			return struct{}{}, eat.MockCl.UpdateForkChoice(ctx, reorgTarget)
		})

		unwindComplete.wait(t)
		assertContractRPCStatePresentWithStorage(t, readContractRPCState(ctx, t, rpcClient, publicationBoundaryAddress, storageSlot), expectedStorage)
		unwindComplete.release()
		replacementOverlay.wait(t)
		assertContractRPCStatePresentWithZeroStorage(t, readContractRPCState(ctx, t, rpcClient, publicationBoundaryAddress, storageSlot))
		replacementOverlay.release()

		replacementCommitted.wait(t)
		assertContractRPCStatePresentWithZeroStorage(t, readContractRPCState(ctx, t, rpcClient, commitPhaseAddress, storageSlot))
		replacementCommitted.release()

		replacementCleared.wait(t)
		assertContractRPCStatePresentWithZeroStorage(t, readContractRPCState(ctx, t, rpcClient, databasePhaseAddress, storageSlot))
		replacementCleared.release()
		awaitAsync(t, reorgToTarget)
		assertCanonicalHead(ctx, t, eat, reorgTarget)

		preUnwindViews.release()
		require.Equal(t, expectedStorage, awaitAsync(t, delayedStorage))
		require.NotEmpty(t, awaitAsync(t, delayedCode))
		require.NotZero(t, awaitAsync(t, delayedNonce))

		cacheProbeHead, err := eat.MockCl.BuildEmptyPayload(ctx, rpcClient.CallContext)
		require.NoError(t, err)
		require.Empty(t, cacheProbeHead.ExecutionPayload.Transactions)
		status, err = eat.MockCl.InsertNewPayload(ctx, cacheProbeHead)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, status.Status)

		cacheProbeOverlay := transitions.hold(t, execmodule.StateTransitionOverlayPublished, 1)
		cacheProbeCleared := transitions.hold(t, execmodule.StateTransitionOverlayCleared, 1)
		advanceToCacheProbe := startAsync(&asyncCalls, func() (struct{}, error) {
			return struct{}{}, eat.MockCl.UpdateForkChoice(ctx, cacheProbeHead)
		})
		cacheProbeOverlay.wait(t)

		// This fresh overlay has no writes for these keys, so reads exercise the
		// shared StateCache instead of the replacement overlay or database fallback.
		for range 2 {
			assertContractRPCStatePresentWithZeroStorage(t, readContractRPCState(ctx, t, rpcClient, storageRefillAddress, storageSlot))
			assertContractRPCStateAbsent(t, readContractRPCState(ctx, t, rpcClient, removedAccountAddress, storageSlot))
		}

		cacheProbeOverlay.release()
		cacheProbeCleared.wait(t)
		cacheProbeCleared.release()
		awaitAsync(t, advanceToCacheProbe)
		assertCanonicalHead(ctx, t, eat, cacheProbeHead)
	})
}

type contractRPCState struct {
	storage common.Hash
	code    hexutil.Bytes
	nonce   hexutil.Uint64
}

func readContractRPCState(
	ctx context.Context,
	t *testing.T,
	client *rpc.Client,
	address common.Address,
	storageSlot common.Hash,
) contractRPCState {
	t.Helper()
	storage, err := readRPCStorage(ctx, client, address, storageSlot)
	require.NoError(t, err)
	code, err := readRPCCode(ctx, client, address)
	require.NoError(t, err)
	nonce, err := readRPCNonce(ctx, client, address)
	require.NoError(t, err)
	return contractRPCState{storage: storage, code: code, nonce: nonce}
}

// These reads keep the caller's context because the test deliberately holds
// requests in flight across a forkchoice update.
func readRPCStorage(ctx context.Context, client *rpc.Client, address common.Address, storageSlot common.Hash) (common.Hash, error) {
	var result common.Hash
	err := client.CallContext(ctx, &result, "eth_getStorageAt", address, storageSlot, "latest")
	return result, err
}

func readRPCCode(ctx context.Context, client *rpc.Client, address common.Address) (hexutil.Bytes, error) {
	var result hexutil.Bytes
	err := client.CallContext(ctx, &result, "eth_getCode", address, "latest")
	return result, err
}

func readRPCNonce(ctx context.Context, client *rpc.Client, address common.Address) (hexutil.Uint64, error) {
	var result hexutil.Uint64
	err := client.CallContext(ctx, &result, "eth_getTransactionCount", address, "latest")
	return result, err
}

func assertContractRPCStatePresentWithStorage(t *testing.T, got contractRPCState, expectedStorage common.Hash) {
	t.Helper()
	require.Equal(t, expectedStorage, got.storage)
	require.NotEmpty(t, got.code)
	require.NotZero(t, got.nonce)
}

func assertContractRPCStatePresentWithZeroStorage(t *testing.T, got contractRPCState) {
	t.Helper()
	require.Zero(t, got.storage)
	require.NotEmpty(t, got.code)
	require.NotZero(t, got.nonce)
}

func assertContractRPCStateAbsent(t *testing.T, got contractRPCState) {
	t.Helper()
	require.Zero(t, got.storage)
	require.Empty(t, got.code)
	require.Zero(t, got.nonce)
}

func uint256Word(value uint64) []byte {
	word := make([]byte, 32)
	binary.BigEndian.PutUint64(word[24:], value)
	return word
}

func stateChurnStorageSlot(index uint64) common.Hash {
	ringKey := common.BytesToHash(crypto.Keccak256([]byte("statechurn"), uint256Word(index)))
	return common.BytesToHash(crypto.Keccak256(ringKey[:], make([]byte, 32)))
}

func stateChurnPokeValue(seed, cursor uint64) uint64 {
	hash := common.BytesToHash(crypto.Keccak256(uint256Word(seed), uint256Word(cursor)))
	return new(big.Int).Mod(new(big.Int).SetBytes(hash[:]), big.NewInt(3)).Uint64()
}

type asyncResult[T any] struct {
	value T
	err   error
}

func startAsync[T any](group *sync.WaitGroup, call func() (T, error)) <-chan asyncResult[T] {
	result := make(chan asyncResult[T], 1)
	group.Go(func() {
		value, err := call()
		result <- asyncResult[T]{value: value, err: err}
	})
	return result
}

func awaitAsync[T any](t *testing.T, result <-chan asyncResult[T]) T {
	t.Helper()
	timer := time.NewTimer(rpcTransitionTimeout)
	defer timer.Stop()
	select {
	case result := <-result:
		require.NoError(t, result.err)
		return result.value
	case <-timer.C:
		t.Fatal("asynchronous operation did not complete")
		var zero T
		return zero
	}
}

func TestStateTransitionControllerFiltersObservations(t *testing.T) {
	type matchKey struct{}

	transitions := newStateTransitionController()
	hold := transitions.holdMatching(t, execmodule.StateTransitionRPCViewBound, 1, func(ctx context.Context) bool {
		matched, _ := ctx.Value(matchKey{}).(bool)
		return matched
	})

	transitions.observe(t.Context(), execmodule.StateTransitionRPCViewBound)
	select {
	case <-hold.reached:
		t.Fatal("unmatched observation reached the hold")
	default:
	}

	matchedCtx, cancel := context.WithCancel(context.WithValue(t.Context(), matchKey{}, true))
	cancel()
	transitions.observe(matchedCtx, execmodule.StateTransitionRPCViewBound)
	select {
	case <-hold.reached:
	default:
		t.Fatal("matched observation did not reach the hold")
	}
}

// stateTransitionController turns inline lifecycle callbacks into
// deterministic barriers.
type stateTransitionController struct {
	mu    sync.Mutex
	holds map[execmodule.StateTransitionPoint]*stateTransitionHold
}

func newStateTransitionController() *stateTransitionController {
	return &stateTransitionController{holds: make(map[execmodule.StateTransitionPoint]*stateTransitionHold)}
}

func (c *stateTransitionController) hold(
	t *testing.T,
	point execmodule.StateTransitionPoint,
	count int,
) *stateTransitionHold {
	t.Helper()
	return c.holdMatching(t, point, count, nil)
}

func (c *stateTransitionController) holdMatching(
	t *testing.T,
	point execmodule.StateTransitionPoint,
	count int,
	matches func(context.Context) bool,
) *stateTransitionHold {
	t.Helper()
	if count < 1 {
		t.Fatal("transition hold count must be positive")
	}
	hold := &stateTransitionHold{
		point:     point,
		remaining: count,
		reached:   make(chan struct{}),
		proceed:   make(chan struct{}),
		matches:   matches,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.holds[point]; exists {
		t.Fatalf("transition %d already has a hold", point)
	}
	c.holds[point] = hold
	t.Cleanup(hold.release)
	return hold
}

func (c *stateTransitionController) observe(ctx context.Context, point execmodule.StateTransitionPoint) {
	c.mu.Lock()
	hold := c.holds[point]
	if hold == nil || hold.matches != nil && !hold.matches(ctx) {
		c.mu.Unlock()
		return
	}
	hold.remaining--
	if hold.remaining == 0 {
		delete(c.holds, point)
		close(hold.reached)
	}
	proceed := hold.proceed
	c.mu.Unlock()

	select {
	case <-proceed:
	case <-ctx.Done():
	}
}

type stateTransitionHold struct {
	point     execmodule.StateTransitionPoint
	remaining int
	reached   chan struct{}
	proceed   chan struct{}
	matches   func(context.Context) bool
	once      sync.Once
}

func (h *stateTransitionHold) wait(t *testing.T) {
	t.Helper()
	timer := time.NewTimer(rpcTransitionTimeout)
	defer timer.Stop()
	select {
	case <-h.reached:
	case <-timer.C:
		t.Fatalf("state transition %d was not reached", h.point)
	}
}

func (h *stateTransitionHold) release() {
	h.once.Do(func() { close(h.proceed) })
}
