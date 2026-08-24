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
	rpcTransitionTimeout = time.Minute
	rpcClientTimeout     = 5 * time.Minute
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
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = stateChurnReorgDepthBudget
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, eat.Close())
	})

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		seed := firstNonZeroStateChurnSeed()
		_, staleStorageAddress, staleStorage, _ := buildChurnChain(ctx, t, eat, 0, nil)
		_, overlayReadAddress, overlayRead, _ := buildChurnChain(ctx, t, eat, 0, nil)
		_, committedReadAddress, committedRead, _ := buildChurnChain(ctx, t, eat, 0, nil)
		_, clearedReadAddress, clearedRead, _ := buildChurnChain(ctx, t, eat, 0, nil)
		base, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		for _, churn := range []*contracts.StateChurn{staleStorage, overlayRead, committedRead, clearedRead} {
			applyStateChurnPoke(ctx, t, eat, churn, seed)
		}
		_, staleAccountAddress, _, _ := buildChurnChain(ctx, t, eat, 0, nil)
		storageSlot := stateChurnStorageSlot(0)
		expectedStorage := common.BigToHash(new(big.Int).SetUint64(stateChurnPokeValue(uint64(seed), 0)))

		rpcClient, err := rpc.DialHTTPWithClient(eat.JsonRpcUrl, &http.Client{Timeout: rpcClientTimeout}, logger)
		require.NoError(t, err)
		defer rpcClient.Close()

		assertContractRPCStatePresent(t, readContractRPCState(ctx, t, rpcClient, staleStorageAddress, storageSlot), expectedStorage)
		assertContractRPCStatePresent(t, readContractRPCState(ctx, t, rpcClient, overlayReadAddress, storageSlot), expectedStorage)
		assertContractRPCStatePresent(t, readContractRPCState(ctx, t, rpcClient, committedReadAddress, storageSlot), expectedStorage)
		assertContractRPCStatePresent(t, readContractRPCState(ctx, t, rpcClient, clearedReadAddress, storageSlot), expectedStorage)
		assertContractRPCStateWithoutStorage(t, readContractRPCState(ctx, t, rpcClient, staleAccountAddress, storageSlot))

		next, err := eat.MockCl.BuildNewPayload(ctx)
		require.NoError(t, err)
		status, err := eat.MockCl.InsertNewPayload(ctx, next)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, status.Status)

		overlayA := transitions.hold(t, execmodule.StateTransitionOverlayPublished, 1)
		advance := startAsync(func() (struct{}, error) {
			return struct{}{}, eat.MockCl.UpdateForkChoice(ctx, next)
		})
		overlayA.wait(t)

		oldViews := transitions.hold(t, execmodule.StateTransitionRPCViewBound, 3)
		oldStorage := startAsync(func() (common.Hash, error) {
			return readRPCStorage(ctx, rpcClient, staleStorageAddress, storageSlot)
		})
		oldCode := startAsync(func() (hexutil.Bytes, error) {
			return readRPCCode(ctx, rpcClient, staleAccountAddress)
		})
		oldNonce := startAsync(func() (hexutil.Uint64, error) {
			return readRPCNonce(ctx, rpcClient, staleAccountAddress)
		})
		oldViews.wait(t)
		overlayA.release()
		awaitAsync(t, advance)
		assertCanonicalHead(ctx, t, eat, next)

		unwind := transitions.hold(t, execmodule.StateTransitionUnwindComplete, 1)
		overlayB := transitions.hold(t, execmodule.StateTransitionOverlayPublished, 1)
		committedB := transitions.hold(t, execmodule.StateTransitionCommitComplete, 1)
		clearedB := transitions.hold(t, execmodule.StateTransitionOverlayCleared, 1)
		reorg := startAsync(func() (struct{}, error) {
			return struct{}{}, eat.MockCl.UpdateForkChoice(ctx, base)
		})

		unwind.wait(t)
		unwind.release()
		overlayB.wait(t)
		assertContractRPCStateWithoutStorage(t, readContractRPCState(ctx, t, rpcClient, overlayReadAddress, storageSlot))
		overlayB.release()

		committedB.wait(t)
		assertContractRPCStateWithoutStorage(t, readContractRPCState(ctx, t, rpcClient, committedReadAddress, storageSlot))
		committedB.release()

		clearedB.wait(t)
		assertContractRPCStateWithoutStorage(t, readContractRPCState(ctx, t, rpcClient, clearedReadAddress, storageSlot))
		clearedB.release()
		awaitAsync(t, reorg)
		assertCanonicalHead(ctx, t, eat, base)

		oldViews.release()
		require.Equal(t, expectedStorage, awaitAsync(t, oldStorage))
		require.NotEmpty(t, awaitAsync(t, oldCode))
		require.NotZero(t, awaitAsync(t, oldNonce))

		for range 2 {
			assertContractRPCStateWithoutStorage(t, readContractRPCState(ctx, t, rpcClient, staleStorageAddress, storageSlot))
			assertContractRPCStateAbsent(t, readContractRPCState(ctx, t, rpcClient, staleAccountAddress, storageSlot))
		}
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

func assertContractRPCStatePresent(t *testing.T, got contractRPCState, expectedStorage common.Hash) {
	t.Helper()
	require.Equal(t, expectedStorage, got.storage)
	require.NotEmpty(t, got.code)
	require.NotZero(t, got.nonce)
}

func assertContractRPCStateWithoutStorage(t *testing.T, got contractRPCState) {
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

func applyStateChurnPoke(
	ctx context.Context,
	t *testing.T,
	eat engineapitester.EngineApiTester,
	churn *contracts.StateChurn,
	seed int64,
) {
	t.Helper()
	transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
	require.NoError(t, err)
	transactOpts.GasLimit = params.MaxTxnGasLimit
	txn, err := churn.Poke(transactOpts, big.NewInt(seed))
	require.NoError(t, err)
	block, err := eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, block.ExecutionPayload, txn.Hash()))
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

func firstNonZeroStateChurnSeed() int64 {
	for seed := uint64(0); ; seed++ {
		if stateChurnPokeValue(seed, 0) != 0 {
			return int64(seed)
		}
	}
}

type asyncResult[T any] struct {
	value T
	err   error
}

func startAsync[T any](call func() (T, error)) <-chan asyncResult[T] {
	result := make(chan asyncResult[T], 1)
	go func() {
		value, err := call()
		result <- asyncResult[T]{value: value, err: err}
	}()
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
	if count < 1 {
		t.Fatal("transition hold count must be positive")
	}
	hold := &stateTransitionHold{
		point:     point,
		remaining: count,
		reached:   make(chan struct{}),
		proceed:   make(chan struct{}),
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
	if hold == nil {
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
