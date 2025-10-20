// Copyright 2025 The Erigon Authors
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

package shutter_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/synctest"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/shutter"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
	shuttercrypto "github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

func TestPoolCleanup(t *testing.T) {
	t.Parallel()
	pt := PoolTest{t}
	pt.Run(func(ctx context.Context, t *testing.T, pool *shutter.Pool, handle PoolTestHandle) {
		// simulate expected contract calls for reading the first ekg after the first block event
		ekg, err := testhelpers.MockEonKeyGeneration(shutter.EonIndex(0), 1, 2, 1)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, ekg)
		// simulate loadSubmissions after the first block
		handle.SimulateFilterLogs(common.HexToAddress(handle.config.SequencerContractAddress), []types.Log{})
		// simulate the first block
		err = handle.SimulateNewBlockChange(ctx)
		require.NoError(t, err)
		// simulate some encrypted txn submissions and simulate a new block
		encTxn1 := MockEncryptedTxn(t, handle.config.ChainId, ekg.Eon())
		encTxn2 := MockEncryptedTxn(t, handle.config.ChainId, ekg.Eon())
		require.Len(t, pool.AllEncryptedTxns(), 0)
		err = handle.SimulateLogEvents(ctx, []types.Log{
			MockTxnSubmittedEventLog(t, handle.config, ekg.Eon(), 1, encTxn1),
			MockTxnSubmittedEventLog(t, handle.config, ekg.Eon(), 2, encTxn2),
		})
		require.NoError(t, err)
		handle.SimulateCachedEonRead(t, ekg)
		err = handle.SimulateNewBlockChange(ctx)
		require.NoError(t, err)
		synctest.Wait()
		require.Len(t, pool.AllEncryptedTxns(), 2)
		// simulate decryption keys
		handle.SimulateCurrentSlot()
		handle.SimulateDecryptionKeys(ctx, t, ekg, 1, encTxn1.IdentityPreimage, encTxn2.IdentityPreimage)
		synctest.Wait()
		require.Len(t, pool.AllDecryptedTxns(), 2)
		// simulate one block passing by - decrypted txns pool should get cleaned up after 1 slot
		handle.SimulateCachedEonRead(t, ekg)
		err = handle.SimulateNewBlockChange(ctx)
		require.NoError(t, err)
		synctest.Wait()
		require.Len(t, pool.AllDecryptedTxns(), 0)
		// simulate more blocks passing by - encrypted txns pool should get cleaned up after config.ReorgDepthAwareness
		handle.SimulateCachedEonRead(t, ekg)
		err = handle.SimulateNewBlockChange(ctx)
		require.NoError(t, err)
		handle.SimulateCachedEonRead(t, ekg)
		err = handle.SimulateNewBlockChange(ctx)
		require.NoError(t, err)
		handle.SimulateCachedEonRead(t, ekg)
		err = handle.SimulateNewBlockChange(ctx)
		require.NoError(t, err)
		synctest.Wait()
		require.Len(t, pool.AllEncryptedTxns(), 0)
	})
}

func TestPoolSkipsBlobTxns(t *testing.T) {
	t.Parallel()
	pt := PoolTest{t}
	pt.Run(func(ctx context.Context, t *testing.T, pool *shutter.Pool, handle PoolTestHandle) {
		// simulate expected contract calls for reading the first ekg after the first block event
		ekg, err := testhelpers.MockEonKeyGeneration(shutter.EonIndex(0), 1, 2, 1)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, ekg)
		// simulate loadSubmissions after the first block
		handle.SimulateFilterLogs(common.HexToAddress(handle.config.SequencerContractAddress), []types.Log{})
		// simulate the first block
		err = handle.SimulateNewBlockChange(ctx)
		require.NoError(t, err)
		// simulate some encrypted txn submissions and simulate a new block
		encBlobTxn1 := MockEncryptedBlobTxn(t, handle.config.ChainId, ekg.Eon())
		encTxn1 := MockEncryptedTxn(t, handle.config.ChainId, ekg.Eon())
		require.Len(t, pool.AllEncryptedTxns(), 0)
		err = handle.SimulateLogEvents(ctx, []types.Log{
			MockTxnSubmittedEventLog(t, handle.config, ekg.Eon(), 1, encBlobTxn1),
			MockTxnSubmittedEventLog(t, handle.config, ekg.Eon(), 2, encTxn1),
		})
		require.NoError(t, err)
		handle.SimulateCachedEonRead(t, ekg)
		err = handle.SimulateNewBlockChange(ctx)
		require.NoError(t, err)
		synctest.Wait()
		require.Len(t, pool.AllEncryptedTxns(), 2)
		// simulate decryption keys
		handle.SimulateCurrentSlot()
		handle.SimulateDecryptionKeys(ctx, t, ekg, 1, encBlobTxn1.IdentityPreimage, encTxn1.IdentityPreimage)
		// verify that only 1 txn gets decrypted and the blob txn gets skipped
		synctest.Wait()
		require.Len(t, pool.AllDecryptedTxns(), 1)
		txns, err := pool.ProvideTxns(
			ctx,
			txnprovider.WithBlockTime(handle.nextBlockTime),
			txnprovider.WithParentBlockNum(handle.nextBlockNum-1),
		)
		require.NoError(t, err)
		require.Len(t, txns, 1)
		require.Equal(t, encTxn1.OriginalTxn.Hash(), txns[0].Hash())
		require.True(t, handle.logHandler.Contains("blob txns not allowed in shutter"))
	})
}

func TestPoolProvideTxnsUsesGasTargetAndTxnsIdFilter(t *testing.T) {
	t.Parallel()
	pt := PoolTest{t}
	pt.Run(func(ctx context.Context, t *testing.T, pool *shutter.Pool, handle PoolTestHandle) {
		// simulate expected contract calls for reading the first ekg after the first block event
		ekg, err := testhelpers.MockEonKeyGeneration(shutter.EonIndex(0), 1, 2, 1)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, ekg)
		// simulate loadSubmissions after the first block
		handle.SimulateFilterLogs(common.HexToAddress(handle.config.SequencerContractAddress), []types.Log{})
		// simulate the first block
		err = handle.SimulateNewBlockChange(ctx)
		require.NoError(t, err)
		// simulate some encrypted txn submissions and simulate a new block
		encTxn1 := MockEncryptedTxn(t, handle.config.ChainId, ekg.Eon())
		encTxn2 := MockEncryptedTxn(t, handle.config.ChainId, ekg.Eon())
		require.Len(t, pool.AllEncryptedTxns(), 0)
		err = handle.SimulateLogEvents(ctx, []types.Log{
			MockTxnSubmittedEventLog(t, handle.config, ekg.Eon(), 1, encTxn1),
			MockTxnSubmittedEventLog(t, handle.config, ekg.Eon(), 2, encTxn2),
		})
		require.NoError(t, err)
		handle.SimulateCachedEonRead(t, ekg)
		err = handle.SimulateNewBlockChange(ctx)
		require.NoError(t, err)
		synctest.Wait()
		require.Len(t, pool.AllEncryptedTxns(), 2)
		// simulate decryption keys
		handle.SimulateCurrentSlot()
		handle.SimulateDecryptionKeys(ctx, t, ekg, 1, encTxn1.IdentityPreimage, encTxn2.IdentityPreimage)
		synctest.Wait()
		require.Len(t, pool.AllDecryptedTxns(), 2)
		gasLimit := encTxn1.GasLimit.Uint64()
		// make sure both have the same gas limit so we can use it as an option for both ProvideTxns requests
		require.Equal(t, gasLimit, encTxn2.GasLimit.Uint64())
		txnsIdFilter := mapset.NewSet[[32]byte]()
		txnsRes1, err := pool.ProvideTxns(
			ctx,
			txnprovider.WithBlockTime(handle.nextBlockTime),
			txnprovider.WithParentBlockNum(handle.nextBlockNum-1),
			txnprovider.WithTxnIdsFilter(txnsIdFilter),
			txnprovider.WithGasTarget(gasLimit),
		)
		require.NoError(t, err)
		require.Len(t, txnsRes1, 1)
		txnsRes2, err := pool.ProvideTxns(
			ctx,
			txnprovider.WithBlockTime(handle.nextBlockTime),
			txnprovider.WithParentBlockNum(handle.nextBlockNum-1),
			txnprovider.WithTxnIdsFilter(txnsIdFilter),
			txnprovider.WithGasTarget(gasLimit),
		)
		require.NoError(t, err)
		require.Len(t, txnsRes2, 1)
		require.Equal(t, 2, txnsIdFilter.Cardinality())
	})
}

type PoolTest struct {
	*testing.T
}

func (t PoolTest) Run(testCase func(ctx context.Context, t *testing.T, pool *shutter.Pool, handle PoolTestHandle)) {
	synctest.Test(t.T, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		logger := testlog.Logger(t, log.LvlTrace)
		logHandler := testhelpers.NewCollectingLogHandler(logger.GetHandler())
		logger.SetHandler(logHandler)
		config := shuttercfg.ConfigByChainName(networkname.Chiado)
		config.ReorgDepthAwareness = 3
		config.BeaconChainGenesisTimestamp = uint64(time.Now().Unix())
		baseTxnProvider := EmptyTxnProvider{}
		ctrl := gomock.NewController(t)
		contractBackend := NewMockContractBackend(ctrl, logger)
		stateChangesClient := NewMockStateChangesClient(ctrl, logger)
		currentBlockNumReader := func(context.Context) (*uint64, error) { return nil, nil }
		slotCalculator := NewMockSlotCalculator(ctrl, config)
		keySenderFactory := NewMockDecryptionKeysSourceFactory(logger)
		pool := shutter.NewPool(
			logger,
			config,
			baseTxnProvider,
			contractBackend,
			stateChangesClient,
			currentBlockNumReader,
			shutter.WithSlotCalculator(slotCalculator),
			shutter.WithDecryptionKeysSourceFactory(keySenderFactory.NewDecryptionKeysSource),
		)

		contractBackend.PrepareMocks()
		slotCalculator.PrepareMocks(t)
		eg := errgroup.Group{}
		eg.Go(func() error { return pool.Run(ctx) })
		handle := PoolTestHandle{
			config:             config,
			logHandler:         logHandler,
			stateChangesClient: stateChangesClient,
			slotCalculator:     slotCalculator,
			contractBackend:    contractBackend,
			keySender:          keySenderFactory.sender,
			nextBlockNum:       1,
			nextBlockTime:      config.BeaconChainGenesisTimestamp + config.SecondsPerSlot,
		}
		// wait before calling the test case to ensure all pool background loops and subscriptions have been initialised
		synctest.Wait()
		testCase(ctx, t, pool, handle)
		cancel()
		err := eg.Wait()
		require.ErrorIs(t, err, context.Canceled)
	})
}

type PoolTestHandle struct {
	config             shuttercfg.Config
	logHandler         *testhelpers.CollectingLogHandler
	stateChangesClient *MockStateChangesClient
	contractBackend    *MockContractBackend
	slotCalculator     *MockSlotCalculator
	keySender          *MockKeySender
	nextBlockNum       uint64
	nextBlockTime      uint64
}

func (h *PoolTestHandle) SimulateNewBlockChange(ctx context.Context) error {
	defer func() {
		h.nextBlockNum++
		h.nextBlockTime += h.config.SecondsPerSlot
	}()
	return h.SimulateStateChange(ctx, MockStateChange{
		batch: &remoteproto.StateChangeBatch{
			ChangeBatch: []*remoteproto.StateChange{
				{BlockHeight: h.nextBlockNum, BlockTime: h.nextBlockTime, Direction: remoteproto.Direction_FORWARD},
			},
		},
	})
}

func (h *PoolTestHandle) SimulateStateChange(ctx context.Context, sc MockStateChange) error {
	return h.stateChangesClient.SimulateStateChange(ctx, sc)
}

func (h *PoolTestHandle) SimulateCallResult(addr common.Address, result []byte) {
	h.contractBackend.SimulateCallResult(addr, result)
}

func (h *PoolTestHandle) SimulateFilterLogs(addr common.Address, logs []types.Log) {
	h.contractBackend.SimulateFilterLogs(addr, logs)
}

func (h *PoolTestHandle) SimulateLogEvents(ctx context.Context, logs []types.Log) error {
	return h.contractBackend.SimulateLogEvents(ctx, logs)
}

func (h *PoolTestHandle) SimulateInitialEonRead(t *testing.T, ekg testhelpers.EonKeyGeneration) {
	ksmAddr := common.HexToAddress(h.config.KeyperSetManagerContractAddress)
	ksmAbi, err := abi.JSON(strings.NewReader(shuttercontracts.KeyperSetManagerABI))
	require.NoError(t, err)
	numKeyperSetsRes, err := ksmAbi.Methods["getNumKeyperSets"].Outputs.PackValues([]any{uint64(1)})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, numKeyperSetsRes)
	activationBlockRes, err := ksmAbi.Methods["getKeyperSetActivationBlock"].Outputs.PackValues([]any{ekg.ActivationBlock})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, activationBlockRes)
	h.SimulateNewEonRead(t, ekg)
}

func (h *PoolTestHandle) SimulateCachedEonRead(t *testing.T, ekg testhelpers.EonKeyGeneration) {
	ksmAddr := common.HexToAddress(h.config.KeyperSetManagerContractAddress)
	ksmAbi, err := abi.JSON(strings.NewReader(shuttercontracts.KeyperSetManagerABI))
	require.NoError(t, err)
	eonIndexRes, err := ksmAbi.Methods["getKeyperSetIndexByBlock"].Outputs.PackValues([]any{uint64(ekg.EonIndex)})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, eonIndexRes)
}

func (h *PoolTestHandle) SimulateNewEonRead(t *testing.T, ekg testhelpers.EonKeyGeneration) {
	ksmAddr := common.HexToAddress(h.config.KeyperSetManagerContractAddress)
	ksmAbi, err := abi.JSON(strings.NewReader(shuttercontracts.KeyperSetManagerABI))
	require.NoError(t, err)
	eonIndexRes, err := ksmAbi.Methods["getKeyperSetIndexByBlock"].Outputs.PackValues([]any{uint64(ekg.EonIndex)})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, eonIndexRes)
	keyperSetAddr := common.HexToAddress("0x0000000000000000000000000000000000005555")
	keyperSetAddrRes, err := ksmAbi.Methods["getKeyperSetAddress"].Outputs.PackValues([]any{keyperSetAddr})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, keyperSetAddrRes)
	keyperSetAbi, err := abi.JSON(strings.NewReader(shuttercontracts.KeyperSetABI))
	require.NoError(t, err)
	thresholdRes, err := keyperSetAbi.Methods["getThreshold"].Outputs.PackValues([]any{ekg.Threshold})
	require.NoError(t, err)
	h.SimulateCallResult(keyperSetAddr, thresholdRes)
	membersRes, err := keyperSetAbi.Methods["getMembers"].Outputs.PackValues([]any{ekg.Members()})
	require.NoError(t, err)
	h.SimulateCallResult(keyperSetAddr, membersRes)
	keyBroadcastAbi, err := abi.JSON(strings.NewReader(shuttercontracts.KeyBroadcastContractABI))
	require.NoError(t, err)
	eonKeyBytes := ekg.EonPublicKey.Marshal()
	eonKeyRes, err := keyBroadcastAbi.Methods["getEonKey"].Outputs.PackValues([]any{eonKeyBytes})
	require.NoError(t, err)
	h.SimulateCallResult(common.HexToAddress(h.config.KeyBroadcastContractAddress), eonKeyRes)
	activationBlockRes, err := ksmAbi.Methods["getKeyperSetActivationBlock"].Outputs.PackValues([]any{ekg.ActivationBlock})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, activationBlockRes)
	finalizedRes, err := keyperSetAbi.Methods["isFinalized"].Outputs.PackValues([]any{true})
	require.NoError(t, err)
	h.SimulateCallResult(keyperSetAddr, finalizedRes)
}

func (h *PoolTestHandle) SimulateCurrentSlot() {
	h.slotCalculator.currentSlotTimestamp.Store(h.nextBlockTime)
}

func (h *PoolTestHandle) SimulateDecryptionKeys(
	ctx context.Context,
	t *testing.T,
	ekg testhelpers.EonKeyGeneration,
	baseTxnIndex uint64,
	ips ...*shutter.IdentityPreimage,
) {
	slot := h.slotCalculator.CalcCurrentSlot()
	err := h.keySender.SimulateDecryptionKeys(ctx, ekg, slot, baseTxnIndex, ips, h.config.InstanceId)
	require.NoError(t, err)
}

type EmptyTxnProvider struct{}

func (p EmptyTxnProvider) ProvideTxns(_ context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, nil
}

func NewMockContractBackend(ctrl *gomock.Controller, logger log.Logger) *MockContractBackend {
	return &MockContractBackend{
		MockBackend:       contracts.NewMockBackend(ctrl),
		logger:            logger,
		mockedCallResults: map[common.Address][][]byte{},
		mockedFilterLogs:  map[common.Address][][]types.Log{},
		subs:              map[common.Address][]chan<- types.Log{},
	}
}

type MockContractBackend struct {
	*contracts.MockBackend
	logger            log.Logger
	mockedCallResults map[common.Address][][]byte
	mockedFilterLogs  map[common.Address][][]types.Log
	subs              map[common.Address][]chan<- types.Log
	mu                sync.Mutex
}

func (cb *MockContractBackend) PrepareMocks() {
	cb.EXPECT().
		SubscribeFilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery, s chan<- types.Log) (ethereum.Subscription, error) {
			cb.mu.Lock()
			defer cb.mu.Unlock()
			addrStrs := make([]string, 0, len(q.Addresses))
			for _, addr := range q.Addresses {
				addrStrs = append(addrStrs, addr.Hex())
				cb.subs[addr] = append(cb.subs[addr], s)
			}
			cb.logger.Trace("--- DEBUG --- called SubscribeFilterLogs", "addrs", strings.Join(addrStrs, ","))
			return MockSubscription{errChan: make(chan error), logger: cb.logger}, nil
		}).
		AnyTimes()

	cb.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, msg ethereum.CallMsg, b *big.Int) ([]byte, error) {
			cb.mu.Lock()
			defer cb.mu.Unlock()
			results := cb.mockedCallResults[*msg.To]
			if len(results) == 0 {
				cb.logger.Trace("--- DEBUG --- ISSUE - no mocked CallContract", "addr", msg.To.String())
				return nil, fmt.Errorf("no mocked call result remaining for addr=%s", msg.To)
			}
			res := results[0]
			cb.mockedCallResults[*msg.To] = results[1:]
			cb.logger.Trace("--- DEBUG --- called CallContract", "addr", msg.To.String())
			return res, nil
		}).
		AnyTimes()

	cb.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			cb.mu.Lock()
			defer cb.mu.Unlock()
			var res []types.Log
			addrStrs := make([]string, 0, len(query.Addresses))
			for _, addr := range query.Addresses {
				logs := cb.mockedFilterLogs[addr]
				if len(logs) == 0 {
					cb.logger.Trace("--- DEBUG --- ISSUE - no mocked FilterLogs", "addr", addr.String())
					return nil, fmt.Errorf("no mocked filter logs for addr=%s", addr)
				}
				res = append(res, logs[0]...)
				cb.mockedFilterLogs[addr] = logs[1:]
				addrStrs = append(addrStrs, addr.Hex())
			}
			cb.logger.Trace("--- DEBUG --- called FilterLogs")
			return res, nil
		}).
		AnyTimes()
}

func (cb *MockContractBackend) SimulateCallResult(addr common.Address, result []byte) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.mockedCallResults[addr] = append(cb.mockedCallResults[addr], result)
}

func (cb *MockContractBackend) SimulateFilterLogs(addr common.Address, logs []types.Log) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.mockedFilterLogs[addr] = append(cb.mockedFilterLogs[addr], logs)
}

func (cb *MockContractBackend) SimulateLogEvents(ctx context.Context, logs []types.Log) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	for _, l := range logs {
		cb.logger.Trace("--- DEBUG --- attempting to send log for", "addr", l.Address.String())
		for _, sub := range cb.subs[l.Address] {
			cb.logger.Trace("--- DEBUG --- sending log event", "addr", l.Address.String())
			select {
			case <-ctx.Done():
				return ctx.Err()
			case sub <- l: // no-op
				cb.logger.Trace("--- DEBUG --- sent log event", "addr", l.Address.String())
			}
		}
	}
	return nil
}

func NewMockStateChangesClient(ctrl *gomock.Controller, logger log.Logger) *MockStateChangesClient {
	return &MockStateChangesClient{ctrl: ctrl, logger: logger}
}

type MockStateChangesClient struct {
	ctrl   *gomock.Controller
	logger log.Logger
	subs   []chan MockStateChange
	mu     sync.Mutex
}

func (c *MockStateChangesClient) StateChanges(
	ctx context.Context,
	_ *remoteproto.StateChangeRequest,
	_ ...grpc.CallOption,
) (remoteproto.KV_StateChangesClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sub := make(chan MockStateChange)
	c.subs = append(c.subs, sub)
	mockStream := remoteproto.NewMockKV_StateChangesClient[*remoteproto.StateChange](c.ctrl)
	mockStream.EXPECT().
		Recv().
		DoAndReturn(func() (*remoteproto.StateChangeBatch, error) {
			select {
			case change := <-sub:
				c.logger.Trace(
					"--- DEBUG --- simulating state change - batch returned by Recv",
					"blockNum", change.batch.ChangeBatch[0].BlockHeight,
					"blockTime", change.batch.ChangeBatch[0].BlockTime,
				)
				return change.batch, change.err
			case <-ctx.Done():
				return nil, io.EOF
			}
		}).
		AnyTimes()
	return mockStream, nil
}

func (c *MockStateChangesClient) SimulateStateChange(ctx context.Context, sc MockStateChange) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var eg errgroup.Group
	for _, sub := range c.subs {
		eg.Go(func() error {
			c.logger.Trace(
				"--- DEBUG --- simulating state change - sending batch",
				"blockNum", sc.batch.ChangeBatch[0].BlockHeight,
				"blockTime", sc.batch.ChangeBatch[0].BlockTime,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case sub <- sc:
				c.logger.Trace(
					"--- DEBUG --- simulating state change - batch sent",
					"blockNum", sc.batch.ChangeBatch[0].BlockHeight,
					"blockTime", sc.batch.ChangeBatch[0].BlockTime,
				)
				return nil
			}
		})
	}
	return eg.Wait()
}

type MockStateChange struct {
	batch *remoteproto.StateChangeBatch
	err   error
}

type MockSubscription struct {
	errChan chan error
	logger  log.Logger
}

func (m MockSubscription) Unsubscribe() {
	m.logger.Trace("--- DEBUG --- called MockSubscription.Unsubscribe")
	close(m.errChan)
}

func (m MockSubscription) Err() <-chan error {
	m.logger.Trace("--- DEBUG --- called MockSubscription.Err")
	return m.errChan
}

func NewMockSlotCalculator(ctrl *gomock.Controller, config shuttercfg.Config) *MockSlotCalculator {
	return &MockSlotCalculator{
		MockSlotCalculator: testhelpers.NewMockSlotCalculator(ctrl),
		real:               shutter.NewBeaconChainSlotCalculator(config.BeaconChainGenesisTimestamp, config.SecondsPerSlot),
	}
}

type MockSlotCalculator struct {
	*testhelpers.MockSlotCalculator
	currentSlotTimestamp atomic.Uint64
	real                 shutter.BeaconChainSlotCalculator
}

func (c *MockSlotCalculator) PrepareMocks(t *testing.T) {
	c.MockSlotCalculator.EXPECT().
		CalcCurrentSlot().
		DoAndReturn(func() uint64 {
			slot, err := c.real.CalcSlot(c.currentSlotTimestamp.Load())
			require.NoError(t, err)
			return slot
		}).
		AnyTimes()
	c.MockSlotCalculator.EXPECT().
		CalcSlot(gomock.Any()).
		DoAndReturn(func(u uint64) (uint64, error) { return c.real.CalcSlot(u) }).
		AnyTimes()
	c.MockSlotCalculator.EXPECT().
		CalcSlotAge(gomock.Any()).
		DoAndReturn(func(u uint64) time.Duration { return c.real.CalcSlotAge(u) }).
		AnyTimes()
	c.MockSlotCalculator.EXPECT().
		SecondsPerSlot().
		DoAndReturn(func() uint64 { return c.real.SecondsPerSlot() }).
		AnyTimes()
	c.MockSlotCalculator.EXPECT().
		CalcSlotStartTimestamp(gomock.Any()).
		DoAndReturn(func(u uint64) uint64 { return c.real.CalcSlotStartTimestamp(u) }).
		AnyTimes()
}

func NewMockDecryptionKeysSourceFactory(logger log.Logger) *MockDecryptionKeysSourceFactory {
	return &MockDecryptionKeysSourceFactory{
		logger: logger,
	}
}

type MockDecryptionKeysSourceFactory struct {
	logger log.Logger
	sender *MockKeySender
}

func (f *MockDecryptionKeysSourceFactory) NewDecryptionKeysSource(validator pubsub.ValidatorEx) shutter.DecryptionKeysSource {
	f.sender = &MockKeySender{validator: validator, logger: f.logger}
	return f.sender
}

type MockKeySender struct {
	mu        sync.Mutex
	subs      []MockDecryptionKeysSubscription
	validator pubsub.ValidatorEx
	logger    log.Logger
}

func (m *MockKeySender) Run(_ context.Context) error {
	return nil
}

func (m *MockKeySender) Subscribe(_ context.Context) (shutter.DecryptionKeysSubscription, error) {
	sub := MockDecryptionKeysSubscription{logger: m.logger, ch: make(chan *pubsub.Message)}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subs = append(m.subs, sub)
	m.logger.Trace("--- DEBUG --- subscribed to mock decryption keys source")
	return sub, nil
}

func (m *MockKeySender) SimulateDecryptionKeys(
	ctx context.Context,
	ekg testhelpers.EonKeyGeneration,
	slot uint64,
	baseTxnIndex uint64,
	ips []*shutter.IdentityPreimage,
	instanceId uint64,
) error {
	envelope, err := testhelpers.DecryptionKeysPublishMsgEnveloped(ekg, slot, baseTxnIndex, ips, instanceId)
	if err != nil {
		return err
	}

	msg := testhelpers.MockDecryptionKeysMsg(shutter.DecryptionKeysTopic, envelope)
	status := m.validator(ctx, "/p2p/mock-key-sender", msg)
	if status != pubsub.ValidationAccept {
		return fmt.Errorf("mock key sender rejected msg")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	var eg errgroup.Group
	for _, sub := range m.subs {
		eg.Go(func() error {
			m.logger.Trace(
				"--- DEBUG --- attempting to send mock decryption keys msg",
				"slot", slot,
				"baseTxnIndex", baseTxnIndex,
				"ips", len(ips),
			)
			return sub.Consume(ctx, msg)
		})
	}

	return eg.Wait()
}

type MockDecryptionKeysSubscription struct {
	logger log.Logger
	ch     chan *pubsub.Message
}

func (s MockDecryptionKeysSubscription) Next(ctx context.Context) (*pubsub.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-s.ch:
		defer s.logger.Trace("--- DEBUG --- mock decryption keys msg returned by Next")
		return msg, nil
	}
}

func (s MockDecryptionKeysSubscription) Consume(ctx context.Context, m *pubsub.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.ch <- m:
		defer s.logger.Trace("--- DEBUG --- consumed mock decryption keys msg")
		return nil
	}
}

func MockTxnSubmittedEventLog(
	t *testing.T,
	config shuttercfg.Config,
	eon shutter.Eon,
	txnIndex uint64,
	submission testhelpers.EncryptedSubmission,
) types.Log {
	sequencerAddr := common.HexToAddress(config.SequencerContractAddress)
	sequencerAbi, err := abi.JSON(strings.NewReader(shuttercontracts.SequencerABI))
	require.NoError(t, err)
	submissionTopic, err := abi.MakeTopics([]any{sequencerAbi.Events["TransactionSubmitted"].ID})
	require.NoError(t, err)
	return types.Log{
		Address: sequencerAddr,
		Topics:  submissionTopic[0],
		Data:    MockTxnSubmittedEventData(t, eon, txnIndex, submission),
	}
}

func MockTxnSubmittedEventData(
	t *testing.T,
	eon shutter.Eon,
	txnIndex uint64,
	submission testhelpers.EncryptedSubmission,
) []byte {
	sequencer, err := abi.JSON(strings.NewReader(shuttercontracts.SequencerABI))
	require.NoError(t, err)
	abiArgs := sequencer.Events["TransactionSubmitted"].Inputs
	var ipPrefix [32]byte
	copy(ipPrefix[:], submission.IdentityPreimage[:32])
	sender := common.BytesToAddress(submission.IdentityPreimage[32:52])
	data, err := abiArgs.Pack(
		uint64(eon.Index),
		txnIndex,
		ipPrefix,
		sender,
		submission.EncryptedTxn.Marshal(),
		submission.GasLimit,
	)
	require.NoError(t, err)
	return data
}

func MockEncryptedTxn(t *testing.T, chainId *uint256.Int, eon shutter.Eon) testhelpers.EncryptedSubmission {
	senderPrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(senderPrivKey.PublicKey)
	txn := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    uint64(99),
			GasLimit: 21_000,
			To:       &senderAddr, // send to self
			Value:    uint256.NewInt(123),
		},
		GasPrice: uint256.NewInt(555),
	}
	signer := types.LatestSignerForChainID(chainId.ToBig())
	signedTxn, err := types.SignTx(txn, *signer, senderPrivKey)
	require.NoError(t, err)
	var signedTxnBuf bytes.Buffer
	err = signedTxn.MarshalBinary(&signedTxnBuf)
	require.NoError(t, err)
	eonPublicKey, err := eon.PublicKey()
	require.NoError(t, err)
	sigma, err := shuttercrypto.RandomSigma(rand.Reader)
	require.NoError(t, err)
	identityPrefix, err := shuttercrypto.RandomSigma(rand.Reader)
	require.NoError(t, err)
	ip := shutter.IdentityPreimageFromSenderPrefix(identityPrefix, senderAddr)
	epochId := shuttercrypto.ComputeEpochID(ip[:])
	encryptedTxn := shuttercrypto.Encrypt(signedTxnBuf.Bytes(), eonPublicKey, epochId, sigma)
	return testhelpers.EncryptedSubmission{
		OriginalTxn:      signedTxn,
		SubmissionTxn:    nil,
		EncryptedTxn:     encryptedTxn,
		EonIndex:         eon.Index,
		IdentityPreimage: ip,
		GasLimit:         new(big.Int).SetUint64(txn.GetGasLimit()),
	}
}

func MockEncryptedBlobTxn(t *testing.T, chainId *uint256.Int, eon shutter.Eon) testhelpers.EncryptedSubmission {
	senderPrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(senderPrivKey.PublicKey)
	signer := types.LatestSignerForChainID(chainId.ToBig())
	txn := types.MakeV1WrappedBlobTxn(chainId)
	signedTxn, err := types.SignTx(txn, *signer, senderPrivKey)
	require.NoError(t, err)
	var signedTxnBuf bytes.Buffer
	err = signedTxn.(*types.BlobTxWrapper).MarshalBinaryWrapped(&signedTxnBuf)
	require.NoError(t, err)
	eonPublicKey, err := eon.PublicKey()
	require.NoError(t, err)
	sigma, err := shuttercrypto.RandomSigma(rand.Reader)
	require.NoError(t, err)
	identityPrefix, err := shuttercrypto.RandomSigma(rand.Reader)
	require.NoError(t, err)
	ip := shutter.IdentityPreimageFromSenderPrefix(identityPrefix, senderAddr)
	epochId := shuttercrypto.ComputeEpochID(ip[:])
	encryptedTxn := shuttercrypto.Encrypt(signedTxnBuf.Bytes(), eonPublicKey, epochId, sigma)
	return testhelpers.EncryptedSubmission{
		OriginalTxn:      signedTxn,
		SubmissionTxn:    nil,
		EncryptedTxn:     encryptedTxn,
		EonIndex:         eon.Index,
		IdentityPreimage: ip,
		GasLimit:         new(big.Int).SetUint64(txn.GetGasLimit()),
	}
}
