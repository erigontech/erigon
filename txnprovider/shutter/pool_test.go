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
	"context"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/shutter"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

func TestPoolCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	pt := NewPoolTest(t)
	pt.Run(func(ctx context.Context, t *testing.T, pool *shutter.Pool, handle PoolTestHandle) {
		// simulate expected contract calls for reading the first eon after the first block event
		eon, err := testhelpers.MockEonKeyGeneration(shutter.EonIndex(0), 1, 2, 1)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, eon)
		// simulate loadSubmissions after the first block
		handle.SimulateFilterLogs(common.HexToAddress(handle.config.SequencerContractAddress), []types.Log{})
		// simulate the first block
		err = handle.SimulateNewBlockChange(ctx, 1, 100_000)
		require.NoError(t, err)

		//
		// TODO - verify that the sub pools get cleaned up
		//
		time.Sleep(time.Second)
	})
}

func NewPoolTest(t *testing.T) PoolTest {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	logger := testlog.Logger(t, log.LvlTrace)
	dataDir := t.TempDir()
	nodeKeyConfig := p2p.NodeKeyConfig{}
	nodeKey, err := nodeKeyConfig.LoadOrGenerateAndSave(nodeKeyConfig.DefaultPath(dataDir))
	require.NoError(t, err)
	poolP2pPrivKeyBytes := make([]byte, 32)
	nodeKey.D.FillBytes(poolP2pPrivKeyBytes)
	poolP2pPrivKey, err := libp2pcrypto.UnmarshalSecp256k1PrivateKey(poolP2pPrivKeyBytes)
	require.NoError(t, err)
	poolPeerId, err := peer.IDFromPrivateKey(poolP2pPrivKey)
	require.NoError(t, err)
	keySenderPrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	keySenderPrivKeyBytes := make([]byte, 32)
	keySenderPrivKey.D.FillBytes(keySenderPrivKeyBytes)
	keySenderP2pPrivKey, err := libp2pcrypto.UnmarshalSecp256k1PrivateKey(keySenderPrivKeyBytes)
	require.NoError(t, err)
	keySender, err := testhelpers.DialDecryptionKeysSender(ctx, logger, 0, keySenderP2pPrivKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := keySender.Close()
		require.NoError(t, err)
	})
	keySenderPeerId, err := peer.IDFromPrivateKey(keySenderP2pPrivKey)
	require.NoError(t, err)
	keySenderListenAddresses, err := keySender.InterfaceListenAddresses()
	require.NoError(t, err)
	var keySenderPort uint64 // get the port that the OS assigned to the key sender (we used localhost:0)
	for _, addr := range keySenderListenAddresses {
		for _, protocol := range addr.Protocols() {
			if protocol.Code == multiaddr.P_TCP {
				keySenderPortStr, err := addr.ValueForProtocol(multiaddr.P_TCP)
				require.NoError(t, err)
				keySenderPort, err = strconv.ParseUint(keySenderPortStr, 10, 64)
				require.NoError(t, err)
			}
		}
	}
	require.Greater(t, keySenderPort, uint64(0))
	require.NoError(t, err)
	keySenderPeerAddr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", keySenderPort, keySenderPeerId)
	config := shuttercfg.ConfigByChainName(networkname.Chiado)
	config.PrivateKey = nodeKey
	config.BootstrapNodes = []string{keySenderPeerAddr}
	config.BeaconChainGenesisTimestamp = 0
	baseTxnProvider := EmptyTxnProvider{}
	ctrl := gomock.NewController(t)
	contractBackend := NewMockContractBackend(ctrl)
	contractBackend.PrepareMocks()
	stateChangesClient := NewMockStateChangesClient(ctrl)
	currentBlockNumReader := func(context.Context) (*uint64, error) { return nil, nil }
	pool := shutter.NewPool(logger, config, baseTxnProvider, contractBackend, stateChangesClient, currentBlockNumReader)
	return PoolTest{
		ctx:                ctx,
		t:                  t,
		config:             config,
		stateChangesClient: stateChangesClient,
		contractBackend:    contractBackend,
		keySender:          keySender,
		poolPeerId:         poolPeerId,
		pool:               pool,
	}
}

type PoolTest struct {
	ctx                context.Context
	t                  *testing.T
	config             shuttercfg.Config
	stateChangesClient *MockStateChangesClient
	contractBackend    *MockContractBackend
	keySender          testhelpers.DecryptionKeysSender
	poolPeerId         peer.ID
	pool               *shutter.Pool
}

func (pt PoolTest) Run(testCase func(ctx context.Context, t *testing.T, pool *shutter.Pool, handle PoolTestHandle)) {
	ctx, cancel := context.WithTimeout(pt.t.Context(), time.Minute)
	pt.t.Cleanup(cancel)
	eg := errgroup.Group{}
	eg.Go(func() error { return pt.pool.Run(ctx) })
	err := pt.keySender.WaitExternalPeerConnection(ctx, pt.poolPeerId)
	require.NoError(pt.t, err)
	handle := PoolTestHandle{
		config:             pt.config,
		stateChangesClient: pt.stateChangesClient,
		contractBackend:    pt.contractBackend,
		pool:               pt.pool,
	}
	testCase(pt.ctx, pt.t, pt.pool, handle)
	cancel()
	err = eg.Wait()
	require.ErrorIs(pt.t, err, context.Canceled)
}

type PoolTestHandle struct {
	config             shuttercfg.Config
	stateChangesClient *MockStateChangesClient
	contractBackend    *MockContractBackend
	pool               *shutter.Pool
}

func (h PoolTestHandle) SimulateNewBlockChange(ctx context.Context, blockNum, blockTime uint64) error {
	return h.SimulateStateChange(ctx, MockStateChange{
		batch: &remoteproto.StateChangeBatch{
			ChangeBatch: []*remoteproto.StateChange{
				{BlockHeight: blockNum, BlockTime: blockTime, Direction: remoteproto.Direction_FORWARD},
			},
		},
	})
}

func (h PoolTestHandle) SimulateStateChange(ctx context.Context, sc MockStateChange) error {
	return h.stateChangesClient.SimulateStateChange(ctx, sc)
}

func (h PoolTestHandle) SimulateCallResult(addr common.Address, result []byte) {
	h.contractBackend.SimulateCallResult(addr, result)
}

func (h PoolTestHandle) SimulateFilterLogs(addr common.Address, logs []types.Log) {
	h.contractBackend.SimulateFilterLogs(addr, logs)
}

func (h PoolTestHandle) SimulateInitialEonRead(t *testing.T, eon testhelpers.EonKeyGeneration) {
	ksmAddr := common.HexToAddress(h.config.KeyperSetManagerContractAddress)
	ksmAbi, err := abi.JSON(strings.NewReader(shuttercontracts.KeyperSetManagerABI))
	require.NoError(t, err)
	numKeyperSetsRes, err := ksmAbi.Methods["getNumKeyperSets"].Outputs.PackValues([]any{uint64(1)})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, numKeyperSetsRes)
	activationBlockRes, err := ksmAbi.Methods["getKeyperSetActivationBlock"].Outputs.PackValues([]any{eon.ActivationBlock})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, activationBlockRes)
	eonIndexRes, err := ksmAbi.Methods["getKeyperSetIndexByBlock"].Outputs.PackValues([]any{uint64(eon.EonIndex)})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, eonIndexRes)
	keyperSetAddr := common.HexToAddress("0x000000000000000000000000000000005555")
	keyperSetAddrRes, err := ksmAbi.Methods["getKeyperSetAddress"].Outputs.PackValues([]any{keyperSetAddr})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, keyperSetAddrRes)
	keyperSetAbi, err := abi.JSON(strings.NewReader(shuttercontracts.KeyperSetABI))
	require.NoError(t, err)
	thresholdRes, err := keyperSetAbi.Methods["getThreshold"].Outputs.PackValues([]any{eon.Threshold})
	require.NoError(t, err)
	h.SimulateCallResult(keyperSetAddr, thresholdRes)
	membersRes, err := keyperSetAbi.Methods["getMembers"].Outputs.PackValues([]any{eon.Members()})
	require.NoError(t, err)
	h.SimulateCallResult(keyperSetAddr, membersRes)
	var eonKeyBytes []byte
	err = eon.EonPublicKey.Unmarshal(eonKeyBytes)
	require.NoError(t, err)
	keyBroadcastAbi, err := abi.JSON(strings.NewReader(shuttercontracts.KeyBroadcastContractABI))
	require.NoError(t, err)
	eonKeyRes, err := keyBroadcastAbi.Methods["getEonKey"].Outputs.PackValues([]any{eonKeyBytes})
	require.NoError(t, err)
	h.SimulateCallResult(common.HexToAddress(h.config.KeyBroadcastContractAddress), eonKeyRes)
	activationBlockRes, err = ksmAbi.Methods["getKeyperSetActivationBlock"].Outputs.PackValues([]any{eon.ActivationBlock})
	require.NoError(t, err)
	h.SimulateCallResult(ksmAddr, activationBlockRes)
	finalizedRes, err := keyperSetAbi.Methods["isFinalized"].Outputs.PackValues([]any{true})
	require.NoError(t, err)
	h.SimulateCallResult(keyperSetAddr, finalizedRes)
}

type EmptyTxnProvider struct{}

func (p EmptyTxnProvider) ProvideTxns(_ context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, nil
}

func NewMockContractBackend(ctrl *gomock.Controller) *MockContractBackend {
	return &MockContractBackend{
		MockBackend:       contracts.NewMockBackend(ctrl),
		mockedCallResults: map[common.Address][][]byte{},
		mockedFilterLogs:  map[common.Address][][]types.Log{},
		subs:              map[common.Address][]chan<- types.Log{},
	}
}

type MockContractBackend struct {
	*contracts.MockBackend
	mockedCallResults map[common.Address][][]byte
	mockedFilterLogs  map[common.Address][][]types.Log
	subs              map[common.Address][]chan<- types.Log
}

func (cb *MockContractBackend) PrepareMocks() {
	cb.EXPECT().
		SubscribeFilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, query ethereum.FilterQuery, logs chan<- types.Log) (ethereum.Subscription, error) {
			for _, addr := range query.Addresses {
				cb.subs[addr] = append(cb.subs[addr], logs)
			}
			return MockSubscription{errChan: make(chan error)}, nil
		}).
		AnyTimes()

	cb.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, msg ethereum.CallMsg, b *big.Int) ([]byte, error) {
			results := cb.mockedCallResults[*msg.To]
			if len(results) == 0 {
				return nil, fmt.Errorf("no mocked call result remaining for addr=%s", msg.To)
			}
			res := results[0]
			cb.mockedCallResults[*msg.To] = results[1:]
			return res, nil
		}).
		AnyTimes()

	cb.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			var res []types.Log
			for _, addr := range query.Addresses {
				logs := cb.mockedFilterLogs[addr]
				if len(logs) == 0 {
					return nil, fmt.Errorf("no mocked filter logs for addr=%s", addr)
				}
				res = append(res, logs[0]...)
				cb.mockedFilterLogs[addr] = logs[1:]
			}
			return res, nil
		}).
		AnyTimes()
}

func (cb *MockContractBackend) SimulateCallResult(addr common.Address, result []byte) {
	cb.mockedCallResults[addr] = append(cb.mockedCallResults[addr], result)
}

func (cb *MockContractBackend) SimulateFilterLogs(addr common.Address, logs []types.Log) {
	cb.mockedFilterLogs[addr] = append(cb.mockedFilterLogs[addr], logs)
}

func NewMockStateChangesClient(ctrl *gomock.Controller) *MockStateChangesClient {
	return &MockStateChangesClient{ctrl: ctrl, ch: make(chan MockStateChange)}
}

type MockStateChangesClient struct {
	ctrl *gomock.Controller
	ch   chan MockStateChange
}

func (c MockStateChangesClient) StateChanges(ctx context.Context, _ *remoteproto.StateChangeRequest, _ ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error) {
	mockStream := remoteproto.NewMockKV_StateChangesClient[*remoteproto.StateChange](c.ctrl)
	mockStream.EXPECT().
		Recv().
		DoAndReturn(func() (*remoteproto.StateChangeBatch, error) {
			select {
			case change := <-c.ch:
				return change.batch, change.err
			case <-ctx.Done():
				return nil, io.EOF
			}
		}).
		AnyTimes()
	return mockStream, nil
}

func (c MockStateChangesClient) SimulateStateChange(ctx context.Context, sc MockStateChange) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.ch <- sc:
		return nil
	}
}

type MockStateChange struct {
	batch *remoteproto.StateChangeBatch
	err   error
}

type MockSubscription struct {
	errChan chan error
}

func (m MockSubscription) Unsubscribe() {
	close(m.errChan)
}

func (m MockSubscription) Err() <-chan error {
	return m.errChan
}
