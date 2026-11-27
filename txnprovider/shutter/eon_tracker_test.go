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

package shutter_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	rpccontracts "github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

func TestEonTrackerRecoversBeforeManagerDeployment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker, handle, notify := startEonTracker(t, context.Canceled, func(handle *PoolTestHandle) {
			handle.config.ReorgDepthAwareness = 5
		})
		initial, err := testhelpers.MockEonKeyGeneration(0, 1, 2, 1)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, initial)
		notify(&remoteproto.StateChange{BlockHeight: 100})
		eon, ok := tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, initial.Eon(), eon)

		manager := common.HexToAddress(handle.config.KeyperSetManagerContractAddress)
		handle.SimulateCallResult(manager, nil)
		handle.contractBackend.EXPECT().CodeAt(gomock.Any(), manager, uint256.NewInt(0)).Return(nil, nil)
		notify(&remoteproto.StateChange{})
		_, ok = tracker.CurrentEon()
		require.False(t, ok, "a reset before deployment must clear the current eon")
		_, ok = tracker.RecentEon(initial.EonIndex)
		require.False(t, ok, "eons from the old deployment must not remain cached")

		redeployed, err := testhelpers.MockEonKeyGeneration(0, 1, 2, 2)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, redeployed)
		notify(&remoteproto.StateChange{BlockHeight: 2})
		eon, ok = tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, redeployed.Eon(), eon)

		next, err := testhelpers.MockEonKeyGeneration(1, 1, 2, 3)
		require.NoError(t, err)
		handle.SimulateNewEonRead(t, next)
		notify(&remoteproto.StateChange{BlockHeight: 10})
		handle.SimulateCachedEonRead(t, next)
		notify(&remoteproto.StateChange{BlockHeight: 16})
		_, ok = tracker.RecentEon(redeployed.EonIndex)
		require.False(t, ok, "cleanup must resume without reaching the height of the old deployment")
		_, ok = tracker.RecentEon(next.EonIndex)
		require.True(t, ok)
	})
}

func TestEonTrackerRecoversBeforeFirstKeyperSetActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker, handle, notify := startEonTracker(t, context.Canceled, nil)
		const activationBlock = 10
		initial, err := testhelpers.MockEonKeyGeneration(0, 1, 2, activationBlock)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, initial)
		notify(&remoteproto.StateChange{BlockHeight: activationBlock})
		eon, ok := tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, initial.Eon(), eon)

		manager := common.HexToAddress(handle.config.KeyperSetManagerContractAddress)
		handle.SimulateCallResult(manager, uint256.NewInt(1).PaddedBytes(32))
		handle.SimulateCallResult(manager, uint256.NewInt(activationBlock).PaddedBytes(32))
		notify(&remoteproto.StateChange{BlockHeight: activationBlock - 1, Direction: remoteproto.Direction_UNWIND})
		_, ok = tracker.CurrentEon()
		require.False(t, ok, "the first keyper set is no longer active")
		_, ok = tracker.RecentEon(initial.EonIndex)
		require.False(t, ok, "the old eon must not remain cached")

		replacement, err := testhelpers.MockEonKeyGeneration(0, 1, 2, activationBlock)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, replacement)
		notify(&remoteproto.StateChange{BlockHeight: activationBlock})
		eon, ok = tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, replacement.Eon(), eon)
	})
}

func TestEonTrackerRecoversFromNoActiveKeyperSetRevert(t *testing.T) {
	unwind := &remoteproto.StateChange{BlockHeight: 9, Direction: remoteproto.Direction_UNWIND}
	for _, tc := range []struct {
		name            string
		activationBlock uint64
		changes         []*remoteproto.StateChange
	}{
		{"unwind removes first registration with earlier activation block", 5, []*remoteproto.StateChange{unwind}},
		{"replacement head has no active set", 10, []*remoteproto.StateChange{unwind, {BlockHeight: 11}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				managerABI, err := abi.JSON(strings.NewReader(contracts.KeyperSetManagerABI))
				require.NoError(t, err)
				noActiveSet, ok := managerABI.Errors["NoActiveKeyperSet"]
				require.True(t, ok)
				head := tc.changes[len(tc.changes)-1].BlockHeight
				tracker, handle, notify := startEonTracker(t, context.Canceled, func(handle *PoolTestHandle) {
					expectTrackerCall(t, handle, common.HexToAddress(handle.config.KeyperSetManagerContractAddress),
						managerABI, "getKeyperSetIndexByBlock", head, head).Return(nil, newKeyperSetRevert(noActiveSet.ID[:4]))
				})
				initial, err := testhelpers.MockEonKeyGeneration(0, 1, 2, tc.activationBlock)
				require.NoError(t, err)
				handle.SimulateInitialEonRead(t, initial)
				notify(&remoteproto.StateChange{BlockHeight: 10})
				eon, ok := tracker.CurrentEon()
				require.True(t, ok)
				require.Equal(t, initial.Eon(), eon)

				notify(tc.changes...)
				_, ok = tracker.CurrentEon()
				require.False(t, ok, "the first keyper set is no longer active")
				_, ok = tracker.RecentEon(initial.EonIndex)
				require.False(t, ok, "the old eon must not remain cached")

				replacement, err := testhelpers.MockEonKeyGeneration(0, 1, 2, 12)
				require.NoError(t, err)
				handle.SimulateInitialEonRead(t, replacement)
				notify(&remoteproto.StateChange{BlockHeight: 12})
				eon, ok = tracker.CurrentEon()
				require.True(t, ok)
				require.Equal(t, replacement.Eon(), eon)
			})
		})
	}
}

func TestEonTrackerKeyperSetIndexRevertRemainsFatal(t *testing.T) {
	managerABI, err := abi.JSON(strings.NewReader(contracts.KeyperSetManagerABI))
	require.NoError(t, err)
	noActiveSet, ok := managerABI.Errors["NoActiveKeyperSet"]
	require.True(t, ok)
	stringType, err := abi.NewType("string", "", nil)
	require.NoError(t, err)
	errorMessage, err := (abi.Arguments{{Type: stringType}}).Pack("unexpected keyper set error")
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"trailing data", append(noActiveSet.ID[:4:4], 0)},
		{"Error(string)", append(common.FromHex("0x08c379a0"), errorMessage...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				revertErr := newKeyperSetRevert(tc.data)
				tracker, handle, notify := startEonTracker(t, revertErr, func(handle *PoolTestHandle) {
					expectTrackerCall(t, handle, common.HexToAddress(handle.config.KeyperSetManagerContractAddress),
						managerABI, "getKeyperSetIndexByBlock", 9, uint64(9)).Return(nil, revertErr)
				})
				initial, err := testhelpers.MockEonKeyGeneration(0, 1, 2, 5)
				require.NoError(t, err)
				handle.SimulateInitialEonRead(t, initial)
				notify(&remoteproto.StateChange{BlockHeight: 10})
				_, ok := tracker.CurrentEon()
				require.True(t, ok)

				notify(&remoteproto.StateChange{BlockHeight: 9, Direction: remoteproto.Direction_UNWIND})
			})
		})
	}
}

func TestEonTrackerDropsKeyAfterUnwindBeforeItsBroadcast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker, handle, notify := startEonTracker(t, context.Canceled, func(handle *PoolTestHandle) {
			broadcastABI, err := abi.JSON(strings.NewReader(contracts.KeyBroadcastContractABI))
			require.NoError(t, err)
			emptyKey, err := broadcastABI.Methods["getEonKey"].Outputs.Pack([]byte(nil))
			require.NoError(t, err)
			expectTrackerCall(t, handle, common.HexToAddress(handle.config.KeyBroadcastContractAddress),
				broadcastABI, "getEonKey", 21, uint64(1)).Return(emptyKey, nil).AnyTimes()
		})
		older, err := testhelpers.MockEonKeyGeneration(0, 1, 2, 10)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, older)
		notify(&remoteproto.StateChange{BlockHeight: 10})

		published, err := testhelpers.MockEonKeyGeneration(1, 1, 2, 20)
		require.NoError(t, err)
		handle.SimulateNewEonRead(t, published)
		notify(&remoteproto.StateChange{BlockHeight: 22})

		handle.SimulateCachedEonRead(t, published)
		manager := common.HexToAddress(handle.config.KeyperSetManagerContractAddress)
		handle.SimulateCallResult(manager, uint256.NewInt(published.ActivationBlock).PaddedBytes(32))
		handle.simulateEonState(t, older)
		notify(&remoteproto.StateChange{BlockHeight: 21, Direction: remoteproto.Direction_UNWIND})

		_, ok := tracker.CurrentEon()
		require.False(t, ok, "the active set has no key on the canonical chain")
		_, ok = tracker.RecentEon(published.EonIndex)
		require.False(t, ok, "the rolled-back key must not remain cached")
		_, ok = tracker.EonByBlockNum(21)
		require.False(t, ok, "the older key is not valid for the active set")
		old, ok := tracker.EonByBlockNum(19)
		require.True(t, ok)
		require.Equal(t, older.Eon(), old)

		replacement, err := testhelpers.MockEonKeyGeneration(1, 1, 2, 20)
		require.NoError(t, err)
		handle.SimulateNewEonRead(t, replacement)
		notify(&remoteproto.StateChange{BlockHeight: 22})
		active, ok := tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, replacement.Eon(), active)
	})
}

func TestEonTrackerReloadsKeyAfterReorgAcrossActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker, handle, notify := startEonTracker(t, context.Canceled, nil)
		older, err := testhelpers.MockEonKeyGeneration(0, 1, 2, 5)
		require.NoError(t, err)
		earlier, err := testhelpers.MockEonKeyGeneration(1, 1, 2, 10)
		require.NoError(t, err)
		initial, err := testhelpers.MockEonKeyGeneration(2, 1, 2, 20)
		require.NoError(t, err)

		handle.SimulateInitialEonRead(t, older)
		notify(&remoteproto.StateChange{BlockHeight: 5})
		handle.SimulateNewEonRead(t, earlier)
		notify(&remoteproto.StateChange{BlockHeight: 10})
		handle.SimulateNewEonRead(t, initial)
		notify(&remoteproto.StateChange{BlockHeight: 20})
		eon, ok := tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, initial.Eon(), eon)
		_, ok = tracker.RecentEon(older.EonIndex)
		require.True(t, ok)

		manager := common.HexToAddress(handle.config.KeyperSetManagerContractAddress)
		handle.SimulateCallResult(manager, uint256.NewInt(2).PaddedBytes(32))
		handle.SimulateCallResult(manager, uint256.NewInt(5).PaddedBytes(32))
		handle.SimulateNewEonRead(t, earlier)
		handle.simulateEonState(t, older)
		notify(&remoteproto.StateChange{BlockHeight: 19, Direction: remoteproto.Direction_UNWIND})

		eon, ok = tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, earlier.Eon(), eon)
		preserved, ok := tracker.RecentEon(older.EonIndex)
		require.True(t, ok, "the older key still exists on the canonical chain")
		require.Equal(t, older.Eon(), preserved)
		_, ok = tracker.RecentEon(initial.EonIndex)
		require.False(t, ok, "the rolled-back active eon must be reloaded")

		replacement, err := testhelpers.MockEonKeyGeneration(2, 1, 2, 20)
		require.NoError(t, err)
		handle.SimulateNewEonRead(t, replacement)
		notify(&remoteproto.StateChange{BlockHeight: 20})
		eon, ok = tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, replacement.Eon(), eon)
	})
}

func startEonTracker(t *testing.T, wantErr error, prepare func(*PoolTestHandle)) (*shutter.KsmEonTracker, *PoolTestHandle, func(...*remoteproto.StateChange)) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	logger := log.New()
	handle := &PoolTestHandle{
		config:          shuttercfg.ConfigByChainName(networkname.Chiado),
		contractBackend: NewMockContractBackend(gomock.NewController(t), logger),
	}
	if prepare != nil {
		prepare(handle)
	}
	handle.contractBackend.PrepareMocks()
	batches := make(chan *remoteproto.StateChangeBatch)
	listener := shutter.NewBlockListener(logger, testhelpers.NewStateChangesClientMock(ctx, batches))
	tracker := shutter.NewKsmEonTracker(logger, handle.config, listener, handle.contractBackend)
	eg, runCtx := errgroup.WithContext(ctx)
	eg.Go(func() error { return listener.Run(runCtx) })
	eg.Go(func() error { return tracker.Run(runCtx) })
	t.Cleanup(func() {
		cancel()
		require.ErrorIs(t, eg.Wait(), wantErr)
	})
	synctest.Wait()
	return tracker, handle, func(changes ...*remoteproto.StateChange) {
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: changes}
		synctest.Wait()
	}
}

func newKeyperSetRevert(data []byte) error {
	return ethapi.NewRevertError(&evmtypes.ExecutionResult{Reverted: true, ReturnData: data})
}

func expectTrackerCall(t *testing.T, handle *PoolTestHandle, address common.Address, contractABI abi.ABI, method string, block uint64, args ...any) *rpccontracts.MockBackendCallContractCall {
	t.Helper()
	input, err := contractABI.Pack(method, args...)
	require.NoError(t, err)
	return handle.contractBackend.EXPECT().CallContract(gomock.Any(), gomock.Cond(func(msg bind.CallMsg) bool {
		return msg.To != nil && *msg.To == address && bytes.Equal(msg.Data, input)
	}), uint256.NewInt(block))
}
