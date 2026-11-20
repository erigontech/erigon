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
	"errors"
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
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

func TestEonTrackerRecoversBeforeManagerDeployment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		logger := log.New()
		backend := NewMockContractBackend(gomock.NewController(t), logger)
		backend.PrepareMocks()
		config := shuttercfg.ConfigByChainName(networkname.Chiado)
		config.ReorgDepthAwareness = 5
		batches := make(chan *remoteproto.StateChangeBatch)
		listener := shutter.NewBlockListener(logger, testhelpers.NewStateChangesClientMock(ctx, batches))
		tracker := shutter.NewKsmEonTracker(logger, config, listener, backend)
		eg, runCtx := errgroup.WithContext(ctx)
		eg.Go(func() error { return listener.Run(runCtx) })
		eg.Go(func() error { return tracker.Run(runCtx) })
		defer func() {
			cancel()
			require.ErrorIs(t, eg.Wait(), context.Canceled)
		}()
		synctest.Wait()

		handle := PoolTestHandle{config: config, contractBackend: backend}
		initial, err := testhelpers.MockEonKeyGeneration(0, 1, 2, 1)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, initial)
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 100}}}
		synctest.Wait()
		eon, ok := tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, initial.Eon(), eon)

		manager := common.HexToAddress(config.KeyperSetManagerContractAddress)
		handle.SimulateCallResult(manager, nil)
		backend.EXPECT().CodeAt(gomock.Any(), manager, uint256.NewInt(0)).Return(nil, nil)
		batches <- &remoteproto.StateChangeBatch{
			StateChangesIncomplete: true,
			ChangeBatch:            []*remoteproto.StateChange{{}},
		}
		synctest.Wait()
		_, ok = tracker.CurrentEon()
		require.False(t, ok, "a reset before deployment must clear the current eon")
		_, ok = tracker.RecentEon(initial.EonIndex)
		require.False(t, ok, "eons from the old deployment must not remain cached")

		redeployed, err := testhelpers.MockEonKeyGeneration(0, 1, 2, 2)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, redeployed)
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 2}}}
		synctest.Wait()
		eon, ok = tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, redeployed.Eon(), eon)

		next, err := testhelpers.MockEonKeyGeneration(1, 1, 2, 3)
		require.NoError(t, err)
		handle.SimulateNewEonRead(t, next)
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 10}}}
		synctest.Wait()
		handle.SimulateCachedEonRead(t, next)
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 16}}}
		synctest.Wait()
		_, ok = tracker.RecentEon(redeployed.EonIndex)
		require.False(t, ok, "cleanup must resume without reaching the height of the old deployment")
		_, ok = tracker.RecentEon(next.EonIndex)
		require.True(t, ok)
	})
}

func TestEonTrackerRecoversBeforeFirstKeyperSetActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		logger := log.New()
		backend := NewMockContractBackend(gomock.NewController(t), logger)
		config := shuttercfg.ConfigByChainName(networkname.Chiado)
		manager := common.HexToAddress(config.KeyperSetManagerContractAddress)
		const activationBlock = 10
		managerABI, err := abi.JSON(strings.NewReader(contracts.KeyperSetManagerABI))
		require.NoError(t, err)
		indexCall, err := managerABI.Pack("getKeyperSetIndexByBlock", uint64(activationBlock-1))
		require.NoError(t, err)
		backend.EXPECT().CallContract(gomock.Any(), gomock.Cond(func(msg bind.CallMsg) bool {
			return msg.To != nil && *msg.To == manager && bytes.Equal(msg.Data, indexCall)
		}), uint256.NewInt(activationBlock-1)).Return(nil, errors.New("execution reverted")).AnyTimes()
		backend.PrepareMocks()
		batches := make(chan *remoteproto.StateChangeBatch)
		listener := shutter.NewBlockListener(logger, testhelpers.NewStateChangesClientMock(ctx, batches))
		tracker := shutter.NewKsmEonTracker(logger, config, listener, backend)
		eg, runCtx := errgroup.WithContext(ctx)
		eg.Go(func() error { return listener.Run(runCtx) })
		eg.Go(func() error { return tracker.Run(runCtx) })
		defer func() {
			cancel()
			require.ErrorIs(t, eg.Wait(), context.Canceled)
		}()
		synctest.Wait()

		handle := PoolTestHandle{config: config, contractBackend: backend}
		initial, err := testhelpers.MockEonKeyGeneration(0, 1, 2, activationBlock)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, initial)
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: activationBlock}}}
		synctest.Wait()
		eon, ok := tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, initial.Eon(), eon)

		handle.SimulateCallResult(manager, uint256.NewInt(1).PaddedBytes(32))
		handle.SimulateCallResult(manager, uint256.NewInt(activationBlock).PaddedBytes(32))
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: activationBlock - 1,
			Direction:   remoteproto.Direction_UNWIND,
		}}}
		synctest.Wait()
		_, ok = tracker.CurrentEon()
		require.False(t, ok, "the first keyper set is no longer active")
		_, ok = tracker.RecentEon(initial.EonIndex)
		require.False(t, ok, "the old eon must not remain cached")

		replacement, err := testhelpers.MockEonKeyGeneration(0, 1, 2, activationBlock)
		require.NoError(t, err)
		handle.SimulateInitialEonRead(t, replacement)
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: activationBlock}}}
		synctest.Wait()
		eon, ok = tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, replacement.Eon(), eon)
	})
}

func TestEonTrackerKeyperSetIndexRevert(t *testing.T) {
	unwind := &remoteproto.StateChange{BlockHeight: 9, Direction: remoteproto.Direction_UNWIND}
	for _, tc := range []struct {
		name            string
		activationBlock uint64
		changes         []*remoteproto.StateChange
		extraRevertData []byte
	}{
		{
			name:            "unwind removes first registration with backdated activation",
			activationBlock: 5,
			changes:         []*remoteproto.StateChange{unwind},
		},
		{
			name:            "replacement head has no active set",
			activationBlock: 10,
			changes:         []*remoteproto.StateChange{unwind, {BlockHeight: 10}},
		},
		{
			name:            "revert payload has trailing data",
			activationBlock: 5,
			changes:         []*remoteproto.StateChange{unwind},
			extraRevertData: []byte{0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				logger := log.New()
				backend := NewMockContractBackend(gomock.NewController(t), logger)
				config := shuttercfg.ConfigByChainName(networkname.Chiado)
				manager := common.HexToAddress(config.KeyperSetManagerContractAddress)
				managerABI, err := abi.JSON(strings.NewReader(contracts.KeyperSetManagerABI))
				require.NoError(t, err)
				noActiveSet, ok := managerABI.Errors["NoActiveKeyperSet"]
				require.True(t, ok)
				revertErr := ethapi.NewRevertError(&evmtypes.ExecutionResult{
					Reverted:   true,
					ReturnData: append(noActiveSet.ID[:4:4], tc.extraRevertData...),
				})
				head := tc.changes[len(tc.changes)-1].BlockHeight
				indexCall, err := managerABI.Pack("getKeyperSetIndexByBlock", head)
				require.NoError(t, err)
				var reorgStarted bool
				indexMsg := gomock.Cond(func(msg bind.CallMsg) bool {
					return reorgStarted && msg.To != nil && *msg.To == manager && bytes.Equal(msg.Data, indexCall)
				})
				backend.EXPECT().CallContract(gomock.Any(), indexMsg, uint256.NewInt(head)).Return(nil, revertErr)
				backend.PrepareMocks()
				batches := make(chan *remoteproto.StateChangeBatch)
				listener := shutter.NewBlockListener(logger, testhelpers.NewStateChangesClientMock(ctx, batches))
				tracker := shutter.NewKsmEonTracker(logger, config, listener, backend)
				eg, runCtx := errgroup.WithContext(ctx)
				eg.Go(func() error { return listener.Run(runCtx) })
				eg.Go(func() error { return tracker.Run(runCtx) })
				defer func() {
					cancel()
					_ = eg.Wait()
				}()
				synctest.Wait()

				handle := PoolTestHandle{config: config, contractBackend: backend}
				initial, err := testhelpers.MockEonKeyGeneration(0, 1, 2, tc.activationBlock)
				require.NoError(t, err)
				handle.SimulateInitialEonRead(t, initial)
				batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 10}}}
				synctest.Wait()
				eon, ok := tracker.CurrentEon()
				require.True(t, ok)
				require.Equal(t, initial.Eon(), eon)

				reorgStarted = true
				batches <- &remoteproto.StateChangeBatch{ChangeBatch: tc.changes}
				synctest.Wait()
				if len(tc.extraRevertData) != 0 {
					cancel()
					require.ErrorIs(t, eg.Wait(), revertErr)
					return
				}
				_, ok = tracker.CurrentEon()
				require.False(t, ok, "the first keyper set is no longer active")
				_, ok = tracker.RecentEon(initial.EonIndex)
				require.False(t, ok, "the old eon must not remain cached")

				replacement, err := testhelpers.MockEonKeyGeneration(0, 1, 2, 11)
				require.NoError(t, err)
				handle.SimulateInitialEonRead(t, replacement)
				batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 11}}}
				synctest.Wait()
				eon, ok = tracker.CurrentEon()
				require.True(t, ok)
				require.Equal(t, replacement.Eon(), eon)

				cancel()
				require.ErrorIs(t, eg.Wait(), context.Canceled)
			})
		})
	}
}

func TestEonTrackerReloadsKeyAfterReorgAcrossActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		logger := log.New()
		backend := NewMockContractBackend(gomock.NewController(t), logger)
		backend.PrepareMocks()
		config := shuttercfg.ConfigByChainName(networkname.Chiado)
		batches := make(chan *remoteproto.StateChangeBatch)
		listener := shutter.NewBlockListener(logger, testhelpers.NewStateChangesClientMock(ctx, batches))
		tracker := shutter.NewKsmEonTracker(logger, config, listener, backend)
		eg, runCtx := errgroup.WithContext(ctx)
		eg.Go(func() error { return listener.Run(runCtx) })
		eg.Go(func() error { return tracker.Run(runCtx) })
		defer func() {
			cancel()
			require.ErrorIs(t, eg.Wait(), context.Canceled)
		}()
		synctest.Wait()

		handle := PoolTestHandle{config: config, contractBackend: backend}
		manager := common.HexToAddress(config.KeyperSetManagerContractAddress)
		earlier, err := testhelpers.MockEonKeyGeneration(1, 1, 2, 10)
		require.NoError(t, err)
		handle.SimulateCallResult(manager, uint256.NewInt(3).PaddedBytes(32))
		handle.SimulateCallResult(manager, uint256.NewInt(5).PaddedBytes(32))
		handle.SimulateNewEonRead(t, earlier)
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 10}}}
		synctest.Wait()
		eon, ok := tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, earlier.Eon(), eon)

		initial, err := testhelpers.MockEonKeyGeneration(2, 1, 2, 20)
		require.NoError(t, err)
		handle.SimulateNewEonRead(t, initial)
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 20}}}
		synctest.Wait()
		eon, ok = tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, initial.Eon(), eon)

		handle.SimulateCallResult(manager, uint256.NewInt(3).PaddedBytes(32))
		handle.SimulateCallResult(manager, uint256.NewInt(5).PaddedBytes(32))
		handle.SimulateNewEonRead(t, earlier)
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: 19,
			Direction:   remoteproto.Direction_UNWIND,
		}}}
		synctest.Wait()
		eon, ok = tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, earlier.Eon(), eon)
		_, ok = tracker.RecentEon(initial.EonIndex)
		require.False(t, ok, "the future eon must not remain cached after a reorg")

		replacement, err := testhelpers.MockEonKeyGeneration(2, 1, 2, 20)
		require.NoError(t, err)
		handle.SimulateNewEonRead(t, replacement)
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 20}}}
		synctest.Wait()
		eon, ok = tracker.CurrentEon()
		require.True(t, ok)
		require.Equal(t, replacement.Eon(), eon)
	})
}
