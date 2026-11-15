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
	"context"
	"testing"
	"testing/synctest"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/shutter"
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
		batches <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 1}}}
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
	})
}
