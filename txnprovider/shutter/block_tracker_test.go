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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
)

func TestBlockTracker(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := testlog.Logger(t, log.LvlTrace)
	recvC := make(chan *remoteproto.StateChangeBatch)
	bl := shutter.NewBlockListener(logger, testhelpers.NewStateChangesClientMock(ctx, recvC))
	btInitialisationWg := &sync.WaitGroup{}
	btInitialisationWg.Add(1)
	bnReader := func(ctx context.Context) (*uint64, error) {
		start := uint64(10)
		btInitialisationWg.Done()
		return &start, nil
	}
	bt := shutter.NewBlockTracker(logger, bl, bnReader)
	eg, egCtx := errgroup.WithContext(ctx)
	defer func(eg *errgroup.Group) {
		cancel()
		err := eg.Wait()
		require.ErrorIs(t, err, context.Canceled)
	}(eg)
	eg.Go(func() error { return bl.Run(egCtx) })
	eg.Go(func() error { return bt.Run(egCtx) })
	btInitialisationWg.Wait() // This ensures the block tracker has been initialised i.e. it observes the block listener

	waitCtx1, waitCtxCancel1 := context.WithTimeout(ctx, 50*time.Millisecond)
	defer waitCtxCancel1()
	err := bt.Wait(waitCtx1, 15)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	recvC <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 15}}}
	waitCtx2, waitCtxCancel2 := context.WithTimeout(ctx, time.Minute)
	defer waitCtxCancel2()
	err = bt.Wait(waitCtx2, 15)
	require.NoError(t, err)
}
