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

package sync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/polygon/heimdall"
)

type captureNewBlockHashesRegistrar struct {
	captured chan event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]]
}

func (captureNewBlockHashesRegistrar) RegisterNewBlockObserver(event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockPacket]]) event.UnregisterFunc {
	return func() {}
}

func (c captureNewBlockHashesRegistrar) RegisterNewBlockHashesObserver(o event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]]) event.UnregisterFunc {
	c.captured <- o
	return func() {}
}

type noopHeimdallRegistrar struct{}

func (noopHeimdallRegistrar) RegisterMilestoneObserver(func(*heimdall.Milestone), ...heimdall.ObserverOption) event.UnregisterFunc {
	return func() {}
}

type noopMinedBlockRegistrar struct{}

func (noopMinedBlockRegistrar) RegisterMinedBlockObserver(func(*types.Block)) event.UnregisterFunc {
	return func() {}
}

func runTipEventsCapturingNewBlockHashesObserver(t *testing.T) (event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]], *TipEvents) {
	t.Helper()
	reg := captureNewBlockHashesRegistrar{captured: make(chan event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]], 1)}
	te := NewTipEvents(testlog.Logger(t, log.LvlCrit), reg, noopHeimdallRegistrar{}, noopMinedBlockRegistrar{})
	ctx, cancel := context.WithCancel(t.Context())
	eg := errgroup.Group{}
	eg.Go(func() error { return te.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	})

	select {
	case obs := <-reg.captured:
		return obs, te
	case <-time.After(5 * time.Second):
		t.Fatal("NewBlockHashes observer was not registered")
		return nil, nil
	}
}

func TestTipEventsEmptyNewBlockHashesDoesNotPanic(t *testing.T) {
	t.Parallel()
	obs, _ := runTipEventsCapturingNewBlockHashesObserver(t)

	require.NotPanics(t, func() {
		obs(&p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]{
			Decoded: &eth.NewBlockHashesPacket{},
			PeerId:  p2p.PeerIdFromUint64(1),
		})
	})
}

func TestTipEventsNewBlockHashesEmitsEvent(t *testing.T) {
	t.Parallel()
	obs, te := runTipEventsCapturingNewBlockHashesObserver(t)

	obs(&p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]{
		Decoded: &eth.NewBlockHashesPacket{{Hash: common.HexToHash("0x1"), Number: 1}},
		PeerId:  p2p.PeerIdFromUint64(1),
	})

	readCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.Equal(t, EventTypeNewBlockHashes, read(readCtx, t, te.Events()).Type)
}
