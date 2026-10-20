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

package bscsync

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func TestBlockEventsSpamGuard(t *testing.T) {
	t.Parallel()
	logger := testlog.Logger(t, log.LvlCrit)
	sg := newBlockEventsSpamGuard(logger)
	peerId1 := p2p.PeerIdFromUint64(1)
	peerId2 := p2p.PeerIdFromUint64(2)
	require.False(t, sg.Spam(peerId1, common.HexToHash("0x0"), 1))
	require.True(t, sg.Spam(peerId1, common.HexToHash("0x0"), 1))
	require.True(t, sg.Spam(peerId1, common.HexToHash("0x0"), 1))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x0"), 1))
	require.True(t, sg.Spam(peerId2, common.HexToHash("0x0"), 1))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x1"), 1))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x0"), 2))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x2"), 2))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x3"), 3))
}

func TestAnnouncedHeads(t *testing.T) {
	t.Parallel()
	h := newAnnouncedHeads()
	_, ok := h.Head()
	require.False(t, ok)

	h.Update(p2p.PeerIdFromUint64(1), 100)
	head, ok := h.Head()
	require.True(t, ok)
	require.Equal(t, uint64(100), head)

	h.Update(p2p.PeerIdFromUint64(2), 90)
	head, _ = h.Head()
	require.Equal(t, uint64(90), head)

	h.Update(p2p.PeerIdFromUint64(3), 1_000_000)
	head, _ = h.Head()
	require.Equal(t, uint64(100), head, "one peer cannot raise the head alone")

	h.Update(p2p.PeerIdFromUint64(1), 50)
	head, _ = h.Head()
	require.Equal(t, uint64(100), head, "a peer's head never goes down")

	h.Remove(p2p.PeerIdFromUint64(3))
	head, _ = h.Head()
	require.Equal(t, uint64(90), head)

	h.Remove(p2p.PeerIdFromUint64(1))
	h.Remove(p2p.PeerIdFromUint64(2))
	_, ok = h.Head()
	require.False(t, ok)
}

type fakeTipRegistrar struct {
	newBlock       event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockPacket]]
	newBlockHashes event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]]
	peerEvent      event.Observer[*sentryproto.PeerEvent]
	registered     chan struct{}
}

func newFakeTipRegistrar() *fakeTipRegistrar {
	return &fakeTipRegistrar{registered: make(chan struct{}, 3)}
}

func (f *fakeTipRegistrar) RegisterNewBlockObserver(o event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockPacket]]) event.UnregisterFunc {
	f.newBlock = o
	f.registered <- struct{}{}
	return func() {}
}

func (f *fakeTipRegistrar) RegisterNewBlockHashesObserver(o event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]]) event.UnregisterFunc {
	f.newBlockHashes = o
	f.registered <- struct{}{}
	return func() {}
}

func (f *fakeTipRegistrar) RegisterPeerEventObserver(o event.Observer[*sentryproto.PeerEvent], _ ...p2p.RegisterOpt) event.UnregisterFunc {
	f.peerEvent = o
	f.registered <- struct{}{}
	return func() {}
}

func (f *fakeTipRegistrar) sendNewBlock(peer uint64, num uint64) *types.Block {
	b := types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(num), Extra: []byte{byte(peer)}}, nil)
	f.newBlock(&p2p.DecodedInboundMessage[*eth.NewBlockPacket]{
		Decoded: &eth.NewBlockPacket{Block: b},
		PeerId:  p2p.PeerIdFromUint64(peer),
	})
	return b
}

func (f *fakeTipRegistrar) sendNewBlockHashes(peer uint64, nums ...uint64) {
	var pkt eth.NewBlockHashesPacket
	for _, n := range nums {
		pkt = append(pkt, struct {
			Hash   common.Hash
			Number uint64
		}{Hash: common.Hash{byte(n >> 8), byte(n)}, Number: n})
	}
	f.newBlockHashes(&p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]{
		Decoded: &pkt,
		PeerId:  p2p.PeerIdFromUint64(peer),
	})
}

func runTipEvents(t *testing.T) (*TipEvents, *fakeTipRegistrar) {
	reg := newFakeTipRegistrar()
	te := NewTipEvents(testlog.Logger(t, log.LvlCrit), reg)
	ctx, cancel := context.WithCancel(t.Context())
	eg := errgroup.Group{}
	eg.Go(func() error { return te.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	})
	for range 3 {
		<-reg.registered
	}
	return te, reg
}

func TestTipEventsFromP2P(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		te, reg := runTipEvents(t)

		b := reg.sendNewBlock(1, 10)
		reg.sendNewBlock(1, 10)
		reg.sendNewBlockHashes(2)
		reg.sendNewBlockHashes(2, 11, 12)
		reg.sendNewBlockHashes(2, 11, 12)

		e := <-te.Events()
		require.Equal(t, EventTypeNewBlock, e.Type)
		require.Equal(t, b.Hash(), e.AsNewBlock().NewBlock.Hash())
		require.Equal(t, EventSourceP2PNewBlock, e.AsNewBlock().Source)
		require.Equal(t, p2p.PeerIdFromUint64(1), e.AsNewBlock().PeerId)

		e = <-te.Events()
		require.Equal(t, EventTypeNewBlockHashes, e.Type)
		require.Len(t, e.AsNewBlockHashes().NewBlockHashes, 2)

		synctest.Wait()
		require.Empty(t, te.Events())
	})
}

func TestTipEventsAnnouncedHead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		te, reg := runTipEvents(t)

		reg.sendNewBlock(1, 100)
		reg.sendNewBlockHashes(2, 98, 99)
		reg.sendNewBlockHashes(3, 500)
		head, ok := te.AnnouncedHead()
		require.True(t, ok)
		require.Equal(t, uint64(100), head)

		reg.peerEvent(&sentryproto.PeerEvent{PeerId: p2p.PeerIdFromUint64(1).H512(), EventId: sentryproto.PeerEvent_Disconnect})
		head, _ = te.AnnouncedHead()
		require.Equal(t, uint64(99), head)
	})
}

func TestTipEventsPushBatch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		te, _ := runTipEvents(t)
		te.PushEvent(Event{Type: EventTypeNewBlockBatch})
		require.Equal(t, EventTypeNewBlockBatch, (<-te.Events()).Type)
	})
}
