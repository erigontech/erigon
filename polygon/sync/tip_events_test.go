// Copyright 2024 The Erigon Authors
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

	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/p2p"
	"github.com/erigontech/erigon/turbo/testlog"
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

func TestTipEventsCompositeChannel(t *testing.T) {
	t.Parallel()

	heimdallEvents := NewEventChannel[Event](3)
	p2pEvents := NewEventChannel[Event](2)
	ch := NewTipEventsCompositeChannel(heimdallEvents, p2pEvents)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		err := ch.Run(ctx)
		println(err)
		return err
	})

	ch.PushEvent(Event{Type: EventTypeNewMilestone})
	ch.PushEvent(Event{Type: EventTypeNewBlockHashes}) // should be dropped due to the following 2 events
	ch.PushEvent(Event{Type: EventTypeNewBlock})
	ch.PushEvent(Event{Type: EventTypeNewBlockHashes})

	events := make([]EventType, 3)
	events[0] = read(ctx, t, ch.Events()).Type
	events[1] = read(ctx, t, ch.Events()).Type
	events[2] = read(ctx, t, ch.Events()).Type
	require.ElementsMatch(t, events, []EventType{EventTypeNewMilestone, EventTypeNewBlock, EventTypeNewBlockHashes})
	require.Len(t, ch.heimdallEventsChannel.events, 0)
	require.Len(t, ch.p2pEventsChannel.events, 0)
	cancel()
	err := eg.Wait()
	require.ErrorIs(t, err, context.Canceled)
}

func read(ctx context.Context, t *testing.T, ch <-chan Event) Event {
	select {
	case e := <-ch:
		return e
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for event")
		return Event{}
	}
}
