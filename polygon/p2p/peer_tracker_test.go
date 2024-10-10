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

package p2p

import (
	"context"
	"encoding/binary"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/polygoncommon"
	"github.com/erigontech/erigon/turbo/testlog"
)

func TestPeerTracker(t *testing.T) {
	t.Parallel()

	test := newPeerTrackerTest(t)
	peerTracker := test.peerTracker
	peerIds := peerTracker.ListPeersMayHaveBlockNum(100)
	require.Len(t, peerIds, 0)

	peerTracker.PeerConnected(PeerIdFromUint64(1))
	peerTracker.PeerConnected(PeerIdFromUint64(2))
	peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
	require.Len(t, peerIds, 2)
	sortPeerIdsAssumingUints(peerIds)
	require.Equal(t, PeerIdFromUint64(1), peerIds[0])
	require.Equal(t, PeerIdFromUint64(2), peerIds[1])

	peerTracker.BlockNumMissing(PeerIdFromUint64(1), 50)
	peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
	require.Len(t, peerIds, 1)
	require.Equal(t, PeerIdFromUint64(2), peerIds[0])

	peerTracker.BlockNumPresent(PeerIdFromUint64(1), 100)
	peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
	require.Len(t, peerIds, 2)
	sortPeerIdsAssumingUints(peerIds)
	require.Equal(t, PeerIdFromUint64(1), peerIds[0])
	require.Equal(t, PeerIdFromUint64(2), peerIds[1])

	peerTracker.PeerDisconnected(PeerIdFromUint64(2))
	peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
	require.Len(t, peerIds, 1)
	require.Equal(t, PeerIdFromUint64(1), peerIds[0])
}

func TestPeerTrackerPeerEventObserver(t *testing.T) {
	t.Parallel()

	alreadyConnectedPeerEnode := "enode://c87922d094c326ca660248b48cf1aa6d0ec6eb3a572c5cb64152008c1e3c8d67f5d9b66df427883aae6b01383c8cc56027eaf7e6062c9b191663076ad397b1e5@194.233.65.96:30303"
	alreadyConnectedPeerId, err := PeerIdFromEnode(alreadyConnectedPeerEnode)
	require.NoError(t, err)
	peerEventsStream := make(chan *sentryproto.PeerEvent)
	test := newPeerTrackerTest(t)
	test.mockPeerProvider(&sentryproto.PeersReply{
		Peers: []*typesproto.PeerInfo{
			{
				Enode: alreadyConnectedPeerEnode,
			},
		},
	})
	test.mockPeerEvents(peerEventsStream)
	peerTracker := test.peerTracker
	test.run(func(ctx context.Context, t *testing.T) {
		peerEventsStream <- &sentryproto.PeerEvent{
			PeerId:  PeerIdFromUint64(1).H512(),
			EventId: sentryproto.PeerEvent_Connect,
		}

		peerEventsStream <- &sentryproto.PeerEvent{
			PeerId:  PeerIdFromUint64(2).H512(),
			EventId: sentryproto.PeerEvent_Connect,
		}

		var peerIds []*PeerId
		waitCond := func(wantPeerIdsLen int) func() bool {
			return func() bool {
				peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
				return len(peerIds) == wantPeerIdsLen
			}
		}
		require.Eventually(t, waitCond(3), time.Second, 5*time.Millisecond)
		require.Len(t, peerIds, 3)
		sortPeerIdsAssumingUints(peerIds)
		require.Equal(t, PeerIdFromUint64(1), peerIds[0])
		require.Equal(t, PeerIdFromUint64(2), peerIds[1])
		require.Equal(t, alreadyConnectedPeerId, peerIds[2])

		peerEventsStream <- &sentryproto.PeerEvent{
			PeerId:  PeerIdFromUint64(1).H512(),
			EventId: sentryproto.PeerEvent_Disconnect,
		}

		peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
		require.Eventually(t, waitCond(2), time.Second, 5*time.Millisecond)
		require.Len(t, peerIds, 2)
		sortPeerIdsAssumingUints(peerIds)
		require.Equal(t, PeerIdFromUint64(2), peerIds[0])
		require.Equal(t, alreadyConnectedPeerId, peerIds[1])
	})
}

//
// TODO test block hashes
//

func newPeerTrackerTest(t *testing.T) *peerTrackerTest {
	ctx, cancel := context.WithCancel(context.Background())
	logger := testlog.Logger(t, log.LvlCrit)
	ctrl := gomock.NewController(t)
	peerProvider := NewMockpeerProvider(ctrl)
	peerEventRegistrar := NewMockpeerEventRegistrar(ctrl)
	peerTracker := NewPeerTracker(logger, peerProvider, peerEventRegistrar, WithPreservingPeerShuffle)
	return &peerTrackerTest{
		ctx:                ctx,
		ctxCancel:          cancel,
		t:                  t,
		peerTracker:        peerTracker,
		peerProvider:       peerProvider,
		peerEventRegistrar: peerEventRegistrar,
	}
}

type peerTrackerTest struct {
	ctx                context.Context
	ctxCancel          context.CancelFunc
	t                  *testing.T
	peerTracker        PeerTracker
	peerProvider       *MockpeerProvider
	peerEventRegistrar *MockpeerEventRegistrar
}

func (ptt *peerTrackerTest) mockPeerProvider(peerReply *sentryproto.PeersReply) {
	ptt.peerProvider.EXPECT().
		Peers(gomock.Any(), gomock.Any()).
		Return(peerReply, nil).
		Times(1)
}

func (ptt *peerTrackerTest) mockPeerEvents(eventStream <-chan *sentryproto.PeerEvent) {
	ptt.peerEventRegistrar.EXPECT().
		RegisterPeerEventObserver(gomock.Any()).
		DoAndReturn(func(observer polygoncommon.Observer[*sentryproto.PeerEvent]) UnregisterFunc {
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case event := <-eventStream:
						observer(event)
					}
				}
			}()

			return UnregisterFunc(cancel)
		}).
		Times(1)
}

func (ptt *peerTrackerTest) run(f func(ctx context.Context, t *testing.T)) {
	var done atomic.Bool
	ptt.t.Run("start", func(t *testing.T) {
		go func() {
			defer done.Store(true)
			err := ptt.peerTracker.Run(ptt.ctx)
			require.ErrorIs(t, err, context.Canceled)
		}()
	})

	ptt.t.Run("test", func(t *testing.T) {
		f(ptt.ctx, t)
	})

	ptt.t.Run("stop", func(t *testing.T) {
		ptt.ctxCancel()
		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

// sortPeerIdsAssumingUints is a hacky way for us to sort peer ids in tests - assuming they are all created
// by using PeerIdFromUint64 and so uint64 value is sorted in first 8 bytes. Sorting is needed since
// ListPeersMayHaveBlockNum returns peer ids from a map and order is non-deterministic.
func sortPeerIdsAssumingUints(peerIds []*PeerId) {
	sort.Slice(peerIds, func(i, j int) bool {
		bytesI := peerIds[i][:8]
		bytesJ := peerIds[j][:8]
		numI := binary.BigEndian.Uint64(bytesI)
		numJ := binary.BigEndian.Uint64(bytesJ)
		return numI < numJ
	})
}
