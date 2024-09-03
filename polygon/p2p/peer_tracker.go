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
	"sync"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type PeerTracker interface {
	Run(ctx context.Context) error
	ListPeersMayHaveBlockNum(blockNum uint64) []*PeerId
	BlockNumPresent(peerId *PeerId, blockNum uint64)
	BlockNumMissing(peerId *PeerId, blockNum uint64)
	PeerConnected(peerId *PeerId)
	PeerDisconnected(peerId *PeerId)
}

func NewPeerTracker(
	logger log.Logger,
	peerProvider peerProvider,
	peerEventRegistrar peerEventRegistrar,
	opts ...PeerTrackerOption,
) PeerTracker {
	pt := &peerTracker{
		logger:             logger,
		peerProvider:       peerProvider,
		peerEventRegistrar: peerEventRegistrar,
		peerSyncProgresses: map[PeerId]*peerSyncProgress{},
		peerShuffle:        RandPeerShuffle,
	}

	for _, opt := range opts {
		opt(pt)
	}

	return pt
}

type peerTracker struct {
	logger             log.Logger
	peerProvider       peerProvider
	peerEventRegistrar peerEventRegistrar
	mu                 sync.Mutex
	peerSyncProgresses map[PeerId]*peerSyncProgress
	peerShuffle        PeerShuffle
}

func (pt *peerTracker) Run(ctx context.Context) error {
	pt.logger.Debug(peerTrackerLogPrefix("running peer tracker component"))

	var unregister polygoncommon.UnregisterFunc
	defer func() { unregister() }()

	err := func() error {
		// we lock the pt for updates so that we:
		//   1. register the observer but buffer the updates coming from it until we do 2.
		//   2. replay the current state of connected peers
		pt.mu.Lock()
		defer pt.mu.Unlock()

		// 1. register the observer
		unregister = pt.peerEventRegistrar.RegisterPeerEventObserver(NewPeerEventObserver(pt))

		// 2. replay the current state of connected peers
		reply, err := pt.peerProvider.Peers(ctx, &emptypb.Empty{})
		if err != nil {
			return err
		}

		for _, peer := range reply.Peers {
			peerId, err := PeerIdFromEnode(peer.Enode)
			if err != nil {
				return err
			}

			pt.peerConnected(peerId)
		}

		pt.logger.Debug(peerTrackerLogPrefix("replayed current state of connected peers"), "count", len(reply.Peers))
		return nil
	}()
	if err != nil {
		return err
	}

	<-ctx.Done()
	return ctx.Err()
}

func (pt *peerTracker) ListPeersMayHaveBlockNum(blockNum uint64) []*PeerId {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	var peerIds []*PeerId
	for _, peerSyncProgress := range pt.peerSyncProgresses {
		if peerSyncProgress.peerMayHaveBlockNum(blockNum) {
			peerIds = append(peerIds, peerSyncProgress.peerId)
		}
	}

	pt.peerShuffle(peerIds)

	return peerIds
}

func (pt *peerTracker) BlockNumPresent(peerId *PeerId, blockNum uint64) {
	pt.updatePeerSyncProgress(peerId, func(psp *peerSyncProgress) {
		psp.blockNumPresent(blockNum)
	})
}

func (pt *peerTracker) BlockNumMissing(peerId *PeerId, blockNum uint64) {
	pt.updatePeerSyncProgress(peerId, func(psp *peerSyncProgress) {
		psp.blockNumMissing(blockNum)
	})
}

func (pt *peerTracker) PeerDisconnected(peerId *PeerId) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.logger.Debug(peerTrackerLogPrefix("peer disconnected"), "peerId", peerId.String())
	delete(pt.peerSyncProgresses, *peerId)
}

func (pt *peerTracker) PeerConnected(peerId *PeerId) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.peerConnected(peerId)
}

func (pt *peerTracker) peerConnected(peerId *PeerId) {
	pt.logger.Debug(peerTrackerLogPrefix("peer connected"), "peerId", peerId.String())

	peerIdVal := *peerId
	if _, ok := pt.peerSyncProgresses[peerIdVal]; !ok {
		pt.peerSyncProgresses[peerIdVal] = &peerSyncProgress{
			peerId: peerId,
		}
	}
}

func (pt *peerTracker) updatePeerSyncProgress(peerId *PeerId, update func(psp *peerSyncProgress)) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	peerSyncProgress, ok := pt.peerSyncProgresses[*peerId]
	if !ok {
		return
	}

	update(peerSyncProgress)
}

func NewPeerEventObserver(peerTracker PeerTracker) polygoncommon.Observer[*sentryproto.PeerEvent] {
	return func(message *sentryproto.PeerEvent) {
		peerId := PeerIdFromH512(message.PeerId)
		switch message.EventId {
		case sentryproto.PeerEvent_Connect:
			peerTracker.PeerConnected(peerId)
		case sentryproto.PeerEvent_Disconnect:
			peerTracker.PeerDisconnected(peerId)
		}
	}
}

func peerTrackerLogPrefix(message string) string {
	return "[p2p.peerTracker] " + message
}
