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
	"sync"

	"github.com/erigontech/erigon-lib/log/v3"

	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type PeerTracker interface {
	ListPeersMayHaveBlockNum(blockNum uint64) []*PeerId
	BlockNumPresent(peerId *PeerId, blockNum uint64)
	BlockNumMissing(peerId *PeerId, blockNum uint64)
	PeerConnected(peerId *PeerId)
	PeerDisconnected(peerId *PeerId)
}

func NewPeerTracker() PeerTracker {
	return newPeerTracker(RandPeerShuffle)
}

func newPeerTracker(peerShuffle PeerShuffle) *peerTracker {
	return &peerTracker{
		peerSyncProgresses: map[PeerId]*peerSyncProgress{},
		peerShuffle:        peerShuffle,
	}
}

type peerTracker struct {
	mu                 sync.Mutex
	peerSyncProgresses map[PeerId]*peerSyncProgress
	peerShuffle        PeerShuffle
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

	delete(pt.peerSyncProgresses, *peerId)
}

func (pt *peerTracker) PeerConnected(peerId *PeerId) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

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

func NewPeerEventObserver(logger log.Logger, peerTracker PeerTracker) polygoncommon.Observer[*sentry.PeerEvent] {
	return func(message *sentry.PeerEvent) {
		peerId := PeerIdFromH512(message.PeerId)

		logger.Debug("[p2p.peerEventObserver] received new peer event", "id", message.EventId, "peerId", peerId)

		switch message.EventId {
		case sentry.PeerEvent_Connect:
			peerTracker.PeerConnected(peerId)
		case sentry.PeerEvent_Disconnect:
			peerTracker.PeerDisconnected(peerId)
		}
	}
}
