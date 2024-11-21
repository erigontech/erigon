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

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

func NewPeerTracker(
	logger log.Logger,
	peerProvider peerProvider,
	peerEventRegistrar peerEventRegistrar,
	opts ...PeerTrackerOption,
) *PeerTracker {
	pt := &PeerTracker{
		logger:                  logger,
		peerProvider:            peerProvider,
		peerEventRegistrar:      peerEventRegistrar,
		peerSyncProgresses:      map[PeerId]*peerSyncProgress{},
		peerKnownBlockAnnounces: map[PeerId]simplelru.LRUCache[common.Hash, struct{}]{},
		peerShuffle:             RandPeerShuffle,
	}

	for _, opt := range opts {
		opt(pt)
	}

	return pt
}

type PeerTracker struct {
	logger                  log.Logger
	peerProvider            peerProvider
	peerEventRegistrar      peerEventRegistrar
	mu                      sync.Mutex
	peerSyncProgresses      map[PeerId]*peerSyncProgress
	peerKnownBlockAnnounces map[PeerId]simplelru.LRUCache[common.Hash, struct{}]
	peerShuffle             PeerShuffle
}

func (pt *PeerTracker) Run(ctx context.Context) error {
	pt.logger.Debug(peerTrackerLogPrefix("running peer tracker component"))

	var peerEventUnreg polygoncommon.UnregisterFunc
	defer func() { peerEventUnreg() }()

	err := func() error {
		// we lock the pt for updates so that we:
		//   1. register the peer connection observer but buffer the updates coming from it until we do 2.
		//   2. replay the current state of connected peers
		pt.mu.Lock()
		defer pt.mu.Unlock()

		// 1. register the observer
		peerEventUnreg = pt.peerEventRegistrar.RegisterPeerEventObserver(newPeerEventObserver(pt))

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

	hashAnnouncesUnreg := pt.peerEventRegistrar.RegisterNewBlockHashesObserver(newBlockHashAnnouncesObserver(pt))
	defer hashAnnouncesUnreg()

	blockAnnouncesUnreg := pt.peerEventRegistrar.RegisterNewBlockObserver(newBlockAnnouncesObserver(pt))
	defer blockAnnouncesUnreg()

	<-ctx.Done()
	return ctx.Err()
}

func (pt *PeerTracker) ListPeersMayHaveBlockNum(blockNum uint64) []*PeerId {
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

func (pt *PeerTracker) BlockNumPresent(peerId *PeerId, blockNum uint64) {
	pt.updatePeerSyncProgress(peerId, func(psp *peerSyncProgress) {
		psp.blockNumPresent(blockNum)
	})
}

func (pt *PeerTracker) BlockNumMissing(peerId *PeerId, blockNum uint64) {
	pt.updatePeerSyncProgress(peerId, func(psp *peerSyncProgress) {
		psp.blockNumMissing(blockNum)
	})
}

func (pt *PeerTracker) ListPeersMayMissBlockHash(blockHash common.Hash) []*PeerId {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	var peerIds []*PeerId
	for peerId, knownBlockAnnounces := range pt.peerKnownBlockAnnounces {
		if !knownBlockAnnounces.Contains(blockHash) {
			peerId := peerId
			peerIds = append(peerIds, &peerId)
		}
	}

	pt.peerShuffle(peerIds)
	return peerIds
}

func (pt *PeerTracker) BlockHashPresent(peerId *PeerId, blockHash common.Hash) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	announcesLru, ok := pt.peerKnownBlockAnnounces[*peerId]
	if !ok || announcesLru.Contains(blockHash) {
		return
	}

	announcesLru.Add(blockHash, struct{}{})
}

func (pt *PeerTracker) PeerDisconnected(peerId *PeerId) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.logger.Debug(peerTrackerLogPrefix("peer disconnected"), "peerId", peerId.String())
	delete(pt.peerSyncProgresses, *peerId)
	delete(pt.peerKnownBlockAnnounces, *peerId)
}

func (pt *PeerTracker) PeerConnected(peerId *PeerId) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.peerConnected(peerId)
}

func (pt *PeerTracker) peerConnected(peerId *PeerId) {
	pt.logger.Debug(peerTrackerLogPrefix("peer connected"), "peerId", peerId.String())

	peerIdVal := *peerId
	if _, ok := pt.peerSyncProgresses[peerIdVal]; !ok {
		pt.peerSyncProgresses[peerIdVal] = &peerSyncProgress{
			peerId: peerId,
		}
	}

	if _, ok := pt.peerKnownBlockAnnounces[peerIdVal]; !ok {
		announcesLru, err := simplelru.NewLRU[common.Hash, struct{}](1024, nil)
		if err != nil {
			panic(err)
		}

		pt.peerKnownBlockAnnounces[peerIdVal] = announcesLru
	}
}

func (pt *PeerTracker) updatePeerSyncProgress(peerId *PeerId, update func(psp *peerSyncProgress)) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	peerSyncProgress, ok := pt.peerSyncProgresses[*peerId]
	if !ok {
		return
	}

	update(peerSyncProgress)
}

func newPeerEventObserver(pt *PeerTracker) polygoncommon.Observer[*sentryproto.PeerEvent] {
	return func(message *sentryproto.PeerEvent) {
		peerId := PeerIdFromH512(message.PeerId)
		switch message.EventId {
		case sentryproto.PeerEvent_Connect:
			pt.PeerConnected(peerId)
		case sentryproto.PeerEvent_Disconnect:
			pt.PeerDisconnected(peerId)
		}
	}
}

func newBlockHashAnnouncesObserver(pt *PeerTracker) polygoncommon.Observer[*DecodedInboundMessage[*eth.NewBlockHashesPacket]] {
	return func(message *DecodedInboundMessage[*eth.NewBlockHashesPacket]) {
		for _, hashOrNum := range *message.Decoded {
			pt.BlockHashPresent(message.PeerId, hashOrNum.Hash)
		}
	}
}

func newBlockAnnouncesObserver(pt *PeerTracker) polygoncommon.Observer[*DecodedInboundMessage[*eth.NewBlockPacket]] {
	return func(message *DecodedInboundMessage[*eth.NewBlockPacket]) {
		pt.BlockHashPresent(message.PeerId, message.Decoded.Block.Hash())
	}
}

func peerTrackerLogPrefix(message string) string {
	return "[p2p.peerTracker] " + message
}
