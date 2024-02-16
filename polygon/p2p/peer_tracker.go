package p2p

import (
	"sync"
)

type PeerTracker interface {
	ListPeersMayHave(blockNum uint64) []PeerId
	BlockNumPresent(peerId PeerId, blockNum uint64)
	BlockNumMissing(peerId PeerId, blockNum uint64)
	PeerDisconnected(peerId PeerId)
}

func newPeerTracker() PeerTracker {
	return &peerTracker{
		peerSyncProgresses: map[PeerId]*peerSyncProgress{},
	}
}

type peerTracker struct {
	mu                 sync.Mutex
	peerSyncProgresses map[PeerId]*peerSyncProgress
	messageListener    MessageListener
}

func (pt *peerTracker) ListPeersMayHave(blockNum uint64) []PeerId {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	var peerIds []PeerId
	for _, peerSyncProgress := range pt.peerSyncProgresses {
		if peerSyncProgress.peerMayHave(blockNum) {
			peerIds = append(peerIds, peerSyncProgress.peerId)
		}
	}

	return peerIds
}

func (pt *peerTracker) BlockNumPresent(peerId PeerId, blockNum uint64) {
	pt.updatePeerSyncProgress(peerId, func(psp *peerSyncProgress) {
		psp.blockNumPresent(blockNum)
	})
}

func (pt *peerTracker) BlockNumMissing(peerId PeerId, blockNum uint64) {
	pt.updatePeerSyncProgress(peerId, func(psp *peerSyncProgress) {
		psp.blockNumMissing(blockNum)
	})
}

func (pt *peerTracker) PeerDisconnected(peerId PeerId) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	delete(pt.peerSyncProgresses, peerId)
}

func (pt *peerTracker) updatePeerSyncProgress(peerId PeerId, update func(psp *peerSyncProgress)) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	peerSyncProgress, ok := pt.peerSyncProgresses[peerId]
	if !ok {
		return
	}

	update(peerSyncProgress)
}
