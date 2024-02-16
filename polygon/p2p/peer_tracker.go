package p2p

import (
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

type PeerTracker interface {
	ListPeersMayHaveBlockNum(blockNum uint64) []PeerId
	BlockNumPresent(peerId PeerId, blockNum uint64)
	BlockNumMissing(peerId PeerId, blockNum uint64)
	PeerDisconnected(peerId PeerId)
}

func NewPeerTracker() PeerTracker {
	return &peerTracker{
		peerSyncProgresses: map[PeerId]*peerSyncProgress{},
	}
}

type peerTracker struct {
	once                     sync.Once
	messageListener          MessageListener
	mu                       sync.Mutex
	peerSyncProgresses       map[PeerId]*peerSyncProgress
	peerEventObserver        messageObserver[*sentry.PeerEvent]
	blockNumPresenceObserver messageObserver[*sentry.InboundMessage]
}

func (pt *peerTracker) ListPeersMayHaveBlockNum(blockNum uint64) []PeerId {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	var peerIds []PeerId
	for _, peerSyncProgress := range pt.peerSyncProgresses {
		if peerSyncProgress.peerMayHaveBlockNum(blockNum) {
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

func NewBlockNumPresenceObserver(peerTracker PeerTracker) messageObserver[*sentry.InboundMessage] {
	return &blockNumPresenceObserver{
		peerTracker: peerTracker,
	}
}

type blockNumPresenceObserver struct {
	peerTracker PeerTracker
}

func (bnpo *blockNumPresenceObserver) Notify(msg *sentry.InboundMessage) {
	if msg.Id == sentry.MessageId_BLOCK_HEADERS_66 {
		var pkt eth.BlockHeadersPacket66
		if err := rlp.DecodeBytes(msg.Data, &pkt); err != nil {
			return
		}

		headers := pkt.BlockHeadersPacket
		if len(headers) > 0 {
			bnpo.peerTracker.BlockNumPresent(PeerIdFromH512(msg.PeerId), headers[len(headers)-1].Number.Uint64())
		}
	}
}

func NewPeerEventObserver(peerTracker PeerTracker) messageObserver[*sentry.PeerEvent] {
	return &peerEventObserver{
		peerTracker: peerTracker,
	}
}

type peerEventObserver struct {
	peerTracker PeerTracker
}

func (peo *peerEventObserver) Notify(msg *sentry.PeerEvent) {
	if msg.EventId == sentry.PeerEvent_Disconnect {
		peo.peerTracker.PeerDisconnected(PeerIdFromH512(msg.PeerId))
	}
}
