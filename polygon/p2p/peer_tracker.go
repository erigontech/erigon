package p2p

import (
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

type PeerTracker interface {
	Start()
	Stop()
	ListPeersMayHave(blockNum uint64) []PeerId
	BlockNumPresent(peerId PeerId, blockNum uint64)
	BlockNumMissing(peerId PeerId, blockNum uint64)
	PeerDisconnected(peerId PeerId)
}

func newPeerTracker(messageListener MessageListener) PeerTracker {
	return &peerTracker{
		messageListener:    messageListener,
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

func (pt *peerTracker) Start() {
	pt.once.Do(func() {
		pt.peerEventObserver = &peerEventObserver{
			pt: pt,
		}

		pt.blockNumPresenceObserver = &blockNumPresenceObserver{
			pt: pt,
		}

		pt.messageListener.RegisterPeerEventObserver(pt.peerEventObserver)
		pt.messageListener.RegisterBlockHeaders66(pt.blockNumPresenceObserver)
	})
}

func (pt *peerTracker) Stop() {
	if pt.blockNumPresenceObserver != nil {
		pt.messageListener.UnregisterBlockHeaders66(pt.blockNumPresenceObserver)
		pt.blockNumPresenceObserver = nil
	}

	if pt.peerEventObserver != nil {
		pt.messageListener.UnregisterPeerEventObserver(pt.peerEventObserver)
		pt.peerEventObserver = nil
	}
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

type blockNumPresenceObserver struct {
	pt PeerTracker
}

func (bnpo *blockNumPresenceObserver) Notify(msg *sentry.InboundMessage) {
	if msg.Id == sentry.MessageId_BLOCK_HEADERS_66 {
		var pkt eth.BlockHeadersPacket66
		if err := rlp.DecodeBytes(msg.Data, &pkt); err != nil {
			return
		}

		headers := pkt.BlockHeadersPacket
		if len(headers) > 0 {
			bnpo.pt.BlockNumPresent(PeerIdFromH512(msg.PeerId), headers[len(headers)-1].Number.Uint64())
		}
	}
}

type peerEventObserver struct {
	pt PeerTracker
}

func (peo *peerEventObserver) Notify(msg *sentry.PeerEvent) {
	if msg.EventId == sentry.PeerEvent_Disconnect {
		peo.pt.PeerDisconnected(PeerIdFromH512(msg.PeerId))
	}
}
