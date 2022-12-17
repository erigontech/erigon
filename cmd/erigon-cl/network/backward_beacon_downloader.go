package network

import (
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/net/context"
)

// Whether the reverse downloader arrived at expected height or condition.
type OnNewBlock func(blk *cltypes.SignedBeaconBlockBellatrix) (finished bool, err error)

type BackwardBeaconDownloader struct {
	ctx            context.Context
	slotToDownload uint64
	expectedRoot   common.Hash
	sentinel       sentinel.SentinelClient // Sentinel
	onNewBlock     OnNewBlock
	segments       []*cltypes.BeaconBlockBellatrix
	finished       bool

	mu sync.Mutex
}

func NewBackwardBeaconDownloader(ctx context.Context, sentinel sentinel.SentinelClient) *BackwardBeaconDownloader {
	return &BackwardBeaconDownloader{
		ctx:      ctx,
		sentinel: sentinel,
	}
}

// SetSlotToDownload sets slot to download.
func (b *BackwardBeaconDownloader) SetSlotToDownload(slot uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.slotToDownload = slot
}

// SetExpectedRoot sets the expected root we expect to download.
func (b *BackwardBeaconDownloader) SetExpectedRoot(root common.Hash) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.expectedRoot = root
}

// SetShouldStopAtFn sets the stop condition.
func (b *BackwardBeaconDownloader) SetOnNewBlock(onNewBlock OnNewBlock) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onNewBlock = onNewBlock
}

// HighestProcessedRoot returns the highest processed block root so far.
func (b *BackwardBeaconDownloader) Finished() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.finished
}

// Progress current progress.
func (b *BackwardBeaconDownloader) Progress() uint64 {
	// Skip if it is not downloading or limit was reached
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.slotToDownload
}

func (b *BackwardBeaconDownloader) RequestMore() {
	go func() {
		count := uint64(10)
		start := b.slotToDownload - count + 1
		responses, err := rpc.SendBeaconBlocksByRangeReq(
			b.ctx,
			start,
			count,
			b.sentinel,
		)
		if err != nil {
			return
		}

		b.mu.Lock()
		defer b.mu.Unlock()
		// Import new blocks, order is forward so reverse the whole packet
		for i := len(responses) - 1; i >= 0; i-- {
			if segment, ok := responses[i].(*cltypes.SignedBeaconBlockBellatrix); ok {
				if b.finished {
					return
				}
				// is this new block root equal to the expected root?
				blockRoot, err := segment.Block.HashTreeRoot()
				if err != nil {
					log.Debug("Could not compute block root while processing packet", "err", err)
					continue
				}
				// No? Reject.
				if blockRoot != b.expectedRoot {
					log.Debug("Bad packet received", "start", start, "tasked", b.slotToDownload-count+1, "got", common.Hash(blockRoot), "received", b.expectedRoot)
					continue
				}
				// Yes? then go for the callback.
				b.finished, err = b.onNewBlock(segment)
				if err != nil {
					log.Debug("Found error while processing packet", "err", err)
					continue
				}
				// set expected root to the segment parent root
				b.expectedRoot = segment.Block.ParentRoot
				b.slotToDownload = segment.Block.Slot - 1 // update slot (might be inexact but whatever)
			}
		}
	}()
}
