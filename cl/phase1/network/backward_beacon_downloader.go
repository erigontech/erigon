package network

import (
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/net/context"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
)

// Whether the reverse downloader arrived at expected height or condition.
type OnNewBlock func(blk *cltypes.SignedBeaconBlock) (finished bool, err error)

type BackwardBeaconDownloader struct {
	ctx            context.Context
	slotToDownload uint64
	expectedRoot   libcommon.Hash
	rpc            *rpc.BeaconRpcP2P
	onNewBlock     OnNewBlock
	finished       bool

	mu sync.Mutex
}

func NewBackwardBeaconDownloader(ctx context.Context, rpc *rpc.BeaconRpcP2P) *BackwardBeaconDownloader {
	return &BackwardBeaconDownloader{
		ctx: ctx,
		rpc: rpc,
	}
}

// SetSlotToDownload sets slot to download.
func (b *BackwardBeaconDownloader) SetSlotToDownload(slot uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.slotToDownload = slot
}

// SetExpectedRoot sets the expected root we expect to download.
func (b *BackwardBeaconDownloader) SetExpectedRoot(root libcommon.Hash) {
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

// Peers returns the current number of peers connected to the BackwardBeaconDownloader.
func (b *BackwardBeaconDownloader) Peers() (uint64, error) {
	return b.rpc.Peers()
}

// RequestMore downloads a range of blocks in a backward manner.
// The function sends a request for a range of blocks starting from a given slot and ending count blocks before it.
// It then processes the response by iterating over the blocks in reverse order and calling a provided callback function onNewBlock on each block.
// If the callback returns an error or signals that the download should be finished, the function will exit.
// If the block's root hash does not match the expected root hash, it will be rejected and the function will continue to the next block.
func (b *BackwardBeaconDownloader) RequestMore() {
	count := uint64(64)
	start := b.slotToDownload - count + 1
	// Overflow? round to 0.
	if start > b.slotToDownload {
		start = 0
	}
	responses, _, err := b.rpc.SendBeaconBlocksByRangeReq(start, count)
	if err != nil {
		return
	}
	// Import new blocks, order is forward so reverse the whole packet
	for i := len(responses) - 1; i >= 0; i-- {
		if b.finished {
			return
		}
		segment := responses[i]
		// is this new block root equal to the expected root?
		blockRoot, err := segment.Block.HashSSZ()
		if err != nil {
			log.Debug("Could not compute block root while processing packet", "err", err)
			continue
		}
		// No? Reject.
		if blockRoot != b.expectedRoot {
			continue
		}
		// Yes? then go for the callback.
		b.finished, err = b.onNewBlock(segment)
		if err != nil {
			log.Warn("Found error while processing packet", "err", err)
			continue
		}
		// set expected root to the segment parent root
		b.expectedRoot = segment.Block.ParentRoot
		b.slotToDownload = segment.Block.Slot - 1 // update slot (might be inexact but whatever)
	}
}
