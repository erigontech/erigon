package network

import (
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/common"
	"golang.org/x/net/context"
)

// Input: the currently highest slot processed and the list of blocks we want to know process
// Output: the new last new highest slot processed and an error possibly?
type ProcessFn func(
	highestSlotProcessed uint64,
	highestBlockRootProcessed common.Hash,
	blocks []*cltypes.SignedBeaconBlockBellatrix) (
	newHighestSlotProcessed uint64,
	newHighestBlockRootProcessed common.Hash,
	err error)

type ForwardBeaconDownloader struct {
	ctx                       context.Context
	highestSlotProcessed      uint64
	highestBlockRootProcessed common.Hash
	targetSlot                uint64
	sentinel                  sentinel.SentinelClient // Sentinel
	process                   ProcessFn
	isDownloading             bool // Should be set to true to set the blocks to download
	limitSegmentsLength       int  // Limit how many blocks we store in the downloader without processing

	segments []*cltypes.SignedBeaconBlockBellatrix // Unprocessed downloaded segments
	mu       sync.Mutex
}

func NewForwardBeaconDownloader(ctx context.Context, sentinel sentinel.SentinelClient) *ForwardBeaconDownloader {
	return &ForwardBeaconDownloader{
		ctx:           ctx,
		segments:      []*cltypes.SignedBeaconBlockBellatrix{},
		sentinel:      sentinel,
		isDownloading: false,
	}
}

// Start begins the gossip listening process.
func (f *ForwardBeaconDownloader) ReceiveGossip(obj cltypes.ObjectSSZ) {
	signedBlock := obj.(*cltypes.SignedBeaconBlockBellatrix)
	if signedBlock.Block.ParentRoot == f.highestBlockRootProcessed {
		f.addSegment(signedBlock)
	}
}

// SetIsDownloading sets isDownloading
func (f *ForwardBeaconDownloader) SetIsDownloading(isDownloading bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.isDownloading = isDownloading
}

// SetLimitSegmentsLength sets the segments limiter.
func (f *ForwardBeaconDownloader) SetLimitSegmentsLength(limitSegmentsLength int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limitSegmentsLength = limitSegmentsLength
}

// SetTargetSlot sets the target slot.
func (f *ForwardBeaconDownloader) SetTargetSlot(targetSlot uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targetSlot = targetSlot
}

// SetProcessFunction sets the function used to process segments.
func (f *ForwardBeaconDownloader) SetProcessFunction(fn ProcessFn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.process = fn
}

// SetHighestProcessedSlot sets the highest processed slot so far.
func (f *ForwardBeaconDownloader) SetHighestProcessedSlot(highestSlotProcessed uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.highestSlotProcessed = highestSlotProcessed
}

// SetHighestProcessedRoot sets the highest processed block root so far.
func (f *ForwardBeaconDownloader) SetHighestProcessedRoot(root common.Hash) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.highestBlockRootProcessed = root
}

// HighestProcessedRoot returns the highest processed block root so far.
func (f *ForwardBeaconDownloader) HighestProcessedRoot() common.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.highestBlockRootProcessed
}

// addSegment process new block segment.
func (f *ForwardBeaconDownloader) addSegment(block *cltypes.SignedBeaconBlockBellatrix) {
	// Skip if it is not downloading or limit was reached
	if !f.isDownloading || len(f.segments) >= f.limitSegmentsLength {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// Skip if does continue the segment.
	f.segments = append(f.segments, block)
}

func (f *ForwardBeaconDownloader) RequestMore() {
	count := uint64(10)

	responses, err := rpc.SendBeaconBlocksByRangeReq(
		f.ctx,
		f.highestSlotProcessed+1,
		count,
		f.sentinel,
	)
	if err != nil {
		return
	}
	for _, response := range responses {
		if segment, ok := response.(*cltypes.SignedBeaconBlockBellatrix); ok {
			f.addSegment(segment)
		}
	}

}

// ProcessBlocks processes blocks we accumulated.
func (f *ForwardBeaconDownloader) ProcessBlocks() error {
	if len(f.segments) == 0 {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var err error
	var highestSlotProcessed uint64
	var highestBlockRootProcessed common.Hash
	if highestSlotProcessed, highestBlockRootProcessed, err = f.process(f.highestSlotProcessed, f.highestBlockRootProcessed, f.segments); err != nil {
		return err
	}
	f.highestSlotProcessed = highestSlotProcessed
	f.highestBlockRootProcessed = highestBlockRootProcessed
	// clear segments
	f.segments = f.segments[:0]
	return nil
}

// GetHighestProcessedSlot retrieve the highest processed slot we accumulated.
func (f *ForwardBeaconDownloader) GetHighestProcessedSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.highestSlotProcessed
}
