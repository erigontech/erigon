package network

import (
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"golang.org/x/net/context"
)

// Input: the currently highest slot processed and the list of blocks we want to know process
// Output: the new last new highest slot processed and an error possibly?
type ProcessFn func(
	highestSlotProcessed uint64,
	blocks []*cltypes.SignedBeaconBlockBellatrix) (newHighestSlotProcessed uint64, err error)

type ForwardBeaconDownloader struct {
	ctx                  context.Context
	highestSlotProcessed uint64
	lastDownloadedSlot   uint64
	sentinel             sentinel.SentinelClient // Sentinel
	process              ProcessFn
	isDownloading        bool // Should be set to true to set the blocks to download
	limitSegmentsLength  int  // Limit how many blocks we store in the downloader without processing

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
	f.addSegment(signedBlock)
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

// SetProcessFunction sets the function used to process segments.
func (f *ForwardBeaconDownloader) SetProcessFunction(fn ProcessFn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.process = fn
}

// SetHighestProcessSlot sets the highest processed slot so far.
func (f *ForwardBeaconDownloader) SetHighestProcessSlot(highestSlotProcessed uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.highestSlotProcessed = highestSlotProcessed
	f.lastDownloadedSlot = highestSlotProcessed
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
	if block.Block.Slot != f.lastDownloadedSlot+1 {
		return
	}
	f.lastDownloadedSlot++
	f.segments = append(f.segments, block)
}

func (f *ForwardBeaconDownloader) RequestMore(count int) {
	for i := 0; i < count; i++ {
		go func() {
			count := uint64(10)
			responses, err := rpc.SendBeaconBlocksByRangeReq(
				f.ctx,
				f.lastDownloadedSlot+1,
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
		}()
	}
}

// ProcessBlocks processes blocks we accumulated.
func (f *ForwardBeaconDownloader) ProcessBlocks() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var err error
	var highestSlotProcessed uint64
	if highestSlotProcessed, err = f.process(f.highestSlotProcessed, f.segments); err != nil {
		return err
	}
	f.lastDownloadedSlot = highestSlotProcessed
	f.highestSlotProcessed = highestSlotProcessed
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
