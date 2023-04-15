package network

import (
	"sync"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"golang.org/x/net/context"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
)

// Input: the currently highest slot processed and the list of blocks we want to know process
// Output: the new last new highest slot processed and an error possibly?
type ProcessFn func(
	highestSlotProcessed uint64,
	highestBlockRootProcessed libcommon.Hash,
	blocks []*cltypes.SignedBeaconBlock) (
	newHighestSlotProcessed uint64,
	newHighestBlockRootProcessed libcommon.Hash,
	err error)

type ForwardBeaconDownloader struct {
	ctx                       context.Context
	highestSlotProcessed      uint64
	highestBlockRootProcessed libcommon.Hash
	rpc                       *rpc.BeaconRpcP2P
	process                   ProcessFn

	segments []*cltypes.SignedBeaconBlock // Unprocessed downloaded segments
	mu       sync.Mutex
}

func NewForwardBeaconDownloader(ctx context.Context, rpc *rpc.BeaconRpcP2P) *ForwardBeaconDownloader {
	return &ForwardBeaconDownloader{
		ctx:      ctx,
		segments: []*cltypes.SignedBeaconBlock{},
		rpc:      rpc,
	}
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
func (f *ForwardBeaconDownloader) SetHighestProcessedRoot(root libcommon.Hash) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.highestBlockRootProcessed = root
}

// HighestProcessedRoot returns the highest processed block root so far.
func (f *ForwardBeaconDownloader) HighestProcessedRoot() libcommon.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.highestBlockRootProcessed
}

// addSegment process new block segment.
func (f *ForwardBeaconDownloader) addSegment(block *cltypes.SignedBeaconBlock) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Skip if does continue the segment.
	f.segments = append(f.segments, block)
}

func (f *ForwardBeaconDownloader) RequestMore() {
	count := uint64(4) // dont need many

	responses, err := f.rpc.SendBeaconBlocksByRangeReq(f.highestSlotProcessed+1, count)
	if err != nil {
		// Wait a bit in this case (we do not need to be super performant here).
		time.Sleep(time.Second)
		return
	}
	for _, response := range responses {
		f.addSegment(response)
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
	var highestBlockRootProcessed libcommon.Hash
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

func (f *ForwardBeaconDownloader) Peers() (uint64, error) {
	return f.rpc.Peers()
}
