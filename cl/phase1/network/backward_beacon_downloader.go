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

package network

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

// Whether the reverse downloader arrived at expected height or condition.
type OnNewBlock func(blk *cltypes.SignedBeaconBlock) (finished bool, err error)

type BackwardBeaconDownloader struct {
	ctx                  context.Context
	slotToDownload       atomic.Uint64
	expectedRoot         common.Hash
	rpc                  *rpc.BeaconRpcP2P
	engine               execution_client.ExecutionEngine
	onNewBlock           OnNewBlock
	finished             atomic.Bool
	reqInterval          *time.Ticker
	db                   kv.RwDB
	sn                   *freezeblocks.CaplinSnapshots
	neverSkip            bool
	pendingResults       *lru.Cache[uint64, *requestResult]
	downloadedBlocksLock sync.Mutex // lock for downloaded blocks

	mu sync.Mutex
}

func NewBackwardBeaconDownloader(ctx context.Context, rpc *rpc.BeaconRpcP2P, sn *freezeblocks.CaplinSnapshots, engine execution_client.ExecutionEngine, db kv.RwDB) *BackwardBeaconDownloader {
	pendingResults, err := lru.New[uint64, *requestResult]("backward_beacon_downloader_pending_results", 2048)
	if err != nil {
		panic(fmt.Sprintf("could not create lru cache for pending results: %v", err))
	}

	return &BackwardBeaconDownloader{
		ctx:            ctx,
		rpc:            rpc,
		db:             db,
		reqInterval:    time.NewTicker(300 * time.Millisecond),
		neverSkip:      true,
		engine:         engine,
		sn:             sn,
		pendingResults: pendingResults,
	}
}

// SetThrottle sets the throttle.
func (b *BackwardBeaconDownloader) SetThrottle(throttle time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reqInterval.Reset(throttle)
}

// SetSlotToDownload sets slot to download.
func (b *BackwardBeaconDownloader) SetSlotToDownload(slot uint64) {
	b.slotToDownload.Store(slot)
}

// SetExpectedRoot sets the expected root we expect to download.
func (b *BackwardBeaconDownloader) SetExpectedRoot(root common.Hash) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.expectedRoot = root
}

// SetExpectedRoot sets the expected root we expect to download.
func (b *BackwardBeaconDownloader) SetNeverSkip(neverSkip bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.neverSkip = neverSkip
}

// SetShouldStopAtFn sets the stop condition.
func (b *BackwardBeaconDownloader) SetOnNewBlock(onNewBlock OnNewBlock) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onNewBlock = onNewBlock
}

func (b *BackwardBeaconDownloader) RPC() *rpc.BeaconRpcP2P {
	return b.rpc
}

// HighestProcessedRoot returns the highest processed block root so far.
func (b *BackwardBeaconDownloader) Finished() bool { return b.finished.Load() }

// Progress current progress.
func (b *BackwardBeaconDownloader) Progress() uint64 {
	// Skip if it is not downloading or limit was reached
	return b.slotToDownload.Load()
}

// Peers returns the current number of peers connected to the BackwardBeaconDownloader.
func (b *BackwardBeaconDownloader) Peers() (uint64, error) {
	return b.rpc.Peers()
}

type requestResult struct {
	block *cltypes.SignedBeaconBlock
	peer  string
}

// RequestChunk requests a chunk of blocks from the remote beacon node.
func (b *BackwardBeaconDownloader) requestChunk(ctx context.Context, start, count uint64, maxSlot uint64) ([]*requestResult, error) {
	fmt.Println("Requesting chunk", start, count, maxSlot)
	// 2. request the chunk
	blocks, peer, err := b.rpc.SendBeaconBlocksByRangeReq(ctx, start, count+1)
	fmt.Println("end Requesting chunk", start, count, maxSlot)
	if err != nil {
		return nil, err
	}

	if len(blocks) == 0 {
		return nil, nil
	}

	var responses []*requestResult
	for _, block := range blocks {
		if block == nil {
			continue
		}

		// filter out blocks that are not in the range
		if block.Block.Slot > maxSlot || block.Block.Slot < start {
			continue
		}

		responses = append(responses, &requestResult{
			block: block,
			peer:  peer,
		})
	}
	return responses, nil
}

func (b *BackwardBeaconDownloader) requestRange(ctx context.Context, downloadRange downloadRange, maxSlot uint64) (downloadedBlocks []*requestResult) {
	// give it a 1 second timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	x := time.Now()

	count := downloadRange.end - downloadRange.start + 1
	// 2. request the chunk
	requestsResult, err := b.requestChunk(ctxWithTimeout, downloadRange.start, count, maxSlot)
	if err != nil {
		log.Trace("Error while requesting chunk", "err", err)
		return
	}
	if requestsResult == nil {
		return
	}
	fmt.Println("Gotten chunk", downloadRange.start, count, len(requestsResult), time.Since(x))
	downloadedBlocks = append(downloadedBlocks, requestsResult...)
	for _, block := range requestsResult {
		if block == nil {
			continue
		}
		// add to the pending results
		b.pendingResults.Add(block.block.Block.Slot, block)
	}
	return downloadedBlocks
}

type downloadRange struct {
	start uint64 // start slot of the range
	end   uint64 // end slot of the range
}

// isRangePresent checks if a range of blocks is present in the map.
func getNeededRanges(start, end uint64, blockCache *lru.Cache[uint64, *requestResult]) []downloadRange {
	// for i := start; i <= end; i++ {
	// 	if !blockCache.Contains(i) {
	// 		return false
	// 	}
	// }
	// return true
	ranges := make([]downloadRange, 0)
	for i := start; i <= end; i++ {
		if blockCache.Contains(i) {
			continue
		}
		// find the next range
		j := i
		for j <= end && !blockCache.Contains(j) {
			j++
		}
		ranges = append(ranges, downloadRange{
			start: i,
			end:   j - 1,
		})
		i = j - 1 // skip to the end of the range
	}
	return ranges
}

// removeNils removes nil blocks from the list.
func removeNils(blocks []*requestResult, tempBuffer []*requestResult) []*requestResult {
	tempBuffer = tempBuffer[:0]
	// remove nils
	for _, block := range blocks {
		if block == nil {
			continue
		}
		tempBuffer = append(tempBuffer, block)
	}
	blocks = blocks[:0]
	for _, block := range tempBuffer {
		if block == nil {
			continue
		}
		blocks = append(blocks, block)
	}
	return blocks
}

// removeDuplicates removes duplicate blocks from the list (assume that the blocks are sorted by slot).
func removeDuplicates(blocks []*requestResult, tempBuffer []*requestResult) []*requestResult {
	tempBuffer = tempBuffer[:0]
	// remove duplicates
	seen := make(map[uint64]bool)
	for _, block := range blocks {
		if block == nil {
			continue
		}
		if seen[block.block.Block.Slot] {
			continue
		}
		seen[block.block.Block.Slot] = true
		tempBuffer = append(tempBuffer, block)
	}
	blocks = blocks[:0]
	for _, block := range tempBuffer {
		if block == nil {
			continue
		}
		blocks = append(blocks, block)
	}
	return blocks
}

// isThereNilBlocksrequests more blocks from the remote beacon node.
func isThereNilBlocks(blocks []*requestResult) bool {
	for _, block := range blocks {
		if block == nil {
			return true
		}
	}
	return false
}

// RequestMore downloads a range of blocks in a backward manner.
// The function sends a request for a range of blocks starting from a given slot and ending count blocks before it.
// It then processes the response by iterating over the blocks in reverse order and calling a provided callback function onNewBlock on each block.
// If the callback returns an error or signals that the download should be finished, the function will exit.
// If the block's root hash does not match the expected root hash, it will be rejected and the function will continue to the next block.
func (b *BackwardBeaconDownloader) RequestMore(ctx context.Context) error {
	subCount := uint64(32)
	chunks := uint64(32)
	count := subCount * chunks // 8 chunks of 32 blocks
	lowerBound := b.slotToDownload.Load() - count + 1
	// Overflow? round to 0.
	if lowerBound > b.slotToDownload.Load() {
		lowerBound = 0
	}
	// 1. initialize the response channel
	downloadedBlocks := make([]*requestResult, 0, count)
	downloadedBlocksTempBuffer := make([]*requestResult, 0, count)

	// iteratively download missing ranges

	var wg sync.WaitGroup

	// re-initialize the map

	startSlot := b.slotToDownload.Load()
	for i := len(downloadedBlocks) - 1; i >= 0; i-- {
		if downloadedBlocks[i] == nil {
			break
		}
		startSlot = downloadedBlocks[i].block.Block.Slot
	}

	for currEndSlot := startSlot; currEndSlot > lowerBound; currEndSlot -= subCount { // inner iterations

		start := currEndSlot - subCount + 1
		if currEndSlot < subCount {
			start = 0
		}

		rangesToDownload := getNeededRanges(start, currEndSlot, b.pendingResults)

		// 2. request the chunk in parallel
		for _, dr := range rangesToDownload {
			// download the range in a goroutine
			wg.Add(1)
			// sleep to avoid flooding the network with requests
			time.Sleep(150 * time.Millisecond)
			go func(downloadRange downloadRange) {
				defer wg.Done()
				fmt.Println(downloadRange)

				// request the range
				downloadedBlocksTemp := b.requestRange(ctx, downloadRange, b.slotToDownload.Load())
				fmt.Println("done")

				// append the downloaded blocks to the main list
				b.downloadedBlocksLock.Lock()
				defer b.downloadedBlocksLock.Unlock()
				downloadedBlocks = append(downloadedBlocks, downloadedBlocksTemp...)

				for _, block := range downloadedBlocksTemp {
					b.pendingResults.Add(block.block.Block.Slot, block)
				}
			}(dr)
		}
		// add the blocks from the pending cache
		for i := start; i <= currEndSlot; i++ {
			blockInCache, ok := b.pendingResults.Get(i)
			if !ok || blockInCache == nil {
				continue
			}
			downloadedBlocks = append(downloadedBlocks, blockInCache)
		}

		if start == 0 {
			break
		}
	}
	fmt.Println("XXY")
	wg.Wait()
	fmt.Println("XXX")

	// remove all nil entries
	downloadedBlocks = removeNils(downloadedBlocks, downloadedBlocksTempBuffer)
	// Sanitize the downloaded blocks
	// sort the blocks by slot
	sort.Slice(downloadedBlocks, func(i, j int) bool {
		return downloadedBlocks[i].block.Block.Slot < downloadedBlocks[j].block.Block.Slot
	})

	// remove duplicates
	downloadedBlocks = removeDuplicates(downloadedBlocks, downloadedBlocksTempBuffer)

	// check if the downloaded blocks have the correct parentRoot
	// if not, remove them from the list
	currentParentRoot := b.expectedRoot
	for i := len(downloadedBlocks) - 1; i >= 0; i-- {
		if downloadedBlocks[i] == nil {
			continue
		}
		currentRoot, err := downloadedBlocks[i].block.Block.HashSSZ()
		if err != nil {
			panic(err)
		}
		if currentRoot != currentParentRoot {
			b.rpc.BanPeer(downloadedBlocks[i].peer)
			// remove the block from the cache
			b.pendingResults.Remove(downloadedBlocks[i].block.Block.Slot)
			// truncate the list
			downloadedBlocks = downloadedBlocks[i:]
			log.Debug("Gotten unexpected root", "got", common.Hash(currentRoot), "expected", common.Hash(currentParentRoot))
			break
		}
		currentParentRoot = downloadedBlocks[i].block.Block.ParentRoot
	}
	fmt.Println("Downloaded blocks", len(downloadedBlocks), startSlot, lowerBound, count, b.slotToDownload.Load())
	if len(downloadedBlocks) == 0 {
		return nil
	}

	fmt.Println("Gotten all blocks", len(downloadedBlocks), b.slotToDownload.Load(), lowerBound, count)

	// Import new blocks, order is forward so reverse the whole packet
	for i := len(downloadedBlocks) - 1; i >= 0; i-- {
		if b.finished.Load() {
			return nil
		}
		segment := downloadedBlocks[i].block
		// is this new block root equal to the expected root?
		blockRoot, err := segment.Block.HashSSZ()
		if err != nil {
			log.Debug("Could not compute block root while processing packet", "err", err)
			continue
		}
		// No? Reject.
		if blockRoot != b.expectedRoot {
			log.Debug("Gotten unexpected root", "got", common.Hash(blockRoot), "expected", b.expectedRoot)
			continue
		}
		// Yes? then go for the callback.
		finished, err := b.onNewBlock(segment)
		b.finished.Store(finished)
		if err != nil {
			log.Warn("Found error while processing packet", "err", err)
			continue
		}
		// set expected root to the segment parent root
		b.expectedRoot = segment.Block.ParentRoot
		if segment.Block.Slot == 0 {
			b.finished.Store(true)
			return nil
		}
		b.slotToDownload.Store(segment.Block.Slot - 1) // update slot (might be inexact but whatever)
	}
	if !b.neverSkip {
		return nil
	}
	// try skipping if the next slot is in db
	tx, err := b.db.BeginRw(b.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	elFrozenBlocks := uint64(math.MaxUint64)
	if b.engine != nil && b.engine.SupportInsertion() {
		elFrozenBlocks = b.engine.FrozenBlocks(ctx)
	}
	clFrozenBlocks := uint64(0)
	if b.sn != nil {
		clFrozenBlocks = b.sn.SegmentsMax()
	}

	updateFrozenBlocksTicker := time.NewTicker(5 * time.Second)
	defer updateFrozenBlocksTicker.Stop()
	// it will stop if we end finding a gap or if we reach the maxIterations
	for {

		select {
		case <-updateFrozenBlocksTicker.C:
			if b.sn != nil {
				clFrozenBlocks = b.sn.SegmentsMax()
			}
			if b.engine != nil && b.engine.SupportInsertion() {
				elFrozenBlocks = b.engine.FrozenBlocks(ctx)
			}
		default:
		}

		// check if the expected root is in db
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, b.expectedRoot)
		if err != nil {
			return err
		}

		if slot == nil || *slot == 0 {
			break
		}

		if b.engine != nil && b.engine.SupportInsertion() {
			blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, b.expectedRoot)
			if err != nil {
				return err
			}
			blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, b.expectedRoot)
			if err != nil {
				return err
			}
			if blockHash == (common.Hash{}) || blockNumber == nil {
				break
			}
			if *blockNumber >= elFrozenBlocks {
				has, err := b.engine.HasBlock(ctx, blockHash)
				if err != nil {
					return err
				}
				if !has {
					break
				}
			}
		}
		if *slot <= clFrozenBlocks {
			break
		}
		b.slotToDownload.Store(*slot - 1)
		if err := beacon_indicies.MarkRootCanonical(b.ctx, tx, *slot, b.expectedRoot); err != nil {
			return err
		}
		b.expectedRoot, err = beacon_indicies.ReadParentBlockRoot(b.ctx, tx, b.expectedRoot)
		if err != nil {
			return err
		}
		// Some cleaning of possible ugly restarts
		newSlotToDownload, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, b.expectedRoot)
		if err != nil {
			return err
		}
		if newSlotToDownload == nil || *newSlotToDownload == 0 {
			continue
		}
		for i := *newSlotToDownload + 1; i < *slot; i++ {
			tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(i))
		}
	}

	return tx.Commit()
}
