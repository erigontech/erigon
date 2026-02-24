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
	context0 "context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

// Whether the reverse downloader arrived at expected height or condition.
type OnNewBlock func(blk *cltypes.SignedBeaconBlock) (finished bool, err error)

// BlockChecker is an interface for checking if a block exists
type BlockChecker interface {
	HasBlock(blockNumber uint64) bool
}

type BackwardBeaconDownloader struct {
	ctx            context0.Context
	slotToDownload atomic.Uint64
	expectedRoot   common.Hash
	rpc            *rpc.BeaconRpcP2P
	engine         execution_client.ExecutionEngine
	onNewBlock     OnNewBlock
	finished       atomic.Bool
	reqInterval    *time.Ticker
	db             kv.RwDB
	sn             *freezeblocks.CaplinSnapshots
	neverSkip      bool
	blockChecker   BlockChecker

	mu sync.Mutex
}

func NewBackwardBeaconDownloader(ctx context0.Context, rpc *rpc.BeaconRpcP2P, sn *freezeblocks.CaplinSnapshots, engine execution_client.ExecutionEngine, db kv.RwDB) *BackwardBeaconDownloader {
	return &BackwardBeaconDownloader{
		ctx:         ctx,
		rpc:         rpc,
		db:          db,
		reqInterval: time.NewTicker(200 * time.Millisecond),
		neverSkip:   true,
		engine:      engine,
		sn:          sn,
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

// SetBlockChecker sets the block checker for skipping already downloaded blocks
func (b *BackwardBeaconDownloader) SetBlockChecker(checker BlockChecker) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blockChecker = checker
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

// RequestMore downloads a range of blocks in a backward manner.
// It requests blocks, processes them in reverse order via the onNewBlock callback,
// and rejects blocks whose root hash doesn't match the expected root.
func (b *BackwardBeaconDownloader) RequestMore(ctx context0.Context) error {
	responses, err := b.fetchBlockRange(ctx)
	if err != nil {
		return err
	}

	if err := b.processResponses(responses); err != nil {
		return err
	}

	if !b.neverSkip {
		return nil
	}

	return b.trySkipToExistingBlock(ctx)
}

// fetchBlockRange requests a range of blocks from peers and waits for a response.
func (b *BackwardBeaconDownloader) fetchBlockRange(ctx context0.Context) ([]*cltypes.SignedBeaconBlock, error) {
	const count = uint64(64)
	start := b.slotToDownload.Load() - count + 1
	if start > b.slotToDownload.Load() { // overflow check
		start = 0
	}

	// Buffered channel prevents goroutine leaks
	received := make(chan []*cltypes.SignedBeaconBlock, 1)
	var requestSent atomic.Bool

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-b.reqInterval.C:
			if requestSent.Swap(true) {
				continue // request already in flight
			}
			go b.sendBlockRequest(ctx, start, count, received, &requestSent)

		case responses := <-received:
			return responses, nil
		}
	}
}

// sendBlockRequest sends a block range request and writes the result to the channel.
func (b *BackwardBeaconDownloader) sendBlockRequest(
	ctx context0.Context,
	start, count uint64,
	received chan<- []*cltypes.SignedBeaconBlock,
	requestSent *atomic.Bool,
) {
	blocks, peerId, err := b.rpc.SendBeaconBlocksByRangeReq(ctx, start, count)
	if err != nil || blocks == nil || len(blocks) == 0 {
		b.rpc.BanPeer(peerId)
		requestSent.Store(false)
		return
	}

	select {
	case received <- blocks:
	default:
		// Response already received, discard
	}
}

// processResponses processes downloaded blocks in reverse order.
func (b *BackwardBeaconDownloader) processResponses(responses []*cltypes.SignedBeaconBlock) error {
	for i := len(responses) - 1; i >= 0; i-- {
		if b.finished.Load() {
			return nil
		}

		block := responses[i]
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			log.Debug("Could not compute block root", "err", err)
			continue
		}

		if blockRoot != b.expectedRoot {
			log.Debug("Unexpected root", "got", common.Hash(blockRoot), "expected", b.expectedRoot)
			continue
		}

		finished, err := b.onNewBlock(block)
		b.finished.Store(finished)
		if err != nil {
			log.Warn("Error processing block", "err", err)
			continue
		}

		b.expectedRoot = block.Block.ParentRoot
		if block.Block.Slot == 0 {
			b.finished.Store(true)
			return nil
		}
		b.slotToDownload.Store(block.Block.Slot - 1)
	}
	return nil
}

// trySkipToExistingBlock attempts to skip ahead if the expected block already exists in the database.
func (b *BackwardBeaconDownloader) trySkipToExistingBlock(ctx context0.Context) error {
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

	refreshTicker := time.NewTicker(5 * time.Second)
	defer refreshTicker.Stop()

	for {
		// Periodically refresh frozen block counts
		select {
		case <-refreshTicker.C:
			if b.sn != nil {
				clFrozenBlocks = b.sn.SegmentsMax()
			}
			if b.engine != nil && b.engine.SupportInsertion() {
				elFrozenBlocks = b.engine.FrozenBlocks(ctx)
			}
		default:
		}

		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, b.expectedRoot)
		if err != nil {
			return err
		}
		if slot == nil || *slot == 0 {
			break
		}

		if !b.canSkipSlot(ctx, tx, elFrozenBlocks, clFrozenBlocks, *slot) {
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

		// Clean up non-canonical slots
		newSlot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, b.expectedRoot)
		if err != nil {
			return err
		}
		if newSlot == nil || *newSlot == 0 {
			continue
		}
		for i := *newSlot + 1; i < *slot; i++ {
			tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(i))
		}
	}

	return tx.Commit()
}

// canSkipSlot checks if we can skip to an existing block at the given slot.
func (b *BackwardBeaconDownloader) canSkipSlot(ctx context0.Context, tx kv.Tx, elFrozenBlocks, clFrozenBlocks, slot uint64) bool {
	if slot <= clFrozenBlocks {
		return false
	}

	if b.engine == nil || !b.engine.SupportInsertion() {
		return true
	}

	blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, b.expectedRoot)
	if err != nil || blockHash == (common.Hash{}) {
		return false
	}

	blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, b.expectedRoot)
	if err != nil {
		log.Warn("Failed to read execution block number", "err", err)
	}
	if err != nil || blockNumber == nil {
		return false
	}

	// Check if block is already in the collector
	if b.blockChecker != nil && b.blockChecker.HasBlock(*blockNumber) {
		return true
	}

	if *blockNumber < elFrozenBlocks {
		return true
	}

	has, err := b.engine.HasBlock(ctx, blockHash)
	return err == nil && has
}
