package bodydownload

import (
	//"context"
	//"github.com/ledgerwatch/turbo-geth/common/dbutils"

	"container/list"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

const BlockBufferSize = 128

// UpdateFromDb reads the state of the database and refreshes the state of the body download
func (bd *BodyDownload) UpdateFromDb(db ethdb.Database) error {
	var headerProgress, bodyProgress uint64
	var err error
	headerProgress, err = stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return err
	}
	bodyProgress, err = stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return err
	}
	bd.lock.Lock()
	defer bd.lock.Unlock()
	if bd.blockChannel != nil {
		close(bd.blockChannel)
	}
	// Resetting for requesting a new range of blocks
	bd.required.Clear()
	bd.requested.Clear()
	bd.delivered.Clear()
	bd.requestQueue = list.New()
	bd.RequestQueueTimer = time.NewTimer(time.Hour)
	bd.requestedMap = make(map[DoubleHash]*types.Header)
	fmt.Printf("UpdateFromDB =====> Resetting required to range [%d - %d]\n", bodyProgress+1, headerProgress+1)
	bd.required.AddRange(bodyProgress+1, headerProgress+1)
	bd.requestedLow = bodyProgress + 1
	bd.requestedHigh = bodyProgress + 1
	// Channel needs to be big enough to allow the producer to finish writing and unblock in any case
	bd.blockChannel = make(chan *types.Block, BlockBufferSize+bd.outstandingLimit)
	return nil
}

func (bd *BodyDownload) RequestMoreBodies(currentTime, timeout uint64, db ethdb.Database) ([]*BodyRequest, *time.Timer) {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	// Give up the requests that were timed out
	var prevTopTime uint64
	if bd.requestQueue.Len() > 0 {
		prevTopTime = bd.requestQueue.Front().Value.(RequestQueueItem).waitUntil
		for peek := bd.requestQueue.Front(); peek != nil && peek.Value.(RequestQueueItem).waitUntil <= currentTime; peek = bd.requestQueue.Front() {
			bd.requestQueue.Remove(peek)
			item := peek.Value.(RequestQueueItem)
			// Check if the blocks are still requested (outstanding, not delivered), and if yes, add the blocks back to required
			if item.requested.Intersects(bd.requested) {
				item.requested.And(bd.requested)
				bd.required.Or(item.requested)
			}
		}
	}
	if bd.required.IsEmpty() {
		fmt.Printf("========> bd.required is empty\n")
	} else {
		fmt.Printf("==========> bd.required min %d, max %d\n", bd.required.Minimum(), bd.required.Maximum())
	}
	var buf [BlockBufferSize]uint64
	var requests []*BodyRequest
	it := bd.required.ManyIterator()
	for bufLen := it.NextMany(buf[:]); bufLen > 0; bufLen = it.NextMany(buf[:]) {
		if buf[bufLen-1] > bd.requestedHigh {
			// Check if there are too many requests outstranding
			if buf[bufLen-1] >= bd.requestedLow+bd.outstandingLimit {
				fmt.Printf("=========> bd.requestedLow = %d, bd.outstadingLimit = %d, buf[bufLen-1] = %d\n", bd.requestedLow, bd.outstandingLimit, buf[bufLen-1])
				break
			}
			bd.requestedHigh = buf[bufLen-1]
		}
		bodyReq := &BodyRequest{BlockNums: make([]uint64, 0, bufLen), Hashes: make([]common.Hash, 0, bufLen)}
		reqBitmap := roaring64.New()
		for _, b := range buf[:bufLen] {
			bd.required.Remove(b)
			bd.requested.Add(b)
			hash, err := rawdb.ReadCanonicalHash(db, b)
			if err == nil {
				header := rawdb.ReadHeader(db, hash, b)
				if header == nil {
					log.Error("Header not found", "block number", b)
				} else if header.UncleHash != types.EmptyUncleHash || header.TxHash != types.EmptyRootHash {
					var doubleHash DoubleHash
					copy(doubleHash[:], header.UncleHash.Bytes())
					copy(doubleHash[common.HashLength:], header.TxHash.Bytes())
					bd.requestedMap[doubleHash] = header
					bodyReq.BlockNums = append(bodyReq.BlockNums, b)
					bodyReq.Hashes = append(bodyReq.Hashes, hash)
					reqBitmap.Add(b)
				} else {
					// Both uncleHash and txHash are empty, no need to request
					blockNum := header.Number.Uint64()
					bd.delivered.Add(blockNum)
					bd.requested.Remove(blockNum)
					bd.deliveries[blockNum-bd.requestedLow] = types.NewBlockWithHeader(header) // Block without uncles and transactions
				}
			} else {
				log.Error("Could not find canonical header", "block number", b)
			}
		}
		if len(bodyReq.BlockNums) > 0 {
			requests = append(requests, bodyReq)
			bd.requestQueue.PushBack(RequestQueueItem{requested: reqBitmap, waitUntil: currentTime + timeout})
		}
		// Iterator becomes invalid when bitmap is modified, so we re-create
		it = bd.required.ManyIterator()
	}
	bd.resetRequestQueueTimer(prevTopTime, currentTime)
	return requests, bd.RequestQueueTimer
}

func (bd *BodyDownload) resetRequestQueueTimer(prevTopTime, currentTime uint64) {
	var nextTopTime uint64
	if bd.requestQueue.Len() > 0 {
		nextTopTime = bd.requestQueue.Front().Value.(RequestQueueItem).waitUntil
	}
	if nextTopTime == prevTopTime {
		return // Nothing changed
	}
	if nextTopTime <= currentTime {
		nextTopTime = currentTime
	}
	bd.RequestQueueTimer.Stop()
	//fmt.Printf("Recreating RequestQueueTimer for delay %d seconds\n", nextTopTime-currentTime)
	bd.RequestQueueTimer = time.NewTimer(time.Duration(nextTopTime-currentTime) * time.Second)
}

// DeliverBody takes the block body received from a peer and adds it to the various data structures
func (bd *BodyDownload) DeliverBody(body *eth.BlockBody) (uint64, bool) {
	uncleHash := types.CalcUncleHash(body.Uncles)
	txHash := types.DeriveSha(types.Transactions(body.Transactions))
	var doubleHash DoubleHash
	copy(doubleHash[:], uncleHash[:])
	copy(doubleHash[common.HashLength:], txHash[:])
	bd.lock.Lock()
	defer bd.lock.Unlock()
	if header, ok := bd.requestedMap[doubleHash]; ok {
		blockNum := header.Number.Uint64()
		bd.delivered.Add(blockNum)
		bd.requested.Remove(blockNum)
		bd.deliveries[blockNum-bd.requestedLow] = types.NewBlockWithHeader(header).WithBody(body.Transactions, body.Uncles)
		delete(bd.requestedMap, doubleHash) // Delivered, cleaning up
		return blockNum, true
	}
	return 0, false
}

func (bd *BodyDownload) FeedDeliveries() {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	var i uint64
	for i = 0; !bd.delivered.IsEmpty() && bd.requestedLow+i == bd.delivered.Minimum(); i++ {
		bd.blockChannel <- bd.deliveries[i] // This is delivery
		bd.required.Remove(bd.requestedLow + i)
		bd.delivered.Remove(bd.requestedLow + i)
	}
	// Move the deliveries back
	if i > 0 {
		copy(bd.deliveries[:], bd.deliveries[i:])
		fmt.Printf("Delivered bodies for blocks [%d - %d]\n", bd.requestedLow, bd.requestedLow+i)
		bd.requestedLow += i
	}
}

func (bd *BodyDownload) PrepareStageData() chan *types.Block {
	return bd.blockChannel
}
