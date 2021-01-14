package bodydownload

import (
	//"context"
	//"github.com/ledgerwatch/turbo-geth/common/dbutils"

	"container/list"
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
	bd.requestedMap = make(map[DoubleHash]uint64)
	for i := 0; i < len(bd.deliveries); i++ {
		bd.deliveries[i] = nil
	}
	//fmt.Printf("UpdateFromDB =====> Resetting required to range [%d - %d]\n", bodyProgress+1, headerProgress+1)
	bd.required.AddRange(bodyProgress+1, headerProgress+1)
	bd.requestedLow = bodyProgress + 1
	// Channel needs to be big enough to allow the producer to finish writing and unblock in any case
	bd.blockChannel = make(chan *types.Block, BlockBufferSize+bd.outstandingLimit)
	return nil
}

func (bd *BodyDownload) ApplyBodyRequest(currentTime, timeout uint64, bodyReq *BodyRequest) *time.Timer {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	var prevTopTime uint64
	if bd.requestQueue.Len() > 0 {
		prevTopTime = bd.requestQueue.Front().Value.(RequestQueueItem).waitUntil
	}
	bd.requestQueue.PushBack(RequestQueueItem{requested: bodyReq.requested, waitUntil: currentTime + timeout})
	bd.requested.Or(bodyReq.requested)
	bd.resetRequestQueueTimer(prevTopTime, currentTime)
	return bd.RequestQueueTimer
}

func (bd *BodyDownload) CancelBodyRequest(bodyReq *BodyRequest) {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	bd.required.Or(bodyReq.requested)
}

func (bd *BodyDownload) CancelExpiredRequests(currentTime uint64) *time.Timer {
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
				//fmt.Printf("CancelExpiredRequests with item.requested.Minimum %d, bd.requested.Minimum %d\n", item.requested.Minimum(), bd.requested.Minimum())
				item.requested.And(bd.requested)    // Compute the intersection (not delivered but timed out blocks) into item.requested
				item.requested.AndNot(bd.delivered) // Remove delivered blocks
				if !item.requested.IsEmpty() {
					item.requested.RemoveRange(0, bd.requestedLow)
				}
				if !item.requested.IsEmpty() {
					bd.requested.AndNot(item.requested) // Remove the intersection from the requsted
					bd.required.Or(item.requested)      // Add the intersection back to required
				}
			}
		}
		//} else {
		//	fmt.Printf("=========> bd.requestQueue is empty\n")
	}
	bd.resetRequestQueueTimer(prevTopTime, currentTime)
	return bd.RequestQueueTimer
}

func (bd *BodyDownload) RequestMoreBodies(db ethdb.Database) *BodyRequest {
	bd.lock.Lock()
	defer bd.lock.Unlock()

	//if bd.required.IsEmpty() {
	//	fmt.Printf("========> bd.required is empty\n")
	//} else {
	//	fmt.Printf("==========> bd.required min %d, max %d\n", bd.required.Minimum(), bd.required.Maximum())
	//}
	var bodyReq *BodyRequest
	blockNums := make([]uint64, 0, BlockBufferSize)
	hashes := make([]common.Hash, 0, BlockBufferSize)
	reqBitmap := roaring64.New()
	empties := roaring64.New() // Accumulate block numbers for empty blocks so we do not modidy bd.required (this would invalidate the iterator)
	it := bd.required.Iterator()
	for len(blockNums) < BlockBufferSize && it.HasNext() {
		b := it.Next()
		if b >= bd.requestedLow+bd.outstandingLimit {
			// Too many outstanding blocks requested
			break
		}
		var hash common.Hash
		var header *types.Header
		var err error
		//if b < bd.requestedLow {
		//	fmt.Printf("b=%d, bd.requestedLow=%d\n", b, bd.requestedLow)
		//}
		if bd.deliveries[b-bd.requestedLow] != nil {
			header = bd.deliveries[b-bd.requestedLow].Header()
			hash = header.Hash()
		} else {
			hash, err = rawdb.ReadCanonicalHash(db, b)
			if err == nil {
				header = rawdb.ReadHeader(db, hash, b)
			} else {
				log.Error("Could not find canonical header", "block number", b)
			}
			if header != nil {
				bd.deliveries[b-bd.requestedLow] = types.NewBlockWithHeader(header) // Block without uncles and transactions
				if header.UncleHash != types.EmptyUncleHash || header.TxHash != types.EmptyRootHash {
					var doubleHash DoubleHash
					copy(doubleHash[:], header.UncleHash.Bytes())
					copy(doubleHash[common.HashLength:], header.TxHash.Bytes())
					bd.requestedMap[doubleHash] = b
				}
			}
		}
		if header == nil {
			log.Error("Header not found", "block number", b)
			panic("")
		} else if header.UncleHash != types.EmptyUncleHash || header.TxHash != types.EmptyRootHash {
			blockNums = append(blockNums, b)
			hashes = append(hashes, hash)
			reqBitmap.Add(b)
		} else {
			// Both uncleHash and txHash are empty, no need to request
			bd.delivered.Add(b)
			empties.Add(b)
		}
	}
	if len(blockNums) > 0 {
		bodyReq = &BodyRequest{BlockNums: blockNums, Hashes: hashes, requested: reqBitmap}
		bd.required.AndNot(reqBitmap)
	}
	if !empties.IsEmpty() {
		bd.required.AndNot(empties)
	}
	return bodyReq
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
	copy(doubleHash[:], uncleHash.Bytes())
	copy(doubleHash[common.HashLength:], txHash.Bytes())
	bd.lock.Lock()
	defer bd.lock.Unlock()
	// Block numbers are added to the bd.delivered bitmap here, only for blocks for which the body has been received, and their double hashes are present in the bd.requesredMap
	// Also, block numbers can be added to bd.delivered for empty blocks, above
	if blockNum, ok := bd.requestedMap[doubleHash]; ok {
		bd.delivered.Add(blockNum)
		bd.requested.Remove(blockNum)
		bd.required.Remove(blockNum) // This is not usually required, but helps deal with the situations when old request is cancelled just before blocks delivered that contained in that request
		bd.deliveries[blockNum-bd.requestedLow] = bd.deliveries[blockNum-bd.requestedLow].WithBody(body.Transactions, body.Uncles)
		delete(bd.requestedMap, doubleHash) // Delivered, cleaning up
		return blockNum, true
	}
	return 0, false
}

func (bd *BodyDownload) FeedDeliveries() {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	//if !bd.requested.IsEmpty() {
	//	fmt.Printf("FeedDeliveries with bd.requested.Minimum %d\n", bd.requested.Minimum())
	//}
	var i uint64
	for i = 0; !bd.delivered.IsEmpty() && bd.requestedLow+i == bd.delivered.Minimum(); i++ {
		bd.blockChannel <- bd.deliveries[i] // This is delivery
		bd.delivered.Remove(bd.requestedLow + i)
	}
	// Move the deliveries back
	// bd.requestedLow can only be moved forward if there are consequitive block numbers present in the bd.delivered map
	if i > 0 {
		copy(bd.deliveries[:], bd.deliveries[i:])
		for j := len(bd.deliveries) - int(i); j < len(bd.deliveries); j++ {
			bd.deliveries[j] = nil
		}
		//fmt.Printf("Delivered bodies for blocks [%d - %d]\n", bd.requestedLow, bd.requestedLow+i)
		bd.requestedLow += i
	}
}

func (bd *BodyDownload) PrepareStageData() chan *types.Block {
	return bd.blockChannel
}
