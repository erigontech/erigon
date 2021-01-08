package bodydownload

import (
	//"context"
	//"github.com/ledgerwatch/turbo-geth/common/dbutils"

	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

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
	bd.required.AddRange(bodyProgress+1, headerProgress+1)
	bd.requestedLow = headerProgress + 1
	bd.requestedHigh = headerProgress + 1
	return nil
}

func (bd *BodyDownload) RequestMoreBodies(currentTime uint64) ([]*BodyRequest, *time.Timer) {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	// Give up the requests that were timed out
	if bd.requestQueue.Len() > 0 {
		var prevTopTime uint64 = bd.requestQueue.Front().Value.(RequestQueueItem).waitUntil
		for peek := bd.requestQueue.Front(); peek != nil && peek.Value.(RequestQueueItem).waitUntil <= currentTime; peek = bd.requestQueue.Front() {
			bd.requestQueue.Remove(peek)
			item := peek.Value.(RequestQueueItem)
			// Check if the blocks are still requested (outstanding, not delivered), and if yes, add the blocks back to required
			if item.requested.Intersects(bd.requested) {
				item.requested.And(bd.requested)
				bd.required.Or(item.requested)
			}
		}
		bd.resetRequestQueueTimer(prevTopTime, currentTime)
	}
	// Check if there are too many requests outstranding
	if bd.requestedLow+bd.outstandingLimit >= bd.requestedHigh {
		return nil, bd.RequestQueueTimer
	}
	var buf [128]uint64
	var requests []*BodyRequest
	it := bd.required.ManyIterator()
	for bufLen := it.NextMany(buf[:]); bufLen > 0; bufLen = it.NextMany(buf[:]) {
		if bd.requestedLow+bd.outstandingLimit >= bd.requestedHigh {
			break
		}
		bodyReq := &BodyRequest{BlockNums: make([]uint64, bufLen)}
		copy(bodyReq.BlockNums[:], buf[:])
		requests = append(requests, bodyReq)
		if buf[bufLen-1] > bd.requestedHigh {
			bd.requestedHigh = buf[bufLen-1]
		}
		for _, b := range buf[:bufLen] {
			bd.required.Remove(b)
			bd.requested.Add(b)
		}
	}
	return requests, nil
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

func (bd *BodyDownload) DeliverBody(body *eth.BlockBody) {
	uncleHash := types.CalcUncleHash(body.Uncles)
	txHash := types.DeriveSha(types.Transactions(body.Transactions))
	var doubleHash DoubleHash
	copy(doubleHash[:], uncleHash[:])
	copy(doubleHash[common.HashLength:], txHash[:])
	if header, ok := bd.requestedMap[doubleHash]; ok {
		blockNum := header.Number.Uint64()
		bd.delivered.Add(blockNum)
		bd.requested.Remove(blockNum)
	}
}
