package bodydownload

import (
	"context"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
)

const BlockBufferSize = 1024

// UpdateFromDb reads the state of the database and refreshes the state of the body download
func (bd *BodyDownload) UpdateFromDb(db ethdb.Database) (headHeight uint64, headHash common.Hash, headTd256 *uint256.Int, err error) {
	var headerProgress, bodyProgress uint64
	headerProgress, err = stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return 0, common.Hash{}, nil, err
	}
	bd.maxProgress = headerProgress + 1
	bodyProgress, err = stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return 0, common.Hash{}, nil, err
	}
	bd.lock.Lock()
	defer bd.lock.Unlock()
	// Resetting for requesting a new range of blocks
	bd.requestedLow = bodyProgress + 1
	bd.lowWaitUntil = 0
	bd.requestHigh = bd.requestedLow + (bd.outstandingLimit / 2)
	bd.requestedMap = make(map[DoubleHash]uint64)
	bd.delivered.Clear()
	bd.deliveredCount = 0
	bd.wastedCount = 0
	for i := 0; i < len(bd.deliveries); i++ {
		bd.deliveries[i] = nil
		bd.requests[i] = nil
	}
	bd.peerMap = make(map[string]int)
	headHeight = bodyProgress
	headHash = rawdb.ReadHeaderByNumber(db, headHeight).Hash()
	var headTd *big.Int
	headTd, err = rawdb.ReadTd(db, headHash, headHeight)
	if err != nil {
		return 0, common.Hash{}, nil, fmt.Errorf("reading total difficulty for head height %d and hash %x: %w", headHeight, headHash, headTd)
	}
	if headTd == nil {
		headTd = new(big.Int)
	}
	headTd256 = new(uint256.Int)
	headTd256.SetFromBig(headTd)
	return headHeight, headHash, headTd256, nil
}

func (bd *BodyDownload) RequestMoreBodies(db ethdb.Database, blockNum uint64, currentTime uint64, blockPropagator adapter.BlockPropagator) (*BodyRequest, uint64) {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	if blockNum < bd.requestedLow {
		blockNum = bd.requestedLow
	}
	var bodyReq *BodyRequest
	blockNums := make([]uint64, 0, BlockBufferSize)
	hashes := make([]common.Hash, 0, BlockBufferSize)
	for ; len(blockNums) < BlockBufferSize && bd.requestedLow <= bd.maxProgress; blockNum++ {
		// Check if we reached highest allowed request block number, and turn back
		if blockNum >= bd.requestedLow+bd.outstandingLimit || blockNum >= bd.maxProgress {
			blockNum = 0
			break // Avoid tight loop
		}
		if bd.delivered.Contains(blockNum) {
			// Already delivered, no need to request
			continue
		}
		req := bd.requests[blockNum-bd.requestedLow]
		if req != nil {
			if currentTime < req.waitUntil {
				continue
			}
			bd.peerMap[string(req.peerID)]++
			bd.requests[blockNum-bd.requestedLow] = nil
		}
		var hash common.Hash
		var header *types.Header
		var err error
		request := true
		if bd.deliveries[blockNum-bd.requestedLow] != nil {
			// If this block was requested before, we don't need to fetch the headers from the database the second time
			header = bd.deliveries[blockNum-bd.requestedLow].Header()
			hash = header.Hash()
		} else {
			hash, err = rawdb.ReadCanonicalHash(db, blockNum)
			if err == nil {
				header = rawdb.ReadHeader(db, hash, blockNum)
			} else {
				log.Error("Could not find canonical header", "block number", blockNum)
			}
			if header != nil {
				if block := bd.prefetchedBlocks.Pop(hash); block != nil {
					// Block is prefetched, no need to request
					bd.deliveries[blockNum-bd.requestedLow] = block

					// Calculate the TD of the block (it's not imported yet, so block.Td is not valid)
					var td *big.Int
					if parent, err := rawdb.ReadTd(db, block.ParentHash(), block.NumberU64()-1); err != nil {
						log.Error("Failed to ReadTd", "err", err, "number", block.NumberU64()-1, "hash", block.ParentHash())
					} else if parent != nil {
						td = new(big.Int).Add(block.Difficulty(), parent)
						go blockPropagator.BroadcastNewBlock(context.Background(), block, td)
					} else {
						log.Error("Propagating dangling block", "number", block.Number(), "hash", hash)
					}
					request = false
				} else {
					bd.deliveries[blockNum-bd.requestedLow] = types.NewBlockWithHeader(header) // Block without uncles and transactions
					if header.UncleHash != types.EmptyUncleHash || header.TxHash != types.EmptyRootHash {
						var doubleHash DoubleHash
						copy(doubleHash[:], header.UncleHash.Bytes())
						copy(doubleHash[common.HashLength:], header.TxHash.Bytes())
						bd.requestedMap[doubleHash] = blockNum
					} else {
						request = false
					}
				}
			}
		}
		if header == nil {
			log.Error("Header not found", "block number", blockNum)
			panic("")
		} else if request {
			blockNums = append(blockNums, blockNum)
			hashes = append(hashes, hash)
		} else {
			// Both uncleHash and txHash are empty (or block is prefetched), no need to request
			bd.delivered.Add(blockNum)
		}
	}
	if len(blockNums) > 0 {
		bodyReq = &BodyRequest{BlockNums: blockNums, Hashes: hashes}
		for _, blockNum := range blockNums {
			bd.requests[blockNum-bd.requestedLow] = bodyReq
		}
	}
	return bodyReq, blockNum
}

func (bd *BodyDownload) RequestSent(bodyReq *BodyRequest, timeWithTimeout uint64, peer []byte) {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	for _, blockNum := range bodyReq.BlockNums {
		if blockNum < bd.requestedLow {
			continue
		}
		req := bd.requests[blockNum-bd.requestedLow]
		if req != nil {
			bd.requests[blockNum-bd.requestedLow].waitUntil = timeWithTimeout
			bd.requests[blockNum-bd.requestedLow].peerID = peer
		}
	}
}

// DeliverBodies takes the block body received from a peer and adds it to the various data structures
func (bd *BodyDownload) DeliverBodies(bodies []*eth.BlockBody) (int, int) {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	reqMap := make(map[uint64]*BodyRequest)
	var delivered, undelivered int
	for _, body := range bodies {
		uncleHash := types.CalcUncleHash(body.Uncles)
		txHash := types.DeriveSha(types.Transactions(body.Transactions))
		var doubleHash DoubleHash
		copy(doubleHash[:], uncleHash.Bytes())
		copy(doubleHash[common.HashLength:], txHash.Bytes())
		// Block numbers are added to the bd.delivered bitmap here, only for blocks for which the body has been received, and their double hashes are present in the bd.requesredMap
		// Also, block numbers can be added to bd.delivered for empty blocks, above
		if blockNum, ok := bd.requestedMap[doubleHash]; ok {
			bd.delivered.Add(blockNum)
			bd.deliveries[blockNum-bd.requestedLow] = bd.deliveries[blockNum-bd.requestedLow].WithBody(body.Transactions, body.Uncles)
			req := bd.requests[blockNum-bd.requestedLow]
			if req != nil {
				if _, ok := reqMap[req.BlockNums[0]]; !ok {
					reqMap[req.BlockNums[0]] = req
				}
			}
			delete(bd.requestedMap, doubleHash) // Delivered, cleaning up
			delivered++
		} else {
			undelivered++
		}
	}
	// Clean up the requests
	for _, req := range reqMap {
		for _, blockNum := range req.BlockNums {
			bd.requests[blockNum-bd.requestedLow] = nil
		}
	}
	return delivered, undelivered
}

func (bd *BodyDownload) DeliverySize(delivered float64, wasted float64) {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	bd.deliveredCount += delivered
	bd.wastedCount += wasted
}

func (bd *BodyDownload) GetDeliveries() []*types.Block {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	var i uint64
	for i = 0; !bd.delivered.IsEmpty() && bd.requestedLow+i == bd.delivered.Minimum(); i++ {
		bd.delivered.Remove(bd.requestedLow + i)
	}
	// Move the deliveries back
	// bd.requestedLow can only be moved forward if there are consecutive block numbers present in the bd.delivered map
	var d []*types.Block
	if i > 0 {
		d = make([]*types.Block, i)
		copy(d, bd.deliveries[:i])
		copy(bd.deliveries[:], bd.deliveries[i:])
		copy(bd.requests[:], bd.requests[i:])
		for j := len(bd.deliveries) - int(i); j < len(bd.deliveries); j++ {
			bd.deliveries[j] = nil
			bd.requests[j] = nil
		}
		bd.requestedLow += i
	}
	return d
}

func (bd *BodyDownload) DeliveryCounts() (float64, float64) {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	return bd.deliveredCount, bd.wastedCount
}

func (bd *BodyDownload) GetPenaltyPeers() [][]byte {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	peers := make([][]byte, len(bd.peerMap))
	i := 0
	for p := range bd.peerMap {
		peers[i] = []byte(p)
		i++
	}
	return peers
}

func (bd *BodyDownload) PrintPeerMap() {
	bd.lock.Lock()
	defer bd.lock.Unlock()
	fmt.Printf("---------------------------\n")
	for p, n := range bd.peerMap {
		fmt.Printf("%s = %d\n", p, n)
	}
	fmt.Printf("---------------------------\n")
	bd.peerMap = make(map[string]int)
}

func (bd *BodyDownload) AddToPrefetch(block *types.Block) {
	if hash := types.CalcUncleHash(block.Uncles()); hash != block.UncleHash() {
		log.Warn("Propagated block has invalid uncles", "have", hash, "exp", block.UncleHash())
		return
	}
	if hash := types.DeriveSha(block.Transactions()); hash != block.TxHash() {
		log.Warn("Propagated block has invalid body", "have", hash, "exp", block.TxHash())
		return
	}
	bd.prefetchedBlocks.Add(block)
}
