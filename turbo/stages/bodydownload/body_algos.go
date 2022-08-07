package bodydownload

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"github.com/ledgerwatch/erigon/turbo/services"
)

const BlockBufferSize = 128

// UpdateFromDb reads the state of the database and refreshes the state of the body download
func (bd *BodyDownload) UpdateFromDb(db kv.Tx) (headHeight uint64, headHash common.Hash, headTd256 *uint256.Int, err error) {
	var headerProgress, bodyProgress uint64
	headerProgress, err = stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return 0, common.Hash{}, nil, err
	}
	bodyProgress, err = stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return 0, common.Hash{}, nil, err
	}
	bd.maxProgress = headerProgress + 1
	// Resetting for requesting a new range of blocks
	bd.requestedLow = bodyProgress + 1
	bd.lowWaitUntil = 0
	bd.requestHigh = bd.requestedLow + (bd.outstandingLimit / 2)
	bd.requestedMap = make(map[DoubleHash]uint64)
	bd.delivered.Clear()
	bd.deliveredCount = 0
	bd.wastedCount = 0
	for i := 0; i < len(bd.deliveriesH); i++ {
		bd.deliveriesH[i] = nil
		bd.deliveriesB[i] = nil
		bd.requests[i] = nil
	}
	bd.peerMap = make(map[[64]byte]int)
	headHeight = bodyProgress
	headHash, err = rawdb.ReadCanonicalHash(db, headHeight)
	if err != nil {
		return 0, common.Hash{}, nil, err
	}
	var headTd *big.Int
	headTd, err = rawdb.ReadTd(db, headHash, headHeight)
	if err != nil {
		return 0, common.Hash{}, nil, fmt.Errorf("reading total difficulty for head height %d and hash %x: %d, %w", headHeight, headHash, headTd, err)
	}
	if headTd == nil {
		headTd = new(big.Int)
	}
	headTd256 = new(uint256.Int)
	overflow := headTd256.SetFromBig(headTd)
	if overflow {
		return 0, [32]byte{}, nil, fmt.Errorf("headTd higher than 2^256-1")
	}
	return headHeight, headHash, headTd256, nil
}

// RequestMoreBodies - returns nil if nothing to request
func (bd *BodyDownload) RequestMoreBodies(tx kv.RwTx, blockReader services.FullBlockReader, blockNum uint64, currentTime uint64, blockPropagator adapter.BlockPropagator) (*BodyRequest, uint64, error) {
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
			bd.peerMap[req.peerID]++
			bd.requests[blockNum-bd.requestedLow] = nil
		}
		var hash common.Hash
		var header *types.Header
		var err error
		request := true
		if bd.deliveriesH[blockNum-bd.requestedLow] != nil {
			// If this block was requested before, we don't need to fetch the headers from the database the second time
			header = bd.deliveriesH[blockNum-bd.requestedLow]
			if header == nil {
				return nil, 0, fmt.Errorf("header not found: %w, blockNum=%d, trace=%s", err, blockNum, dbg.Stack())
			}
			hash = header.Hash()
		} else {
			hash, err = rawdb.ReadCanonicalHash(tx, blockNum)
			if err != nil {
				return nil, 0, fmt.Errorf("could not find canonical header: %w, blockNum=%d, trace=%s", err, blockNum, dbg.Stack())
			}

			header, err = blockReader.Header(context.Background(), tx, hash, blockNum)
			if err != nil {
				return nil, 0, fmt.Errorf("header not found: %w, blockNum=%d, trace=%s", err, blockNum, dbg.Stack())
			}
			if header == nil {
				return nil, 0, fmt.Errorf("header not found: blockNum=%d, hash=%x, trace=%s", blockNum, hash, dbg.Stack())
			}

			if block := bd.prefetchedBlocks.Pop(hash); block != nil {
				// Block is prefetched, no need to request
				bd.deliveriesH[blockNum-bd.requestedLow] = block.Header()
				bd.deliveriesB[blockNum-bd.requestedLow] = block.RawBody()

				// Calculate the TD of the block (it's not imported yet, so block.Td is not valid)
				if parent, err := rawdb.ReadTd(tx, block.ParentHash(), block.NumberU64()-1); err != nil {
					log.Error("Failed to ReadTd", "err", err, "number", block.NumberU64()-1, "hash", block.ParentHash())
				} else if parent != nil {
					if block.Difficulty().Sign() != 0 { // don't propagate proof-of-stake blocks
						td := new(big.Int).Add(block.Difficulty(), parent)
						go blockPropagator(context.Background(), block, td)
					}
				} else {
					log.Error("Propagating dangling block", "number", block.Number(), "hash", hash)
				}
				request = false
			} else {
				bd.deliveriesH[blockNum-bd.requestedLow] = header
				if header.UncleHash != types.EmptyUncleHash || header.TxHash != types.EmptyRootHash {
					// Perhaps we already have this block
					block = rawdb.ReadBlock(tx, hash, blockNum)
					if block == nil {
						var doubleHash DoubleHash
						copy(doubleHash[:], header.UncleHash.Bytes())
						copy(doubleHash[common.HashLength:], header.TxHash.Bytes())
						bd.requestedMap[doubleHash] = blockNum
					} else {
						bd.deliveriesB[blockNum-bd.requestedLow] = block.RawBody()
						request = false
					}
				} else {
					bd.deliveriesB[blockNum-bd.requestedLow] = &types.RawBody{}
					request = false
				}
			}
		}
		if request {
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
	return bodyReq, blockNum, nil
}

func (bd *BodyDownload) RequestSent(bodyReq *BodyRequest, timeWithTimeout uint64, peer [64]byte) {
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
func (bd *BodyDownload) DeliverBodies(txs *[][][]byte, uncles *[][]*types.Header, lenOfP2PMsg uint64, peerID [64]byte) {
	bd.deliveryCh <- Delivery{txs: txs, uncles: uncles, lenOfP2PMessage: lenOfP2PMsg, peerID: peerID}

	select {
	case bd.DeliveryNotify <- struct{}{}:
	default:
	}
}

// RawTransaction implements core/types.DerivableList interface for hashing
type RawTransactions [][]byte

func (rt RawTransactions) Len() int {
	return len(rt)
}

// EncodeIndex is part of core/types.DerivableList
// It strips the transaction envelope from the transaction RLP
func (rt RawTransactions) EncodeIndex(i int, w *bytes.Buffer) {
	if len(rt[i]) > 0 {
		firstByte := rt[i][0]
		if firstByte >= 128 && firstByte < 184 {
			// RLP string < 56 bytes long, just strip first byte
			w.Write(rt[i][1:]) //nolint:errcheck
			return
		} else if firstByte >= 184 && firstByte < 192 {
			// RLP string >= 56 bytes long, firstByte-183 is the length of encoded size
			w.Write(rt[i][1+firstByte-183:]) //nolint:errcheck
			return
		}
	}
	w.Write(rt[i]) //nolint:errcheck
}

func (bd *BodyDownload) doDeliverBodies() (err error) {
Loop:
	for {
		var delivery Delivery

		select { // read as much as we can, but don't wait
		case delivery = <-bd.deliveryCh:
		default:
			break Loop
		}

		if delivery.txs == nil {
			log.Warn("nil transactions delivered", "peer_id", delivery.peerID, "p2p_msg_len", delivery.lenOfP2PMessage)
		}
		if delivery.uncles == nil {
			log.Warn("nil uncles delivered", "peer_id", delivery.peerID, "p2p_msg_len", delivery.lenOfP2PMessage)
		}
		if delivery.txs == nil || delivery.uncles == nil {
			log.Debug("delivery body processing has been skipped due to nil tx|data")
			continue
		}

		reqMap := make(map[uint64]*BodyRequest)
		txs, uncles, lenOfP2PMessage, _ := *delivery.txs, *delivery.uncles, delivery.lenOfP2PMessage, delivery.peerID
		var delivered, undelivered int

		for i := range txs {
			uncleHash := types.CalcUncleHash(uncles[i])
			txHash := types.DeriveSha(RawTransactions(txs[i]))
			var doubleHash DoubleHash
			copy(doubleHash[:], uncleHash.Bytes())
			copy(doubleHash[common.HashLength:], txHash.Bytes())

			// Block numbers are added to the bd.delivered bitmap here, only for blocks for which the body has been received, and their double hashes are present in the bd.requestedMap
			// Also, block numbers can be added to bd.delivered for empty blocks, above
			blockNum, ok := bd.requestedMap[doubleHash]
			if !ok {
				undelivered++
				continue
			}
			req := bd.requests[blockNum-bd.requestedLow]
			if req != nil {
				if _, ok := reqMap[req.BlockNums[0]]; !ok {
					reqMap[req.BlockNums[0]] = req
				}
			}
			delete(bd.requestedMap, doubleHash) // Delivered, cleaning up

			bd.deliveriesB[blockNum-bd.requestedLow] = &types.RawBody{Transactions: txs[i], Uncles: uncles[i]}
			bd.delivered.Add(blockNum)
			delivered++
		}
		// Clean up the requests
		for _, req := range reqMap {
			for _, blockNum := range req.BlockNums {
				bd.requests[blockNum-bd.requestedLow] = nil
			}
		}
		total := delivered + undelivered
		if total > 0 {
			// Approximate numbers
			bd.DeliverySize(float64(lenOfP2PMessage)*float64(delivered)/float64(delivered+undelivered), float64(lenOfP2PMessage)*float64(undelivered)/float64(delivered+undelivered))
		}
	}
	return nil
}

func (bd *BodyDownload) DeliverySize(delivered float64, wasted float64) {
	bd.deliveredCount += delivered
	bd.wastedCount += wasted
}

func (bd *BodyDownload) GetDeliveries() ([]*types.Header, []*types.RawBody, error) {
	err := bd.doDeliverBodies() // TODO: join this 2 funcs and simplify
	if err != nil {
		return nil, nil, err
	}

	var i uint64
	for i = 0; !bd.delivered.IsEmpty() && bd.requestedLow+i == bd.delivered.Minimum(); i++ {
		bd.delivered.Remove(bd.requestedLow + i)
	}
	// Move the deliveries back
	// bd.requestedLow can only be moved forward if there are consecutive block numbers present in the bd.delivered map
	var headers []*types.Header
	var rawBodies []*types.RawBody
	if i > 0 {
		headers = make([]*types.Header, i)
		rawBodies = make([]*types.RawBody, i)
		copy(headers, bd.deliveriesH[:i])
		copy(rawBodies, bd.deliveriesB[:i])
		copy(bd.deliveriesH, bd.deliveriesH[i:])
		copy(bd.deliveriesB, bd.deliveriesB[i:])
		copy(bd.requests, bd.requests[i:])
		for j := len(bd.deliveriesH) - int(i); j < len(bd.deliveriesH); j++ {
			bd.deliveriesH[j] = nil
			bd.deliveriesB[j] = nil
			bd.requests[j] = nil
		}
		bd.requestedLow += i
	}
	return headers, rawBodies, nil
}

func (bd *BodyDownload) DeliveryCounts() (float64, float64) {
	return bd.deliveredCount, bd.wastedCount
}

func (bd *BodyDownload) GetPenaltyPeers() [][64]byte {
	peers := make([][64]byte, len(bd.peerMap))
	i := 0
	for p := range bd.peerMap {
		peers[i] = p
		i++
	}
	return peers
}

func (bd *BodyDownload) PrintPeerMap() {
	fmt.Printf("---------------------------\n")
	for p, n := range bd.peerMap {
		fmt.Printf("%s = %d\n", p, n)
	}
	fmt.Printf("---------------------------\n")
	bd.peerMap = make(map[[64]byte]int)
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

func (bd *BodyDownload) AddMinedBlock(block *types.Block) error {
	bd.AddToPrefetch(block)
	return nil
}
