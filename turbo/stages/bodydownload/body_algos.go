package bodydownload

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"github.com/ledgerwatch/erigon/turbo/services"
)

const BlockBufferSize = 128

// UpdateFromDb reads the state of the database and refreshes the state of the body download
func (bd *BodyDownload) UpdateFromDb(db kv.Tx) (headHeight, headTime uint64, headHash common.Hash, headTd256 *uint256.Int, err error) {
	var headerProgress, bodyProgress uint64
	headerProgress, err = stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return 0, 0, common.Hash{}, nil, err
	}
	bodyProgress, err = stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return 0, 0, common.Hash{}, nil, err
	}
	bd.maxProgress = headerProgress + 1
	// Resetting for requesting a new range of blocks
	bd.requestedLow = bodyProgress + 1
	bd.lowWaitUntil = 0
	bd.requestHigh = bd.requestedLow + (bd.outstandingLimit / 2)
	bd.requestedMap = make(map[TripleHash]uint64)
	bd.delivered.Clear()
	bd.deliveredCount = 0
	bd.wastedCount = 0
	bd.deliveriesH = make(map[uint64]*types.Header)
	bd.requests = make(map[uint64]*BodyRequest)
	bd.peerMap = make(map[[64]byte]int)
	headHeight = bodyProgress
	headHash, err = rawdb.ReadCanonicalHash(db, headHeight)
	if err != nil {
		return 0, 0, common.Hash{}, nil, err
	}
	var headTd *big.Int
	headTd, err = rawdb.ReadTd(db, headHash, headHeight)
	if err != nil {
		return 0, 0, common.Hash{}, nil, fmt.Errorf("reading total difficulty for head height %d and hash %x: %d, %w", headHeight, headHash, headTd, err)
	}
	if headTd == nil {
		headTd = new(big.Int)
	}
	headTd256 = new(uint256.Int)
	overflow := headTd256.SetFromBig(headTd)
	if overflow {
		return 0, 0, common.Hash{}, nil, fmt.Errorf("headTd higher than 2^256-1")
	}
	headTime = 0
	headHeader := rawdb.ReadHeader(db, headHash, headHeight)
	if headHeader != nil {
		headTime = headHeader.Time
	}
	return headHeight, headTime, headHash, headTd256, nil
}

// RequestMoreBodies - returns nil if nothing to request
func (bd *BodyDownload) RequestMoreBodies(tx kv.RwTx, blockReader services.FullBlockReader, blockNum uint64, currentTime uint64, blockPropagator adapter.BlockPropagator) (*BodyRequest, uint64, error) {
	var bodyReq *BodyRequest
	blockNums := make([]uint64, 0, BlockBufferSize)
	hashes := make([]common.Hash, 0, BlockBufferSize)

	if blockNum < bd.requestedLow {
		blockNum = bd.requestedLow
	}

	for ; len(blockNums) < BlockBufferSize && bd.requestedLow <= bd.maxProgress; blockNum++ {
		// Check if we reached the highest allowed request block number, and turn back
		if blockNum >= bd.requestedLow+bd.outstandingLimit || blockNum >= bd.maxProgress {
			blockNum = 0
			break // Avoid tight loop
		}
		if bd.delivered.Contains(blockNum) {
			// Already delivered, no need to request
			continue
		}

		req := bd.requests[blockNum]
		if req != nil {
			if currentTime < req.waitUntil {
				continue
			}
			bd.peerMap[req.peerID]++
			bd.requests[blockNum] = nil
		}

		// check in the bucket if that has been received either in this run or a previous one.
		// if we already have the body we can continue on to populate header info and then skip
		// the body request altogether
		var err error
		key := common2.EncodeTs(blockNum)
		var bodyInBucket bool
		if !bd.UsingExternalTx {
			bodyInBucket, err = tx.Has("BodiesStage", key)
			if err != nil {
				return nil, blockNum, err
			}
		} else {
			_, bodyInBucket = bd.bodyCache[blockNum]
		}

		if bodyInBucket {
			bd.delivered.Add(blockNum)
			continue
		}

		var hash common.Hash
		var header *types.Header
		request := true
		if bd.deliveriesH[blockNum] != nil {
			// If this block was requested before, we don't need to fetch the headers from the database the second time
			header = bd.deliveriesH[blockNum]
			if header == nil {
				return nil, blockNum, fmt.Errorf("header not found: %w, blockNum=%d, trace=%s", err, blockNum, dbg.Stack())
			}
			hash = header.Hash()

			// check here if we have the block prefetched as this could have come in as part of a data race
			// we want to avoid an infinite loop if the header was populated in deliveriesH before the block
			// was added to the prefetched cache
			if hasPrefetched := bd.checkPrefetchedBlock(hash, tx, blockNum, blockPropagator); hasPrefetched {
				request = false
			}
		} else {
			hash, err = rawdb.ReadCanonicalHash(tx, blockNum)
			if err != nil {
				return nil, blockNum, fmt.Errorf("could not find canonical header: %w, blockNum=%d, trace=%s", err, blockNum, dbg.Stack())
			}

			header, err = blockReader.Header(context.Background(), tx, hash, blockNum)
			if err != nil {
				return nil, blockNum, fmt.Errorf("header not found: %w, blockNum=%d, trace=%s", err, blockNum, dbg.Stack())
			}
			if header == nil {
				return nil, blockNum, fmt.Errorf("header not found: blockNum=%d, hash=%x, trace=%s", blockNum, hash, dbg.Stack())
			}

			if hasPrefetched := bd.checkPrefetchedBlock(hash, tx, blockNum, blockPropagator); hasPrefetched {
				request = false
			} else {
				bd.deliveriesH[blockNum] = header
				if header.UncleHash != types.EmptyUncleHash || header.TxHash != types.EmptyRootHash ||
					(header.WithdrawalsHash != nil && *header.WithdrawalsHash != types.EmptyRootHash) {
					// Perhaps we already have this block
					block := rawdb.ReadBlock(tx, hash, blockNum)
					if block == nil {
						var tripleHash TripleHash
						copy(tripleHash[:], header.UncleHash.Bytes())
						copy(tripleHash[length.Hash:], header.TxHash.Bytes())
						if header.WithdrawalsHash != nil {
							copy(tripleHash[2*length.Hash:], header.WithdrawalsHash.Bytes())
						} else {
							copy(tripleHash[2*length.Hash:], types.EmptyRootHash.Bytes())
						}
						bd.requestedMap[tripleHash] = blockNum
					} else {
						err = bd.addBodyToBucket(tx, blockNum, block.RawBody())
						if err != nil {
							log.Error("Failed to add block body to bucket", "err", err, "number", block.NumberU64()-1, "hash", block.ParentHash())
						}
						request = false
					}
				} else {
					err = bd.addBodyToBucket(tx, blockNum, &types.RawBody{})
					if err != nil {
						log.Error("Failed to add block body to bucket", "err", err, "number", blockNum, "hash", hash)
					}
					request = false
				}
			}
		}

		if request {
			blockNums = append(blockNums, blockNum)
			hashes = append(hashes, hash)
		} else {
			// uncleHash, txHash, and withdrawalsHash are all empty (or block is prefetched), no need to request
			bd.delivered.Add(blockNum)
		}
	}
	if len(blockNums) > 0 {
		bodyReq = &BodyRequest{BlockNums: blockNums, Hashes: hashes}
		for _, num := range blockNums {
			bd.requests[num] = bodyReq
		}
	}

	return bodyReq, blockNum, nil
}

// checks if we have the block prefetched, returns true if found and stored or false if not present
func (bd *BodyDownload) checkPrefetchedBlock(hash common.Hash, tx kv.RwTx, blockNum uint64, blockPropagator adapter.BlockPropagator) bool {
	block := bd.prefetchedBlocks.Pop(hash)

	if block == nil {
		return false
	}

	// Block is prefetched, no need to request
	bd.deliveriesH[blockNum] = block.Header()

	// make sure we have the body in the bucket for later use
	bd.addBodyToBucket(tx, blockNum, block.RawBody())

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

	return true
}

func (bd *BodyDownload) RequestSent(bodyReq *BodyRequest, timeWithTimeout uint64, peer [64]byte) {
	for _, blockNum := range bodyReq.BlockNums {
		if blockNum < bd.requestedLow {
			continue
		}
		req := bd.requests[blockNum]
		if req != nil {
			bd.requests[blockNum].waitUntil = timeWithTimeout
			bd.requests[blockNum].peerID = peer
		}
	}
}

// DeliverBodies takes the block body received from a peer and adds it to the various data structures
func (bd *BodyDownload) DeliverBodies(txs [][][]byte, uncles [][]*types.Header, withdrawals []types.Withdrawals, lenOfP2PMsg uint64, peerID [64]byte) {
	bd.deliveryCh <- Delivery{txs: txs, uncles: uncles, withdrawals: withdrawals, lenOfP2PMessage: lenOfP2PMsg, peerID: peerID}

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

func (bd *BodyDownload) DeliverySize(delivered float64, wasted float64) {
	bd.deliveredCount += delivered
	bd.wastedCount += wasted
}

func (bd *BodyDownload) GetDeliveries(tx kv.RwTx) (uint64, uint64, error) {
	var delivered, undelivered int
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
		if delivery.withdrawals == nil {
			log.Warn("nil withdrawals delivered", "peer_id", delivery.peerID, "p2p_msg_len", delivery.lenOfP2PMessage)
		}
		if delivery.txs == nil || delivery.uncles == nil || delivery.withdrawals == nil {
			log.Debug("delivery body processing has been skipped due to nil tx|data")
			continue
		}

		reqMap := make(map[uint64]*BodyRequest)
		txs, uncles, withdrawals, lenOfP2PMessage := delivery.txs, delivery.uncles, delivery.withdrawals, delivery.lenOfP2PMessage

		for i := range txs {
			uncleHash := types.CalcUncleHash(uncles[i])
			txHash := types.DeriveSha(RawTransactions(txs[i]))
			withdrawalsHash := types.DeriveSha(withdrawals[i])
			var tripleHash TripleHash
			copy(tripleHash[:], uncleHash.Bytes())
			copy(tripleHash[length.Hash:], txHash.Bytes())
			copy(tripleHash[2*length.Hash:], withdrawalsHash.Bytes())

			// Block numbers are added to the bd.delivered bitmap here, only for blocks for which the body has been received, and their double hashes are present in the bd.requestedMap
			// Also, block numbers can be added to bd.delivered for empty blocks, above
			blockNum, ok := bd.requestedMap[tripleHash]
			if !ok {
				undelivered++
				continue
			}
			req := bd.requests[blockNum]
			if req != nil {
				if _, ok := reqMap[req.BlockNums[0]]; !ok {
					reqMap[req.BlockNums[0]] = req
				}
			}
			delete(bd.requestedMap, tripleHash) // Delivered, cleaning up

			err := bd.addBodyToBucket(tx, blockNum, &types.RawBody{Transactions: txs[i], Uncles: uncles[i], Withdrawals: withdrawals[i]})
			if err != nil {
				return 0, 0, err
			}
			bd.delivered.Add(blockNum)
			delivered++
		}
		// Clean up the requests
		for _, req := range reqMap {
			for _, blockNum := range req.BlockNums {
				bd.requests[blockNum] = nil
			}
		}
		total := delivered + undelivered
		if total > 0 {
			// Approximate numbers
			bd.DeliverySize(float64(lenOfP2PMessage)*float64(delivered)/float64(delivered+undelivered), float64(lenOfP2PMessage)*float64(undelivered)/float64(delivered+undelivered))
		}
	}

	return bd.requestedLow, uint64(delivered), nil
}

// NextProcessingCount returns the count of contiguous block numbers ready to process from the
// requestedLow minimum value.
// the requestedLow count is increased by the number returned
func (bd *BodyDownload) NextProcessingCount() uint64 {
	var i uint64
	for i = 0; !bd.delivered.IsEmpty() && bd.requestedLow+i == bd.delivered.Minimum(); i++ {
		bd.delivered.Remove(bd.requestedLow + i)
	}
	bd.requestedLow += i
	return i
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
	bd.prefetchedBlocks.Add(block)
}

// GetHeader returns a header by either loading from the deliveriesH slice populated when running RequestMoreBodies
// or if the code is continuing from a previous run and this isn't present, by reading from the DB as the RequestMoreBodies would have.
// as the requestedLow count is incremented before a call to this function we need the process count so that we can anticipate this,
// effectively reversing time a little to get the actual position we need in the slice prior to requestedLow being incremented
func (bd *BodyDownload) GetHeader(blockNum uint64, blockReader services.FullBlockReader, tx kv.Tx) (*types.Header, common.Hash, error) {
	var header *types.Header
	if bd.deliveriesH[blockNum] != nil {
		header = bd.deliveriesH[blockNum]
	} else {
		hash, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return nil, common.Hash{}, err
		}
		header, err = blockReader.Header(context.Background(), tx, hash, blockNum)
		if err != nil {
			return nil, common.Hash{}, err
		}
		if header == nil {
			return nil, common.Hash{}, fmt.Errorf("header not found: blockNum=%d, hash=%x, trace=%s", blockNum, hash, dbg.Stack())
		}
	}
	return header, header.Hash(), nil
}

func (bd *BodyDownload) addBodyToBucket(tx kv.RwTx, key uint64, body *types.RawBody) error {
	if !bd.UsingExternalTx {
		// use the kv store to hold onto bodies as we're anticipating a lot of memory usage
		writer := bytes.NewBuffer(nil)
		err := body.EncodeRLP(writer)
		if err != nil {
			return err
		}
		rlpBytes := common.CopyBytes(writer.Bytes())
		writer.Reset()
		writer.WriteString(hexutil.Encode(rlpBytes))

		k := common2.EncodeTs(key)
		err = tx.Put("BodiesStage", k, writer.Bytes())
		if err != nil {
			return err
		}
	} else {
		// use an in memory cache as we're near the top of the chain
		bd.bodyCache[key] = body
	}

	bd.bodiesAdded = true
	return nil
}

func (bd *BodyDownload) GetBlockFromCache(tx kv.RwTx, blockNum uint64) (*types.RawBody, error) {
	if !bd.UsingExternalTx {
		key := common2.EncodeTs(blockNum)
		body, err := tx.GetOne("BodiesStage", key)
		if err != nil {
			return nil, err
		}

		var rawBody types.RawBody
		fromHex := common.CopyBytes(common.FromHex(string(body)))
		bodyReader := bytes.NewReader(fromHex)
		stream := rlp.NewStream(bodyReader, 0)
		err = rawBody.DecodeRLP(stream)
		if err != nil {
			log.Error("Unexpected body from bucket", "err", err, "block", blockNum)
			return nil, fmt.Errorf("%w, nextBlock=%d", err, blockNum)
		}

		return &rawBody, nil
	} else {
		return bd.bodyCache[blockNum], nil
	}
}

func (bd *BodyDownload) ClearBodyCache() {
	bd.bodyCache = make(map[uint64]*types.RawBody)
}

func (bd *BodyDownload) HasAddedBodies() bool {
	return bd.bodiesAdded
}

func (bd *BodyDownload) ResetAddedBodies() {
	bd.bodiesAdded = false
}
