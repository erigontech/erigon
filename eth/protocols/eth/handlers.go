// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// handleGetBlockHeaders handles Block header query, collect the requested headers and reply
func handleGetBlockHeaders(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the complex header query
	var query GetBlockHeadersPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	tx, err := backend.DB().BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response, err := AnswerGetBlockHeadersQuery(tx, &query)
	if err != nil {
		return err
	}
	tx.Rollback()
	return peer.SendBlockHeaders(response)
}

// handleGetBlockHeaders66 is the eth/66 version of handleGetBlockHeaders
func handleGetBlockHeaders66(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the complex header query
	var query GetBlockHeadersPacket66
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v", err, msg)
	}
	tx, err := backend.DB().BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response, err := AnswerGetBlockHeadersQuery(tx, query.GetBlockHeadersPacket)
	if err != nil {
		return err
	}
	tx.Rollback()
	return peer.ReplyBlockHeaders(query.RequestId, response)
}

func AnswerGetBlockHeadersQuery(db ethdb.KVGetter, query *GetBlockHeadersPacket) ([]*types.Header, error) {
	hashMode := query.Origin.Hash != (common.Hash{})
	first := true
	maxNonCanonical := uint64(100)

	// Gather headers until the fetch or network limits is reached
	var (
		bytes   common.StorageSize
		headers []*types.Header
		unknown bool
		err     error
		lookups int
	)
	for !unknown && len(headers) < int(query.Amount) && bytes < softResponseLimit &&
		len(headers) < maxHeadersServe && lookups < 2*maxHeadersServe {
		lookups++
		// Retrieve the next header satisfying the query
		var origin *types.Header
		if hashMode {
			if first {
				first = false
				origin, err = rawdb.ReadHeaderByHash(db, query.Origin.Hash)
				if err != nil {
					return nil, err
				}
				if origin != nil {
					query.Origin.Number = origin.Number.Uint64()
				}
			} else {
				origin = rawdb.ReadHeader(db, query.Origin.Hash, query.Origin.Number)
			}
		} else {
			origin = rawdb.ReadHeaderByNumber(db, query.Origin.Number)
		}
		if origin == nil {
			break
		}
		headers = append(headers, origin)
		bytes += estHeaderSize

		// Advance to the next header of the query
		switch {
		case hashMode && query.Reverse:
			// Hash based traversal towards the genesis block
			ancestor := query.Skip + 1
			if ancestor == 0 {
				unknown = true
			} else {
				query.Origin.Hash, query.Origin.Number = rawdb.ReadAncestor(db, query.Origin.Hash, query.Origin.Number, ancestor, &maxNonCanonical)
				unknown = (query.Origin.Hash == common.Hash{})
			}
		case hashMode && !query.Reverse:
			// Hash based traversal towards the leaf block
			var (
				current = origin.Number.Uint64()
				next    = current + query.Skip + 1
			)
			if next <= current {
				//infos, _ := json.MarshalIndent(peer.Peer.Info(), "", "  ")
				//log.Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next, "attacker", infos)
				log.Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next)
				unknown = true
			} else {
				if header := rawdb.ReadHeaderByNumber(db, next); header != nil {
					nextHash := header.Hash()
					expOldHash, _ := rawdb.ReadAncestor(db, nextHash, next, query.Skip+1, &maxNonCanonical)
					if expOldHash == query.Origin.Hash {
						query.Origin.Hash, query.Origin.Number = nextHash, next
					} else {
						unknown = true
					}
				} else {
					unknown = true
				}
			}
		case query.Reverse:
			// Number based traversal towards the genesis block
			if query.Origin.Number >= query.Skip+1 {
				query.Origin.Number -= query.Skip + 1
			} else {
				unknown = true
			}

		case !query.Reverse:
			// Number based traversal towards the leaf block
			query.Origin.Number += query.Skip + 1
		}
	}
	return headers, nil
}

func handleGetBlockBodies(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the block body retrieval message
	var query GetBlockBodiesPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	tx, err := backend.DB().BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response := AnswerGetBlockBodiesQuery(tx, query)
	tx.Rollback()
	return peer.SendBlockBodiesRLP(response)
}

func handleGetBlockBodies66(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the block body retrieval message
	var query GetBlockBodiesPacket66
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	tx, err := backend.DB().BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response := AnswerGetBlockBodiesQuery(tx, query.GetBlockBodiesPacket)
	tx.Rollback()
	return peer.ReplyBlockBodiesRLP(query.RequestId, response)
}

func AnswerGetBlockBodiesQuery(db ethdb.Tx, query GetBlockBodiesPacket) []rlp.RawValue { //nolint:unparam
	// Gather blocks until the fetch or network limits is reached
	var (
		bytes  int
		bodies []rlp.RawValue
	)
	for lookups, hash := range query {
		if bytes >= softResponseLimit || len(bodies) >= maxBodiesServe ||
			lookups >= 2*maxBodiesServe {
			break
		}
		number := rawdb.ReadHeaderNumber(db, hash)
		if number == nil {
			continue
		}
		data := rawdb.ReadBodyRLP(ethdb.NewRoTxDb(db), hash, *number)
		if len(data) == 0 {
			continue
		}
		bodies = append(bodies, data)
		bytes += len(data)
	}
	return bodies
}

func handleGetNodeData(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the trie node data retrieval message
	var query GetNodeDataPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	response := answerGetNodeDataQuery(backend, query, peer)
	return peer.SendNodeData(response)
}

func handleGetNodeData66(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the trie node data retrieval message
	var query GetNodeDataPacket66
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	response := answerGetNodeDataQuery(backend, query.GetNodeDataPacket, peer)
	return peer.ReplyNodeData(query.RequestId, response)
}

func answerGetNodeDataQuery(backend Backend, query GetNodeDataPacket, peer *Peer) [][]byte {
	return nil
}

func handleGetReceipts(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the block receipts retrieval message
	var query GetReceiptsPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	tx, err := backend.DB().BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response, err := AnswerGetReceiptsQuery(tx, query)
	if err != nil {
		return err
	}
	tx.Rollback()
	return peer.SendReceiptsRLP(response)
}

func handleGetReceipts66(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the block receipts retrieval message
	var query GetReceiptsPacket66
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	tx, err := backend.DB().BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response, err := AnswerGetReceiptsQuery(tx, query.GetReceiptsPacket)
	if err != nil {
		return err
	}
	tx.Rollback()
	return peer.ReplyReceiptsRLP(query.RequestId, response)
}

func AnswerGetReceiptsQuery(db ethdb.Tx, query GetReceiptsPacket) ([]rlp.RawValue, error) { //nolint:unparam
	// Gather state data until the fetch or network limits is reached
	var (
		bytes    int
		receipts []rlp.RawValue
	)
	for lookups, hash := range query {
		if bytes >= softResponseLimit || len(receipts) >= maxReceiptsServe ||
			lookups >= 2*maxReceiptsServe {
			break
		}
		// Retrieve the requested block's receipts
		results := rawdb.ReadReceiptsByHash(ethdb.NewRoTxDb(db), hash)
		if results == nil {
			header, err := rawdb.ReadHeaderByHash(db, hash)
			if err != nil {
				return nil, err
			}
			if header == nil || header.ReceiptHash != types.EmptyRootHash {
				continue
			}
		}
		// If known, encode and queue for response packet
		if encoded, err := rlp.EncodeToBytes(results); err != nil {
			return nil, fmt.Errorf("failed to encode receipt: %w", err)
		} else {
			receipts = append(receipts, encoded)
			bytes += len(encoded)
		}
	}
	return receipts, nil
}

func handleNewBlockhashes(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of new block announcements just arrived
	ann := new(NewBlockHashesPacket)
	if err := msg.Decode(ann); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	// Mark the hashes as present at the remote node
	for _, block := range *ann {
		peer.markBlock(block.Hash)
	}
	// Deliver them all to the backend for queuing
	return backend.Handle(peer, ann)
}

func handleNewBlock(backend Backend, msg Decoder, peer *Peer) error {
	// Retrieve and decode the propagated block
	ann := new(NewBlockPacket)
	if err := msg.Decode(ann); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	if hash := types.CalcUncleHash(ann.Block.Uncles()); hash != ann.Block.UncleHash() {
		log.Warn("Propagated block has invalid uncles", "have", hash, "exp", ann.Block.UncleHash())
		return nil // TODO(karalabe): return error eventually, but wait a few releases
	}
	if hash := types.DeriveSha(ann.Block.Transactions()); hash != ann.Block.TxHash() {
		log.Warn("Propagated block has invalid body", "have", hash, "exp", ann.Block.TxHash())
		return nil // TODO(karalabe): return error eventually, but wait a few releases
	}
	if err := ann.sanityCheck(); err != nil {
		return err
	}
	ann.Block.ReceivedAt = msg.Time()
	ann.Block.ReceivedFrom = peer

	// Mark the peer as owning the block
	peer.markBlock(ann.Block.Hash())

	return backend.Handle(peer, ann)
}

func handleBlockHeaders(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of headers arrived to one of our previous requests
	res := new(BlockHeadersPacket)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	return backend.Handle(peer, res)
}

func handleBlockHeaders66(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of headers arrived to one of our previous requests
	res := new(BlockHeadersPacket66)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	return backend.Handle(peer, &res.BlockHeadersPacket)
}

func handleBlockBodies(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of block bodies arrived to one of our previous requests
	res := new(BlockBodiesPacket)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	return backend.Handle(peer, res)
}

func handleBlockBodies66(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of block bodies arrived to one of our previous requests
	res := new(BlockBodiesPacket66)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	return backend.Handle(peer, &res.BlockBodiesPacket)
}

func handleNodeData(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of node state data arrived to one of our previous requests
	res := new(NodeDataPacket)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	return backend.Handle(peer, res)
}

func handleNodeData66(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of node state data arrived to one of our previous requests
	res := new(NodeDataPacket66)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	return backend.Handle(peer, &res.NodeDataPacket)
}

func handleReceipts(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of receipts arrived to one of our previous requests
	res := new(ReceiptsPacket)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	return backend.Handle(peer, res)
}

func handleReceipts66(backend Backend, msg Decoder, peer *Peer) error {
	// A batch of receipts arrived to one of our previous requests
	res := new(ReceiptsPacket66)
	if err := msg.Decode(res); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	return backend.Handle(peer, &res.ReceiptsPacket)
}

func handleNewPooledTransactionHashes(backend Backend, msg Decoder, peer *Peer) error {
	// New transaction announcement arrived, make sure we have
	// a valid and fresh chain to handle them
	if !backend.AcceptTxs() {
		return nil
	}
	ann := new(NewPooledTransactionHashesPacket)
	if err := msg.Decode(ann); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	// Schedule all the unknown hashes for retrieval
	for _, hash := range *ann {
		peer.markTransaction(hash)
	}
	return backend.Handle(peer, ann)
}

func handleGetPooledTransactions(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the pooled transactions retrieval message
	var query GetPooledTransactionsPacket
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}

	hashes, txs := AnswerGetPooledTransactions(backend.TxPool(), query)
	return peer.SendPooledTransactionsRLP(hashes, txs)
}

func handleGetPooledTransactions66(backend Backend, msg Decoder, peer *Peer) error {
	// Decode the pooled transactions retrieval message
	var query GetPooledTransactionsPacket66
	if err := msg.Decode(&query); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	hashes, txs := AnswerGetPooledTransactions(backend.TxPool(), query.GetPooledTransactionsPacket)
	return peer.ReplyPooledTransactionsRLP(query.RequestId, hashes, txs)
}

func AnswerGetPooledTransactions(txPool TxPool, query GetPooledTransactionsPacket) ([]common.Hash, []rlp.RawValue) { //nolint:unparam
	// Gather transactions until the fetch or network limits is reached
	var (
		bytes  int
		hashes []common.Hash
		txs    []rlp.RawValue
	)
	for _, hash := range query {
		if bytes >= softResponseLimit {
			break
		}
		// Retrieve the requested transaction, skipping if unknown to us
		tx := txPool.Get(hash)
		if tx == nil {
			continue
		}
		// If known, encode and queue for response packet
		if encoded, err := rlp.EncodeToBytes(tx); err != nil {
			log.Error("Failed to encode transaction", "err", err)
		} else {
			hashes = append(hashes, hash)
			txs = append(txs, encoded)
			bytes += len(encoded)
		}
	}
	return hashes, txs
}

func handleTransactions(backend Backend, msg Decoder, peer *Peer) error {
	// Transactions arrived, make sure we have a valid and fresh chain to handle them
	if !backend.AcceptTxs() {
		return nil
	}
	// Transactions can be processed, parse all of them and deliver to the pool
	var txs TransactionsPacket
	if err := msg.Decode(&txs); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	for i, tx := range txs {
		// Validate and mark the remote transaction
		if tx == nil {
			return fmt.Errorf("%w: transaction %d is nil", errDecode, i)
		}
		peer.markTransaction(tx.Hash())
	}
	return backend.Handle(peer, &txs)
}

func handlePooledTransactions(backend Backend, msg Decoder, peer *Peer) error {
	// Transactions arrived, make sure we have a valid and fresh chain to handle them
	if !backend.AcceptTxs() {
		return nil
	}
	// Transactions can be processed, parse all of them and deliver to the pool
	var txs PooledTransactionsPacket
	if err := msg.Decode(&txs); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	for i, tx := range txs {
		// Validate and mark the remote transaction
		if tx == nil {
			return fmt.Errorf("%w: transaction %d is nil", errDecode, i)
		}
		peer.markTransaction(tx.Hash())
	}
	return backend.Handle(peer, &txs)
}

func handlePooledTransactions66(backend Backend, msg Decoder, peer *Peer) error {
	// Transactions arrived, make sure we have a valid and fresh chain to handle them
	if !backend.AcceptTxs() {
		return nil
	}
	// Transactions can be processed, parse all of them and deliver to the pool
	var txs PooledTransactionsPacket66
	if err := msg.Decode(&txs); err != nil {
		return fmt.Errorf("%w: message %v: %v", errDecode, msg, err)
	}
	for i, tx := range txs.PooledTransactionsPacket {
		// Validate and mark the remote transaction
		if tx == nil {
			return fmt.Errorf("%w: transaction %d is nil", errDecode, i)
		}
		peer.markTransaction(tx.Hash())
	}
	return backend.Handle(peer, &txs.PooledTransactionsPacket)
}
