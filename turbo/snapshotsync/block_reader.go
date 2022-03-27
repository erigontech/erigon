package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
)

// BlockReader can read blocks from db and snapshots
type BlockReader struct {
}

func NewBlockReader() *BlockReader {
	return &BlockReader{}
}

func (back *BlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (common.Hash, error) {
	return rawdb.ReadCanonicalHash(tx, blockHeight)
}

func (back *BlockReader) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (*types.Header, error) {
	h := rawdb.ReadHeader(tx, hash, blockHeight)
	return h, nil
}

func (back *BlockReader) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	body, _, _ = rawdb.ReadBody(tx, hash, blockHeight)
	return body, nil
}

func (back *BlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	return rawdb.ReadBodyWithTransactions(tx, hash, blockHeight)
}

func (back *BlockReader) BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
	body, err := back.Body(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	bodyRlp, err = rlp.EncodeToBytes(body)
	if err != nil {
		return nil, err
	}
	return bodyRlp, nil
}

func (back *BlockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (*types.Header, error) {
	h := rawdb.ReadHeaderByNumber(tx, blockHeight)
	return h, nil
}

func (back *BlockReader) HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error) {
	return rawdb.ReadHeaderByHash(tx, hash)
}

func (back *BlockReader) BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockHeight)
	if err != nil {
		return nil, nil, fmt.Errorf("requested non-canonical hash %x. canonical=%x", hash, canonicalHash)
	}
	if canonicalHash == hash {
		block, senders, err = rawdb.ReadBlockWithSenders(tx, hash, blockHeight)
		if err != nil {
			return nil, nil, err
		}
		return block, senders, nil
	}

	return rawdb.NonCanonicalBlockWithSenders(tx, hash, blockHeight)
}

func (back *BlockReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
	n, err := rawdb.ReadTxLookupEntry(tx, txnHash)
	if err != nil {
		return 0, false, err
	}
	if n == nil {
		return 0, false, nil
	}
	return *n, true, nil
}

//func (back *BlockReader) TxnByHashDeprecated(ctx context.Context, tx kv.Getter, txnHash common.Hash) (txn types.Transaction, blockHash common.Hash, blockNum, txnIndex uint64, err error) {
//	return rawdb.ReadTransactionByHash(tx, txnHash)
//}
//func (back *BlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
//	return rawdb.ReadBodyWithTransactions(tx, hash, blockHeight)
//}

type RemoteBlockReader struct {
	client remote.ETHBACKENDClient
}

func NewRemoteBlockReader(client remote.ETHBACKENDClient) *RemoteBlockReader {
	return &RemoteBlockReader{client}
}

func (back *RemoteBlockReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
	reply, err := back.client.TxnLookup(ctx, &remote.TxnLookupRequest{TxnHash: gointerfaces.ConvertHashToH256(txnHash)})
	if err != nil {
		return 0, false, err
	}
	if reply == nil {
		return 0, false, nil
	}
	return reply.BlockNumber, true, nil
}

func (back *RemoteBlockReader) BlockWithSenders(ctx context.Context, _ kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	reply, err := back.client.Block(ctx, &remote.BlockRequest{BlockHash: gointerfaces.ConvertHashToH256(hash), BlockHeight: blockHeight})
	if err != nil {
		return nil, nil, err
	}

	block = &types.Block{}
	err = rlp.Decode(bytes.NewReader(reply.BlockRlp), block)
	if err != nil {
		return nil, nil, err
	}
	senders = make([]common.Address, len(reply.Senders)/20)
	for i := range senders {
		senders[i].SetBytes(reply.Senders[i*20 : (i+1)*20])
	}
	if len(senders) == block.Transactions().Len() { //it's fine if no senders provided - they can be lazy recovered
		block.SendersToTxs(senders)
	}
	return block, senders, nil
}

func (back *RemoteBlockReader) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (*types.Header, error) {
	block, _, err := back.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Header(), nil
}
func (back *RemoteBlockReader) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	block, _, err := back.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Body(), nil
}
func (back *RemoteBlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	block, _, err := back.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Body(), nil
}

func (back *RemoteBlockReader) BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
	body, err := back.BodyWithTransactions(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	bodyRlp, err = rlp.EncodeToBytes(body)
	if err != nil {
		return nil, err
	}
	return bodyRlp, nil
}

// BlockReaderWithSnapshots can read blocks from db and snapshots
type BlockReaderWithSnapshots struct {
	sn *RoSnapshots
}

func NewBlockReaderWithSnapshots(snapshots *RoSnapshots) *BlockReaderWithSnapshots {
	return &BlockReaderWithSnapshots{sn: snapshots}
}
func (back *BlockReaderWithSnapshots) HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (h *types.Header, err error) {
	ok, err := back.sn.ViewHeaders(blockHeight, func(segment *HeaderSegment) error {
		if segment.idxHeaderHash == nil {
			fmt.Printf("why? %d, %d, %d, %d, %d\n", blockHeight, segment.From, segment.To, back.sn.segmentsAvailable.Load(), back.sn.idxAvailable.Load())
			back.sn.PrintDebug()
			for _, sn := range back.sn.Headers.segments {
				if sn.idxHeaderHash == nil {
					fmt.Printf("seg with nil idx: %d,%d\n", segment.From, segment.To)
				}
			}
			fmt.Printf("==== end debug print ====\n")
		}
		h, err = back.headerFromSnapshot(blockHeight, segment, nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ok {
		return h, nil
	}
	return rawdb.ReadHeaderByNumber(tx, blockHeight), nil
}

// HeaderByHash - will search header in all snapshots starting from recent
func (back *BlockReaderWithSnapshots) HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (h *types.Header, err error) {
	h, err = rawdb.ReadHeaderByHash(tx, hash)
	if err != nil {
		return nil, err
	}
	if h != nil {
		return h, nil
	}

	buf := make([]byte, 128)
	if err := back.sn.Headers.View(func(segments []*HeaderSegment) error {
		for i := len(segments) - 1; i >= 0; i-- {
			if segments[i].idxHeaderHash == nil {
				continue
			}

			h, err = back.headerFromSnapshotByHash(hash, segments[i], buf)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return h, nil
}

func (back *BlockReaderWithSnapshots) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (h common.Hash, err error) {
	ok, err := back.sn.ViewHeaders(blockHeight, func(segment *HeaderSegment) error {
		header, err := back.headerFromSnapshot(blockHeight, segment, nil)
		if err != nil {
			return err
		}
		if header == nil {
			return nil
		}
		h = header.Hash()
		return nil
	})
	if err != nil {
		return h, err
	}
	if ok {
		return h, nil
	}

	return rawdb.ReadCanonicalHash(tx, blockHeight)
}

func (back *BlockReaderWithSnapshots) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (h *types.Header, err error) {
	ok, err := back.sn.ViewHeaders(blockHeight, func(segment *HeaderSegment) error {
		h, err = back.headerFromSnapshot(blockHeight, segment, nil)
		if err != nil {
			return err
		}
		return nil
	})
	if ok {
		return h, nil
	}

	h = rawdb.ReadHeader(tx, hash, blockHeight)
	return h, nil
}

func (back *BlockReaderWithSnapshots) ReadHeaderByNumber(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (h *types.Header, err error) {
	ok, err := back.sn.ViewHeaders(blockHeight, func(segment *HeaderSegment) error {
		h, err = back.headerFromSnapshot(blockHeight, segment, nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return h, nil
	}

	h = rawdb.ReadHeader(tx, hash, blockHeight)
	return h, nil
}

func (back *BlockReaderWithSnapshots) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	var baseTxnID uint64
	var txsAmount uint32
	ok, err := back.sn.ViewBodies(blockHeight, func(seg *BodySegment) error {
		body, baseTxnID, txsAmount, err = back.bodyFromSnapshot(blockHeight, seg, nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ok {
		ok, err = back.sn.ViewTxs(blockHeight, func(seg *TxnSegment) error {
			txs, senders, err := back.txsFromSnapshot(baseTxnID, txsAmount, seg, nil)
			if err != nil {
				return err
			}
			body.Transactions = txs
			body.SendersToTxs(senders)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if ok {
			return body, nil
		}
	}

	body, err = rawdb.ReadBodyWithTransactions(tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (back *BlockReaderWithSnapshots) BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
	body, err := back.BodyWithTransactions(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	bodyRlp, err = rlp.EncodeToBytes(body)
	if err != nil {
		return nil, err
	}
	return bodyRlp, nil
}

func (back *BlockReaderWithSnapshots) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	ok, err := back.sn.ViewBodies(blockHeight, func(seg *BodySegment) error {
		body, _, _, err = back.bodyFromSnapshot(blockHeight, seg, nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ok {
		return body, nil
	}
	body, _, _ = rawdb.ReadBody(tx, hash, blockHeight)
	return body, nil
}

func (back *BlockReaderWithSnapshots) BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	var buf []byte
	var h *types.Header
	ok, err := back.sn.ViewHeaders(blockHeight, func(seg *HeaderSegment) error {
		headerOffset := seg.idxHeaderHash.Lookup2(blockHeight - seg.idxHeaderHash.BaseDataID())
		gg := seg.seg.MakeGetter()
		gg.Reset(headerOffset)
		buf, _ = gg.Next(buf[:0])
		h = &types.Header{}
		if err = rlp.DecodeBytes(buf[1:], h); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return
	}
	if ok {
		var b *types.BodyForStorage
		ok, err = back.sn.ViewBodies(blockHeight, func(seg *BodySegment) error {
			bodyOffset := seg.idxBodyNumber.Lookup2(blockHeight - seg.idxBodyNumber.BaseDataID())
			gg := seg.seg.MakeGetter()
			gg.Reset(bodyOffset)
			buf, _ = gg.Next(buf[:0])
			b = &types.BodyForStorage{}
			reader := bytes.NewReader(buf)
			if err = rlp.Decode(reader, b); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return
		}
		if ok {
			if b.TxAmount <= 2 {
				block = types.NewBlockFromStorage(hash, h, nil, b.Uncles)
				if len(senders) != block.Transactions().Len() {
					return block, senders, nil // no senders is fine - will recover them on the fly
				}
				block.SendersToTxs(senders)
				return block, senders, nil
			}
			reader := bytes.NewReader(nil)
			txs := make([]types.Transaction, b.TxAmount-2)
			senders = make([]common.Address, b.TxAmount-2)
			ok, err = back.sn.ViewTxs(blockHeight, func(seg *TxnSegment) error {
				if b.BaseTxId < seg.IdxTxnHash.BaseDataID() {
					return fmt.Errorf(".idx file has wrong baseDataID? %d<%d, %s", b.BaseTxId, seg.IdxTxnHash.BaseDataID(), seg.Seg.FilePath())
				}
				r := recsplit.NewIndexReader(seg.IdxTxnId)
				binary.BigEndian.PutUint64(buf[:8], b.BaseTxId-seg.IdxTxnId.BaseDataID())
				txnOffset := r.Lookup(buf[:8])
				gg := seg.Seg.MakeGetter()
				gg.Reset(txnOffset)
				stream := rlp.NewStream(reader, 0)
				buf, _ = gg.Next(buf[:0]) //first system-tx
				for i := uint32(0); i < b.TxAmount-2; i++ {
					buf, _ = gg.Next(buf[:0])
					if len(buf) < 1+20 {
						return fmt.Errorf("segment %s has too short record: len(buf)=%d < 21", seg.Seg.FilePath(), len(buf))
					}
					senders[i].SetBytes(buf[1 : 1+20])
					txRlp := buf[1+20:]
					reader.Reset(txRlp)
					stream.Reset(reader, 0)
					txs[i], err = types.DecodeTransaction(stream)
					if err != nil {
						return err
					}
					txs[i].SetSender(senders[i])
				}
				return nil
			})
			if err != nil {
				return nil, nil, err
			}
			if ok {
				block = types.NewBlockFromStorage(hash, h, txs, b.Uncles)
				if len(senders) != block.Transactions().Len() {
					return block, senders, nil // no senders is fine - will recover them on the fly
				}
				block.SendersToTxs(senders)
				return block, senders, nil
			}
		}
	}
	canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockHeight)
	if err != nil {
		return nil, nil, fmt.Errorf("requested non-canonical hash %x. canonical=%x", hash, canonicalHash)
	}
	if canonicalHash == hash {
		block, senders, err = rawdb.ReadBlockWithSenders(tx, hash, blockHeight)
		if err != nil {
			return nil, nil, err
		}
		return block, senders, nil
	}
	return rawdb.NonCanonicalBlockWithSenders(tx, hash, blockHeight)
}

func (back *BlockReaderWithSnapshots) headerFromSnapshot(blockHeight uint64, sn *HeaderSegment, buf []byte) (*types.Header, error) {
	headerOffset := sn.idxHeaderHash.Lookup2(blockHeight - sn.idxHeaderHash.BaseDataID())
	gg := sn.seg.MakeGetter()
	gg.Reset(headerOffset)
	buf, _ = gg.Next(buf[:0])
	h := &types.Header{}
	if err := rlp.DecodeBytes(buf[1:], h); err != nil {
		return nil, err
	}
	return h, nil
}

// headerFromSnapshotByHash - getting header by hash AND ensure that it has correct hash
// because HeaderByHash method will search header in all snapshots - and may request header which doesn't exists
// but because our indices are based on PerfectHashMap, no way to know is given key exists or not, only way -
// to make sure is to fetch it and compare hash
func (back *BlockReaderWithSnapshots) headerFromSnapshotByHash(hash common.Hash, sn *HeaderSegment, buf []byte) (*types.Header, error) {
	reader := recsplit.NewIndexReader(sn.idxHeaderHash)
	localID := reader.Lookup(hash[:])
	headerOffset := sn.idxHeaderHash.Lookup2(localID)
	gg := sn.seg.MakeGetter()
	gg.Reset(headerOffset)
	buf, _ = gg.Next(buf[:0])
	if hash[0] != buf[0] {
		return nil, nil
	}

	h := &types.Header{}
	if err := rlp.DecodeBytes(buf[1:], h); err != nil {
		return nil, err
	}
	if h.Hash() != hash {
		return nil, nil
	}
	return h, nil
}

func (back *BlockReaderWithSnapshots) bodyFromSnapshot(blockHeight uint64, sn *BodySegment, buf []byte) (*types.Body, uint64, uint32, error) {
	bodyOffset := sn.idxBodyNumber.Lookup2(blockHeight - sn.idxBodyNumber.BaseDataID())

	gg := sn.seg.MakeGetter()
	gg.Reset(bodyOffset)
	buf, _ = gg.Next(buf[:0])
	b := &types.BodyForStorage{}
	reader := bytes.NewReader(buf)
	if err := rlp.Decode(reader, b); err != nil {
		return nil, 0, 0, err
	}

	if b.BaseTxId < sn.idxBodyNumber.BaseDataID() {
		return nil, 0, 0, fmt.Errorf(".idx file has wrong baseDataID? %d<%d, %s", b.BaseTxId, sn.idxBodyNumber.BaseDataID(), sn.seg.FilePath())
	}

	body := new(types.Body)
	body.Uncles = b.Uncles
	return body, b.BaseTxId + 1, b.TxAmount - 2, nil // empty txs in the beginning and end of block
}

func (back *BlockReaderWithSnapshots) txsFromSnapshot(baseTxnID uint64, txsAmount uint32, txsSeg *TxnSegment, buf []byte) ([]types.Transaction, []common.Address, error) {
	txs := make([]types.Transaction, txsAmount)
	senders := make([]common.Address, txsAmount)
	reader := bytes.NewReader(buf)
	if txsAmount > 0 {
		r := recsplit.NewIndexReader(txsSeg.IdxTxnId)
		binary.BigEndian.PutUint64(buf[:8], baseTxnID-txsSeg.IdxTxnId.BaseDataID())
		txnOffset := r.Lookup(buf[:8])
		gg := txsSeg.Seg.MakeGetter()
		gg.Reset(txnOffset)
		stream := rlp.NewStream(reader, 0)
		for i := uint32(0); i < txsAmount; i++ {
			buf, _ = gg.Next(buf[:0])
			senders[i].SetBytes(buf[1 : 1+20])
			txRlp := buf[1+20:]
			reader.Reset(txRlp)
			stream.Reset(reader, 0)
			var err error
			txs[i], err = types.DecodeTransaction(stream)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	return txs, senders, nil
}

func (back *BlockReaderWithSnapshots) txnByHash(txnHash common.Hash, segments []*TxnSegment, buf []byte) (txn types.Transaction, blockNum, txnID uint64, err error) {
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.IdxTxnId == nil || sn.IdxTxnHash == nil || sn.IdxTxnHash2BlockNum == nil {
			continue
		}

		reader := recsplit.NewIndexReader(sn.IdxTxnHash)
		offset := reader.Lookup(txnHash[:])
		gg := sn.Seg.MakeGetter()
		gg.Reset(offset)
		buf, _ = gg.Next(buf[:0])
		// first byte txnHash check - reducing false-positives 256 times. Allows don't store and don't calculate full hash of entity - when checking many snapshots.
		if txnHash[0] != buf[0] {
			continue
		}

		reader2 := recsplit.NewIndexReader(sn.IdxTxnHash2BlockNum)
		blockNum = reader2.Lookup(txnHash[:])
		sender := buf[1 : 1+20]
		txn, err = types.DecodeTransaction(rlp.NewStream(bytes.NewReader(buf[1+20:]), uint64(len(buf))))
		if err != nil {
			return
		}
		txn.SetSender(common.BytesToAddress(sender))
		// final txnHash check  - completely avoid false-positives
		if txn.Hash() == txnHash {
			return
		}
	}
	return
}

// TxnLookup - find blockNumber and txnID by txnHash
func (back *BlockReaderWithSnapshots) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
	n, err := rawdb.ReadTxLookupEntry(tx, txnHash)
	if err != nil {
		return 0, false, err
	}
	if n != nil {
		return *n, true, nil
	}

	var txn types.Transaction
	var blockNum uint64
	if err := back.sn.Txs.View(func(segments []*TxnSegment) error {
		txn, blockNum, _, err = back.txnByHash(txnHash, segments, nil)
		if err != nil {
			return err
		}
		if txn == nil {
			return nil
		}
		return nil
	}); err != nil {
		return 0, false, err
	}
	if txn == nil {
		return 0, false, nil
	}
	return blockNum, true, nil
}
