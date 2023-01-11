package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
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

func (back *BlockReader) Snapshots() *RoSnapshots { return nil }

func (back *BlockReader) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (*types.Header, error) {
	h := rawdb.ReadHeader(tx, hash, blockHeight)
	return h, nil
}

func (back *BlockReader) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, txAmount uint32, err error) {
	body, _, txAmount = rawdb.ReadBody(tx, hash, blockHeight)
	return body, txAmount, nil
}

func (back *BlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	return rawdb.ReadBodyWithTransactions(tx, hash, blockHeight)
}

func (back *BlockReader) BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
	body, _, err := back.Body(ctx, tx, hash, blockHeight)
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
func (back *BlockReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (txn types.Transaction, err error) {
	canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return nil, err
	}
	var k [8 + 32]byte
	binary.BigEndian.PutUint64(k[:], blockNum)
	copy(k[8:], canonicalHash[:])
	b, err := rawdb.ReadBodyForStorageByKey(tx, k[:])
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}

	txn, err = rawdb.CanonicalTxnByID(tx, b.BaseTxId+1+uint64(i))
	if err != nil {
		return nil, err
	}
	return txn, nil
}

type RemoteBlockReader struct {
	client remote.ETHBACKENDClient
}

func (back *RemoteBlockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (*types.Header, error) {
	canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockHeight)
	if err != nil {
		return nil, err
	}
	block, _, err := back.BlockWithSenders(ctx, tx, canonicalHash, blockHeight)
	if err != nil {
		return nil, err
	}
	return block.Header(), nil
}

func (back *RemoteBlockReader) Snapshots() *RoSnapshots { return nil }

func (back *RemoteBlockReader) HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error) {
	blockNum := rawdb.ReadHeaderNumber(tx, hash)
	if blockNum == nil {
		return nil, nil
	}
	block, _, err := back.BlockWithSenders(ctx, tx, hash, *blockNum)
	if err != nil {
		return nil, err
	}
	return block.Header(), nil
}

func (back *RemoteBlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (common.Hash, error) {
	return rawdb.ReadCanonicalHash(tx, blockHeight)
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

func (back *RemoteBlockReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (txn types.Transaction, err error) {
	canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return nil, err
	}
	b, err := back.BodyWithTransactions(ctx, tx, canonicalHash, blockNum)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	if i < 0 {
		return nil, nil
	}
	if len(b.Transactions) <= i {
		return nil, nil
	}
	return b.Transactions[i], nil
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
func (back *RemoteBlockReader) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, txAmount uint32, err error) {
	block, _, err := back.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, 0, err
	}
	if block == nil {
		return nil, 0, nil
	}
	return block.Body(), uint32(len(block.Body().Transactions)), nil
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

func (back *BlockReaderWithSnapshots) Snapshots() *RoSnapshots { return back.sn }

func (back *BlockReaderWithSnapshots) HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (h *types.Header, err error) {
	ok, err := back.sn.ViewHeaders(blockHeight, func(segment *HeaderSegment) error {
		h, _, err = back.headerFromSnapshot(blockHeight, segment, nil)
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
	h = rawdb.ReadHeaderByNumber(tx, blockHeight)
	return h, nil
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
			if h != nil {
				break
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
		header, _, err := back.headerFromSnapshot(blockHeight, segment, nil)
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
		h, _, err = back.headerFromSnapshot(blockHeight, segment, nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return h, err
	}
	if ok {
		return h, nil
	}

	h = rawdb.ReadHeader(tx, hash, blockHeight)
	return h, nil
}

func (back *BlockReaderWithSnapshots) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	var baseTxnID uint64
	var txsAmount uint32
	var buf []byte
	ok, err := back.sn.ViewBodies(blockHeight, func(seg *BodySegment) error {
		body, baseTxnID, txsAmount, buf, err = back.bodyFromSnapshot(blockHeight, seg, buf)
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
			txs, senders, err := back.txsFromSnapshot(baseTxnID, txsAmount, seg, buf)
			if err != nil {
				return err
			}
			if txs == nil {
				return nil
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

func (back *BlockReaderWithSnapshots) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, txAmount uint32, err error) {
	ok, err := back.sn.ViewBodies(blockHeight, func(seg *BodySegment) error {
		body, _, txAmount, _, err = back.bodyFromSnapshot(blockHeight, seg, nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if ok {
		return body, txAmount, nil
	}
	body, _, txAmount = rawdb.ReadBody(tx, hash, blockHeight)
	return body, txAmount, nil
}

func (back *BlockReaderWithSnapshots) BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	var buf []byte
	var h *types.Header
	ok, err := back.sn.ViewHeaders(blockHeight, func(seg *HeaderSegment) error {
		h, buf, err = back.headerFromSnapshot(blockHeight, seg, buf)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if ok && h != nil {
		var b *types.Body
		var baseTxnId uint64
		var txsAmount uint32
		ok, err = back.sn.ViewBodies(blockHeight, func(seg *BodySegment) error {
			b, baseTxnId, txsAmount, buf, err = back.bodyFromSnapshot(blockHeight, seg, buf)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
		if ok && b != nil {
			if txsAmount == 0 {
				block = types.NewBlockFromStorage(hash, h, nil, b.Uncles, b.Withdrawals)
				if len(senders) != block.Transactions().Len() {
					return block, senders, nil // no senders is fine - will recover them on the fly
				}
				block.SendersToTxs(senders)
				return block, senders, nil
			}
			var txs []types.Transaction
			var senders []common.Address
			ok, err = back.sn.ViewTxs(blockHeight, func(seg *TxnSegment) error {
				txs, senders, err = back.txsFromSnapshot(baseTxnId, txsAmount, seg, buf)
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return nil, nil, err
			}
			if ok {
				block = types.NewBlockFromStorage(hash, h, txs, b.Uncles, b.Withdrawals)
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

func (back *BlockReaderWithSnapshots) headerFromSnapshot(blockHeight uint64, sn *HeaderSegment, buf []byte) (*types.Header, []byte, error) {
	if sn.idxHeaderHash == nil {
		return nil, buf, nil
	}
	headerOffset := sn.idxHeaderHash.OrdinalLookup(blockHeight - sn.idxHeaderHash.BaseDataID())
	gg := sn.seg.MakeGetter()
	gg.Reset(headerOffset)
	if !gg.HasNext() {
		return nil, buf, nil
	}
	buf, _ = gg.Next(buf[:0])
	if len(buf) == 0 {
		return nil, buf, nil
	}
	h := &types.Header{}
	if err := rlp.DecodeBytes(buf[1:], h); err != nil {
		return nil, buf, err
	}
	return h, buf, nil
}

// headerFromSnapshotByHash - getting header by hash AND ensure that it has correct hash
// because HeaderByHash method will search header in all snapshots - and may request header which doesn't exists
// but because our indices are based on PerfectHashMap, no way to know is given key exists or not, only way -
// to make sure is to fetch it and compare hash
func (back *BlockReaderWithSnapshots) headerFromSnapshotByHash(hash common.Hash, sn *HeaderSegment, buf []byte) (*types.Header, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Errorf("%+v, snapshot: %d-%d, trace: %s", rec, sn.ranges.from, sn.ranges.to, dbg.Stack()))
		}
	}() // avoid crash because Erigon's core does many things

	if sn.idxHeaderHash == nil {
		return nil, nil
	}
	reader := recsplit.NewIndexReader(sn.idxHeaderHash)
	localID := reader.Lookup(hash[:])
	headerOffset := sn.idxHeaderHash.OrdinalLookup(localID)
	gg := sn.seg.MakeGetter()
	gg.Reset(headerOffset)
	if !gg.HasNext() {
		return nil, nil
	}
	buf, _ = gg.Next(buf[:0])
	if len(buf) > 1 && hash[0] != buf[0] {
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

func (back *BlockReaderWithSnapshots) bodyFromSnapshot(blockHeight uint64, sn *BodySegment, buf []byte) (*types.Body, uint64, uint32, []byte, error) {
	b, buf, err := back.bodyForStorageFromSnapshot(blockHeight, sn, buf)
	if err != nil {
		return nil, 0, 0, buf, err
	}

	body := new(types.Body)
	body.Uncles = b.Uncles
	var txsAmount uint32
	if b.TxAmount >= 2 {
		txsAmount = b.TxAmount - 2
	}
	return body, b.BaseTxId + 1, txsAmount, buf, nil // empty txs in the beginning and end of block
}

func (back *BlockReaderWithSnapshots) bodyForStorageFromSnapshot(blockHeight uint64, sn *BodySegment, buf []byte) (*types.BodyForStorage, []byte, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Errorf("%+v, snapshot: %d-%d, trace: %s", rec, sn.ranges.from, sn.ranges.to, dbg.Stack()))
		}
	}() // avoid crash because Erigon's core does many things

	if sn.idxBodyNumber == nil {
		return nil, buf, nil
	}
	bodyOffset := sn.idxBodyNumber.OrdinalLookup(blockHeight - sn.idxBodyNumber.BaseDataID())

	gg := sn.seg.MakeGetter()
	gg.Reset(bodyOffset)
	if !gg.HasNext() {
		return nil, buf, nil
	}
	buf, _ = gg.Next(buf[:0])
	if len(buf) == 0 {
		return nil, buf, nil
	}
	b := &types.BodyForStorage{}
	reader := bytes.NewReader(buf)
	if err := rlp.Decode(reader, b); err != nil {
		return nil, buf, err
	}

	if b.BaseTxId < sn.idxBodyNumber.BaseDataID() {
		return nil, buf, fmt.Errorf(".idx file has wrong baseDataID? %d<%d, %s", b.BaseTxId, sn.idxBodyNumber.BaseDataID(), sn.seg.FilePath())
	}
	return b, buf, nil
}

func (back *BlockReaderWithSnapshots) txsFromSnapshot(baseTxnID uint64, txsAmount uint32, txsSeg *TxnSegment, buf []byte) (txs []types.Transaction, senders []common.Address, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Errorf("%+v, snapshot: %d-%d, trace: %s", rec, txsSeg.ranges.from, txsSeg.ranges.to, dbg.Stack()))
		}
	}() // avoid crash because Erigon's core does many things

	if txsSeg.IdxTxnHash == nil {
		return nil, nil, nil
	}
	if baseTxnID < txsSeg.IdxTxnHash.BaseDataID() {
		return nil, nil, fmt.Errorf(".idx file has wrong baseDataID? %d<%d, %s", baseTxnID, txsSeg.IdxTxnHash.BaseDataID(), txsSeg.Seg.FilePath())
	}

	txs = make([]types.Transaction, txsAmount)
	senders = make([]common.Address, txsAmount)
	if txsAmount == 0 {
		return txs, senders, nil
	}
	txnOffset := txsSeg.IdxTxnHash.OrdinalLookup(baseTxnID - txsSeg.IdxTxnHash.BaseDataID())
	gg := txsSeg.Seg.MakeGetter()
	gg.Reset(txnOffset)
	reader := bytes.NewReader(buf)
	stream := rlp.NewStream(reader, 0)
	for i := uint32(0); i < txsAmount; i++ {
		if !gg.HasNext() {
			return nil, nil, nil
		}
		buf, _ = gg.Next(buf[:0])
		if len(buf) < 1+20 {
			return nil, nil, fmt.Errorf("segment %s has too short record: len(buf)=%d < 21", txsSeg.Seg.FilePath(), len(buf))
		}
		senders[i].SetBytes(buf[1 : 1+20])
		txRlp := buf[1+20:]
		reader.Reset(txRlp)
		stream.Reset(reader, 0)
		txs[i], err = types.DecodeTransaction(stream)
		if err != nil {
			return nil, nil, err
		}
		txs[i].SetSender(senders[i])
	}

	return txs, senders, nil
}

func (back *BlockReaderWithSnapshots) txnByID(txnID uint64, sn *TxnSegment, buf []byte) (txn types.Transaction, err error) {
	offset := sn.IdxTxnHash.OrdinalLookup(txnID - sn.IdxTxnHash.BaseDataID())
	gg := sn.Seg.MakeGetter()
	gg.Reset(offset)
	if !gg.HasNext() {
		return nil, nil
	}
	buf, _ = gg.Next(buf[:0])
	sender, txnRlp := buf[1:1+20], buf[1+20:]

	txn, err = types.DecodeTransaction(rlp.NewStream(bytes.NewReader(txnRlp), uint64(len(txnRlp))))
	if err != nil {
		return
	}
	txn.SetSender(*(*common.Address)(sender)) // see: https://tip.golang.org/ref/spec#Conversions_from_slice_to_array_pointer
	return
}

func (back *BlockReaderWithSnapshots) txnByHash(txnHash common.Hash, segments []*TxnSegment, buf []byte) (txn types.Transaction, blockNum, txnID uint64, err error) {
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.IdxTxnHash == nil || sn.IdxTxnHash2BlockNum == nil {
			continue
		}

		reader := recsplit.NewIndexReader(sn.IdxTxnHash)
		txnId := reader.Lookup(txnHash[:])
		offset := sn.IdxTxnHash.OrdinalLookup(txnId)
		gg := sn.Seg.MakeGetter()
		gg.Reset(offset)
		// first byte txnHash check - reducing false-positives 256 times. Allows don't store and don't calculate full hash of entity - when checking many snapshots.
		if !gg.MatchPrefix([]byte{txnHash[0]}) {
			continue
		}
		buf, _ = gg.Next(buf[:0])
		senderByte, txnRlp := buf[1:1+20], buf[1+20:]
		sender := *(*common.Address)(senderByte)

		txn, err = types.DecodeTransaction(rlp.NewStream(bytes.NewReader(txnRlp), uint64(len(txnRlp))))
		if err != nil {
			return
		}

		txn.SetSender(sender) // see: https://tip.golang.org/ref/spec#Conversions_from_slice_to_array_pointer

		reader2 := recsplit.NewIndexReader(sn.IdxTxnHash2BlockNum)
		blockNum = reader2.Lookup(txnHash[:])

		// final txnHash check  - completely avoid false-positives
		if txn.Hash() == txnHash {
			return
		}
	}
	return
}

// TxnByIdxInBlock - doesn't include system-transactions in the begin/end of block
// return nil if 0 < i < body.TxAmount
func (back *BlockReaderWithSnapshots) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (txn types.Transaction, err error) {
	var b *types.BodyForStorage
	ok, err := back.sn.ViewBodies(blockNum, func(segment *BodySegment) error {
		b, _, err = back.bodyForStorageFromSnapshot(blockNum, segment, nil)
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if ok {
		// if block has no transactions, or requested txNum out of non-system transactions length
		if b.TxAmount == 2 || i == -1 || i >= int(b.TxAmount-2) {
			return nil, nil
		}

		ok, err = back.sn.Txs.ViewSegment(blockNum, func(segment *TxnSegment) error {
			// +1 because block has system-txn in the beginning of block
			txn, err = back.txnByID(b.BaseTxId+1+uint64(i), segment, nil)
			if err != nil {
				return err
			}
			if txn == nil {
				return nil
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if ok {
			return txn, nil
		}
		return nil, nil
	}

	canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return nil, err
	}
	var k [8 + 32]byte
	binary.BigEndian.PutUint64(k[:], blockNum)
	copy(k[8:], canonicalHash[:])
	b, err = rawdb.ReadBodyForStorageByKey(tx, k[:])
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}

	txn, err = rawdb.CanonicalTxnByID(tx, b.BaseTxId+1+uint64(i))
	if err != nil {
		return nil, err
	}
	return txn, nil
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
