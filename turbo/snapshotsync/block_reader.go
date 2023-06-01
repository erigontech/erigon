package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/turbo/services"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
)

type RemoteBlockReader struct {
	client remote.ETHBACKENDClient
}

func (r *RemoteBlockReader) CurrentBlock(db kv.Tx) (*types.Block, error) {
	headHash := rawdb.ReadHeadBlockHash(db)
	headNumber := rawdb.ReadHeaderNumber(db, headHash)
	if headNumber == nil {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(context.Background(), db, headHash, *headNumber)
	return block, err
}
func (r *RemoteBlockReader) RawTransactions(ctx context.Context, tx kv.Getter, fromBlock, toBlock uint64) (txs [][]byte, err error) {
	panic("not implemented")
}
func (r *RemoteBlockReader) BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error) {
	hash, err := r.CanonicalHash(ctx, db, number)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (libcommon.Hash{}) {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, db, hash, number)
	return block, err
}
func (r *RemoteBlockReader) BlockByHash(ctx context.Context, db kv.Tx, hash libcommon.Hash) (*types.Block, error) {
	number := rawdb.ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, db, hash, *number)
	return block, err
}
func (r *RemoteBlockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (*types.Header, error) {
	canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockHeight)
	if err != nil {
		return nil, err
	}
	block, _, err := r.BlockWithSenders(ctx, tx, canonicalHash, blockHeight)
	if err != nil {
		return nil, err
	}
	return block.Header(), nil
}

func (r *RemoteBlockReader) Snapshots() services.BlockSnapshots { panic("not implemented") }

func (r *RemoteBlockReader) HeaderByHash(ctx context.Context, tx kv.Getter, hash libcommon.Hash) (*types.Header, error) {
	blockNum := rawdb.ReadHeaderNumber(tx, hash)
	if blockNum == nil {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, tx, hash, *blockNum)
	if err != nil {
		return nil, err
	}
	return block.Header(), nil
}

func (r *RemoteBlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (libcommon.Hash, error) {
	return rawdb.ReadCanonicalHash(tx, blockHeight)
}

func NewRemoteBlockReader(client remote.ETHBACKENDClient) *RemoteBlockReader {
	return &RemoteBlockReader{client}
}
func (r *RemoteBlockReader) TxsV3Enabled() bool {
	panic("not implemented")
}

func (r *RemoteBlockReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash libcommon.Hash) (uint64, bool, error) {
	reply, err := r.client.TxnLookup(ctx, &remote.TxnLookupRequest{TxnHash: gointerfaces.ConvertHashToH256(txnHash)})
	if err != nil {
		return 0, false, err
	}
	if reply == nil {
		return 0, false, nil
	}
	return reply.BlockNumber, true, nil
}

func (r *RemoteBlockReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (txn types.Transaction, err error) {
	canonicalHash, err := r.CanonicalHash(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	b, err := r.BodyWithTransactions(ctx, tx, canonicalHash, blockNum)
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

func (r *RemoteBlockReader) BlockWithSenders(ctx context.Context, _ kv.Getter, hash libcommon.Hash, blockHeight uint64) (block *types.Block, senders []libcommon.Address, err error) {
	reply, err := r.client.Block(ctx, &remote.BlockRequest{BlockHash: gointerfaces.ConvertHashToH256(hash), BlockHeight: blockHeight})
	if err != nil {
		return nil, nil, err
	}

	block = &types.Block{}
	err = rlp.Decode(bytes.NewReader(reply.BlockRlp), block)
	if err != nil {
		return nil, nil, err
	}
	senders = make([]libcommon.Address, len(reply.Senders)/20)
	for i := range senders {
		senders[i].SetBytes(reply.Senders[i*20 : (i+1)*20])
	}
	if len(senders) == block.Transactions().Len() { //it's fine if no senders provided - they can be lazy recovered
		block.SendersToTxs(senders)
	}
	return block, senders, nil
}

func (r *RemoteBlockReader) Header(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (*types.Header, error) {
	block, _, err := r.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Header(), nil
}
func (r *RemoteBlockReader) Body(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (body *types.Body, txAmount uint32, err error) {
	block, _, err := r.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, 0, err
	}
	if block == nil {
		return nil, 0, nil
	}
	return block.Body(), uint32(len(block.Body().Transactions)), nil
}
func (r *RemoteBlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (body *types.Body, err error) {
	block, _, err := r.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Body(), nil
}

func (r *RemoteBlockReader) BodyRlp(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
	body, err := r.BodyWithTransactions(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	bodyRlp, err = rlp.EncodeToBytes(body)
	if err != nil {
		return nil, err
	}
	return bodyRlp, nil
}

// BlockReader can read blocks from db and snapshots
type BlockReader struct {
	sn             *RoSnapshots
	TransactionsV3 bool
}

func NewBlockReader(snapshots services.BlockSnapshots, transactionsV3 bool) *BlockReader {
	return &BlockReader{sn: snapshots.(*RoSnapshots), TransactionsV3: transactionsV3}
}

func (r *BlockReader) Snapshots() services.BlockSnapshots { return r.sn }

func (r *BlockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (h *types.Header, err error) {
	blockHash, err := rawdb.ReadCanonicalHash(tx, blockHeight)
	if err != nil {
		return nil, err
	}
	if blockHash == (libcommon.Hash{}) {
		return nil, nil
	}
	h = rawdb.ReadHeader(tx, blockHash, blockHeight)
	if h != nil {
		return h, nil
	}

	view := r.sn.View()
	defer view.Close()
	seg, ok := view.HeadersSegment(blockHeight)
	if !ok {
		return
	}

	h, _, err = r.headerFromSnapshot(blockHeight, seg, nil)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// HeaderByHash - will search header in all snapshots starting from recent
func (r *BlockReader) HeaderByHash(ctx context.Context, tx kv.Getter, hash libcommon.Hash) (h *types.Header, err error) {
	h, err = rawdb.ReadHeaderByHash(tx, hash)
	if err != nil {
		return nil, err
	}
	if h != nil {
		return h, nil
	}

	view := r.sn.View()
	defer view.Close()
	segments := view.Headers()

	buf := make([]byte, 128)
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].idxHeaderHash == nil {
			continue
		}

		h, err = r.headerFromSnapshotByHash(hash, segments[i], buf)
		if err != nil {
			return nil, err
		}
		if h != nil {
			break
		}
	}
	return h, nil
}

var emptyHash = libcommon.Hash{}

func (r *BlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (h libcommon.Hash, err error) {
	h, err = rawdb.ReadCanonicalHash(tx, blockHeight)
	if err != nil {
		return h, err
	}
	if h != emptyHash {
		return h, nil
	}

	view := r.sn.View()
	defer view.Close()
	seg, ok := view.HeadersSegment(blockHeight)
	if !ok {
		return
	}

	header, _, err := r.headerFromSnapshot(blockHeight, seg, nil)
	if err != nil {
		return h, err
	}
	if header == nil {
		return h, nil
	}
	h = header.Hash()
	return h, nil
}

func (r *BlockReader) Header(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (h *types.Header, err error) {
	h = rawdb.ReadHeader(tx, hash, blockHeight)
	if h != nil {
		return h, nil
	}

	view := r.sn.View()
	defer view.Close()
	seg, ok := view.HeadersSegment(blockHeight)
	if !ok {
		return
	}
	h, _, err = r.headerFromSnapshot(blockHeight, seg, nil)
	if err != nil {
		return h, err
	}
	return h, nil
}

func (r *BlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (body *types.Body, err error) {
	body, err = rawdb.ReadBodyWithTransactions(tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if body != nil {
		return body, nil
	}

	view := r.sn.View()
	defer view.Close()

	var baseTxnID uint64
	var txsAmount uint32
	var buf []byte
	seg, ok := view.BodiesSegment(blockHeight)
	if !ok {
		return nil, nil
	}
	body, baseTxnID, txsAmount, buf, err = r.bodyFromSnapshot(blockHeight, seg, buf)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}
	txnSeg, ok := view.TxsSegment(blockHeight)
	if !ok {
		return nil, nil
	}
	txs, senders, err := r.txsFromSnapshot(baseTxnID, txsAmount, txnSeg, buf)
	if err != nil {
		return nil, err
	}
	if txs == nil {
		return nil, nil
	}
	body.Transactions = txs
	body.SendersToTxs(senders)
	return body, nil
}

func (r *BlockReader) BodyRlp(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
	body, err := r.BodyWithTransactions(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	bodyRlp, err = rlp.EncodeToBytes(body)
	if err != nil {
		return nil, err
	}
	return bodyRlp, nil
}

func (r *BlockReader) Body(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (body *types.Body, txAmount uint32, err error) {
	blocksAvailable := r.sn.BlocksAvailable()
	if blocksAvailable == 0 || blockHeight > blocksAvailable {
		body, _, txAmount = rawdb.ReadBody(tx, hash, blockHeight)
		return body, txAmount, nil
	}
	view := r.sn.View()
	defer view.Close()

	seg, ok := view.BodiesSegment(blockHeight)
	if !ok {
		return
	}
	body, _, txAmount, _, err = r.bodyFromSnapshot(blockHeight, seg, nil)
	if err != nil {
		return nil, 0, err
	}
	return body, txAmount, nil
}

func (r *BlockReader) BlockWithSenders(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (block *types.Block, senders []libcommon.Address, err error) {
	return r.blockWithSenders(ctx, tx, hash, blockHeight, false)
}
func (r *BlockReader) blockWithSenders(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64, forceCanonical bool) (block *types.Block, senders []libcommon.Address, err error) {
	blocksAvailable := r.sn.BlocksAvailable()
	if blocksAvailable == 0 || blockHeight > blocksAvailable {
		if r.TransactionsV3 {
			if forceCanonical {
				canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockHeight)
				if err != nil {
					return nil, nil, fmt.Errorf("requested non-canonical hash %x. canonical=%x", hash, canonicalHash)
				}
				if canonicalHash != hash {
					return nil, nil, nil
				}
			}

			block, senders, err = rawdb.ReadBlockWithSenders(tx, hash, blockHeight)
			if err != nil {
				return nil, nil, err
			}
			return block, senders, nil
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
		if forceCanonical {
			return nil, nil, err
		}
		return rawdb.NonCanonicalBlockWithSenders(tx, hash, blockHeight)
	}

	view := r.sn.View()
	defer view.Close()
	seg, ok := view.HeadersSegment(blockHeight)
	if !ok {
		return
	}

	var buf []byte
	h, buf, err := r.headerFromSnapshot(blockHeight, seg, buf)
	if err != nil {
		return nil, nil, err
	}
	if h == nil {
		return
	}

	var b *types.Body
	var baseTxnId uint64
	var txsAmount uint32
	bodySeg, ok := view.BodiesSegment(blockHeight)
	if !ok {
		return
	}
	b, baseTxnId, txsAmount, buf, err = r.bodyFromSnapshot(blockHeight, bodySeg, buf)
	if err != nil {
		return nil, nil, err
	}
	if b == nil {
		return
	}
	if txsAmount == 0 {
		block = types.NewBlockFromStorage(hash, h, nil, b.Uncles, b.Withdrawals)
		if len(senders) != block.Transactions().Len() {
			return block, senders, nil // no senders is fine - will recover them on the fly
		}
		block.SendersToTxs(senders)
		return block, senders, nil
	}

	txnSeg, ok := view.TxsSegment(blockHeight)
	if !ok {
		return
	}
	var txs []types.Transaction
	txs, senders, err = r.txsFromSnapshot(baseTxnId, txsAmount, txnSeg, buf)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return
	}
	block = types.NewBlockFromStorage(hash, h, txs, b.Uncles, b.Withdrawals)
	if len(senders) != block.Transactions().Len() {
		return block, senders, nil // no senders is fine - will recover them on the fly
	}
	block.SendersToTxs(senders)
	return block, senders, nil
}

func (r *BlockReader) headerFromSnapshot(blockHeight uint64, sn *HeaderSegment, buf []byte) (*types.Header, []byte, error) {
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
func (r *BlockReader) headerFromSnapshotByHash(hash libcommon.Hash, sn *HeaderSegment, buf []byte) (*types.Header, error) {
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

func (r *BlockReader) bodyFromSnapshot(blockHeight uint64, sn *BodySegment, buf []byte) (*types.Body, uint64, uint32, []byte, error) {
	b, buf, err := r.bodyForStorageFromSnapshot(blockHeight, sn, buf)
	if err != nil {
		return nil, 0, 0, buf, err
	}

	body := new(types.Body)
	body.Uncles = b.Uncles
	body.Withdrawals = b.Withdrawals
	var txsAmount uint32
	if b.TxAmount >= 2 {
		txsAmount = b.TxAmount - 2
	}
	return body, b.BaseTxId + 1, txsAmount, buf, nil // empty txs in the beginning and end of block
}

func (r *BlockReader) bodyForStorageFromSnapshot(blockHeight uint64, sn *BodySegment, buf []byte) (*types.BodyForStorage, []byte, error) {
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

func (r *BlockReader) txsFromSnapshot(baseTxnID uint64, txsAmount uint32, txsSeg *TxnSegment, buf []byte) (txs []types.Transaction, senders []libcommon.Address, err error) {
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
	senders = make([]libcommon.Address, txsAmount)
	if txsAmount == 0 {
		return txs, senders, nil
	}
	txnOffset := txsSeg.IdxTxnHash.OrdinalLookup(baseTxnID - txsSeg.IdxTxnHash.BaseDataID())
	gg := txsSeg.Seg.MakeGetter()
	gg.Reset(txnOffset)
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
		txs[i], err = types.DecodeTransaction(txRlp)
		if err != nil {
			return nil, nil, err
		}
		txs[i].SetSender(senders[i])
	}

	return txs, senders, nil
}

func (r *BlockReader) txnByID(txnID uint64, sn *TxnSegment, buf []byte) (txn types.Transaction, err error) {
	offset := sn.IdxTxnHash.OrdinalLookup(txnID - sn.IdxTxnHash.BaseDataID())
	gg := sn.Seg.MakeGetter()
	gg.Reset(offset)
	if !gg.HasNext() {
		return nil, nil
	}
	buf, _ = gg.Next(buf[:0])
	sender, txnRlp := buf[1:1+20], buf[1+20:]

	txn, err = types.DecodeTransaction(txnRlp)
	if err != nil {
		return
	}
	txn.SetSender(*(*libcommon.Address)(sender)) // see: https://tip.golang.org/ref/spec#Conversions_from_slice_to_array_pointer
	return
}

func (r *BlockReader) txnByHash(txnHash libcommon.Hash, segments []*TxnSegment, buf []byte) (txn types.Transaction, blockNum, txnID uint64, err error) {
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
		sender := *(*libcommon.Address)(senderByte)

		txn, err = types.DecodeTransaction(txnRlp)
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
func (r *BlockReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (txn types.Transaction, err error) {
	blocksAvailable := r.sn.BlocksAvailable()
	if blocksAvailable == 0 || blockNum > blocksAvailable {
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

		txn, err = rawdb.CanonicalTxnByID(tx, b.BaseTxId+1+uint64(i), canonicalHash, r.TransactionsV3)
		if err != nil {
			return nil, err
		}
		return txn, nil
	}

	view := r.sn.View()
	defer view.Close()
	seg, ok := view.BodiesSegment(blockNum)
	if !ok {
		return
	}

	var b *types.BodyForStorage
	b, _, err = r.bodyForStorageFromSnapshot(blockNum, seg, nil)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return
	}

	// if block has no transactions, or requested txNum out of non-system transactions length
	if b.TxAmount == 2 || i == -1 || i >= int(b.TxAmount-2) {
		return nil, nil
	}

	txnSeg, ok := view.TxsSegment(blockNum)
	if !ok {
		return
	}
	// +1 because block has system-txn in the beginning of block
	return r.txnByID(b.BaseTxId+1+uint64(i), txnSeg, nil)
}

// TxnLookup - find blockNumber and txnID by txnHash
func (r *BlockReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash libcommon.Hash) (uint64, bool, error) {
	n, err := rawdb.ReadTxLookupEntry(tx, txnHash)
	if err != nil {
		return 0, false, err
	}
	if n != nil {
		return *n, true, nil
	}

	view := r.sn.View()
	defer view.Close()

	var txn types.Transaction
	var blockNum uint64
	txn, blockNum, _, err = r.txnByHash(txnHash, view.Txs(), nil)
	if err != nil {
		return 0, false, err
	}
	if txn == nil {
		return 0, false, nil
	}
	return blockNum, true, nil
}

func (r *BlockReader) LastTxNumInSnapshot(blockNum uint64) (uint64, bool, error) {
	view := r.sn.View()
	defer view.Close()

	sn, ok := view.TxsSegment(blockNum)
	if !ok {
		return 0, false, nil
	}

	lastTxnID := sn.IdxTxnHash.BaseDataID() + uint64(sn.Seg.Count())
	return lastTxnID, true, nil
}

func (r *BlockReader) IterateBodies(f func(blockNum, baseTxNum, txAmount uint64) error) error {
	view := r.sn.View()
	defer view.Close()

	for _, sn := range view.Bodies() {
		sn := sn
		defer sn.seg.EnableMadvNormal().DisableReadAhead()

		var buf []byte
		g := sn.seg.MakeGetter()
		blockNum := sn.ranges.from
		var b types.BodyForStorage
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
			if err := rlp.DecodeBytes(buf, &b); err != nil {
				return err
			}
			if err := f(blockNum, b.BaseTxId, uint64(b.TxAmount)); err != nil {
				return err
			}
			blockNum++
		}
	}
	return nil
}
func (r *BlockReader) TxsV3Enabled() bool { return r.TransactionsV3 }
func (r *BlockReader) BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error) {
	hash, err := rawdb.ReadCanonicalHash(db, number)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (libcommon.Hash{}) {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, db, hash, number)
	return block, err
}
func (r *BlockReader) BlockByHash(ctx context.Context, db kv.Tx, hash libcommon.Hash) (*types.Block, error) {
	number := rawdb.ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, db, hash, *number)
	return block, err
}
func (r *BlockReader) CurrentBlock(db kv.Tx) (*types.Block, error) {
	headHash := rawdb.ReadHeadBlockHash(db)
	headNumber := rawdb.ReadHeaderNumber(db, headHash)
	if headNumber == nil {
		return nil, nil
	}
	block, _, err := r.blockWithSenders(context.Background(), db, headHash, *headNumber, true)
	return block, err
}
func (r *BlockReader) RawTransactions(ctx context.Context, tx kv.Getter, fromBlock, toBlock uint64) (txs [][]byte, err error) {
	return rawdb.RawTransactionsRange(tx, fromBlock, toBlock)
}
