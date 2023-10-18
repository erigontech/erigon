package freezeblocks

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/eth/ethconfig"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type RemoteBlockReader struct {
	client remote.ETHBACKENDClient
}

func (r *RemoteBlockReader) CanPruneTo(uint64) uint64 {
	panic("not implemented")
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
func (r *RemoteBlockReader) ReadAncestor(db kv.Getter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
	panic("not implemented")
}
func (r *RemoteBlockReader) HeadersRange(ctx context.Context, walker func(header *types.Header) error) error {
	panic("not implemented")
}

func (r *RemoteBlockReader) BadHeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (blockHeight *uint64, err error) {
	return rawdb.ReadBadHeaderNumber(tx, hash)
}

func (r *RemoteBlockReader) BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error) {
	hash, err := r.CanonicalHash(ctx, db, number)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (common.Hash{}) {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, db, hash, number)
	return block, err
}
func (r *RemoteBlockReader) BlockByHash(ctx context.Context, db kv.Tx, hash common.Hash) (*types.Block, error) {
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

func (r *RemoteBlockReader) Snapshots() services.BlockSnapshots    { panic("not implemented") }
func (r *RemoteBlockReader) BorSnapshots() services.BlockSnapshots { panic("not implemented") }
func (r *RemoteBlockReader) FrozenBlocks() uint64                  { panic("not supported") }
func (r *RemoteBlockReader) FrozenBorBlocks() uint64               { panic("not supported") }
func (r *RemoteBlockReader) FrozenFiles() (list []string)          { panic("not supported") }
func (r *RemoteBlockReader) FreezingCfg() ethconfig.BlocksFreezing { panic("not supported") }

func (r *RemoteBlockReader) HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error) {
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

func (r *RemoteBlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (common.Hash, error) {
	return rawdb.ReadCanonicalHash(tx, blockHeight)
}

func NewRemoteBlockReader(client remote.ETHBACKENDClient) *RemoteBlockReader {
	return &RemoteBlockReader{client}
}

func (r *RemoteBlockReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
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

func (r *RemoteBlockReader) HasSenders(ctx context.Context, _ kv.Getter, hash common.Hash, blockHeight uint64) (bool, error) {
	panic("HasSenders is low-level method, don't use it in RPCDaemon")
}

func (r *RemoteBlockReader) BlockWithSenders(ctx context.Context, _ kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	reply, err := r.client.Block(ctx, &remote.BlockRequest{BlockHash: gointerfaces.ConvertHashToH256(hash), BlockHeight: blockHeight})
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

func (r *RemoteBlockReader) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (*types.Header, error) {
	block, _, err := r.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Header(), nil
}
func (r *RemoteBlockReader) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, txAmount uint32, err error) {
	block, _, err := r.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, 0, err
	}
	if block == nil {
		return nil, 0, nil
	}
	return block.Body(), uint32(len(block.Body().Transactions)), nil
}
func (r *RemoteBlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	block, _, err := r.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Body(), nil
}

func (r *RemoteBlockReader) BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
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

func (r *RemoteBlockReader) EventLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
	reply, err := r.client.BorEvent(ctx, &remote.BorEventRequest{BorTxHash: gointerfaces.ConvertHashToH256(txnHash)})
	if err != nil {
		return 0, false, err
	}
	if reply == nil || len(reply.EventRlps) == 0 {
		return 0, false, nil
	}
	return reply.BlockNumber, true, nil
}

func (r *RemoteBlockReader) EventsByBlock(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) ([]rlp.RawValue, error) {
	borTxnHash := types.ComputeBorTxHash(blockHeight, hash)
	reply, err := r.client.BorEvent(ctx, &remote.BorEventRequest{BorTxHash: gointerfaces.ConvertHashToH256(borTxnHash)})
	if err != nil {
		return nil, err
	}
	result := make([]rlp.RawValue, len(reply.EventRlps))
	for i, r := range reply.EventRlps {
		result[i] = rlp.RawValue(r)
	}
	return result, nil
}

func (r *RemoteBlockReader) Span(ctx context.Context, tx kv.Getter, spanId uint64) ([]byte, error) {
	return nil, nil
}

// BlockReader can read blocks from db and snapshots
type BlockReader struct {
	sn    *RoSnapshots
	borSn *BorRoSnapshots
}

func NewBlockReader(snapshots services.BlockSnapshots, borSnapshots services.BlockSnapshots) *BlockReader {
	return &BlockReader{sn: snapshots.(*RoSnapshots), borSn: borSnapshots.(*BorRoSnapshots)}
}

func (r *BlockReader) CanPruneTo(currentBlockInDB uint64) uint64 {
	return CanDeleteTo(currentBlockInDB, r.sn.BlocksAvailable())
}
func (r *BlockReader) Snapshots() services.BlockSnapshots { return r.sn }
func (r *BlockReader) BorSnapshots() services.BlockSnapshots {
	if r.borSn != nil {
		return r.borSn
	}

	return nil
}

func (r *BlockReader) FrozenBlocks() uint64    { return r.sn.BlocksAvailable() }
func (r *BlockReader) FrozenBorBlocks() uint64 { return r.borSn.BlocksAvailable() }
func (r *BlockReader) FrozenFiles() []string {
	files := r.sn.Files()
	if r.borSn != nil {
		files = append(files, r.borSn.Files()...)
	}
	sort.Strings(files)
	return files
}
func (r *BlockReader) FreezingCfg() ethconfig.BlocksFreezing { return r.sn.Cfg() }

func (r *BlockReader) HeadersRange(ctx context.Context, walker func(header *types.Header) error) error {
	return ForEachHeader(ctx, r.sn, walker)
}

func (r *BlockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (h *types.Header, err error) {
	blockHash, err := rawdb.ReadCanonicalHash(tx, blockHeight)
	if err != nil {
		return nil, err
	}
	if blockHash == (common.Hash{}) {
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
func (r *BlockReader) HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (h *types.Header, err error) {
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

var emptyHash = common.Hash{}

func (r *BlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (h common.Hash, err error) {
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

func (r *BlockReader) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (h *types.Header, err error) {
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

func (r *BlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
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

func (r *BlockReader) BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
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

func (r *BlockReader) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, txAmount uint32, err error) {
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

func (r *BlockReader) HasSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bool, error) {
	blocksAvailable := r.sn.BlocksAvailable()
	if blocksAvailable == 0 || blockHeight > blocksAvailable {
		return rawdb.HasSenders(tx, hash, blockHeight)
	}
	return true, nil
}

func (r *BlockReader) BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	return r.blockWithSenders(ctx, tx, hash, blockHeight, false)
}
func (r *BlockReader) blockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64, forceCanonical bool) (block *types.Block, senders []common.Address, err error) {
	blocksAvailable := r.sn.BlocksAvailable()
	if blocksAvailable == 0 || blockHeight > blocksAvailable {
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
func (r *BlockReader) headerFromSnapshotByHash(hash common.Hash, sn *HeaderSegment, buf []byte) (*types.Header, error) {
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

	return b, buf, nil
}

func (r *BlockReader) txsFromSnapshot(baseTxnID uint64, txsAmount uint32, txsSeg *TxnSegment, buf []byte) (txs []types.Transaction, senders []common.Address, err error) {
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
	txn.SetSender(*(*common.Address)(sender)) // see: https://tip.golang.org/ref/spec#Conversions_from_slice_to_array_pointer
	return
}

func (r *BlockReader) txnByHash(txnHash common.Hash, segments []*TxnSegment, buf []byte) (txn types.Transaction, blockNum, txnID uint64, err error) {
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
func (r *BlockReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, txIdxInBlock int) (txn types.Transaction, err error) {
	blocksAvailable := r.sn.BlocksAvailable()
	if blocksAvailable == 0 || blockNum > blocksAvailable {
		canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return nil, err
		}
		return rawdb.TxnByIdxInBlock(tx, canonicalHash, blockNum, txIdxInBlock)
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
	if b.TxAmount == 2 || txIdxInBlock == -1 || txIdxInBlock >= int(b.TxAmount-2) {
		return nil, nil
	}

	txnSeg, ok := view.TxsSegment(blockNum)
	if !ok {
		return
	}
	// +1 because block has system-txn in the beginning of block
	return r.txnByID(b.BaseTxId+1+uint64(txIdxInBlock), txnSeg, nil)
}

// TxnLookup - find blockNumber and txnID by txnHash
func (r *BlockReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
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

func (r *BlockReader) FirstTxNumNotInSnapshots() uint64 {
	view := r.sn.View()
	defer view.Close()

	sn, ok := view.TxsSegment(r.sn.BlocksAvailable())
	if !ok {
		return 0
	}

	lastTxnID := sn.IdxTxnHash.BaseDataID() + uint64(sn.Seg.Count())
	return lastTxnID
}

func (r *BlockReader) IterateFrozenBodies(f func(blockNum, baseTxNum, txAmount uint64) error) error {
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
func (r *BlockReader) BadHeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (blockHeight *uint64, err error) {
	return rawdb.ReadBadHeaderNumber(tx, hash)
}
func (r *BlockReader) BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error) {
	hash, err := rawdb.ReadCanonicalHash(db, number)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (common.Hash{}) {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, db, hash, number)
	return block, err
}
func (r *BlockReader) BlockByHash(ctx context.Context, db kv.Tx, hash common.Hash) (*types.Block, error) {
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
func (r *BlockReader) ReadAncestor(db kv.Getter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
	if ancestor > number {
		return common.Hash{}, 0
	}
	if ancestor == 1 {
		header, err := r.Header(context.Background(), db, hash, number)
		if err != nil {
			panic(err)
		}
		// in this case it is cheaper to just read the header
		if header != nil {
			return header.ParentHash, number - 1
		}
		return common.Hash{}, 0
	}
	for ancestor != 0 {
		h, err := r.CanonicalHash(context.Background(), db, number)
		if err != nil {
			panic(err)
		}
		if h == hash {
			ancestorHash, err := r.CanonicalHash(context.Background(), db, number-ancestor)
			if err != nil {
				panic(err)
			}
			h, err := r.CanonicalHash(context.Background(), db, number)
			if err != nil {
				panic(err)
			}
			if h == hash {
				number -= ancestor
				return ancestorHash, number
			}
		}
		if *maxNonCanonical == 0 {
			return common.Hash{}, 0
		}
		*maxNonCanonical--
		ancestor--
		header, err := r.Header(context.Background(), db, hash, number)
		if err != nil {
			panic(err)
		}
		if header == nil {
			return common.Hash{}, 0
		}
		hash = header.ParentHash
		number--
	}
	return hash, number
}

func (r *BlockReader) EventLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
	n, err := rawdb.ReadBorTxLookupEntry(tx, txnHash)
	if err != nil {
		return 0, false, err
	}
	if n != nil {
		return *n, true, nil
	}

	view := r.borSn.View()
	defer view.Close()

	blockNum, ok, err := r.borBlockByEventHash(txnHash, view.Events(), nil)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	return blockNum, true, nil
}

func (r *BlockReader) borBlockByEventHash(txnHash common.Hash, segments []*BorEventSegment, buf []byte) (blockNum uint64, ok bool, err error) {
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.IdxBorTxnHash == nil {
			continue
		}
		if sn.IdxBorTxnHash.KeyCount() == 0 {
			continue
		}
		reader := recsplit.NewIndexReader(sn.IdxBorTxnHash)
		blockEventId := reader.Lookup(txnHash[:])
		offset := sn.IdxBorTxnHash.OrdinalLookup(blockEventId)
		gg := sn.seg.MakeGetter()
		gg.Reset(offset)
		if !gg.MatchPrefix(txnHash[:]) {
			continue
		}
		buf, _ = gg.Next(buf[:0])
		blockNum = binary.BigEndian.Uint64(buf[length.Hash:])
		ok = true
		return
	}
	return
}

func (r *BlockReader) EventsByBlock(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) ([]rlp.RawValue, error) {
	if blockHeight >= r.FrozenBorBlocks() {
		c, err := tx.Cursor(kv.BorEventNums)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		var k, v []byte
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], blockHeight)
		result := []rlp.RawValue{}
		if k, v, err = c.Seek(buf[:]); err != nil {
			return nil, err
		}
		if !bytes.Equal(k, buf[:]) {
			return result, nil
		}
		startEventId := binary.BigEndian.Uint64(v)
		var endEventId uint64
		if k, v, err = c.Next(); err != nil {
			return nil, err
		}
		if k == nil {
			endEventId = math.MaxUint64
		} else {
			endEventId = binary.BigEndian.Uint64(v)
		}
		c1, err := tx.Cursor(kv.BorEvents)
		if err != nil {
			return nil, err
		}
		defer c1.Close()
		binary.BigEndian.PutUint64(buf[:], startEventId)
		for k, v, err = c1.Seek(buf[:]); err == nil && k != nil; k, v, err = c1.Next() {
			eventId := binary.BigEndian.Uint64(k)
			if eventId >= endEventId {
				break
			}
			result = append(result, rlp.RawValue(common.Copy(v)))
		}
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	borTxHash := types.ComputeBorTxHash(blockHeight, hash)
	view := r.borSn.View()
	defer view.Close()
	segments := view.Events()
	var buf []byte
	result := []rlp.RawValue{}
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.ranges.from > blockHeight {
			continue
		}
		if sn.ranges.to <= blockHeight {
			continue
		}
		if sn.IdxBorTxnHash == nil {
			continue
		}
		if sn.IdxBorTxnHash.KeyCount() == 0 {
			continue
		}
		reader := recsplit.NewIndexReader(sn.IdxBorTxnHash)
		blockEventId := reader.Lookup(borTxHash[:])
		offset := sn.IdxBorTxnHash.OrdinalLookup(blockEventId)
		gg := sn.seg.MakeGetter()
		gg.Reset(offset)
		for gg.HasNext() && gg.MatchPrefix(borTxHash[:]) {
			buf, _ = gg.Next(buf[:0])
			result = append(result, rlp.RawValue(common.Copy(buf[length.Hash+length.BlockNum+8:])))
		}
	}
	return result, nil
}

func (r *BlockReader) LastFrozenEventID() uint64 {
	view := r.borSn.View()
	defer view.Close()
	segments := view.Events()
	if len(segments) == 0 {
		return 0
	}
	lastSegment := segments[len(segments)-1]
	var lastEventID uint64
	gg := lastSegment.seg.MakeGetter()
	var buf []byte
	for gg.HasNext() {
		buf, _ = gg.Next(buf[:0])
		lastEventID = binary.BigEndian.Uint64(buf[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
	}
	return lastEventID
}

func (r *BlockReader) LastFrozenSpanID() uint64 {
	view := r.borSn.View()
	defer view.Close()
	segments := view.Spans()
	if len(segments) == 0 {
		return 0
	}
	lastSegment := segments[len(segments)-1]
	var lastSpanID uint64
	if lastSegment.ranges.to > zerothSpanEnd {
		lastSpanID = (lastSegment.ranges.to - zerothSpanEnd - 1) / spanLength
	}
	return lastSpanID
}

func (r *BlockReader) Span(ctx context.Context, tx kv.Getter, spanId uint64) ([]byte, error) {
	// Compute starting block of the span
	var endBlock uint64
	if spanId > 0 {
		endBlock = (spanId)*spanLength + zerothSpanEnd
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], spanId)
	if endBlock >= r.FrozenBorBlocks() {
		v, err := tx.GetOne(kv.BorSpans, buf[:])
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, fmt.Errorf("span %d not found (db)", spanId)
		}
		return common.Copy(v), nil
	}
	view := r.borSn.View()
	defer view.Close()
	segments := view.Spans()
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.idx == nil {
			continue
		}
		var spanFrom uint64
		if sn.ranges.from > zerothSpanEnd {
			spanFrom = 1 + (sn.ranges.from-zerothSpanEnd-1)/spanLength
		}
		if spanId < spanFrom {
			continue
		}
		var spanTo uint64
		if sn.ranges.to > zerothSpanEnd {
			spanTo = 1 + (sn.ranges.to-zerothSpanEnd-1)/spanLength
		}
		if spanId >= spanTo {
			continue
		}
		if sn.idx.KeyCount() == 0 {
			continue
		}
		offset := sn.idx.OrdinalLookup(spanId - sn.idx.BaseDataID())
		gg := sn.seg.MakeGetter()
		gg.Reset(offset)
		result, _ := gg.Next(nil)
		return common.Copy(result), nil
	}
	return nil, fmt.Errorf("span %d not found (snapshots)", spanId)
}

// ---- Data Integrity part ----

func (r *BlockReader) ensureHeaderNumber(n uint64, seg *HeaderSegment) error {
	h, _, err := r.headerFromSnapshot(n, seg, nil)
	if err != nil {
		return err
	}
	if h == nil {
		return fmt.Errorf("ensureHeaderNumber: not found header: %d", n)
	}
	if h.Number.Uint64() != n {
		return fmt.Errorf("ensureHeaderNumber: requested header: %d, got: %d", n, h.Number.Uint64())
	}
	return nil
}

func (r *BlockReader) Integrity(ctx context.Context) error {
	view := r.sn.View()
	defer view.Close()
	for _, seg := range view.Headers() {
		if err := r.ensureHeaderNumber(seg.ranges.from, seg); err != nil {
			return err
		}
		if err := r.ensureHeaderNumber(seg.ranges.to-1, seg); err != nil {
			return err
		}
	}
	return nil
}
