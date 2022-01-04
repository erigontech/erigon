package snapshotsync

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
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

func (back *BlockReader) Body(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	body, _, _ = rawdb.ReadBody(tx, hash, blockHeight)
	return body, nil
}

func (back *BlockReader) BodyRlp(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
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

func (back *BlockReader) BlockWithSenders(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
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

type RemoteBlockReader struct {
	client remote.ETHBACKENDClient
}

func NewRemoteBlockReader(client remote.ETHBACKENDClient) *RemoteBlockReader {
	return &RemoteBlockReader{client}
}

func (back *RemoteBlockReader) BlockWithSenders(ctx context.Context, _ kv.Tx, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
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

func (back *RemoteBlockReader) Header(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (*types.Header, error) {
	block, _, err := back.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Header(), nil
}
func (back *RemoteBlockReader) Body(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	block, _, err := back.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Body(), nil
}

func (back *RemoteBlockReader) BodyRlp(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
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

// BlockReaderWithSnapshots can read blocks from db and snapshots
type BlockReaderWithSnapshots struct {
	sn   *AllSnapshots
	lock sync.RWMutex
}

func NewBlockReaderWithSnapshots(snapshots *AllSnapshots) *BlockReaderWithSnapshots {
	return &BlockReaderWithSnapshots{sn: snapshots}
}
func (back *BlockReaderWithSnapshots) HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (*types.Header, error) {
	sn, ok := back.sn.Blocks(blockHeight)
	if !ok {
		h := rawdb.ReadHeaderByNumber(tx, blockHeight)
		return h, nil
	}
	return back.headerFromSnapshot(blockHeight, sn)
}

// HeaderByHash - will search header in all snapshots starting from recent
func (back *BlockReaderWithSnapshots) HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error) {
	h, err := rawdb.ReadHeaderByHash(tx, hash)
	if err != nil {
		return nil, err
	}
	if h != nil {
		return h, nil
	}

	for i := len(back.sn.blocks) - 1; i >= 0; i-- {
		h, err := back.headerFromSnapshotByHash(hash, back.sn.blocks[i])
		if err != nil {
			return nil, nil
		}
		if h != nil {
			return h, nil
		}
	}
	return nil, nil
}

func (back *BlockReaderWithSnapshots) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (common.Hash, error) {
	sn, ok := back.sn.Blocks(blockHeight)
	if !ok {
		return rawdb.ReadCanonicalHash(tx, blockHeight)
	}

	h, err := back.headerFromSnapshot(blockHeight, sn)
	if err != nil {
		return common.Hash{}, err
	}
	if h == nil {
		return common.Hash{}, err
	}
	return h.Hash(), nil
}

func (back *BlockReaderWithSnapshots) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (*types.Header, error) {
	sn, ok := back.sn.Blocks(blockHeight)
	if !ok {
		h := rawdb.ReadHeader(tx, hash, blockHeight)
		return h, nil
	}

	return back.headerFromSnapshot(blockHeight, sn)
}

func (back *BlockReaderWithSnapshots) ReadHeaderByNumber(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (*types.Header, error) {
	sn, ok := back.sn.Blocks(blockHeight)
	if !ok {
		h := rawdb.ReadHeader(tx, hash, blockHeight)
		return h, nil
	}

	return back.headerFromSnapshot(blockHeight, sn)
}

func (back *BlockReaderWithSnapshots) Body(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	sn, ok := back.sn.Blocks(blockHeight)
	if !ok {
		canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockHeight)
		if err != nil {
			return nil, fmt.Errorf("requested non-canonical hash %x. canonical=%x", hash, canonicalHash)
		}
		body, baseTxID, txsAmount := rawdb.ReadBody(tx, hash, blockHeight)
		if body == nil {
			return nil, fmt.Errorf("body not found for block %d,%x", blockHeight, hash)
		}
		if canonicalHash == hash {
			body.Transactions, err = rawdb.CanonicalTransactions(tx, baseTxID, txsAmount)
			if err != nil {
				return nil, err
			}
			return body, nil
		}
		body.Transactions, err = rawdb.NonCanonicalTransactions(tx, baseTxID, txsAmount)
		if err != nil {
			return nil, err
		}
		return body, nil
	}

	body, _, _, _, err = back.bodyFromSnapshot(blockHeight, sn)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (back *BlockReaderWithSnapshots) BodyRlp(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
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

func (back *BlockReaderWithSnapshots) BlockWithSenders(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	sn, ok := back.sn.Blocks(blockHeight)
	if !ok {
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

	buf := make([]byte, 16)

	back.lock.Lock()
	defer back.lock.Unlock()

	headerOffset := sn.HeaderHashIdx.Lookup2(blockHeight - sn.HeaderHashIdx.BaseDataID())
	bodyOffset := sn.BodyNumberIdx.Lookup2(blockHeight - sn.BodyNumberIdx.BaseDataID())

	gg := sn.Headers.MakeGetter()
	gg.Reset(headerOffset)
	buf, _ = gg.Next(buf[:0])
	h := &types.Header{}
	if err = rlp.DecodeBytes(buf[1:], h); err != nil {
		return nil, nil, err
	}

	gg = sn.Bodies.MakeGetter()
	gg.Reset(bodyOffset)
	buf, _ = gg.Next(buf[:0])
	b := &types.BodyForStorage{}
	reader := bytes.NewReader(buf)
	if err = rlp.Decode(reader, b); err != nil {
		return nil, nil, err
	}

	if b.BaseTxId < sn.TxnHashIdx.BaseDataID() {
		return nil, nil, fmt.Errorf(".idx file has wrong baseDataID? %d<%d, %s", b.BaseTxId, sn.TxnHashIdx.BaseDataID(), sn.Transactions.FilePath())
	}

	txs := make([]types.Transaction, b.TxAmount)
	senders = make([]common.Address, b.TxAmount)
	if b.TxAmount > 0 {
		txnOffset := sn.TxnHashIdx.Lookup2(b.BaseTxId - sn.TxnHashIdx.BaseDataID()) // need subtract baseID of indexFile
		gg = sn.Transactions.MakeGetter()
		gg.Reset(txnOffset)
		stream := rlp.NewStream(reader, 0)
		for i := uint32(0); i < b.TxAmount; i++ {
			buf, _ = gg.Next(buf[:0])
			senders[i].SetBytes(buf[1 : 1+20])
			txRlp := buf[1+20:]
			reader.Reset(txRlp)
			stream.Reset(reader, 0)
			txs[i], err = types.DecodeTransaction(stream)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	block = types.NewBlockFromStorage(hash, h, txs, b.Uncles)
	if len(senders) != block.Transactions().Len() {
		return block, senders, nil // no senders is fine - will recover them on the fly
	}
	block.SendersToTxs(senders)
	return block, senders, nil
}

func (back *BlockReaderWithSnapshots) headerFromSnapshot(blockHeight uint64, sn *BlocksSnapshot) (*types.Header, error) {
	buf := make([]byte, 16)

	headerOffset := sn.HeaderHashIdx.Lookup2(blockHeight - sn.HeaderHashIdx.BaseDataID())
	gg := sn.Headers.MakeGetter()
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
func (back *BlockReaderWithSnapshots) headerFromSnapshotByHash(hash common.Hash, sn *BlocksSnapshot) (*types.Header, error) {
	buf := make([]byte, 16)

	localID := sn.HeaderHashIdx.Lookup(hash[:])
	headerOffset := sn.HeaderHashIdx.Lookup2(localID)
	gg := sn.Headers.MakeGetter()
	gg.Reset(headerOffset)
	buf, _ = gg.Next(buf[:0])
	if hash[0] != buf[1] {
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

func (back *BlockReaderWithSnapshots) bodyFromSnapshot(blockHeight uint64, sn *BlocksSnapshot) (*types.Body, []common.Address, uint64, uint32, error) {
	buf := make([]byte, 16)

	bodyOffset := sn.BodyNumberIdx.Lookup2(blockHeight - sn.BodyNumberIdx.BaseDataID())

	gg := sn.Bodies.MakeGetter()
	gg.Reset(bodyOffset)
	buf, _ = gg.Next(buf[:0])
	b := &types.BodyForStorage{}
	reader := bytes.NewReader(buf)
	if err := rlp.Decode(reader, b); err != nil {
		return nil, nil, 0, 0, err
	}

	if b.BaseTxId < sn.TxnHashIdx.BaseDataID() {
		return nil, nil, 0, 0, fmt.Errorf(".idx file has wrong baseDataID? %d<%d, %s", b.BaseTxId, sn.TxnHashIdx.BaseDataID(), sn.Transactions.FilePath())
	}

	txs := make([]types.Transaction, b.TxAmount)
	senders := make([]common.Address, b.TxAmount)
	if b.TxAmount > 0 {
		txnOffset := sn.TxnHashIdx.Lookup2(b.BaseTxId - sn.TxnHashIdx.BaseDataID()) // need subtract baseID of indexFile
		gg = sn.Transactions.MakeGetter()
		gg.Reset(txnOffset)
		stream := rlp.NewStream(reader, 0)
		for i := uint32(0); i < b.TxAmount; i++ {
			buf, _ = gg.Next(buf[:0])
			senders[i].SetBytes(buf[1 : 1+20])
			txRlp := buf[1+20:]
			reader.Reset(txRlp)
			stream.Reset(reader, 0)
			var err error
			txs[i], err = types.DecodeTransaction(stream)
			if err != nil {
				return nil, nil, 0, 0, err
			}
		}
	}

	body := new(types.Body)
	body.Uncles = b.Uncles
	return body, senders, b.BaseTxId, b.TxAmount, nil
}
