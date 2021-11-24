package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

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

func (back *BlockReader) Header(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (*types.Header, error) {
	h := rawdb.ReadHeader(tx, hash, blockHeight)
	return h, nil
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

// BlockReaderWithSnapshots can read blocks from db and snapshots
type BlockReaderWithSnapshots struct {
	sn *AllSnapshots
}

func NewBlockReaderWithSnapshots(snapshots *AllSnapshots) *BlockReaderWithSnapshots {
	return &BlockReaderWithSnapshots{sn: snapshots}
}

func (back *BlockReaderWithSnapshots) Header(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (*types.Header, error) {
	sn, ok := back.sn.Blocks(blockHeight)
	if !ok {
		h := rawdb.ReadHeader(tx, hash, blockHeight)
		return h, nil
	}

	buf := make([]byte, 16)

	n := binary.PutUvarint(buf, blockHeight)
	headerOffset := sn.Headers.Idx.Lookup2(sn.Headers.Idx.Lookup(buf[:n]))
	gg := sn.Headers.Segment.MakeGetter()
	gg.Reset(headerOffset)
	buf, _ = gg.Next(buf[:0])
	h := &types.Header{}
	if err := rlp.DecodeBytes(buf, h); err != nil {
		return nil, err
	}
	return h, nil
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

	n := binary.PutUvarint(buf, blockHeight)
	headerOffset := sn.Headers.Idx.Lookup2(sn.Headers.Idx.Lookup(buf[:n]))
	bodyOffset := sn.Bodies.Idx.Lookup2(sn.Bodies.Idx.Lookup(buf[:n]))

	gg := sn.Headers.Segment.MakeGetter()
	gg.Reset(headerOffset)
	buf, _ = gg.Next(buf[:0])
	h := &types.Header{}
	if err = rlp.DecodeBytes(buf, h); err != nil {
		return nil, nil, err
	}

	gg = sn.Bodies.Segment.MakeGetter()
	gg.Reset(bodyOffset)
	buf, _ = gg.Next(buf[:0])
	b := &types.BodyForStorage{}
	if err = rlp.DecodeBytes(buf, b); err != nil {
		return nil, nil, err
	}

	n = binary.PutUvarint(buf, b.BaseTxId)
	txnOffset := sn.Transactions.Idx.Lookup2(sn.Transactions.Idx.Lookup(buf[:n]))
	gg = sn.Transactions.Segment.MakeGetter()
	gg.Reset(txnOffset)
	reader := bytes.NewReader(nil)
	stream := rlp.NewStream(reader, 0)
	txs := make([]types.Transaction, b.TxAmount)
	senders = make([]common.Address, b.TxAmount)
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

	block = types.NewBlockFromStorage(hash, h, txs, b.Uncles)
	if len(senders) != block.Transactions().Len() {
		return block, senders, nil // no senders is fine - will recover them on the fly
	}
	block.SendersToTxs(senders)
	return block, senders, nil
}
