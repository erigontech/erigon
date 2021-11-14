package snapshotsync

import (
	"bytes"
	"context"

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

func (back *BlockReader) WithSenders(tx kv.Tx, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	block, senders, err = rawdb.ReadBlockWithSenders(tx, hash, blockHeight)
	if err != nil {
		return nil, nil, err
	}
	//TODO: read snapshots
	return block, senders, nil
}

// RemoteBlockReader can read blocks from db and snapshots
type RemoteBlockReader struct {
	back remote.ETHBACKENDClient
}

func NewRemoteBlockReader(back remote.ETHBACKENDClient) *RemoteBlockReader {
	return &RemoteBlockReader{back: back}
}

func (r *RemoteBlockReader) WithSenders(tx kv.Tx, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	reply, err := r.back.Block(context.Background(), &remote.BlockRequest{BlockHash: gointerfaces.ConvertHashToH256(hash), BlockHeight: blockHeight})
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

	block.SendersToTxs(senders)
	return block, senders, nil
}
