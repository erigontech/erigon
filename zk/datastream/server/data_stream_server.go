package server

import (
	"bytes"
	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
)

type BookmarkType byte

var BlockBookmarkType BookmarkType = 0

var (
	EntryTypeUpdateGer  = datastreamer.EntryType(4)
	EntryTypeL2BlockEnd = datastreamer.EntryType(3)
	EntryTypeL2Tx       = datastreamer.EntryType(2)
)

type DataStreamServer struct {
	stream *datastreamer.StreamServer
}

func NewDataStreamServer(stream *datastreamer.StreamServer) *DataStreamServer {
	return &DataStreamServer{stream: stream}
}

func (srv *DataStreamServer) AddBookmark(t BookmarkType, marker uint64) error {
	bookmark := types.Bookmark{Type: byte(t), From: marker}
	_, err := srv.stream.AddStreamBookmark(bookmark.Encode())
	return err
}

func (srv *DataStreamServer) AddBlockStart(block *types2.Block, batchNumber uint64, forkId uint16, ger libcommon.Hash) error {
	b := &types.StartL2Block{
		BatchNumber:    batchNumber,
		L2BlockNumber:  block.NumberU64(),
		Timestamp:      int64(block.Time()),
		GlobalExitRoot: ger,
		Coinbase:       block.Coinbase(),
		ForkId:         forkId,
	}
	_, err := srv.stream.AddStreamEntry(1, types.EncodeStartL2Block(b))
	return err
}

func (srv *DataStreamServer) AddBlockEnd(blockNumber uint64, blockHash, stateRoot libcommon.Hash) error {
	end := &types.EndL2Block{
		L2BlockNumber: blockNumber,
		L2Blockhash:   blockHash,
		StateRoot:     stateRoot,
	}
	_, err := srv.stream.AddStreamEntry(3, types.EncodeEndL2Block(end))
	return err
}

func (srv *DataStreamServer) AddGerUpdate(batchNumber uint64, ger libcommon.Hash, fork uint16, block *types2.Block) (uint64, error) {
	update := types.GerUpdate{
		BatchNumber:    batchNumber,
		Timestamp:      block.Time(),
		GlobalExitRoot: ger,
		Coinbase:       block.Coinbase(),
		ForkId:         fork,
		StateRoot:      block.Root(),
	}

	return srv.stream.AddStreamEntry(EntryTypeUpdateGer, update.EncodeToBytes())
}

func (srv *DataStreamServer) AddGerUpdateFromDb(ger *types.GerUpdate) (uint64, error) {
	return srv.stream.AddStreamEntry(EntryTypeUpdateGer, ger.EncodeToBytes())
}

func (srv *DataStreamServer) AddTransaction(
	effectiveGasPricePercentage uint8,
	stateRoot libcommon.Hash,
	fork uint16,
	tx types2.Transaction,
) (uint64, error) {
	buf := make([]byte, 0)
	writer := bytes.NewBuffer(buf)
	err := tx.EncodeRLP(writer)
	if err != nil {
		return 0, err
	}

	encoded := writer.Bytes()

	if fork >= 5 {
		encoded = append(encoded, effectiveGasPricePercentage)
	}

	length := len(encoded)

	l2Tx := types.L2Transaction{
		EffectiveGasPricePercentage: effectiveGasPricePercentage,
		IsValid:                     1, // TODO: SEQ: we don't store this value anywhere currently as a sync node
		StateRoot:                   stateRoot,
		EncodedLength:               uint32(length),
		Encoded:                     encoded,
	}

	return srv.stream.AddStreamEntry(EntryTypeL2Tx, types.EncodeL2Transaction(l2Tx))
}
