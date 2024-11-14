package server

import (
	"context"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	dslog "github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

//go:generate mockgen -typed=true -destination=../mocks/stream_server_mock.go -package=mocks . StreamServer
//go:generate mockgen -typed=true -destination=../mocks/data_stream_server_mock.go -package=mocks . DataStreamServer

type StreamServer interface {
	Start() error
	StartAtomicOp() error
	AddStreamEntry(etype datastreamer.EntryType, data []byte) (uint64, error)
	AddStreamBookmark(bookmark []byte) (uint64, error)
	CommitAtomicOp() error
	RollbackAtomicOp() error
	TruncateFile(entryNum uint64) error
	UpdateEntryData(entryNum uint64, etype datastreamer.EntryType, data []byte) error
	GetHeader() datastreamer.HeaderEntry
	GetEntry(entryNum uint64) (datastreamer.FileEntry, error)
	GetBookmark(bookmark []byte) (uint64, error)
	GetFirstEventAfterBookmark(bookmark []byte) (datastreamer.FileEntry, error)
	GetDataBetweenBookmarks(bookmarkFrom, bookmarkTo []byte) ([]byte, error)
	BookmarkPrintDump()
}

type DataStreamServer interface {
	GetStreamServer() StreamServer
	GetChainId() uint64
	IsLastEntryBatchEnd() (isBatchEnd bool, err error)
	GetHighestBlockNumber() (uint64, error)
	GetHighestBatchNumber() (uint64, error)
	GetHighestClosedBatch() (uint64, error)
	GetHighestClosedBatchNoCache() (uint64, error)
	UnwindToBlock(blockNumber uint64) error
	UnwindToBatchStart(batchNumber uint64) error
	ReadBatches(start uint64, end uint64) ([][]*types.FullL2Block, error)
	WriteWholeBatchToStream(logPrefix string, tx kv.Tx, reader DbReader, prevBatchNum, batchNum uint64) error
	WriteBlocksToStreamConsecutively(ctx context.Context, logPrefix string, tx kv.Tx, reader DbReader, from, to uint64) error
	WriteBlockWithBatchStartToStream(logPrefix string, tx kv.Tx, reader DbReader, forkId, batchNum, prevBlockBatchNum uint64, prevBlock, block eritypes.Block) (err error)
	UnwindIfNecessary(logPrefix string, reader DbReader, blockNum, prevBlockBatchNum, batchNum uint64) error
	WriteBatchEnd(reader DbReader, batchNumber uint64, stateRoot *common.Hash, localExitRoot *common.Hash) (err error)
	WriteGenesisToStream(genesis *eritypes.Block, reader *hermez_db.HermezDbReader, tx kv.Tx) error
}

type DataStreamServerFactory interface {
	CreateStreamServer(port uint16, version uint8, systemID uint64, streamType datastreamer.StreamType, fileName string, writeTimeout time.Duration, inactivityTimeout time.Duration, inactivityCheckInterval time.Duration, cfg *dslog.Config) (StreamServer, error)
	CreateDataStreamServer(stream StreamServer, chainId uint64) DataStreamServer
}
