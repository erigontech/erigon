package server

import (
	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/erigon/zk/utils"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"

	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

type DbReader interface {
	GetLocalExitRootForBatchNo(batchNo uint64) (common.Hash, error)
	GetBatchGlobalExitRootsProto(lastBatchNumber, batchNumber uint64) ([]types.GerUpdateProto, error)
	GetForkId(batchNumber uint64) (uint64, error)
	GetBlockGlobalExitRoot(blockNumber uint64) (libcommon.Hash, error)
	GetBlockL1BlockHash(blockNumber uint64) (libcommon.Hash, error)
	GetBlockL1InfoTreeIndex(blockNumber uint64) (uint64, error)
	GetL1InfoTreeUpdate(index uint64) (*zktypes.L1InfoTreeUpdate, error)
	GetBlockInfoRoot(blockNumber uint64) (libcommon.Hash, error)
	GetIntermediateTxStateRoot(blockNumber uint64, txHash libcommon.Hash) (libcommon.Hash, error)
	GetEffectiveGasPricePercentage(txHash libcommon.Hash) (uint8, error)
	GetHighestBlockInBatch(batchNumber uint64) (uint64, error)
	GetInvalidBatch(batchNumber uint64) (bool, error)
	GetBatchNoByL2Block(blockNumber uint64) (uint64, error)
	CheckBatchNoByL2Block(l2BlockNo uint64) (uint64, bool, error)
}

type BookmarkType byte

const (
	EtrogBatchNumber = 7
)

type DataStreamServer struct {
	stream  *datastreamer.StreamServer
	chainId uint64
	highestBlockWritten,
	highestBatchWritten *uint64
}

type DataStreamEntry interface {
	EntryType() types.EntryType
	Bytes(bigEndian bool) []byte
}

type DataStreamEntryProto interface {
	Marshal() ([]byte, error)
	Type() types.EntryType
}

func NewDataStreamServer(stream *datastreamer.StreamServer, chainId uint64) *DataStreamServer {
	return &DataStreamServer{
		stream:              stream,
		chainId:             chainId,
		highestBlockWritten: nil,
		highestBatchWritten: nil,
	}
}

func (srv *DataStreamServer) GetChainId() uint64 {
	return srv.chainId
}

type DataStreamEntries struct {
	index   int
	entries []DataStreamEntryProto
}

func (d *DataStreamEntries) Add(entry DataStreamEntryProto) {
	d.entries[d.index] = entry
	d.index++
}

func (d *DataStreamEntries) AddMany(entries []DataStreamEntryProto) {
	for _, e := range entries {
		d.Add(e)
	}
}

func (d *DataStreamEntries) Entries() []DataStreamEntryProto {
	return d.entries
}

func NewDataStreamEntries(size int) *DataStreamEntries {
	return &DataStreamEntries{
		entries: make([]DataStreamEntryProto, size),
	}
}

func (srv *DataStreamServer) CommitEntriesToStreamProto(entries []DataStreamEntryProto, latestBlockNum, latestBatchNum *uint64) error {
	for _, entry := range entries {
		entryType := entry.Type()

		em, err := entry.Marshal()
		if err != nil {
			return err
		}

		if entryType == types.BookmarkEntryType {
			if _, err = srv.stream.AddStreamBookmark(em); err != nil {
				return err
			}
		} else {
			if _, err = srv.stream.AddStreamEntry(datastreamer.EntryType(entryType), em); err != nil {
				return err
			}
		}
	}

	if latestBlockNum != nil {
		srv.highestBlockWritten = latestBlockNum
	}

	if latestBatchNum != nil {
		srv.highestBatchWritten = latestBatchNum
	}
	return nil
}

func createBlockWithBatchCheckStreamEntriesProto(
	chainId uint64,
	reader DbReader,
	tx kv.Tx,
	block,
	lastBlock *eritypes.Block,
	batchNumber,
	lastBatchNumber uint64,
	l1InfoTreeMinTimestamps map[uint64]uint64,
	isBatchEnd bool,
	transactionsToIncludeByIndex []int, // passing nil here will include all transactions in the blocks
) ([]DataStreamEntryProto, error) {
	var err error
	var startEntriesProto, blockEntriesProto, endEntriesProto []DataStreamEntryProto

	gers, err := reader.GetBatchGlobalExitRootsProto(lastBatchNumber, batchNumber)
	if err != nil {
		return nil, err
	}

	// we might have a series of empty batches to account for, so we need to know the gap
	batchGap := batchNumber - lastBatchNumber
	isBatchStart := batchGap > 0

	// filter transactions by indexes that should be included
	filteredTransactions := filterTransactionByIndexes(block.Transactions(), transactionsToIncludeByIndex)

	blockNum := block.NumberU64()
	// batch start
	// BATCH BOOKMARK
	if isBatchStart {
		if startEntriesProto, err = createBatchStartEntriesProto(reader, tx, batchNumber, lastBatchNumber, batchGap, chainId, block.Root(), gers); err != nil {
			return nil, err
		}
	}

	forkId, err := reader.GetForkId(batchNumber)
	if err != nil {
		return nil, err
	}

	deltaTimestamp := block.Time() - lastBlock.Time()
	if blockNum == 1 {
		deltaTimestamp = block.Time()
		l1InfoTreeMinTimestamps[0] = 0
	}

	blockEntries, err := createFullBlockStreamEntriesProto(reader, tx, block, filteredTransactions, forkId, deltaTimestamp, batchNumber, l1InfoTreeMinTimestamps)
	if err != nil {
		return nil, err
	}
	blockEntriesProto = blockEntries.Entries()

	if isBatchEnd {
		localExitRoot, err := utils.GetBatchLocalExitRootFromSCStorage(batchNumber, reader, tx)
		if err != nil {
			return nil, err
		}
		blockRoot := block.Root()
		if endEntriesProto, err = addBatchEndEntriesProto(tx, batchNumber, lastBatchNumber, &blockRoot, gers, &localExitRoot); err != nil {
			return nil, err
		}
	}

	entries := NewDataStreamEntries(len(startEntriesProto) + len(blockEntriesProto) + len(endEntriesProto))
	entries.AddMany(startEntriesProto)
	entries.AddMany(blockEntriesProto)
	entries.AddMany(endEntriesProto)

	return entries.Entries(), nil
}

func createFullBlockStreamEntriesProto(
	reader DbReader,
	tx kv.Tx,
	block *eritypes.Block,
	filteredTransactions eritypes.Transactions,
	forkId,
	deltaTimestamp,
	batchNumber uint64,
	l1InfoTreeMinTimestamps map[uint64]uint64,
) (*DataStreamEntries, error) {
	entries := NewDataStreamEntries(len(filteredTransactions) + 2)
	blockNum := block.NumberU64()
	// L2 BLOCK BOOKMARK
	entries.Add(newL2BlockBookmarkEntryProto(blockNum))

	ger, err := reader.GetBlockGlobalExitRoot(blockNum)
	if err != nil {
		return nil, err
	}
	l1BlockHash, err := reader.GetBlockL1BlockHash(blockNum)
	if err != nil {
		return nil, err
	}

	l1InfoIndex, err := reader.GetBlockL1InfoTreeIndex(blockNum)
	if err != nil {
		return nil, err
	}

	if l1InfoIndex > 0 {
		// get the l1 info data, so we can add the min timestamp to the map
		l1Info, err := reader.GetL1InfoTreeUpdate(l1InfoIndex)
		if err != nil {
			return nil, err
		}
		if l1Info != nil {
			l1InfoTreeMinTimestamps[l1InfoIndex] = l1Info.Timestamp
		}
	}

	blockInfoRoot, err := reader.GetBlockInfoRoot(blockNum)
	if err != nil {
		return nil, err
	}

	// L2 BLOCK
	entries.Add(newL2BlockProto(block, block.Hash().Bytes(), batchNumber, ger, uint32(deltaTimestamp), uint32(l1InfoIndex), l1BlockHash, l1InfoTreeMinTimestamps[l1InfoIndex], blockInfoRoot))

	var transaction DataStreamEntryProto
	isEtrog := forkId <= EtrogBatchNumber
	for _, tx := range filteredTransactions {
		if transaction, err = createTransactionEntryProto(reader, tx, blockNum, isEtrog); err != nil {
			return nil, err
		}
		entries.Add(transaction)
	}

	return entries, nil
}

func createTransactionEntryProto(
	reader DbReader,
	tx eritypes.Transaction,
	blockNum uint64,
	isEtrog bool,
) (txProto DataStreamEntryProto, err error) {
	effectiveGasPricePercentage, err := reader.GetEffectiveGasPricePercentage(tx.Hash())
	if err != nil {
		return nil, err
	}

	var intermediateRoot libcommon.Hash
	if isEtrog {
		if intermediateRoot, err = reader.GetIntermediateTxStateRoot(blockNum, tx.Hash()); err != nil {
			return nil, err
		}
	}

	// TRANSACTION

	if txProto, err = newTransactionProto(effectiveGasPricePercentage, intermediateRoot, tx, blockNum); err != nil {
		return nil, err
	}

	return txProto, nil
}

func CreateAndBuildStreamEntryBytesProto(
	chainId uint64,
	block *eritypes.Block,
	reader *hermez_db.HermezDbReader,
	tx kv.Tx,
	lastBlock *eritypes.Block,
	batchNumber uint64,
	lastBatchNumber uint64,
	l1InfoTreeMinTimestamps map[uint64]uint64,
	isBatchEnd bool,
	transactionsToIncludeByIndex []int, // passing nil here will include all transactions in the blocks
) (result []byte, err error) {
	entries, err := createBlockWithBatchCheckStreamEntriesProto(chainId, reader, tx, block, lastBlock, batchNumber, lastBatchNumber, l1InfoTreeMinTimestamps, isBatchEnd, transactionsToIncludeByIndex)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		b, err := encodeEntryToBytesProto(entry)
		if err != nil {
			return nil, err
		}
		result = append(result, b...)
	}

	return result, nil
}

func (srv *DataStreamServer) GetHighestBlockNumber() (uint64, error) {
	if srv.highestBlockWritten != nil {
		return *srv.highestBlockWritten, nil
	}

	header := srv.stream.GetHeader()

	if header.TotalEntries == 0 {
		return 0, nil
	}

	//find end block entry to delete from it onward
	entryNum := header.TotalEntries - 1
	var err error
	var entry datastreamer.FileEntry
	for {
		entry, err = srv.stream.GetEntry(entryNum)
		if err != nil {
			return 0, err
		}
		if uint32(entry.Type) == uint32(types.EntryTypeL2Block) || uint32(entry.Type) == uint32(types.EntryTypeL2Tx) {
			break
		}
		entryNum -= 1
	}

	if uint32(entry.Type) == uint32(types.EntryTypeL2Block) {
		l2Block, err := types.UnmarshalL2Block(entry.Data)
		if err != nil {
			return 0, err
		}

		return l2Block.L2BlockNumber, nil
	} else if uint32(entry.Type) == uint32(types.EntryTypeL2Tx) {
		tx, err := types.UnmarshalTx(entry.Data)
		if err != nil {
			return 0, err
		}

		return tx.L2BlockNumber, nil
	}

	return 0, nil
}

func (srv *DataStreamServer) GetHighestBatchNumber() (uint64, error) {
	if srv.highestBatchWritten != nil {
		return *srv.highestBatchWritten, nil
	}

	header := srv.stream.GetHeader()

	if header.TotalEntries == 0 {
		return 0, nil
	}

	entryNum := header.TotalEntries - 1
	var err error
	var entry datastreamer.FileEntry
	for {
		entry, err = srv.stream.GetEntry(entryNum)
		if err != nil {
			return 0, err
		}
		if entry.Type == datastreamer.EntryType(1) {
			break
		}
		entryNum -= 1
	}

	batch, err := types.UnmarshalBatchStart(entry.Data)
	if err != nil {
		return 0, err
	}

	srv.highestBatchWritten = &batch.Number

	return batch.Number, nil
}

// must be done on offline server
// finds the position of the block bookmark entry and deletes from it onward
// blockNumber 10 would return the stream to before block 10 bookmark
func (srv *DataStreamServer) UnwindToBlock(blockNumber uint64) error {
	// check if server is online

	// find blockend entry
	bookmark := types.NewBookmarkProto(blockNumber, datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)
	marshalled, err := bookmark.Marshal()
	if err != nil {
		return err
	}
	entryNum, err := srv.stream.GetBookmark(marshalled)
	if err != nil {
		return err
	}

	return srv.stream.TruncateFile(entryNum)
}

// must be done on offline server
// finds the position of the endBlock entry for the given number
// and unwinds the datastream file to it
func (srv *DataStreamServer) UnwindToBatchStart(batchNumber uint64) error {
	// check if server is online

	// find blockend entry
	bookmark := types.NewBookmarkProto(batchNumber, datastream.BookmarkType_BOOKMARK_TYPE_BATCH)
	marshalled, err := bookmark.Marshal()
	if err != nil {
		return err
	}
	entryNum, err := srv.stream.GetBookmark(marshalled)
	if err != nil {
		return err
	}

	return srv.stream.TruncateFile(entryNum)
}
