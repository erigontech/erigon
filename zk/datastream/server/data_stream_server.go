package server

import (
	"bytes"
	"encoding/binary"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
)

type BookmarkType byte

type OperationMode int

const (
	StandardOperationMode OperationMode = iota
	ExecutorOperationMode
	EtrogBatchNumber = 7
)

type DataStreamServer struct {
	stream  *datastreamer.StreamServer
	chainId uint64
	mode    OperationMode
}

type DataStreamEntry interface {
	EntryType() types.EntryType
	Bytes(bigEndian bool) []byte
}

type DataStreamEntryProto interface {
	Marshal() ([]byte, error)
	Type() types.EntryType
}

func NewDataStreamServer(stream *datastreamer.StreamServer, chainId uint64, mode OperationMode) *DataStreamServer {
	return &DataStreamServer{
		stream:  stream,
		chainId: chainId,
		mode:    mode,
	}
}

func (srv *DataStreamServer) CommitEntriesToStreamProto(entries []DataStreamEntryProto) error {
	for _, entry := range entries {
		entryType := entry.Type()

		em, err := entry.Marshal()
		if err != nil {
			return err
		}

		if entryType == types.BookmarkEntryType {
			_, err = srv.stream.AddStreamBookmark(em)
			if err != nil {
				return err
			}
		} else {
			_, err = srv.stream.AddStreamEntry(datastreamer.EntryType(entryType), em)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (srv *DataStreamServer) CreateBatchBookmarkEntryProto(batchNo uint64) *types.BookmarkProto {
	return &types.BookmarkProto{
		BookMark: &datastream.BookMark{
			Type:  datastream.BookmarkType_BOOKMARK_TYPE_BATCH,
			Value: batchNo,
		},
	}
}

func (srv *DataStreamServer) CreateL2BlockBookmarkEntryProto(blockNo uint64) *types.BookmarkProto {
	return &types.BookmarkProto{
		BookMark: &datastream.BookMark{
			Type:  datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK,
			Value: blockNo,
		},
	}
}

func (srv *DataStreamServer) CreateL2BlockProto(
	block *eritypes.Block,
	blockHash []byte,
	batchNumber uint64,
	ger libcommon.Hash,
	deltaTimestamp uint32,
	l1InfoIndex uint32,
	l1BlockHash libcommon.Hash,
	minTimestamp uint64,
	blockInfoRoot libcommon.Hash,
) *types.L2BlockProto {
	return &types.L2BlockProto{
		L2Block: &datastream.L2Block{
			Number:          block.NumberU64(),
			BatchNumber:     batchNumber,
			Timestamp:       block.Time(),
			DeltaTimestamp:  deltaTimestamp,
			MinTimestamp:    minTimestamp,
			L1Blockhash:     l1BlockHash.Bytes(),
			L1InfotreeIndex: l1InfoIndex,
			Hash:            blockHash,
			StateRoot:       block.Root().Bytes(),
			GlobalExitRoot:  ger.Bytes(),
			Coinbase:        block.Coinbase().Bytes(),
			BlockInfoRoot:   blockInfoRoot.Bytes(),
		},
	}
}

func (srv *DataStreamServer) CreateTransactionProto(
	effectiveGasPricePercentage uint8,
	stateRoot libcommon.Hash,
	tx eritypes.Transaction,
	blockNumber uint64,
) (*types.TxProto, error) {
	buf := make([]byte, 0)
	writer := bytes.NewBuffer(buf)
	err := tx.EncodeRLP(writer)
	if err != nil {
		return nil, err
	}

	encoded := writer.Bytes()

	return &types.TxProto{
		Transaction: &datastream.Transaction{
			EffectiveGasPricePercentage: uint32(effectiveGasPricePercentage),
			IsValid:                     true, // TODO: SEQ: we don't store this value anywhere currently as a sync node
			ImStateRoot:                 stateRoot.Bytes(),
			Encoded:                     encoded,
			L2BlockNumber:               blockNumber,
		},
	}, nil
}

func (srv *DataStreamServer) CreateBatchStartProto(batchNo, chainId, forkId uint64, batchType datastream.BatchType) *types.BatchStartProto {
	return &types.BatchStartProto{
		BatchStart: &datastream.BatchStart{
			Number:  batchNo,
			ForkId:  forkId,
			ChainId: chainId,
			Type:    batchType,
		},
	}
}

func (srv *DataStreamServer) CreateBatchEndProto(localExitRoot, stateRoot libcommon.Hash, batchNumber uint64) *types.BatchEndProto {
	return &types.BatchEndProto{
		BatchEnd: &datastream.BatchEnd{
			LocalExitRoot: localExitRoot.Bytes(),
			StateRoot:     stateRoot.Bytes(),
			Number:        batchNumber,
		},
	}
}

func (srv *DataStreamServer) CreateGerUpdateProto(
	batchNumber, timestamp uint64,
	ger libcommon.Hash,
	coinbase libcommon.Address,
	forkId uint64,
	chainId uint64,
	stateRoot libcommon.Hash,
) *types.GerUpdateProto {
	return &types.GerUpdateProto{
		UpdateGER: &datastream.UpdateGER{
			BatchNumber:    batchNumber,
			Timestamp:      timestamp,
			GlobalExitRoot: ger.Bytes(),
			Coinbase:       coinbase.Bytes(),
			ForkId:         forkId,
			ChainId:        chainId,
			StateRoot:      stateRoot.Bytes(),
			Debug:          nil,
		},
	}
}

func (srv *DataStreamServer) CreateStreamEntriesProto(
	block *eritypes.Block,
	reader *hermez_db.HermezDbReader,
	tx kv.Tx,
	lastBlock *eritypes.Block,
	batchNumber uint64,
	lastBatchNumber uint64,
	gers []types.GerUpdateProto,
	l1InfoTreeMinTimestamps map[uint64]uint64,
) (*[]DataStreamEntryProto, error) {
	blockNum := block.NumberU64()

	entryCount := 2                         // l2 block bookmark + l2 block
	entryCount += len(block.Transactions()) // transactions
	entryCount += len(gers)

	var err error
	if lastBatchNumber != batchNumber {
		// we know we have some batch bookmarks to add, but we need to figure out how many because there
		// could be empty batches in between blocks that could contain ger updates and we need to handle
		// all of those scenarios
		entryCount += int(3 * (batchNumber - lastBatchNumber)) // batch bookmark + batch start + batch end
	}

	entries := make([]DataStreamEntryProto, entryCount)
	index := 0

	// BATCH BOOKMARK
	if batchNumber != lastBatchNumber {
		for i := 0; i < int(batchNumber-lastBatchNumber); i++ {
			workingBatch := lastBatchNumber + uint64(i)
			nextWorkingBatch := workingBatch + 1

			// handle any gers that need to be written before closing the batch down
			for _, ger := range gers {
				if ger.BatchNumber == workingBatch {
					entries[index] = &ger
					index++
				}
			}

			// now to fetch the LER for the batch - based on the last block of the batch
			var localExitRoot libcommon.Hash
			if workingBatch > 0 {
				checkBatch := workingBatch
				for ; checkBatch > 0; checkBatch-- {
					blocks, err := reader.GetL2BlockNosByBatch(checkBatch)
					if err != nil {
						return nil, err
					}
					if len(blocks) == 0 {
						continue
					}
					lastBlockNumber := blocks[len(blocks)-1]
					stateReader := state.NewPlainState(tx, lastBlockNumber, nil)
					rawLer, err := stateReader.ReadAccountStorage(state.GER_MANAGER_ADDRESS, 1, &state.GLOBAL_EXIT_ROOT_POS_1)
					if err != nil {
						return nil, err
					}
					localExitRoot = libcommon.BytesToHash(rawLer)
				}
			}

			// seal off the last batch
			root := lastBlock.Root()
			end := srv.CreateBatchEndProto(localExitRoot, root, workingBatch)
			entries[index] = end
			index++

			// bookmark for new batch
			batchBookmark := srv.CreateBatchBookmarkEntryProto(nextWorkingBatch)
			entries[index] = batchBookmark
			index++

			// new batch starting
			batchType := datastream.BatchType_BATCH_TYPE_REGULAR
			if batchNumber == 1 {
				batchType = datastream.BatchType_BATCH_TYPE_INJECTED
			}
			fork, err := reader.GetForkId(nextWorkingBatch)
			if err != nil {
				return nil, err
			}
			batch := srv.CreateBatchStartProto(nextWorkingBatch, srv.chainId, fork, batchType)
			entries[index] = batch
			index++
		}
	}

	deltaTimestamp := block.Time() - lastBlock.Time()

	// todo: temporary for now whilst we aren't handling more than one injected batch as a sequencer
	// this could go into the DB as a quick lookup to check for injected batches
	if block.NumberU64() == 1 {
		deltaTimestamp = block.Time()
		l1InfoTreeMinTimestamps[0] = block.Time()
	}

	// L2 BLOCK BOOKMARK
	l2blockBookmark := srv.CreateL2BlockBookmarkEntryProto(blockNum)
	entries[index] = l2blockBookmark
	index++

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

	forkId, err := reader.GetForkId(batchNumber)
	if err != nil {
		return nil, err
	}

	blockInfoRoot, err := reader.GetBlockInfoRoot(blockNum)
	if err != nil {
		return nil, err
	}

	blockHash := block.Hash().Bytes()

	// L2 BLOCK
	l2Block := srv.CreateL2BlockProto(block, blockHash, batchNumber, ger, uint32(deltaTimestamp), uint32(l1InfoIndex), l1BlockHash, l1InfoTreeMinTimestamps[l1InfoIndex], blockInfoRoot)
	entries[index] = l2Block
	index++

	for _, tx := range block.Transactions() {
		effectiveGasPricePercentage, err := reader.GetEffectiveGasPricePercentage(tx.Hash())
		if err != nil {
			return nil, err
		}

		var intermediateRoot libcommon.Hash
		if forkId < EtrogBatchNumber {
			intermediateRoot, err = reader.GetIntermediateTxStateRoot(block.NumberU64(), tx.Hash())
			if err != nil {
				return nil, err
			}
		}

		// TRANSACTION
		transaction, err := srv.CreateTransactionProto(effectiveGasPricePercentage, intermediateRoot, tx, blockNum)
		entries[index] = transaction
		index++
	}

	return &entries, nil
}

func (srv *DataStreamServer) CreateAndBuildStreamEntryBytesProto(
	block *eritypes.Block,
	reader *hermez_db.HermezDbReader,
	tx kv.Tx,
	lastBlock *eritypes.Block,
	batchNumber uint64,
	lastBatchNumber uint64,
	l1InfoTreeMinTimestamps map[uint64]uint64,
) ([]byte, error) {
	gersInBetween, err := reader.GetBatchGlobalExitRootsProto(lastBatchNumber, batchNumber)
	if err != nil {
		return nil, err
	}

	entries, err := srv.CreateStreamEntriesProto(block, reader, tx, lastBlock, batchNumber, lastBatchNumber, gersInBetween, l1InfoTreeMinTimestamps)
	if err != nil {
		return nil, err
	}

	var result []byte
	for _, entry := range *entries {
		b, err := encodeEntryToBytesProto(entry)
		if err != nil {
			return nil, err
		}
		result = append(result, b...)
	}

	return result, nil
}

func (srv *DataStreamServer) GetHighestBlockNumber() (uint64, error) {
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
		if entry.Type == datastreamer.EntryType(2) {
			break
		}
		entryNum -= 1
	}

	l2Block, err := types.UnmarshalL2Block(entry.Data)
	if err != nil {
		return 0, err
	}

	return l2Block.L2BlockNumber, nil
}

// must be done on offline server
// finds the position of the endBlock entry for the given number
// and unwinds the datastream file to it
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

	//find end block entry to delete from it onward
	for {
		entry, err := srv.stream.GetEntry(entryNum)
		if err != nil {
			return err
		}
		if entry.Type == datastreamer.EntryType(3) {
			break
		}
		entryNum -= 1
	}

	return srv.stream.TruncateFile(entryNum + 1)
}

const (
	PACKET_TYPE_DATA = 2
	// NOOP_ENTRY_NUMBER is used because we don't care about the entry number when feeding an atrificial
	// stream to the executor, if this ever changes then we'll need to populate an actual number
	NOOP_ENTRY_NUMBER = 0
)

func encodeEntryToBytesProto(entry DataStreamEntryProto) ([]byte, error) {
	data, err := entry.Marshal()
	if err != nil {
		return nil, err
	}
	var totalLength = 1 + 4 + 4 + 8 + uint32(len(data))
	buf := make([]byte, 1)
	buf[0] = PACKET_TYPE_DATA
	buf = binary.BigEndian.AppendUint32(buf, totalLength)
	buf = binary.BigEndian.AppendUint32(buf, uint32(entry.Type()))
	buf = binary.BigEndian.AppendUint64(buf, uint64(NOOP_ENTRY_NUMBER))
	buf = append(buf, data...)
	return buf, nil
}
