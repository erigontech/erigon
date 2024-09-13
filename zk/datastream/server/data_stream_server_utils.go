package server

import (
	"bytes"
	"encoding/binary"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/utils"
)

func newBatchBookmarkEntryProto(batchNo uint64) *types.BookmarkProto {
	return &types.BookmarkProto{
		BookMark: &datastream.BookMark{
			Type:  datastream.BookmarkType_BOOKMARK_TYPE_BATCH,
			Value: batchNo,
		},
	}
}

func newL2BlockBookmarkEntryProto(blockNo uint64) *types.BookmarkProto {
	return &types.BookmarkProto{
		BookMark: &datastream.BookMark{
			Type:  datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK,
			Value: blockNo,
		},
	}
}

func newL2BlockProto(
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

func newL2BlockEndProto(
	blockNumber uint64,
) *types.L2BlockEndProto {
	return &types.L2BlockEndProto{
		Number: blockNumber,
	}
}

func newTransactionProto(
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

func newBatchStartProto(batchNo, chainId, forkId uint64, batchType datastream.BatchType) *types.BatchStartProto {
	return &types.BatchStartProto{
		BatchStart: &datastream.BatchStart{
			Number:  batchNo,
			ForkId:  forkId,
			ChainId: chainId,
			Type:    batchType,
		},
	}
}

func newBatchEndProto(localExitRoot, stateRoot libcommon.Hash, batchNumber uint64) *types.BatchEndProto {
	return &types.BatchEndProto{
		BatchEnd: &datastream.BatchEnd{
			LocalExitRoot: localExitRoot.Bytes(),
			StateRoot:     stateRoot.Bytes(),
			Number:        batchNumber,
		},
	}
}

func newGerUpdateProto(
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

func createBatchStartEntriesProto(
	reader DbReader,
	tx kv.Tx,
	batchNumber, lastBatchNumber, batchGap, chainId uint64,
	root libcommon.Hash,
	gers []types.GerUpdateProto,
) (*DataStreamEntries, error) {
	var err error
	var batchStartEntries []DataStreamEntryProto

	batchGapEntriesCount := int(batchGap) - 1
	if batchGapEntriesCount < 0 {
		batchGapEntriesCount = 0
	}
	entries := NewDataStreamEntries(2 + 3*batchGapEntriesCount + len(gers))

	// if we have a gap of more than 1 batch then we need to write in the batch start and ends for these empty batches
	if batchGap > 1 {
		var localExitRoot libcommon.Hash
		for i := 1; i < int(batchGap); i++ {
			workingBatch := lastBatchNumber + uint64(i)

			// bookmark for new batch
			if batchStartEntries, err = addBatchStartEntries(reader, workingBatch, chainId); err != nil {
				return nil, err
			}
			// entries = append(entries, batchStartEntries...)
			entries.AddMany(batchStartEntries)

			// see if we have any gers to handle
			for _, ger := range gers {
				upd := ger.UpdateGER
				if upd.BatchNumber == workingBatch {
					entries.Add(
						newGerUpdateProto(upd.BatchNumber, upd.Timestamp, libcommon.BytesToHash(upd.GlobalExitRoot), libcommon.BytesToAddress(upd.Coinbase), upd.ForkId, upd.ChainId, libcommon.BytesToHash(upd.StateRoot)),
					)
				}
			}

			// seal off the last batch
			if localExitRoot, err = utils.GetBatchLocalExitRootFromSCStorageForLatestBlock(workingBatch, reader, tx); err != nil {
				return nil, err
			}
			entries.Add(newBatchEndProto(localExitRoot, root, workingBatch))
		}
	}

	// now write in the batch start for this batch
	if batchStartEntries, err = addBatchStartEntries(reader, batchNumber, chainId); err != nil {
		return nil, err
	}
	entries.AddMany(batchStartEntries)
	return entries, nil
}

func addBatchEndEntriesProto(
	batchNumber uint64,
	root *libcommon.Hash,
	gers []types.GerUpdateProto,
	localExitRoot *libcommon.Hash,
) ([]DataStreamEntryProto, error) {
	entries := make([]DataStreamEntryProto, 0, len(gers)+1)

	// see if we have any gers to handle
	for _, ger := range gers {
		upd := ger.UpdateGER
		if upd.BatchNumber == batchNumber {
			entries = append(entries, newGerUpdateProto(upd.BatchNumber, upd.Timestamp, libcommon.BytesToHash(upd.GlobalExitRoot), libcommon.BytesToAddress(upd.Coinbase), upd.ForkId, upd.ChainId, libcommon.BytesToHash(upd.StateRoot)))
		}
	}

	// seal off the last batch
	entries = append(entries, newBatchEndProto(*localExitRoot, *root, batchNumber))

	return entries, nil
}

func getBatchTypeAndFork(batchNumber uint64, reader DbReader) (datastream.BatchType, uint64, error) {
	var batchType datastream.BatchType
	invalidBatch, err := reader.GetInvalidBatch(batchNumber)
	if err != nil {
		return datastream.BatchType_BATCH_TYPE_UNSPECIFIED, 0, err
	}
	if invalidBatch {
		batchType = datastream.BatchType_BATCH_TYPE_INVALID
	} else if batchNumber == 1 {
		batchType = datastream.BatchType_BATCH_TYPE_INJECTED
	} else {
		batchType = datastream.BatchType_BATCH_TYPE_REGULAR
	}

	fork, err := reader.GetForkId(batchNumber)
	if err != nil {
		return datastream.BatchType_BATCH_TYPE_UNSPECIFIED, 0, err
	}

	if fork == 0 && batchNumber > 1 {
		// iterate backwards, this only happens for empty batches pre etrog
		for batchNumber > 1 {
			batchNumber--
			fork, err = reader.GetForkId(batchNumber)
			if err != nil {
				return datastream.BatchType_BATCH_TYPE_UNSPECIFIED, 0, err
			}
			if fork != 0 {
				break
			}
		}
	}
	return batchType, fork, nil
}

func addBatchStartEntries(reader DbReader, batchNum, chainId uint64) ([]DataStreamEntryProto, error) {
	entries := make([]DataStreamEntryProto, 2)
	entries[0] = newBatchBookmarkEntryProto(batchNum)
	batchType, fork, err := getBatchTypeAndFork(batchNum, reader)
	if err != nil {
		return nil, err
	}

	entries[1] = newBatchStartProto(batchNum, chainId, fork, batchType)
	return entries, nil
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
