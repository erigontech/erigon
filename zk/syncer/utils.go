package syncer

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"github.com/ledgerwatch/erigon/zk/utils"
)

const (
	sequenceBatchesMethodName         = "sequenceBatches"
	sequenceBatchesValidiumMethodName = "sequenceBatchesValidium"
)

func GetAccInputDataCalcFunction(l1InfoRoot common.Hash, decodedSequenceInteerface interface{}) (accInputHashCalcFn func(prevAccInputHash common.Hash, index int) *common.Hash, totalSequenceBatches int, err error) {
	switch decodedSequence := decodedSequenceInteerface.(type) {
	case *SequenceBatchesCalldataPreEtrog:
		accInputHashCalcFn = func(prevAccInputHash common.Hash, index int) *common.Hash {
			return utils.CalculatePreEtrogAccInputHash(prevAccInputHash, decodedSequence.Batches[index].Transactions, decodedSequence.Batches[index].GlobalExitRoot, decodedSequence.Batches[index].Timestamp, decodedSequence.L2Coinbase)
		}
		totalSequenceBatches = len(decodedSequence.Batches)
	case *SequenceBatchesCalldataEtrog:
		accInputHashCalcFn = func(prevAccInputHash common.Hash, index int) *common.Hash {
			return utils.CalculateEtrogAccInputHash(prevAccInputHash, decodedSequence.Batches[index].Transactions, l1InfoRoot, decodedSequence.Batches[index].ForcedTimestamp, decodedSequence.L2Coinbase, decodedSequence.Batches[index].ForcedBlockHashL1)
		}
		totalSequenceBatches = len(decodedSequence.Batches)
	case *SequenceBatchesCalldataElderberry:
		accInputHashCalcFn = func(prevAccInputHash common.Hash, index int) *common.Hash {
			return utils.CalculateEtrogAccInputHash(prevAccInputHash, decodedSequence.Batches[index].Transactions, l1InfoRoot, decodedSequence.MaxSequenceTimestamp, decodedSequence.L2Coinbase, decodedSequence.Batches[index].ForcedBlockHashL1)
		}
		totalSequenceBatches = len(decodedSequence.Batches)
	case *SequenceBatchesCalldataValidiumPreEtrog:
		accInputHashCalcFn = func(prevAccInputHash common.Hash, index int) *common.Hash {
			return utils.CalculatePreEtrogValidiumAccInputHash(prevAccInputHash, decodedSequence.Batches[index].TransactionsHash, decodedSequence.Batches[index].GlobalExitRoot, decodedSequence.Batches[index].Timestamp, decodedSequence.L2Coinbase)
		}
		totalSequenceBatches = len(decodedSequence.Batches)
	case *SequenceBatchesCalldataValidiumEtrog:
		accInputHashCalcFn = func(prevAccInputHash common.Hash, index int) *common.Hash {
			return utils.CalculateEtrogValidiumAccInputHash(prevAccInputHash, decodedSequence.Batches[index].TransactionsHash, l1InfoRoot, decodedSequence.Batches[index].ForcedTimestamp, decodedSequence.L2Coinbase, decodedSequence.Batches[index].ForcedBlockHashL1)
		}
		totalSequenceBatches = len(decodedSequence.Batches)
	case *SequenceBatchesCalldataValidiumElderberry:
		accInputHashCalcFn = func(prevAccInputHash common.Hash, index int) *common.Hash {
			return utils.CalculateEtrogValidiumAccInputHash(prevAccInputHash, decodedSequence.Batches[index].TransactionsHash, l1InfoRoot, decodedSequence.MaxSequenceTimestamp, decodedSequence.L2Coinbase, decodedSequence.Batches[index].ForcedBlockHashL1)
		}
		totalSequenceBatches = len(decodedSequence.Batches)
	default:
		return nil, 0, fmt.Errorf("unexpected type of decoded sequence calldata: %T", decodedSequenceInteerface)
	}

	return accInputHashCalcFn, totalSequenceBatches, nil
}

func DecodeSequenceBatchesCalldata(data []byte) (calldata interface{}, err error) {
	methodSig := hex.EncodeToString(data[:4])
	abiString := contracts.SequenceBatchesMapping[methodSig]
	if abiString == "" {
		return nil, fmt.Errorf("no abi found for method signature: %s", methodSig)
	}
	abi, err := abi.JSON(strings.NewReader(abiString))
	if err != nil {
		return nil, fmt.Errorf("error parsing etrogPolygonZkEvmAbi to json: %v", err)
	}

	// recover Method from signature and ABI
	method, err := abi.MethodById(data)
	if err != nil {
		return nil, fmt.Errorf("error recovering method from signature: %v", err)
	}

	//sanitycheck
	if method.Name != sequenceBatchesMethodName && method.Name != sequenceBatchesValidiumMethodName {
		return nil, fmt.Errorf("method name is not expected, got: %s", method.Name)
	}

	unpackedCalldata := make(map[string]interface{})
	if err := method.Inputs.UnpackIntoMap(unpackedCalldata, data[4:] /*first 4 bytes are method signature and not needed */); err != nil {
		return nil, fmt.Errorf("error unpacking data: %v", err)
	}

	switch methodSig {
	case contracts.SequenceBatchesPreEtrog:
		if method.Name == sequenceBatchesMethodName {
			return decodePreEtrogSequenceBatchesCallData(unpackedCalldata), nil
		} else {
			return decodePreEtrogSequenceBatchesValidiumCallData(unpackedCalldata), nil
		}
	case contracts.SequenceBatchesIdv5_0:
		if method.Name == sequenceBatchesMethodName {
			return decodeEtrogSequenceBatchesCallData(unpackedCalldata), nil
		} else {
			return decodeEtrogSequenceBatchesValidiumCallData(unpackedCalldata), nil
		}
	case contracts.SequenceBatchesIdv6_6:
		if method.Name == sequenceBatchesMethodName {
			return decodeElderberryBatchesCallData(unpackedCalldata), nil
		} else {
			return decodeElderberryBatchesValidiumCallData(unpackedCalldata), nil
		}
	default:
		return nil, fmt.Errorf("no decoder found for method signature: %s", methodSig)
	}
}

type SequencedBatchElderberry struct {
	Transactions         []byte
	ForcedGlobalExitRoot common.Hash
	ForcedTimestamp      uint64
	ForcedBlockHashL1    common.Hash
}

type SequenceBatchesCalldataElderberry struct {
	Batches              []SequencedBatchElderberry
	InitSequencedBatch   uint64
	L2Coinbase           common.Address
	MaxSequenceTimestamp uint64
}

func decodeElderberryBatchesCallData(unpackedCalldata map[string]interface{}) *SequenceBatchesCalldataElderberry {
	unpackedbatches := unpackedCalldata["batches"].([]struct {
		Transactions         []uint8   `json:"transactions"`
		ForcedGlobalExitRoot [32]uint8 `json:"forcedGlobalExitRoot"`
		ForcedTimestamp      uint64    `json:"forcedTimestamp"`
		ForcedBlockHashL1    [32]uint8 `json:"forcedBlockHashL1"`
	})

	calldata := &SequenceBatchesCalldataElderberry{
		Batches:              make([]SequencedBatchElderberry, len(unpackedbatches)),
		InitSequencedBatch:   unpackedCalldata["initSequencedBatch"].(uint64),
		L2Coinbase:           unpackedCalldata["l2Coinbase"].(common.Address),
		MaxSequenceTimestamp: unpackedCalldata["maxSequenceTimestamp"].(uint64),
	}

	for i, batch := range unpackedbatches {
		calldata.Batches[i] = SequencedBatchElderberry{
			Transactions:         batch.Transactions,
			ForcedGlobalExitRoot: common.BytesToHash(batch.ForcedGlobalExitRoot[:]),
			ForcedTimestamp:      batch.ForcedTimestamp,
			ForcedBlockHashL1:    common.BytesToHash(batch.ForcedBlockHashL1[:]),
		}
	}

	return calldata
}

type SequencedBatchValidiumElderberry struct {
	TransactionsHash     common.Hash
	ForcedGlobalExitRoot common.Hash
	ForcedTimestamp      uint64
	ForcedBlockHashL1    common.Hash
}

type SequenceBatchesCalldataValidiumElderberry struct {
	Batches              []SequencedBatchValidiumElderberry
	InitSequencedBatch   uint64
	L2Coinbase           common.Address
	MaxSequenceTimestamp uint64
}

func decodeElderberryBatchesValidiumCallData(unpackedCalldata map[string]interface{}) *SequenceBatchesCalldataValidiumElderberry {
	unpackedbatches := unpackedCalldata["batches"].([]struct {
		TransactionsHash     [32]uint8 `json:"transactionsHash"`
		ForcedGlobalExitRoot [32]uint8 `json:"forcedGlobalExitRoot"`
		ForcedTimestamp      uint64    `json:"forcedTimestamp"`
		ForcedBlockHashL1    [32]uint8 `json:"forcedBlockHashL1"`
	})

	calldata := &SequenceBatchesCalldataValidiumElderberry{
		Batches:              make([]SequencedBatchValidiumElderberry, len(unpackedbatches)),
		InitSequencedBatch:   unpackedCalldata["initSequencedBatch"].(uint64),
		L2Coinbase:           unpackedCalldata["l2Coinbase"].(common.Address),
		MaxSequenceTimestamp: unpackedCalldata["maxSequenceTimestamp"].(uint64),
	}

	for i, batch := range unpackedbatches {
		calldata.Batches[i] = SequencedBatchValidiumElderberry{
			TransactionsHash:     common.BytesToHash(batch.TransactionsHash[:]),
			ForcedGlobalExitRoot: common.BytesToHash(batch.ForcedGlobalExitRoot[:]),
			ForcedTimestamp:      batch.ForcedTimestamp,
			ForcedBlockHashL1:    common.BytesToHash(batch.ForcedBlockHashL1[:]),
		}
	}

	return calldata
}

type SequencedBatchEtrog struct {
	Transactions         []uint8
	ForcedGlobalExitRoot common.Hash
	ForcedTimestamp      uint64
	ForcedBlockHashL1    common.Hash
}

type SequenceBatchesCalldataEtrog struct {
	Batches    []SequencedBatchEtrog
	L2Coinbase common.Address
}

func decodeEtrogSequenceBatchesCallData(unpackedCalldata map[string]interface{}) *SequenceBatchesCalldataEtrog {
	unpackedbatches := unpackedCalldata["batches"].([]struct {
		Transactions         []uint8   `json:"transactions"`
		ForcedGlobalExitRoot [32]uint8 `json:"forcedGlobalExitRoot"`
		ForcedTimestamp      uint64    `json:"forcedTimestamp"`
		ForcedBlockHashL1    [32]uint8 `json:"forcedBlockHashL1"`
	})

	calldata := &SequenceBatchesCalldataEtrog{
		Batches:    make([]SequencedBatchEtrog, len(unpackedbatches)),
		L2Coinbase: unpackedCalldata["l2Coinbase"].(common.Address),
	}

	for i, batch := range unpackedbatches {
		calldata.Batches[i] = SequencedBatchEtrog{
			Transactions:         batch.Transactions,
			ForcedGlobalExitRoot: common.BytesToHash(batch.ForcedGlobalExitRoot[:]),
			ForcedTimestamp:      batch.ForcedTimestamp,
			ForcedBlockHashL1:    batch.ForcedBlockHashL1,
		}
	}

	return calldata
}

type SequencedBatchValidiumEtrog struct {
	TransactionsHash     common.Hash
	ForcedGlobalExitRoot common.Hash
	ForcedTimestamp      uint64
	ForcedBlockHashL1    common.Hash
}

type SequenceBatchesCalldataValidiumEtrog struct {
	Batches    []SequencedBatchValidiumEtrog
	L2Coinbase common.Address
}

func decodeEtrogSequenceBatchesValidiumCallData(unpackedCalldata map[string]interface{}) *SequenceBatchesCalldataValidiumEtrog {
	unpackedbatches := unpackedCalldata["batches"].([]struct {
		TransactionsHash     [32]uint8 `json:"transactionsHash"`
		ForcedGlobalExitRoot [32]uint8 `json:"forcedGlobalExitRoot"`
		ForcedTimestamp      uint64    `json:"forcedTimestamp"`
		ForcedBlockHashL1    [32]uint8 `json:"forcedBlockHashL1"`
	})

	calldata := &SequenceBatchesCalldataValidiumEtrog{
		Batches:    make([]SequencedBatchValidiumEtrog, len(unpackedbatches)),
		L2Coinbase: unpackedCalldata["l2Coinbase"].(common.Address),
	}

	for i, batch := range unpackedbatches {
		calldata.Batches[i] = SequencedBatchValidiumEtrog{
			TransactionsHash:     common.BytesToHash(batch.TransactionsHash[:]),
			ForcedGlobalExitRoot: common.BytesToHash(batch.ForcedGlobalExitRoot[:]),
			ForcedTimestamp:      batch.ForcedTimestamp,
			ForcedBlockHashL1:    batch.ForcedBlockHashL1,
		}
	}

	return calldata
}

type SequencedBatchPreEtrog struct {
	Transactions       []uint8
	GlobalExitRoot     common.Hash
	Timestamp          uint64
	MinForcedTimestamp uint64
}

type SequenceBatchesCalldataPreEtrog struct {
	Batches    []SequencedBatchPreEtrog
	L2Coinbase common.Address
}

func decodePreEtrogSequenceBatchesCallData(unpackedCalldata map[string]interface{}) *SequenceBatchesCalldataPreEtrog {
	unpackedbatches := unpackedCalldata["batches"].([]struct {
		Transactions       []uint8   `json:"transactions"`
		GlobalExitRoot     [32]uint8 `json:"globalExitRoot"`
		Timestamp          uint64    `json:"timestamp"`
		MinForcedTimestamp uint64    `json:"minForcedTimestamp"`
	})

	calldata := &SequenceBatchesCalldataPreEtrog{
		Batches:    make([]SequencedBatchPreEtrog, len(unpackedbatches)),
		L2Coinbase: unpackedCalldata["l2Coinbase"].(common.Address),
	}

	for i, batch := range unpackedbatches {
		calldata.Batches[i] = SequencedBatchPreEtrog{
			Transactions:       batch.Transactions,
			GlobalExitRoot:     common.BytesToHash(batch.GlobalExitRoot[:]),
			Timestamp:          batch.Timestamp,
			MinForcedTimestamp: batch.MinForcedTimestamp,
		}
	}

	return calldata
}

type SequencedBatchValidiumPreEtrog struct {
	TransactionsHash   common.Hash
	GlobalExitRoot     common.Hash
	Timestamp          uint64
	MinForcedTimestamp uint64
}

type SequenceBatchesCalldataValidiumPreEtrog struct {
	Batches    []SequencedBatchValidiumPreEtrog
	L2Coinbase common.Address
}

func decodePreEtrogSequenceBatchesValidiumCallData(unpackedCalldata map[string]interface{}) *SequenceBatchesCalldataValidiumPreEtrog {
	unpackedbatches := unpackedCalldata["batches"].([]struct {
		TransactionsHash   [32]uint8 `json:"transactionsHash"`
		GlobalExitRoot     [32]uint8 `json:"globalExitRoot"`
		Timestamp          uint64    `json:"timestamp"`
		MinForcedTimestamp uint64    `json:"minForcedTimestamp"`
	})

	calldata := &SequenceBatchesCalldataValidiumPreEtrog{
		Batches:    make([]SequencedBatchValidiumPreEtrog, len(unpackedbatches)),
		L2Coinbase: unpackedCalldata["l2Coinbase"].(common.Address),
	}

	for i, batch := range unpackedbatches {
		calldata.Batches[i] = SequencedBatchValidiumPreEtrog{
			TransactionsHash:   common.BytesToHash(batch.TransactionsHash[:]),
			GlobalExitRoot:     common.BytesToHash(batch.GlobalExitRoot[:]),
			Timestamp:          batch.Timestamp,
			MinForcedTimestamp: batch.MinForcedTimestamp,
		}
	}

	return calldata
}
