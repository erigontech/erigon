package l1_data

import (
	"github.com/ledgerwatch/erigon/accounts/abi"
	"strings"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"encoding/json"
	"fmt"
)

type RollupBaseEtrogBatchData struct {
	Transactions         []byte
	ForcedGlobalExitRoot [32]byte
	ForcedTimestamp      uint64
	ForcedBlockHashL1    [32]byte
}

func DecodeL1BatchData(txData []byte) (uint64, [][]byte, error) {
	smcAbi, err := abi.JSON(strings.NewReader(contracts.SequenceBatchesAbi))
	if err != nil {
		return 0, nil, err
	}

	method, err := smcAbi.MethodById(txData[:4])
	if err != nil {
		return 0, nil, err
	}

	// Unpack method inputs
	data, err := method.Inputs.Unpack(txData[4:])
	if err != nil {
		return 0, nil, err
	}

	initialSequence, ok := data[2].(uint64)
	if !ok {
		return 0, nil, fmt.Errorf("expected position 2 in the l1 call data to be uint64")
	}

	var sequences []RollupBaseEtrogBatchData

	bytedata, err := json.Marshal(data[0])
	if err != nil {
		return 0, nil, err
	}
	err = json.Unmarshal(bytedata, &sequences)
	if err != nil {
		return 0, nil, err
	}

	batchL2Datas := make([][]byte, len(sequences))
	for idx, sequence := range sequences {
		batchL2Datas[idx] = sequence.Transactions
	}

	return initialSequence, batchL2Datas, err
}
