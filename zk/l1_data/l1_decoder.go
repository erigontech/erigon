package l1_data

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/zk/contracts"
)

type RollupBaseEtrogBatchData struct {
	Transactions         []byte
	ForcedGlobalExitRoot [32]byte
	ForcedTimestamp      uint64
	ForcedBlockHashL1    [32]byte
}

func DecodeL1BatchData(txData []byte) (uint64, [][]byte, common.Address, error) {
	smcAbi, err := abi.JSON(strings.NewReader(contracts.SequenceBatchesAbi))
	if err != nil {
		return 0, nil, common.Address{}, err
	}

	method, err := smcAbi.MethodById(txData[:4])
	if err != nil {
		return 0, nil, common.Address{}, err
	}

	// Unpack method inputs
	data, err := method.Inputs.Unpack(txData[4:])
	if err != nil {
		return 0, nil, common.Address{}, err
	}

	initialSequence, ok := data[2].(uint64)
	if !ok {
		return 0, nil, common.Address{}, fmt.Errorf("expected position 2 in the l1 call data to be uint64")
	}

	coinbase, ok := data[3].(common.Address)
	if !ok {
		return 0, nil, common.Address{}, fmt.Errorf("expected position 3 in the l1 call data to be address")
	}

	var sequences []RollupBaseEtrogBatchData

	bytedata, err := json.Marshal(data[0])
	if err != nil {
		return 0, nil, coinbase, err
	}
	err = json.Unmarshal(bytedata, &sequences)
	if err != nil {
		return 0, nil, coinbase, err
	}

	batchL2Datas := make([][]byte, len(sequences))
	for idx, sequence := range sequences {
		batchL2Datas[idx] = sequence.Transactions
	}

	return initialSequence, batchL2Datas, coinbase, err
}
