package l1_data

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"github.com/ledgerwatch/erigon/zk/da"
)

type RollupBaseEtrogBatchData struct {
	Transactions         []byte
	ForcedGlobalExitRoot [32]byte
	ForcedTimestamp      uint64
	ForcedBlockHashL1    [32]byte
}

type ValidiumBatchData struct {
	TransactionsHash     [32]byte
	ForcedGlobalExitRoot [32]byte
	ForcedTimestamp      uint64
	ForcedBlockHashL1    [32]byte
}

func BuildSequencesForRollup(data []byte) ([]RollupBaseEtrogBatchData, error) {
	var sequences []RollupBaseEtrogBatchData
	err := json.Unmarshal(data, &sequences)
	return sequences, err
}

func BuildSequencesForValidium(data []byte, daUrl string) ([]RollupBaseEtrogBatchData, error) {
	var sequences []RollupBaseEtrogBatchData
	var validiumSequences []ValidiumBatchData
	err := json.Unmarshal(data, &validiumSequences)

	if err != nil {
		return nil, err
	}

	for _, validiumSequence := range validiumSequences {
		hash := common.BytesToHash(validiumSequence.TransactionsHash[:])
		data, err := da.GetOffChainData(context.Background(), daUrl, hash)
		if err != nil {
			return nil, err
		}

		actualTransactionsHash := crypto.Keccak256Hash(data)
		if actualTransactionsHash != hash {
			return nil, fmt.Errorf("unable to fetch off chain data for hash %s, got %s intead", hash.String(), actualTransactionsHash.String())
		}

		sequences = append(sequences, RollupBaseEtrogBatchData{
			Transactions:         data,
			ForcedGlobalExitRoot: validiumSequence.ForcedGlobalExitRoot,
			ForcedTimestamp:      validiumSequence.ForcedTimestamp,
			ForcedBlockHashL1:    validiumSequence.ForcedBlockHashL1,
		})
	}

	return sequences, nil
}

func DecodeL1BatchData(txData []byte, daUrl string) ([][]byte, common.Address, uint64, error) {
	// we need to know which version of the ABI to use here so lets find it
	idAsString := fmt.Sprintf("%x", txData[:4])
	abiMapped, found := contracts.SequenceBatchesMapping[idAsString]
	if !found {
		return nil, common.Address{}, 0, fmt.Errorf("unknown l1 call data")
	}

	smcAbi, err := abi.JSON(strings.NewReader(abiMapped))
	if err != nil {
		return nil, common.Address{}, 0, err
	}

	method, err := smcAbi.MethodById(txData[:4])
	if err != nil {
		return nil, common.Address{}, 0, err
	}

	// Unpack method inputs
	data, err := method.Inputs.Unpack(txData[4:])
	if err != nil {
		return nil, common.Address{}, 0, err
	}

	var coinbase common.Address
	var limitTimstamp uint64

	isValidium := false

	switch idAsString {
	case contracts.SequenceBatchesIdv5_0:
		cb, ok := data[1].(common.Address)
		if !ok {
			return nil, common.Address{}, 0, fmt.Errorf("expected position 1 in the l1 call data to be address")
		}
		coinbase = cb
	case contracts.SequenceBatchesIdv6_6:
		cb, ok := data[3].(common.Address)
		if !ok {
			return nil, common.Address{}, 0, fmt.Errorf("expected position 3 in the l1 call data to be address")
		}
		coinbase = cb
		ts, ok := data[1].(uint64)
		if !ok {
			return nil, common.Address{}, 0, fmt.Errorf("expected position 1 in the l1 call data to be the limit timestamp")
		}
		limitTimstamp = ts
	case contracts.SequenceBatchesValidiumElderBerry:
		if daUrl == "" {
			return nil, common.Address{}, 0, fmt.Errorf("data availability url is required for validium")
		}
		isValidium = true
		cb, ok := data[3].(common.Address)
		if !ok {
			return nil, common.Address{}, 0, fmt.Errorf("expected position 3 in the l1 call data to be address")
		}
		coinbase = cb
		ts, ok := data[1].(uint64)
		if !ok {
			return nil, common.Address{}, 0, fmt.Errorf("expected position 1 in the l1 call data to be the limit timestamp")
		}
		limitTimstamp = ts
	default:
		return nil, common.Address{}, 0, fmt.Errorf("unknown l1 call data")
	}

	var sequences []RollupBaseEtrogBatchData

	bytedata, err := json.Marshal(data[0])
	if err != nil {
		return nil, coinbase, 0, err
	}

	if isValidium {
		sequences, err = BuildSequencesForValidium(bytedata, daUrl)
	} else {
		sequences, err = BuildSequencesForRollup(bytedata)
	}

	if err != nil {
		return nil, coinbase, 0, err
	}

	batchL2Datas := make([][]byte, len(sequences))
	for idx, sequence := range sequences {
		batchL2Datas[idx] = sequence.Transactions
	}

	return batchL2Datas, coinbase, limitTimstamp, err
}
