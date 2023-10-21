package requests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/core/types"
)

func Compare(expected types.Log, actual types.Log) ([]error, bool) {
	var errs []error

	switch {
	case expected.Address != actual.Address:
		errs = append(errs, fmt.Errorf("expected address: %v, actual address %v", expected.Address, actual.Address))
	case expected.TxHash != actual.TxHash:
		errs = append(errs, fmt.Errorf("expected txhash: %v, actual txhash %v", expected.TxHash, actual.TxHash))
	case expected.BlockHash != actual.BlockHash:
		errs = append(errs, fmt.Errorf("expected blockHash: %v, actual blockHash %v", expected.BlockHash, actual.BlockHash))
	case expected.BlockNumber != actual.BlockNumber:
		errs = append(errs, fmt.Errorf("expected blockNumber: %v, actual blockNumber %v", expected.BlockNumber, actual.BlockNumber))
	case expected.TxIndex != actual.TxIndex:
		errs = append(errs, fmt.Errorf("expected txIndex: %v, actual txIndex %v", expected.TxIndex, actual.TxIndex))
	case !hashSlicesAreEqual(expected.Topics, actual.Topics):
		errs = append(errs, fmt.Errorf("expected topics: %v, actual topics %v", expected.Topics, actual.Topics))
	}

	return errs, len(errs) == 0
}

func NewLog(hash libcommon.Hash, blockNum uint64, address libcommon.Address, topics []libcommon.Hash, data hexutility.Bytes, txIndex uint, blockHash libcommon.Hash, index hexutil.Uint, removed bool) types.Log {
	return types.Log{
		Address:     address,
		Topics:      topics,
		Data:        data,
		BlockNumber: blockNum,
		TxHash:      hash,
		TxIndex:     txIndex,
		BlockHash:   blockHash,
		Index:       txIndex,
		Removed:     removed,
	}
}

func (reqGen *requestGenerator) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	var result []types.Log

	if err := reqGen.callCli(&result, Methods.ETHGetLogs, query); err != nil {
		return nil, err
	}

	return result, nil
}

func (reqGen *requestGenerator) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return reqGen.Subscribe(ctx, Methods.ETHLogs, ch, query)
}

// ParseResponse converts any of the models interfaces to a string for readability
func parseResponse(resp interface{}) (string, error) {
	result, err := json.Marshal(resp)
	if err != nil {
		return "", fmt.Errorf("error trying to marshal response: %v", err)
	}

	return string(result), nil
}

func hashSlicesAreEqual(s1, s2 []libcommon.Hash) bool {
	if len(s1) != len(s2) {
		return false
	}

	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			return false
		}
	}

	return true
}
