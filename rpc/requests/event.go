// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package requests

import (
	"context"
	"fmt"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/fastjson"
	"github.com/erigontech/erigon-lib/types"
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

func NewLog(hash common.Hash, blockNum uint64, address common.Address, topics []common.Hash, data hexutil.Bytes, txIndex uint, blockHash common.Hash, index hexutil.Uint, removed bool) types.Log {
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

	if err := reqGen.rpcCall(ctx, &result, Methods.ETHGetLogs, query); err != nil {
		return nil, err
	}

	return result, nil
}

func (reqGen *requestGenerator) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return reqGen.Subscribe(ctx, Methods.ETHLogs, ch, query)
}

// ParseResponse converts any of the models interfaces to a string for readability
func parseResponse(resp interface{}) (string, error) {
	result, err := fastjson.Marshal(resp)
	if err != nil {
		return "", fmt.Errorf("error trying to marshal response: %v", err)
	}

	return string(result), nil
}

func hashSlicesAreEqual(s1, s2 []common.Hash) bool {
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
