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
	"encoding/json"
	"fmt"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon/execution/types"
)

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
func parseResponse(resp any) (string, error) {
	result, err := json.Marshal(resp)
	if err != nil {
		return "", fmt.Errorf("error trying to marshal response: %v", err)
	}

	return string(result), nil
}
