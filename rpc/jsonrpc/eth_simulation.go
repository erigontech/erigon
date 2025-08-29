// Copyright 2025 The Erigon Authors
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

package jsonrpc

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/transactions"
)

// SimulationRequest represents the parameters for an eth_simulateV1 call.
type SimulationRequest struct {
	BlockStateCalls        []SimulatedBlock `json:"blockStateCalls"`
	TraceTransfers         bool             `json:"traceTransfers"`
	Validation             bool             `json:"validation"`
	ReturnFullTransactions bool             `json:"returnFullTransactions"`
}

// SimulatedBlock defines the simulation for a single block.
type SimulatedBlock struct {
	BlockOverrides *transactions.BlockOverrides `json:"blockOverrides,omitempty"`
	StateOverrides *ethapi.StateOverrides       `json:"stateOverrides,omitempty"`
	Calls          []ethapi.CallArgs            `json:"calls"`
}

// SimulationResult represents the result of an eth_simulateV1 call.
type SimulationResult struct {
	Results []CallResult
}

// CallResult represents the result of a single call in the simulation.
type CallResult struct {
	ReturnData string         `json:"returnData"`
	Logs       []*types.Log   `json:"logs"`
	GasUsed    hexutil.Uint64 `json:"gasUsed"`
	Status     string         `json:"status"`
	Error      interface{}    `json:"error,omitempty"`
}

type RPCBlock map[string]interface{}

// SimulatedBlockResult represents the result of the simulated calls for a single block (i.e. one SimulatedBlock).
type SimulatedBlockResult map[string]interface{}

// SimulateV1 implements the eth_simulateV1 JSON-RPC method.
func (api *APIImpl) SimulateV1(ctx context.Context, req SimulationRequest, blockParameter rpc.BlockNumberOrHash) ([]SimulatedBlockResult, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	blockNumber, hash, _, err := rpchelper.GetBlockNumber(ctx, blockParameter, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}

	blockNumberOrHash := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumber - 1))
	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, blockNumberOrHash, 0, api.filters, api.stateCache, api._txNumReader)
	if err != nil {
		return nil, err
	}

	overrideBlockHash := transactions.BlockHashOverrides{}

	simulatedBlockResults := make([]SimulatedBlockResult, 0, len(req.BlockStateCalls))

	// Iterate over each given SimulatedBlock
	for _, bsc := range req.BlockStateCalls {
		additionalFields := make(map[string]interface{})
		blockResult, err := ethapi.RPCMarshalBlock(block, true, req.ReturnFullTransactions, additionalFields)
		if err != nil {
			return nil, err
		}
		var callResults []CallResult
		for _, call := range bsc.Calls {
			result, logs, err := transactions.DoCall(ctx, api.engine(), call, tx, blockNumberOrHash, block.Header(), bsc.StateOverrides, bsc.BlockOverrides,
				overrideBlockHash, api.GasCap, chainConfig, stateReader, api._blockReader, api.evmCallTimeout)
			if err != nil {
				return nil, err
			}
			// Marshal logs to be an empty array instead of nil when empty
			if logs == nil {
				logs = []*types.Log{}
			}

			callResult := CallResult{Logs: logs, GasUsed: hexutil.Uint64(result.GasUsed)}
			if len(result.ReturnData) > api.ReturnDataLimit {
				callResult.ReturnData = "0x"
				callResult.Status = "0x0"
				callResult.Error = rpc.NewJsonError(
					fmt.Errorf("call returned result on length %d exceeding --rpc.returndata.limit %d", len(result.ReturnData), api.ReturnDataLimit))
			} else {
				callResult.ReturnData = fmt.Sprintf("0x%x", result.ReturnData)
				if result.Failed() {
					callResult.Status = "0x0"
					if len(result.Revert()) > 0 {
						// If the result contains a revert reason, try to unpack and return it.
						callResult.Error = rpc.NewJsonError(ethapi.NewRevertError(result))
					} else {
						// Otherwise, we just capture the error message.
						callResult.Error = rpc.NewJsonError(result.Err)
					}
				} else {
					// If the call was successful, we capture the return data, the gas used and logs.
					callResult.Status = "0x1"
				}
			}
			callResults = append(callResults, callResult)
		}
		blockResult["calls"] = callResults
		simulatedBlockResults = append(simulatedBlockResults, blockResult)
	}

	return simulatedBlockResults, nil
}
