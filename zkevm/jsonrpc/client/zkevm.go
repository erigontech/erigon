package client

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
)

// BatchNumber returns the latest batch number
func (c *Client) BatchNumber(ctx context.Context) (uint64, error) {
	response, err := JSONRPCCall(c.url, "zkevm_batchNumber")
	if err != nil {
		return 0, err
	}

	if response.Error != nil {
		return 0, fmt.Errorf("%v %v", response.Error.Code, response.Error.Message)
	}

	var result string
	err = json.Unmarshal(response.Result, &result)
	if err != nil {
		return 0, err
	}

	bigBatchNumber := hex.DecodeBig(result)
	batchNumber := bigBatchNumber.Uint64()

	return batchNumber, nil
}

// BatchByNumber returns a batch from the current canonical chain. If number is nil, the
// latest known batch is returned.
func (c *Client) BatchByNumber(ctx context.Context, number *big.Int) (*types.Batch, error) {
	response, err := JSONRPCCall(c.url, "zkevm_getBatchByNumber", types.ToBatchNumArg(number), true)
	if err != nil {
		return nil, err
	}

	if response.Error != nil {
		return nil, fmt.Errorf("%v %v", response.Error.Code, response.Error.Message)
	}

	var result *types.Batch
	err = json.Unmarshal(response.Result, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *Client) BatchBatchesByNumber(ctx context.Context, numbers []*big.Int) ([]*types.Batch, error) {
	methods := make([]string, len(numbers))
	paramGroups := make([][]interface{}, len(numbers))

	for i, num := range numbers {
		methods[i] = "zkevm_getBatchByNumber"
		paramGroups[i] = []interface{}{types.ToBatchNumArg(num), true}
	}

	responses, err := JSONRPCBatchCall(c.url, methods, paramGroups...)
	if err != nil {
		return nil, err
	}

	batches := make([]*types.Batch, len(responses))
	for i, res := range responses {
		var batch types.Batch
		if err := json.Unmarshal(res.Result, &batch); err != nil {
			return nil, err
		}
		batches[i] = &batch
	}

	return batches, nil
}
