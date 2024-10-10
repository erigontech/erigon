package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/zkevm/encoding"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
	"github.com/ledgerwatch/log/v3"
)

type L1GasPrice struct {
	timestamp time.Time
	gasPrice  *big.Int
}

func (api *APIImpl) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	cc, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	chainId := cc.ChainID
	if !api.isZkNonSequencer(chainId) {
		return api.GasPrice_nonRedirected(ctx)
	}

	if api.BaseAPI.gasless {
		var price hexutil.Big
		return &price, nil
	}

	res, err := client.JSONRPCCall(api.l2RpcUrl, "eth_gasPrice")
	if err != nil {
		return nil, err
	}

	if res.Error != nil {
		return nil, fmt.Errorf("RPC error response: %s", res.Error.Message)
	}

	var resultString string
	if err := json.Unmarshal(res.Result, &resultString); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %v", err)
	}

	price, ok := big.NewInt(0).SetString(resultString[2:], 16)
	if !ok {
		return nil, fmt.Errorf("failed to convert result to big.Int")
	}

	return (*hexutil.Big)(price), nil
}

func (api *APIImpl) GasPrice_nonRedirected(ctx context.Context) (*hexutil.Big, error) {
	if api.BaseAPI.gasless {
		var price hexutil.Big
		return &price, nil
	}

	// if gas price timestamp is older than 3 seconds, update it
	if time.Since(api.L1GasPrice.timestamp) > 3*time.Second || api.L1GasPrice.gasPrice == nil {
		l1GasPrice, err := api.l1GasPrice()
		if err != nil {
			return nil, err
		}
		api.L1GasPrice = L1GasPrice{
			timestamp: time.Now(),
			gasPrice:  l1GasPrice,
		}
	}

	// Apply factor to calculate l2 gasPrice
	factor := big.NewFloat(0).SetFloat64(api.GasPriceFactor)
	res := new(big.Float).Mul(factor, big.NewFloat(0).SetInt(api.L1GasPrice.gasPrice))

	// Store l2 gasPrice calculated
	result := new(big.Int)
	res.Int(result)
	minGasPrice := big.NewInt(0).SetUint64(api.DefaultGasPrice)
	if minGasPrice.Cmp(result) == 1 { // minGasPrice > result
		result = minGasPrice
	}
	maxGasPrice := new(big.Int).SetUint64(api.MaxGasPrice)
	if api.MaxGasPrice > 0 && result.Cmp(maxGasPrice) == 1 { // result > maxGasPrice
		result = maxGasPrice
	}

	var truncateValue *big.Int
	log.Debug("Full L2 gas price value: ", result, ". Length: ", len(result.String()))
	numLength := len(result.String())
	if numLength > 3 { //nolint:gomnd
		aux := "%0" + strconv.Itoa(numLength-3) + "d" //nolint:gomnd
		var ok bool
		value := result.String()[:3] + fmt.Sprintf(aux, 0)
		truncateValue, ok = new(big.Int).SetString(value, encoding.Base10)
		if !ok {
			return nil, fmt.Errorf("failed to convert result to big.Int")
		}
	} else {
		truncateValue = result
	}

	if truncateValue == nil {
		return nil, fmt.Errorf("truncateValue nil value detected")
	}

	return (*hexutil.Big)(truncateValue), nil
}

func (api *APIImpl) l1GasPrice() (*big.Int, error) {
	res, err := client.JSONRPCCall(api.L1RpcUrl, "eth_gasPrice")
	if err != nil {
		return nil, err
	}

	if res.Error != nil {
		return nil, fmt.Errorf("RPC error response: %s", res.Error.Message)
	}

	var resultString string
	if err := json.Unmarshal(res.Result, &resultString); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %v", err)
	}

	price, ok := big.NewInt(0).SetString(resultString[2:], 16)
	if !ok {
		return nil, fmt.Errorf("failed to convert result to big.Int")
	}

	return price, nil
}
