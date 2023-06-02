package requests

import (
	"bytes"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/core/types"
)

func BlockNumber(reqGen *RequestGenerator, logger log.Logger) (uint64, error) {
	var b models.EthBlockNumber

	req := reqGen.BlockNumber()
	res := reqGen.Erigon(models.ETHBlockNumber, req, &b)
	number := uint64(b.Number)

	if res.Err != nil {
		return number, fmt.Errorf("error getting current block number: %v", res.Err)
	}

	return number, nil
}

func GetBlockByNumber(reqGen *RequestGenerator, blockNum uint64, withTxs bool, logger log.Logger) (models.EthBlockByNumber, error) {
	var b models.EthBlockByNumber

	req := reqGen.GetBlockByNumber(blockNum, withTxs)

	res := reqGen.Erigon(models.ETHGetBlockByNumber, req, &b)
	if res.Err != nil {
		return b, fmt.Errorf("error getting block by number: %v", res.Err)
	}

	if b.Error != nil {
		return b, fmt.Errorf("error populating response object: %v", b.Error)
	}

	return b, nil
}

func GetBlockByNumberDetails(reqGen *RequestGenerator, blockNum string, withTxs bool, logger log.Logger) (map[string]interface{}, error) {
	var b struct {
		models.CommonResponse
		Result interface{} `json:"result"`
	}

	req := reqGen.GetBlockByNumberI(blockNum, withTxs)

	res := reqGen.Erigon(models.ETHGetBlockByNumber, req, &b)
	if res.Err != nil {
		return nil, fmt.Errorf("error getting block by number: %v", res.Err)
	}

	if b.Error != nil {
		return nil, fmt.Errorf("error populating response object: %v", b.Error)
	}

	m, ok := b.Result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot convert type")
	}

	return m, nil
}

func GetTransactionCount(reqGen *RequestGenerator, address libcommon.Address, blockNum models.BlockNumber, logger log.Logger) (models.EthGetTransactionCount, error) {
	var b models.EthGetTransactionCount

	if res := reqGen.Erigon(models.ETHGetTransactionCount, reqGen.GetTransactionCount(address, blockNum), &b); res.Err != nil {
		return b, fmt.Errorf("error getting transaction count: %v", res.Err)
	}

	if b.Error != nil {
		return b, fmt.Errorf("error populating response object: %v", b.Error)
	}

	return b, nil
}

func SendTransaction(reqGen *RequestGenerator, signedTx *types.Transaction, logger log.Logger) (*libcommon.Hash, error) {
	var b models.EthSendRawTransaction

	var buf bytes.Buffer
	if err := (*signedTx).MarshalBinary(&buf); err != nil {
		return nil, fmt.Errorf("failed to marshal binary: %v", err)
	}

	if res := reqGen.Erigon(models.ETHSendRawTransaction, reqGen.SendRawTransaction(buf.Bytes()), &b); res.Err != nil {
		return nil, fmt.Errorf("could not make to request to eth_sendRawTransaction: %v", res.Err)
	}

	return &b.TxnHash, nil
}
