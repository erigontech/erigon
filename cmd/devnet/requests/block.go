package requests

import (
	"bytes"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/core/types"
)

func BlockNumber(reqId int, logger log.Logger) (uint64, error) {
	reqGen := initialiseRequestGenerator(reqId, logger)
	var b rpctest.EthBlockNumber

	req := reqGen.BlockNumber()
	res := reqGen.Erigon(models.ETHBlockNumber, req, &b)
	number := uint64(b.Number)

	if res.Err != nil {
		return number, fmt.Errorf("error getting current block number: %v", res.Err)
	}

	return number, nil
}

func GetBlockByNumber(reqId int, blockNum uint64, withTxs bool, logger log.Logger) (rpctest.EthBlockByNumber, error) {
	reqGen := initialiseRequestGenerator(reqId, logger)
	var b rpctest.EthBlockByNumber

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

func GetBlockByNumberDetails(reqId int, blockNum string, withTxs bool, logger log.Logger) (map[string]interface{}, error) {
	reqGen := initialiseRequestGenerator(reqId, logger)
	var b struct {
		rpctest.CommonResponse
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

func GetTransactionCount(reqId int, address libcommon.Address, blockNum models.BlockNumber, logger log.Logger) (rpctest.EthGetTransactionCount, error) {
	reqGen := initialiseRequestGenerator(reqId, logger)
	var b rpctest.EthGetTransactionCount

	if res := reqGen.Erigon(models.ETHGetTransactionCount, reqGen.GetTransactionCount(address, blockNum), &b); res.Err != nil {
		return b, fmt.Errorf("error getting transaction count: %v", res.Err)
	}

	if b.Error != nil {
		return b, fmt.Errorf("error populating response object: %v", b.Error)
	}

	return b, nil
}

func SendTransaction(reqId int, signedTx *types.Transaction, logger log.Logger) (*libcommon.Hash, error) {
	reqGen := initialiseRequestGenerator(reqId, logger)
	var b rpctest.EthSendRawTransaction

	var buf bytes.Buffer
	if err := (*signedTx).MarshalBinary(&buf); err != nil {
		return nil, fmt.Errorf("failed to marshal binary: %v", err)
	}

	if res := reqGen.Erigon(models.ETHSendRawTransaction, reqGen.SendRawTransaction(buf.Bytes()), &b); res.Err != nil {
		return nil, fmt.Errorf("could not make to request to eth_sendRawTransaction: %v", res.Err)
	}

	return &b.TxnHash, nil
}
