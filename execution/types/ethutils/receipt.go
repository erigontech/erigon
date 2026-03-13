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

package ethutils

import (
	"encoding/json"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func MarshalReceipt(
	receipt *types.Receipt,
	txn types.Transaction,
	chainConfig *chain.Config,
	header *types.Header,
	txnHash common.Hash,
	signed bool,
	withBlockTimestamp bool,
) map[string]any {
	var chainId *big.Int
	switch t := txn.(type) {
	case *types.LegacyTx:
		if t.Protected() {
			chainId = types.DeriveChainId(&t.V).ToBig()
		}
	default:
		chainId = txn.GetChainID().ToBig()
	}

	var from accounts.Address
	if signed {
		signer := types.LatestSignerForChainID(chainId)
		from, _ = txn.Sender(*signer)
	}

	var logsToMarshal any

	if withBlockTimestamp {
		if receipt.Logs != nil {
			rpcLogs := make([]*types.RPCLog, 0, len(receipt.Logs))
			for _, l := range receipt.Logs {
				rpcLogs = append(rpcLogs, types.ToRPCTransactionLog(l, header, txnHash, uint64(receipt.TransactionIndex)))
			}
			logsToMarshal = rpcLogs
		} else {
			logsToMarshal = make([]*types.RPCLog, 0)
		}
	} else {
		if receipt.Logs == nil {
			logsToMarshal = make([]*types.Log, 0)
		} else {
			logsToMarshal = receipt.Logs
		}
	}

	fields := map[string]any{
		"blockHash":         receipt.BlockHash,
		"blockNumber":       hexutil.Uint64(receipt.BlockNumber.Uint64()),
		"transactionHash":   txnHash,
		"transactionIndex":  hexutil.Uint64(receipt.TransactionIndex),
		"from":              from,
		"to":                txn.GetTo(),
		"type":              hexutil.Uint(txn.Type()),
		"gasUsed":           hexutil.Uint64(receipt.GasUsed),
		"cumulativeGasUsed": hexutil.Uint64(receipt.CumulativeGasUsed),
		"contractAddress":   nil,
		"logs":              logsToMarshal,
		"logsBloom":         types.CreateBloom(types.Receipts{receipt}),
	}

	if !chainConfig.IsLondon(header.Number.Uint64()) {
		fields["effectiveGasPrice"] = (*hexutil.Big)(txn.GetTipCap().ToBig())
	} else {
		baseFee := header.BaseFee
		gasPrice := new(uint256.Int).Add(baseFee, txn.GetEffectiveGasTip(baseFee))
		fields["effectiveGasPrice"] = (*hexutil.Big)(gasPrice.ToBig())
	}

	// Assign status if postState is empty.
	if len(receipt.PostState) == 0 {
		// Assign receipt status.
		fields["status"] = hexutil.Uint64(receipt.Status)
	} else {
		fields["root"] = hexutil.Bytes(receipt.PostState)
	}

	// If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
	if receipt.ContractAddress != (common.Address{}) {
		fields["contractAddress"] = receipt.ContractAddress
	}

	// Set derived blob related fields
	numBlobs := len(txn.GetBlobHashes())
	if numBlobs > 0 {
		if header.ExcessBlobGas == nil {
			log.Warn("excess blob gas not set when trying to marshal blob tx")
		} else {
			blobGasPrice, err := misc.GetBlobGasPrice(chainConfig, *header.ExcessBlobGas, header.Time)
			if err != nil {
				log.Error(err.Error())
			}
			fields["blobGasPrice"] = (*hexutil.Big)(blobGasPrice.ToBig())
			fields["blobGasUsed"] = hexutil.Uint64(misc.GetBlobGasUsed(numBlobs))
		}
	}

	return fields
}

func MarshalSubscribeReceipt(protoReceipt *remoteproto.SubscribeReceiptsReply) map[string]any {
	receipt := make(map[string]any)

	// Basic metadata - convert to proper hex strings
	blockHash := common.Hash(gointerfaces.ConvertH256ToHash(protoReceipt.BlockHash))
	receipt["blockHash"] = blockHash
	receipt["blockNumber"] = hexutil.Uint64(protoReceipt.BlockNumber)
	txHash := common.Hash(gointerfaces.ConvertH256ToHash(protoReceipt.TransactionHash))
	receipt["transactionHash"] = txHash
	receipt["transactionIndex"] = hexutil.Uint64(protoReceipt.TransactionIndex)

	// From address as hex string
	from := common.Address(gointerfaces.ConvertH160toAddress(protoReceipt.From))
	receipt["from"] = from

	// To can be null for contract creation
	if protoReceipt.To != nil {
		toAddr := common.Address(gointerfaces.ConvertH160toAddress(protoReceipt.To))
		if toAddr != (common.Address{}) {
			receipt["to"] = toAddr
		} else {
			receipt["to"] = nil
		}
	} else {
		receipt["to"] = nil
	}

	receipt["type"] = hexutil.Uint64(protoReceipt.Type)
	receipt["status"] = hexutil.Uint64(protoReceipt.Status)
	receipt["cumulativeGasUsed"] = hexutil.Uint64(protoReceipt.CumulativeGasUsed)
	receipt["gasUsed"] = hexutil.Uint64(protoReceipt.GasUsed)

	if protoReceipt.ContractAddress != nil {
		addr := common.Address(gointerfaces.ConvertH160toAddress(protoReceipt.ContractAddress))
		if addr != (common.Address{}) {
			receipt["contractAddress"] = addr
		} else {
			receipt["contractAddress"] = nil
		}
	} else {
		receipt["contractAddress"] = nil
	}

	if len(protoReceipt.LogsBloom) > 0 {
		receipt["logsBloom"] = hexutil.Bytes(protoReceipt.LogsBloom)
	}

	logs := make([]map[string]any, 0, len(protoReceipt.Logs))
	for _, protoLog := range protoReceipt.Logs {
		logEntry := make(map[string]any)

		if protoLog.Address != nil {
			logEntry["address"] = common.Address(gointerfaces.ConvertH160toAddress(protoLog.Address))
		}

		topics := make([]common.Hash, len(protoLog.Topics))
		for i, topic := range protoLog.Topics {
			topics[i] = common.Hash(gointerfaces.ConvertH256ToHash(topic))
		}
		logEntry["topics"] = topics
		logEntry["data"] = hexutil.Bytes(protoLog.Data)
		logEntry["transactionHash"] = txHash

		logs = append(logs, logEntry)
	}
	receipt["logs"] = logs

	if protoReceipt.BaseFee != nil {
		baseFee := gointerfaces.ConvertH256ToUint256Int(protoReceipt.BaseFee)
		receipt["effectiveGasPrice"] = (*hexutil.Big)(baseFee.ToBig())
	}

	if protoReceipt.BlobGasUsed > 0 {
		receipt["blobGasUsed"] = hexutil.Uint64(protoReceipt.BlobGasUsed)
	}
	if protoReceipt.BlobGasPrice != nil {
		blobGasPrice := gointerfaces.ConvertH256ToUint256Int(protoReceipt.BlobGasPrice)
		receipt["blobGasPrice"] = (*hexutil.Big)(blobGasPrice.ToBig())
	}

	return receipt
}

func LogReceipts(level log.Lvl, msg string, receipts types.Receipts, txns types.Transactions, cc *chain.Config, header *types.Header, logger log.Logger) {
	if len(receipts) == 0 {
		// no-op, can happen if vmConfig.NoReceipts=true or vmConfig.StatelessExec=true
		logger.Log(level, msg, "block", header.Number.Uint64(), "receipts", "")
		return
	}

	// note we do not return errors from this func since this is a debug-only
	// informative feature that is best-effort and should not interfere with execution
	if len(receipts) != len(txns) {
		logger.Error("receipts and txns sizes differ", "receiptsLen", receipts.Len(), "txnsLen", txns.Len())
		return
	}

	marshalled := make([]map[string]any, 0, len(receipts))
	for i, receipt := range receipts {
		txn := txns[i]
		marshalled = append(marshalled, MarshalReceipt(receipt, txn, cc, header, txn.Hash(), true, false))
	}

	result, err := json.Marshal(marshalled)
	if err != nil {
		logger.Error("marshalling error when logging receipts", "err", err)
		return
	}
	logger.Log(level, msg, "block", header.Number.Uint64(), "receipts", string(result))
}
