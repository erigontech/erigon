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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

//go:generate go run github.com/erigontech/erigon/cmd/tools/jsongen -type RPCReceipt

// RPCReceipt is the RPC form of a receipt.
type RPCReceipt struct {
	BlockHash         common.Hash          `json:"blockHash" ethjson:"data"`
	BlockNumber       hexutil.Uint64       `json:"blockNumber" ethjson:"quantity"`
	TransactionHash   common.Hash          `json:"transactionHash" ethjson:"data"`
	TransactionIndex  hexutil.Uint64       `json:"transactionIndex" ethjson:"quantity"`
	From              *common.Address      `json:"from" ethjson:"data"`
	To                *common.Address      `json:"to" ethjson:"data"`
	Type              hexutil.Uint         `json:"type" ethjson:"quantity"`
	GasUsed           hexutil.Uint64       `json:"gasUsed" ethjson:"quantity"`
	CumulativeGasUsed hexutil.Uint64       `json:"cumulativeGasUsed" ethjson:"quantity"`
	ContractAddress   *common.Address      `json:"contractAddress" ethjson:"data"`
	Logs              jsonstream.Marshaler `json:"logs" ethjson:"objects"`
	LogsBloom         *types.Bloom         `json:"logsBloom" ethjson:"data"`
	EffectiveGasPrice *hexutil.U256        `json:"effectiveGasPrice" ethjson:"quantity"`

	Status       *hexutil.Uint64 `json:"status,omitempty" ethjson:"quantity"`
	Root         hexutil.Bytes   `json:"root,omitempty" ethjson:"data"`
	BlobGasPrice *hexutil.U256   `json:"blobGasPrice,omitempty" ethjson:"quantity"`
	BlobGasUsed  *hexutil.Uint64 `json:"blobGasUsed,omitempty" ethjson:"quantity"`
}

func MarshalReceipt(
	receipt *types.Receipt,
	txn types.Transaction,
	chainConfig *chain.Config,
	header *types.Header,
	signed bool,
	withBlockTimestamp bool,
) *RPCReceipt {
	var chainId *uint256.Int
	switch t := txn.(type) {
	case *types.LegacyTx:
		if t.Protected() {
			chainId, _ = types.DeriveChainId(&t.V)
		}
	default:
		chainId = txn.GetChainID()
	}

	var from *common.Address
	if signed {
		signer := types.LatestSignerForChainID(chainId)
		sender, _ := txn.Sender(*signer)
		address := sender.Value()
		from = &address
	}

	var logsToMarshal jsonstream.Marshaler

	switch {
	case withBlockTimestamp:
		rpcLogs := make(types.RPCLogs, 0, len(receipt.Logs))
		for _, l := range receipt.Logs {
			rpcLogs = append(rpcLogs, types.ToRPCTransactionLog(l, header))
		}
		logsToMarshal = rpcLogs
	case receipt.Logs == nil:
		logsToMarshal = types.Logs{}
	default:
		logsToMarshal = receipt.Logs
	}

	result := &RPCReceipt{
		BlockHash:         receipt.BlockHash,
		BlockNumber:       hexutil.Uint64(receipt.BlockNumber.Uint64()),
		TransactionHash:   receipt.TxHash,
		TransactionIndex:  hexutil.Uint64(receipt.TransactionIndex),
		From:              from,
		To:                txn.GetTo(),
		Type:              hexutil.Uint(txn.Type()),
		GasUsed:           hexutil.Uint64(receipt.GasUsed),
		CumulativeGasUsed: hexutil.Uint64(receipt.CumulativeGasUsed),
		Logs:              logsToMarshal,
		LogsBloom:         receipt.LogsBloom(),
	}

	if !chainConfig.IsLondon(header.Number.Uint64()) {
		result.EffectiveGasPrice = (*hexutil.U256)(new(uint256.Int).Set(txn.GetTipCap()))
	} else {
		baseFee := header.BaseFee
		effectiveTip := txn.GetEffectiveGasTip(baseFee)
		var gasPrice uint256.Int
		gasPrice.Add(baseFee, &effectiveTip)
		result.EffectiveGasPrice = (*hexutil.U256)(&gasPrice)
	}

	// Assign status if postState is empty.
	if len(receipt.PostState) == 0 {
		status := hexutil.Uint64(receipt.Status)
		result.Status = &status
	} else {
		result.Root = receipt.PostState
	}

	// If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
	if receipt.ContractAddress != (common.Address{}) {
		contractAddress := receipt.ContractAddress
		result.ContractAddress = &contractAddress
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
			blobGasUsed := hexutil.Uint64(misc.GetBlobGasUsed(numBlobs))
			result.BlobGasPrice = (*hexutil.U256)(&blobGasPrice)
			result.BlobGasUsed = &blobGasUsed
		}
	}

	return result
}

// RPCLogFromProto is the log a subscription delivers, the same object eth_getLogs returns. The
// address and both hashes must be set, as every backend sets them.
func RPCLogFromProto(l *remoteproto.SubscribeLogsReply) *types.RPCLog {
	lg := &types.RPCLog{
		Log: types.Log{
			Address:     gointerfaces.ConvertH160toAddress(l.Address),
			Topics:      make([]common.Hash, len(l.Topics)),
			Data:        l.Data,
			BlockNumber: hexutil.Uint64(l.BlockNumber),
			TxHash:      gointerfaces.ConvertH256ToHash(l.TransactionHash),
			TxIndex:     hexutil.Uint(l.TransactionIndex),
			BlockHash:   gointerfaces.ConvertH256ToHash(l.BlockHash),
			Index:       hexutil.Uint(l.LogIndex),
			Removed:     l.Removed,
		},
		BlockTimestamp: hexutil.Uint64(l.BlockTimestamp),
	}
	for i, topic := range l.Topics {
		lg.Topics[i] = gointerfaces.ConvertH256ToHash(topic)
	}
	return lg
}

func MarshalSubscribeReceipt(protoReceipt *remoteproto.SubscribeReceiptsReply) *RPCReceipt {
	txHash := common.Hash(gointerfaces.ConvertH256ToHash(protoReceipt.TransactionHash))
	status := hexutil.Uint64(protoReceipt.Status)
	result := &RPCReceipt{
		BlockHash:         common.Hash(gointerfaces.ConvertH256ToHash(protoReceipt.BlockHash)),
		BlockNumber:       hexutil.Uint64(protoReceipt.BlockNumber),
		TransactionHash:   txHash,
		TransactionIndex:  hexutil.Uint64(protoReceipt.TransactionIndex),
		From:              addressOrNil(protoReceipt.From),
		To:                addressOrNil(protoReceipt.To),
		Type:              hexutil.Uint(protoReceipt.Type),
		GasUsed:           hexutil.Uint64(protoReceipt.GasUsed),
		CumulativeGasUsed: hexutil.Uint64(protoReceipt.CumulativeGasUsed),
		ContractAddress:   addressOrNil(protoReceipt.ContractAddress),
		Status:            &status,
	}
	if n := len(protoReceipt.LogsBloom); n == types.BloomByteLength {
		bloom := types.BytesToBloom(protoReceipt.LogsBloom)
		result.LogsBloom = &bloom
	} else if n != 0 {
		log.Warn("[rpc] subscribed receipt has a malformed logs bloom", "len", n, "txHash", txHash)
	}

	logs := make(types.RPCLogs, len(protoReceipt.Logs))
	for i, protoLog := range protoReceipt.Logs {
		logs[i] = RPCLogFromProto(protoLog)
	}
	result.Logs = logs

	// A backend that does not send the effective gas price yet leaves the base fee as the
	// closest approximation it carries.
	if protoReceipt.EffectiveGasPrice != nil {
		result.EffectiveGasPrice = (*hexutil.U256)(gointerfaces.ConvertH256ToUint256Int(protoReceipt.EffectiveGasPrice))
	} else if protoReceipt.BaseFee != nil {
		result.EffectiveGasPrice = (*hexutil.U256)(gointerfaces.ConvertH256ToUint256Int(protoReceipt.BaseFee))
	}
	if protoReceipt.BlobGasUsed > 0 {
		blobGasUsed := hexutil.Uint64(protoReceipt.BlobGasUsed)
		result.BlobGasUsed = &blobGasUsed
	}
	if protoReceipt.BlobGasPrice != nil {
		result.BlobGasPrice = (*hexutil.U256)(gointerfaces.ConvertH256ToUint256Int(protoReceipt.BlobGasPrice))
	}
	return result
}

func addressOrNil(h160 *typesproto.H160) *common.Address {
	if h160 == nil {
		return nil
	}
	addr := common.Address(gointerfaces.ConvertH160toAddress(h160))
	return &addr
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

	marshalled := make([]*RPCReceipt, 0, len(receipts))
	for i, receipt := range receipts {
		txn := txns[i]
		marshalled = append(marshalled, MarshalReceipt(receipt, txn, cc, header, true, false))
	}

	result, err := json.Marshal(marshalled)
	if err != nil {
		logger.Error("marshalling error when logging receipts", "err", err)
		return
	}
	logger.Log(level, msg, "block", header.Number.Uint64(), "receipts", string(result))
}
