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
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/execution/types"
)

func MarshalReceipt(
	receipt *types.Receipt,
	txn types.Transaction,
	chainConfig *chain.Config,
	header *types.Header,
	txnHash common.Hash,
	signed bool,
	withBlockTimestamp bool,
) map[string]interface{} {
	var chainId *big.Int
	switch t := txn.(type) {
	case *types.LegacyTx:
		if t.Protected() {
			chainId = types.DeriveChainId(&t.V).ToBig()
		}
	default:
		chainId = txn.GetChainID().ToBig()
	}

	var from common.Address
	if signed {
		signer := types.LatestSignerForChainID(chainId)
		from, _ = txn.Sender(*signer)
	}

	var logsToMarshal interface{}

	if withBlockTimestamp {
		if receipt.Logs != nil {
			rpcLogs := []*types.RPCLog{}
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

	fields := map[string]interface{}{
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
		baseFee, _ := uint256.FromBig(header.BaseFee)
		gasPrice := new(big.Int).Add(header.BaseFee, txn.GetEffectiveGasTip(baseFee).ToBig())
		fields["effectiveGasPrice"] = (*hexutil.Big)(gasPrice)
	}

	// Assign receipt status.
	fields["status"] = hexutil.Uint64(receipt.Status)

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
