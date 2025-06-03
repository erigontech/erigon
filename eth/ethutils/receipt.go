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

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/execution/consensus/misc"
)

func MarshalReceipt(
	receipt *types.Receipt,
	txn types.Transaction,
	chainConfig *chain.Config,
	header *types.Header,
	txnHash common.Hash,
	signed bool,
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
		signer := types.NewArbitrumSigner(*types.LatestSignerForChainID(chainId))
		from, _ = signer.Sender(txn)
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
		"logs":              receipt.Logs,
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
	if receipt.Logs == nil {
		fields["logs"] = [][]*types.Log{}
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

	// Set arbitrum related fields
	if chainConfig.IsArbitrum() {
		fields["gasUsedForL1"] = hexutil.Uint64(receipt.GasUsedForL1)

		if chainConfig.IsArbitrumNitro(header.Number) {
			fields["effectiveGasPrice"] = hexutil.Uint64(header.BaseFee.Uint64())
			fields["l1BlockNumber"] = hexutil.Uint64(types.DeserializeHeaderExtraInformation(header).L1BlockNumber)
		} else {
			arbTx, ok := txn.(*types.ArbitrumLegacyTxData)
			if !ok {
				log.Error("Expected transaction to contain arbitrum data", "txHash", txn.Hash())
			} else {
				fields["effectiveGasPrice"] = hexutil.Uint64(arbTx.EffectiveGasPrice)
				fields["l1BlockNumber"] = hexutil.Uint64(arbTx.L1BlockNumber)
			}
		}

		// todo
		// If blockMetadata exists for the block containing this tx, then we will determine if it was timeboosted or not
		// and add that info to the receipt object
		// blockMetadata, err := backend.BlockMetadataByNumber(ctx, blockNumber)
		// if err != nil {
		// 	return nil, err
		// }
		// if blockMetadata != nil {
		// 	fields["timeboosted"], err = blockMetadata.IsTxTimeboosted(txIndex)
		// 	if err != nil {
		// 		log.Error("Error checking if a tx was timeboosted", "txIndex", txIndex, "txHash", tx.Hash(), "err", err)
		// 	}
		// }
	}
	return fields
}
