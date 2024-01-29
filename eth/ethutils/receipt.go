package ethutils

import (
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/types"
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
		signer := types.LatestSignerForChainID(chainId)
		from, _ = txn.Sender(*signer)
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
		fields["effectiveGasPrice"] = hexutil.Uint64(txn.GetPrice().Uint64())
	} else {
		baseFee, _ := uint256.FromBig(header.BaseFee)
		gasPrice := new(big.Int).Add(header.BaseFee, txn.GetEffectiveGasTip(baseFee).ToBig())
		fields["effectiveGasPrice"] = hexutil.Uint64(gasPrice.Uint64())
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
			blobGasPrice, err := misc.GetBlobGasPrice(chainConfig, *header.ExcessBlobGas)
			if err != nil {
				log.Error(err.Error())
			}
			fields["blobGasPrice"] = blobGasPrice
			fields["blobGasUsed"] = hexutil.Uint64(misc.GetBlobGasUsed(numBlobs))
		}
	}

	return fields
}
