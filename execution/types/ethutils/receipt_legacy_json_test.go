// Copyright 2026 The Erigon Authors
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
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// legacyMarshalReceipt is the map[string]any builder this package used before
// RPCReceipt. The tests keep it as an oracle: the typed output must still encode
// to the same JSON object.
func legacyMarshalReceipt(
	receipt *types.Receipt,
	txn types.Transaction,
	chainConfig *chain.Config,
	header *types.Header,
	txnHash common.Hash,
	signed bool,
	withBlockTimestamp bool,
) map[string]any {
	var chainId *uint256.Int
	switch t := txn.(type) {
	case *types.LegacyTx:
		if t.Protected() {
			chainId, _ = types.DeriveChainId(&t.V)
		}
	default:
		chainId = txn.GetChainID()
	}

	var from accounts.Address
	if signed {
		signer := types.LatestSignerForChainID(chainId)
		from, _ = txn.Sender(*signer)
	}

	logsBloom := receipt.Bloom
	if logsBloom.IsEmpty() && len(receipt.Logs) > 0 {
		logsBloom = types.CreateBloom(types.Receipts{receipt})
	}

	var logsToMarshal any
	if withBlockTimestamp {
		if receipt.Logs != nil {
			rpcLogs := make([]*types.RPCLog, 0, len(receipt.Logs))
			for _, l := range receipt.Logs {
				rpcLogs = append(rpcLogs, types.ToRPCTransactionLog(l, header))
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
		"logsBloom":         logsBloom,
	}

	if !chainConfig.IsLondon(header.Number.Uint64()) {
		fields["effectiveGasPrice"] = (*hexutil.U256)(new(uint256.Int).Set(txn.GetTipCap()))
	} else {
		baseFee := header.BaseFee
		effectiveTip := txn.GetEffectiveGasTip(baseFee)
		var gasPrice uint256.Int
		gasPrice.Add(baseFee, &effectiveTip)
		fields["effectiveGasPrice"] = (*hexutil.U256)(&gasPrice)
	}

	if len(receipt.PostState) == 0 {
		fields["status"] = hexutil.Uint64(receipt.Status)
	} else {
		fields["root"] = hexutil.Bytes(receipt.PostState)
	}

	if receipt.ContractAddress != (common.Address{}) {
		fields["contractAddress"] = receipt.ContractAddress
	}

	numBlobs := len(txn.GetBlobHashes())
	if numBlobs > 0 && header.ExcessBlobGas != nil {
		blobGasPrice, _ := misc.GetBlobGasPrice(chainConfig, *header.ExcessBlobGas, header.Time)
		fields["blobGasPrice"] = (*hexutil.U256)(&blobGasPrice)
		fields["blobGasUsed"] = hexutil.Uint64(misc.GetBlobGasUsed(numBlobs))
	}

	return fields
}

func dynamicFeeTx(to *common.Address) types.DynamicFeeTransaction {
	return types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: 3, GasLimit: 21000, To: to, Value: *uint256.NewInt(5)},
		ChainID:  *uint256.NewInt(1337),
		TipCap:   *uint256.NewInt(2),
		FeeCap:   *uint256.NewInt(100),
	}
}

func receiptTxVariants() map[string]types.Transaction {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	dynamic, create := dynamicFeeTx(&to), dynamicFeeTx(nil)
	protected := &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 1, GasLimit: 21000, To: &to}, GasPrice: *uint256.NewInt(100)}
	protected.V = *uint256.NewInt(2711)
	return map[string]types.Transaction{
		"legacy":          &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 1, GasLimit: 21000, To: &to}, GasPrice: *uint256.NewInt(100)},
		"legacyProtected": protected,
		"dynamic":         &dynamic,
		"create":          &create,
		"blob":            &types.BlobTx{DynamicFeeTransaction: dynamicFeeTx(&to), MaxFeePerBlobGas: *uint256.NewInt(9), BlobVersionedHashes: []common.Hash{{0x01}, {0x02}}},
	}
}

// TestMarshalReceiptMatchesLegacyJSON sweeps the receipt shapes that decide
// whether a field is null, omitted or empty: nil versus empty logs, pre-Byzantium
// post state versus status, a zero status, creation and blob transactions.
func TestMarshalReceiptMatchesLegacyJSON(t *testing.T) {
	log := &types.Log{Address: common.HexToAddress("0x11"), Topics: []common.Hash{{0x01}}, Data: []byte{0xaa}}
	var presetBloom types.Bloom
	presetBloom[0] = 0x80
	excessBlobGas := uint64(0x40000)

	for _, cfg := range []struct {
		name   string
		config *chain.Config
	}{
		{name: "berlin", config: chain.TestChainBerlinConfig},
		{name: "osaka", config: chain.TestChainOsakaConfig},
	} {
		for txName, txn := range receiptTxVariants() {
			for _, logs := range []struct {
				name  string
				value types.Logs
			}{
				{name: "nil", value: nil},
				{name: "empty", value: types.Logs{}},
				{name: "set", value: types.Logs{log}},
			} {
				for _, bloom := range []types.Bloom{{}, presetBloom} {
					for _, state := range []struct {
						name      string
						postState []byte
						status    uint64
					}{
						{name: "failed", status: types.ReceiptStatusFailed},
						{name: "successful", status: types.ReceiptStatusSuccessful},
						{name: "postState", postState: []byte{0x0b}},
					} {
						for _, contract := range []common.Address{{}, common.HexToAddress("0x22")} {
							for _, withBlockTimestamp := range []bool{false, true} {
								if txName == "blob" && cfg.name == "berlin" {
									continue
								}
								header := &types.Header{Number: *uint256.NewInt(7), Time: 1_750_000_000}
								if cfg.name == "osaka" {
									header.BaseFee = uint256.NewInt(50)
									header.ExcessBlobGas = &excessBlobGas
								}
								receipt := &types.Receipt{
									PostState:         state.postState,
									Status:            state.status,
									CumulativeGasUsed: 42_000,
									Bloom:             bloom,
									Logs:              logs.value,
									TxHash:            common.HexToHash("0xbeef"),
									ContractAddress:   contract,
									GasUsed:           21_000,
									BlockHash:         common.HexToHash("0xb10c"),
									BlockNumber:       uint256.NewInt(7),
									TransactionIndex:  3,
								}
								name := fmt.Sprintf("%s/%s/logs=%s/bloom=%v/%s/contract=%v/ts=%v", cfg.name, txName, logs.name, !bloom.IsEmpty(), state.name, contract != (common.Address{}), withBlockTimestamp)
								want, err := json.Marshal(legacyMarshalReceipt(receipt, txn, cfg.config, header, receipt.TxHash, true, withBlockTimestamp))
								require.NoError(t, err)
								got, err := json.Marshal(MarshalReceipt(receipt, txn, cfg.config, header, receipt.TxHash, true, withBlockTimestamp))
								require.NoError(t, err)
								require.JSONEq(t, string(want), string(got), name)
							}
						}
					}
				}
			}
		}
	}
}
