package engine_types

import (
	"encoding/json"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/core/types"
)

// SignedTransaction contains transaction payload and signature as defined in EIP-6493
// TODO: maybe this belongs in the cl package?
// TODO: json tags are not final

type SignedTransaction struct {
	Payload   TransactionPayload   `json:"payload"`
	Signature TransactionSignature `json:"signature"`
}

type TransactionPayload struct {
	Type                byte               `json:"type"`
	ChainID             *uint256.Int       `json:"chainId,omitempty"`
	Nonce               uint64             `json:"nonce"`
	GasPrice            *uint256.Int       `json:"gasPrice,omitempty"`
	Gas                 uint64             `json:"gas"`
	To                  *common.Address    `json:"to"`
	Value               *uint256.Int       `json:"value"`
	Input               []byte             `json:"input"`
	Accesses            *types2.AccessList `json:"accessList,omitempty"`
	Tip                 *uint256.Int       `json:"maxPriorityFeePerGas,omitempty"`
	MaxFeePerBlobGas    *uint256.Int       `json:"maxFeePerBlobGas,omitempty"`
	BlobVersionedHashes *[]common.Hash     `json:"blobVersionedHashes,omitempty"`
}

type TransactionSignature struct {
	From      common.Address `json:"from"`
	Signature []byte         `json:"signature"` // TODO: this needs to be of particular size (see EIP)
}

func UnmarshalTransctionFromJson(data []byte, blobTxnsAreWrappedWithBlobs bool) (types.Transaction, error) {
	tx := &SignedTransaction{}
	err := json.Unmarshal(data, tx)
	if err != nil {
		return nil, err
	}

	legacyTx := types.LegacyTx{
		CommonTx: types.CommonTx{
			TransactionMisc: types.TransactionMisc{},
			Nonce:           tx.Payload.Nonce,
			Gas:             tx.Payload.Gas,
			To:              tx.Payload.To,
			Value:           tx.Payload.Value,
			Data:            tx.Payload.Input,
		},
		GasPrice: tx.Payload.GasPrice,
	}

	// TODO: v,r,s extraction

	switch int(tx.Payload.Type) {
	case types.LegacyTxType:
		return &legacyTx, nil
	case types.DynamicFeeTxType:
		return &types.DynamicFeeTransaction{
			CommonTx:   legacyTx.CommonTx,
			ChainID:    tx.Payload.ChainID,
			Tip:        tx.Payload.Tip,
			FeeCap:     tx.Payload.GasPrice,
			AccessList: *tx.Payload.Accesses,
		}, nil

	case types.AccessListTxType:
		return &types.AccessListTx{
			LegacyTx:   legacyTx,
			ChainID:    tx.Payload.ChainID,
			AccessList: *tx.Payload.Accesses,
		}, nil

	case types.BlobTxType:
		return &types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx:   legacyTx.CommonTx,
				ChainID:    tx.Payload.ChainID,
				Tip:        tx.Payload.Tip,
				FeeCap:     tx.Payload.GasPrice,
				AccessList: *tx.Payload.Accesses,
			},
			BlobVersionedHashes: *tx.Payload.BlobVersionedHashes,
		}, nil
		case types.SSZTxType:
			return &types.SSZTransaction{
				Transaction: 

			}

	}

}

func DecodeTransactionsJson(txs [][]byte) ([]types.Transaction, error) {
	result := make([]types.Transaction, len(txs))
	var err error
	for i := range txs {
		result[i], err = UnmarshalTransctionFromJson(txs[i], false /* blobTxnsAreWrappedWithBlobs*/)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}
