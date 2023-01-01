package cltypes

import (
	"math/big"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core/types"
)

func (e *ExecutionPayload) Header() *types.Header {
	var baseFee *big.Int

	if e.BaseFeePerGas != nil {
		// Trim and reverse it.
		baseFeeBytes := common.CopyBytes(e.BaseFeePerGas)
		for baseFeeBytes[len(baseFeeBytes)-1] == 0 && len(baseFeeBytes) > 0 {
			baseFeeBytes = baseFeeBytes[:len(baseFeeBytes)-1]
		}
		for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
			baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
		}
		var overflow bool
		baseFee = new(big.Int).SetBytes(baseFeeBytes)
		if overflow {
			panic("NewPayload BaseFeePerGas overflow")
		}
	}
	return &types.Header{
		ParentHash:  e.ParentHash,
		UncleHash:   types.EmptyUncleHash,
		Coinbase:    e.FeeRecipient,
		Root:        e.StateRoot,
		ReceiptHash: e.ReceiptsRoot,
		Bloom:       types.BytesToBloom(e.LogsBloom),
		Nonce:       serenity.SerenityNonce,
		Difficulty:  serenity.SerenityDifficulty,
		Number:      big.NewInt(int64(e.BlockNumber)),
		GasLimit:    e.GasLimit,
		GasUsed:     e.GasUsed,
		Time:        e.Timestamp,
		Extra:       e.ExtraData,
		MixDigest:   e.PrevRandao,
		BaseFee:     baseFee,
		TxHash:      types.DeriveSha(types.BinaryTransactions(e.Transactions)),
	}
}

func (e *ExecutionPayload) BlockBody() *types.RawBody {
	return &types.RawBody{
		Transactions: e.Transactions,
	}
}
