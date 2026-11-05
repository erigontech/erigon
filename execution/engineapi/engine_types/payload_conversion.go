// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engine_types

import (
	"fmt"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

// ExecutionPayloadFromBlock converts a built block and its committed BAL sidecar to an Engine API payload.
func ExecutionPayloadFromBlock(block *types.Block) (*ExecutionPayload, error) {
	header := block.Header()

	encodedTxs, err := types.MarshalTransactionsBinary(block.Transactions())
	if err != nil {
		return nil, err
	}
	txs := make([]hexutil.Bytes, len(encodedTxs))
	for i, tx := range encodedTxs {
		txs[i] = tx
	}

	bloom := header.Bloom
	ep := &ExecutionPayload{
		ParentHash:    header.ParentHash,
		FeeRecipient:  header.Coinbase,
		StateRoot:     header.Root,
		ReceiptsRoot:  header.ReceiptHash,
		LogsBloom:     bloom[:],
		PrevRandao:    header.MixDigest,
		BlockNumber:   hexutil.Uint64(header.Number.Uint64()),
		GasLimit:      hexutil.Uint64(header.GasLimit),
		GasUsed:       hexutil.Uint64(header.GasUsed),
		Timestamp:     hexutil.Uint64(header.Time),
		ExtraData:     header.Extra,
		BaseFeePerGas: (*hexutil.U256)(header.BaseFee),
		BlockHash:     block.Hash(),
		Transactions:  txs,
	}
	if block.Withdrawals() != nil {
		ep.Withdrawals = block.Withdrawals()
	}
	if header.BlobGasUsed != nil {
		bgu := hexutil.Uint64(*header.BlobGasUsed)
		ep.BlobGasUsed = &bgu
	}
	if header.ExcessBlobGas != nil {
		ebg := hexutil.Uint64(*header.ExcessBlobGas)
		ep.ExcessBlobGas = &ebg
	}
	if header.SlotNumber != nil {
		sn := hexutil.Uint64(*header.SlotNumber)
		ep.SlotNumber = &sn
	}
	if header.BlockAccessListHash != nil && block.BlockAccessListSidecar() != nil {
		encoded, err := block.BlockAccessListSidecar().Bytes()
		if err != nil {
			return nil, fmt.Errorf("encode block access list: %w", err)
		}
		bal := hexutil.Bytes(encoded)
		ep.BlockAccessList = &bal
	}

	return ep, nil
}
