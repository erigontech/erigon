// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engine_types

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

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

func (p *ExecutionPayload) ToEth1Block(version clparams.StateVersion, beaconCfg *clparams.BeaconChainConfig) (*cltypes.Eth1Block, error) {
	block := cltypes.NewEth1Block(version, beaconCfg)
	block.ParentHash = p.ParentHash
	block.FeeRecipient = p.FeeRecipient
	block.StateRoot = p.StateRoot
	block.ReceiptsRoot = p.ReceiptsRoot
	block.PrevRandao = p.PrevRandao
	block.BlockNumber = uint64(p.BlockNumber)
	block.GasLimit = uint64(p.GasLimit)
	block.GasUsed = uint64(p.GasUsed)
	block.Time = uint64(p.Timestamp)
	block.BlockHash = p.BlockHash

	if len(p.LogsBloom) == 256 {
		copy(block.LogsBloom[:], p.LogsBloom)
	}

	if p.ExtraData != nil {
		block.Extra = solid.NewExtraData()
		block.Extra.SetBytes(p.ExtraData)
	}

	if p.BaseFeePerGas != nil {
		_, _ = (*uint256.Int)(p.BaseFeePerGas).MarshalSSZAppend(block.BaseFeePerGas[:0])
	}

	if p.BlobGasUsed != nil {
		block.BlobGasUsed = uint64(*p.BlobGasUsed)
	}
	if p.ExcessBlobGas != nil {
		block.ExcessBlobGas = uint64(*p.ExcessBlobGas)
	}

	txBytes := make([][]byte, len(p.Transactions))
	for i, tx := range p.Transactions {
		txBytes[i] = tx
	}
	block.Transactions = solid.NewTransactionsSSZFromTransactions(txBytes)

	if p.Withdrawals != nil {
		maxWithdrawals := 16
		if beaconCfg != nil {
			maxWithdrawals = int(beaconCfg.MaxWithdrawalsPerPayload)
		}
		block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](maxWithdrawals, 44)
		for _, w := range p.Withdrawals {
			block.Withdrawals.Append(&cltypes.Withdrawal{
				Index:     uint64(w.Index),
				Validator: uint64(w.Validator),
				Address:   w.Address,
				Amount:    uint64(w.Amount),
			})
		}
	}

	if p.SlotNumber != nil {
		block.SlotNumber = uint64(*p.SlotNumber)
	}
	if p.BlockAccessList != nil && len(*p.BlockAccessList) > 0 {
		maxBytes := uint64(1073741824) // MAX_BYTES_PER_TRANSACTION default
		if beaconCfg != nil {
			maxBytes = beaconCfg.MaxBytesPerTransaction
		}
		block.BlockAccessList = solid.NewByteListSSZ(maxBytes)
		if err := block.BlockAccessList.SetBytes(*p.BlockAccessList); err != nil {
			return nil, err
		}
	}

	return block, nil
}
