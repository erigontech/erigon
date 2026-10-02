// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engineadapter

import (
	"errors"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

// ToEth1Block converts a payload to CL form without filling missing extra data or withdrawals.
// A non-nil beacon configuration is required.
func ToEth1Block(p *engine_types.ExecutionPayload, version clparams.StateVersion, beaconCfg *clparams.BeaconChainConfig) (*cltypes.Eth1Block, error) {
	if beaconCfg == nil {
		return nil, errors.New("beacon config is required")
	}
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
		block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(beaconCfg.MaxWithdrawalsPerPayload), 44)
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
		block.BlockAccessList = solid.NewByteListSSZ(beaconCfg.MaxBytesPerTransaction)
		if err := block.BlockAccessList.SetBytes(*p.BlockAccessList); err != nil {
			return nil, err
		}
	}

	return block, nil
}

func ExecutionPayloadFromSSZBlock(block *cltypes.Eth1Block, version clparams.StateVersion) *engine_types.ExecutionPayload {
	baseFee := new(uint256.Int)
	_ = baseFee.UnmarshalSSZ(block.BaseFeePerGas[:])
	body := block.Body()
	p := &engine_types.ExecutionPayload{
		ParentHash:    block.ParentHash,
		FeeRecipient:  block.FeeRecipient,
		StateRoot:     block.StateRoot,
		ReceiptsRoot:  block.ReceiptsRoot,
		LogsBloom:     block.LogsBloom[:],
		PrevRandao:    block.PrevRandao,
		BlockNumber:   hexutil.Uint64(block.BlockNumber),
		GasLimit:      hexutil.Uint64(block.GasLimit),
		GasUsed:       hexutil.Uint64(block.GasUsed),
		Timestamp:     hexutil.Uint64(block.Time),
		ExtraData:     block.Extra.Bytes(),
		BaseFeePerGas: (*hexutil.U256)(baseFee),
		BlockHash:     block.BlockHash,
		Transactions:  make([]hexutil.Bytes, 0, len(body.Transactions)),
		Withdrawals:   body.Withdrawals,
	}
	for _, tx := range body.Transactions {
		p.Transactions = append(p.Transactions, tx)
	}
	if version >= clparams.DenebVersion {
		bg, ebg := hexutil.Uint64(block.BlobGasUsed), hexutil.Uint64(block.ExcessBlobGas)
		p.BlobGasUsed, p.ExcessBlobGas = &bg, &ebg
	}
	if version >= clparams.GloasVersion {
		bal := hexutil.Bytes{}
		if block.BlockAccessList != nil {
			bal = block.BlockAccessList.Bytes()
		}
		p.BlockAccessList = &bal
		slot := hexutil.Uint64(block.SlotNumber)
		p.SlotNumber = &slot
	}
	return p
}
