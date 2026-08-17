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

package ethapi

import (
	"errors"
	"maps"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// BlockHashOverrides maps block numbers to their hash overrides,
// used to intercept BLOCKHASH opcode calls during simulation.
type BlockHashOverrides map[uint64]common.Hash

// BlockOverrides is the unified set of block-level fields that can be
// overridden during simulation or call execution (eth_call, eth_estimateGas,
// eth_simulateV1, eth_callMany).
type BlockOverrides struct {
	Number        *hexutil.U256           `json:"number"`
	Difficulty    *hexutil.U256           `json:"difficulty"`
	Time          *hexutil.Uint64         `json:"time"`
	GasLimit      *hexutil.Uint64         `json:"gasLimit"`
	FeeRecipient  *common.Address         `json:"feeRecipient"`
	PrevRandao    *common.Hash            `json:"prevRandao"`
	BaseFeePerGas *hexutil.U256           `json:"baseFeePerGas"`
	BlobBaseFee   *hexutil.U256           `json:"blobBaseFee"`
	BeaconRoot    *common.Hash            `json:"beaconRoot"`
	BlockHash     *map[uint64]common.Hash `json:"blockHash"`
	Withdrawals   *types.Withdrawals      `json:"withdrawals"`
}

// Override applies overrides to an EVM block context used in single-call
// methods (eth_call, eth_estimateGas). BeaconRoot and Withdrawals are
// rejected because they are not meaningful in a single-call context.
func (overrides *BlockOverrides) Override(context *evmtypes.BlockContext) error {
	if overrides == nil {
		return nil
	}
	if overrides.BeaconRoot != nil {
		return errors.New(`block override "beaconRoot" is not supported for this RPC method`)
	}
	if overrides.Withdrawals != nil {
		return errors.New(`block override "withdrawals" is not supported for this RPC method`)
	}
	if overrides.BlockHash != nil {
		return errors.New(`block override "blockHash" is not supported for this RPC method`)
	}
	if overrides.Number != nil {
		context.BlockNumber = overrides.Number.Uint64()
	}
	if overrides.Difficulty != nil {
		context.Difficulty.Set((*uint256.Int)(overrides.Difficulty))
	}
	if overrides.PrevRandao != nil {
		context.PrevRanDao = overrides.PrevRandao
	}
	if overrides.Time != nil {
		context.Time = uint64(*overrides.Time)
	}
	if overrides.GasLimit != nil {
		context.GasLimit = uint64(*overrides.GasLimit)
		context.MaxGasLimit = false
	}
	if overrides.FeeRecipient != nil {
		context.Coinbase = accounts.InternAddress(*overrides.FeeRecipient)
	}
	if overrides.BaseFeePerGas != nil {
		context.BaseFee.Set((*uint256.Int)(overrides.BaseFeePerGas))
	}
	if overrides.BlobBaseFee != nil {
		context.BlobBaseFee.Set((*uint256.Int)(overrides.BlobBaseFee))
	}
	return nil
}

// OverrideBaseFee returns baseFee with BaseFeePerGas applied if set, applying it
// even when baseFee is nil (pre-London block). It exists separately from Override
// because some callers need the overridden base fee before a BlockContext exists.
func (overrides *BlockOverrides) OverrideBaseFee(baseFee *uint256.Int) *uint256.Int {
	if overrides == nil || overrides.BaseFeePerGas == nil {
		return baseFee
	}
	return new(uint256.Int).Set((*uint256.Int)(overrides.BaseFeePerGas))
}

// OverrideHeader returns a modified copy of header with the overridden fields
// applied. Used by eth_simulateV1 to build block headers before execution.
// BeaconRoot and Withdrawals are handled separately by the caller at the
// block-assembly level.
func (overrides *BlockOverrides) OverrideHeader(header *types.Header) *types.Header {
	if overrides == nil {
		return header
	}
	h := types.CopyHeader(header)
	if overrides.Number != nil {
		h.Number.Set((*uint256.Int)(overrides.Number))
	}
	if overrides.Difficulty != nil {
		h.Difficulty.Set((*uint256.Int)(overrides.Difficulty))
	}
	if overrides.Time != nil {
		h.Time = uint64(*overrides.Time)
	}
	if overrides.GasLimit != nil {
		h.GasLimit = uint64(*overrides.GasLimit)
	}
	if overrides.FeeRecipient != nil {
		h.Coinbase = *overrides.FeeRecipient
	}
	if overrides.BaseFeePerGas != nil {
		baseFee := new(uint256.Int)
		baseFee.Set((*uint256.Int)(overrides.BaseFeePerGas))
		h.BaseFee = baseFee
	}
	if overrides.PrevRandao != nil {
		h.MixDigest = *overrides.PrevRandao
	}
	return h
}

// OverrideBlockContext applies overrides to an EVM block context used in
// simulation (eth_simulateV1, eth_callMany). Unlike Override, it does not
// reject BeaconRoot or Withdrawals — those are handled at the block-assembly
// level by the caller.
func (overrides *BlockOverrides) OverrideBlockContext(blockCtx *evmtypes.BlockContext, blockHashOverrides BlockHashOverrides) {
	if overrides == nil {
		return
	}
	if overrides.Number != nil {
		blockCtx.BlockNumber = overrides.Number.Uint64()
	}
	if overrides.Difficulty != nil {
		blockCtx.Difficulty.Set((*uint256.Int)(overrides.Difficulty))
	}
	if overrides.Time != nil {
		blockCtx.Time = uint64(*overrides.Time)
	}
	if overrides.GasLimit != nil {
		blockCtx.GasLimit = uint64(*overrides.GasLimit)
	}
	if overrides.FeeRecipient != nil {
		blockCtx.Coinbase = accounts.InternAddress(*overrides.FeeRecipient)
	}
	if overrides.PrevRandao != nil {
		blockCtx.PrevRanDao = overrides.PrevRandao
	}
	if overrides.BaseFeePerGas != nil {
		blockCtx.BaseFee.Set((*uint256.Int)(overrides.BaseFeePerGas))
	}
	if overrides.BlobBaseFee != nil {
		blockCtx.BlobBaseFee.Set((*uint256.Int)(overrides.BlobBaseFee))
	}
	if overrides.BlockHash != nil {
		maps.Copy(blockHashOverrides, *overrides.BlockHash)
	}
}
