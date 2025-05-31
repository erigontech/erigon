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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/core/vm/evmtypes"
)

type BlockOverrides struct {
	Number        *hexutil.Big    `json:"number"`
	PrevRanDao    *common.Hash    `json:"prevRandao"`
	Time          *hexutil.Uint64 `json:"time"`
	GasLimit      *hexutil.Uint64 `json:"gasLimit"`
	FeeRecipient  *common.Address `json:"feeRecipient"`
	BaseFeePerGas *hexutil.Big    `json:"baseFee"`
	BlobBaseFee   *hexutil.Big    `json:"blobBaseFee"`
}

func (overrides *BlockOverrides) Override(context evmtypes.BlockContext) error {

	if overrides.Number != nil {
		context.BlockNumber = overrides.Number.Uint64()
	}

	if overrides.PrevRanDao != nil {
		context.PrevRanDao = overrides.PrevRanDao
	}

	if overrides.Time != nil {
		context.Time = overrides.Time.Uint64()
	}

	if overrides.GasLimit != nil {
		context.Time = overrides.GasLimit.Uint64()
	}

	if overrides.FeeRecipient != nil {
		context.Coinbase = common.Address(overrides.FeeRecipient.Bytes())
	}

	if overrides.BaseFeePerGas != nil {
		overflow := context.BaseFee.SetFromBig(overrides.BaseFeePerGas.ToInt())
		if overflow {
			return errors.New("BlockOverrides.BaseFee uint256 overflow")
		}
	}

	if overrides.BlobBaseFee != nil {
		overflow := context.BlobBaseFee.SetFromBig(overrides.BlobBaseFee.ToInt())
		if overflow {
			return errors.New("BlockOverrides.BlobBaseFee uint256 overflow")
		}
	}
	return nil
}
