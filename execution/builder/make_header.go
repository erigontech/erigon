// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package builder

import (
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/misc"
	"github.com/erigontech/erigon/execution/types"
)

func MakeEmptyHeader(parent *types.Header, chainConfig *chain.Config, timestamp uint64, targetGasLimit *uint64) *types.Header {
	header := types.NewEmptyHeaderForAssembling()
	header.Root = parent.Root
	header.ParentHash = parent.Hash()
	header.Number = new(big.Int).Add(parent.Number, common.Big1)
	header.Difficulty = common.Big0
	header.Time = timestamp

	parentGasLimit := parent.GasLimit
	// Set baseFee and GasLimit if we are on an EIP-1559 chain
	if chainConfig.IsLondon(header.Number.Uint64()) {
		header.BaseFee = misc.CalcBaseFee(chainConfig, parent)
		if !chainConfig.IsLondon(parent.Number.Uint64()) {
			parentGasLimit = parent.GasLimit * params.ElasticityMultiplier
		}
	}
	if targetGasLimit != nil {
		header.GasLimit = misc.CalcGasLimit(parentGasLimit, *targetGasLimit)
	} else {
		header.GasLimit = parentGasLimit
	}

	if chainConfig.IsCancun(header.Time) {
		excessBlobGas := misc.CalcExcessBlobGas(chainConfig, parent, header.Time)
		header.ExcessBlobGas = &excessBlobGas
		header.BlobGasUsed = new(uint64)
	}

	return header
}
