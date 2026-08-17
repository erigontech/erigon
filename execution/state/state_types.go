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

package state

import (
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

type (
	CanTransferFunc func(*IntraBlockState, common.Address, *uint256.Int) bool

	TransferFunc func(*IntraBlockState, common.Address, common.Address, *uint256.Int, bool)

	GetHashFunc func(uint64) common.Hash
)

type BlockContext struct {
	CanTransfer CanTransferFunc
	Transfer    TransferFunc
	GetHash     GetHashFunc

	Coinbase      common.Address
	GasLimit      uint64
	MaxGasLimit   bool // overrides GasLimit to 2^256-1, for compatibility with OpenEthereum's trace_call
	BlockNumber   uint64
	Time          uint64
	Difficulty    *big.Int
	BaseFee       *uint256.Int
	PrevRanDao    *common.Hash
	ExcessBlobGas *uint64
}

type TxContext struct {
	TxHash     common.Hash
	Origin     common.Address
	GasPrice   *uint256.Int
	BlobHashes []common.Hash
}
