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

package protocol

import (
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/abi"
)

// MainnetRollupID is the rollup id of L1 in call hashes and entries.
const MainnetRollupID uint64 = 0

var callHashArgs = abi.Arguments{
	{Type: mustType("bool", nil)},
	{Type: addressType},
	{Type: uint64Type},
	{Type: addressType},
	{Type: uint64Type},
	{Type: uint256Type},
	{Type: uint64Type},
	{Type: bytesType},
}

// CallHashInput holds the arguments of EEZBase.computeCrossChainCallHash.
// A chain executing a call hashes it with its own rollup id as target and
// zero call gas; a proxy call hashes the destination pair and the gas policy
// of the source chain's manager.
type CallHashInput struct {
	IsStatic       bool
	SourceAddress  common.Address
	SourceRollupID uint64
	TargetAddress  common.Address
	TargetRollupID uint64
	Value          *big.Int // uint256
	CallGas        uint64
	Data           []byte
}

// CrossChainCallHash returns keccak256(abi.encode(isStatic, sourceAddress,
// sourceRollupId, targetAddress, targetRollupId, value, callGas, data)).
func CrossChainCallHash(in *CallHashInput) (common.Hash, error) {
	if err := checkUint256(in.Value); err != nil {
		return common.Hash{}, fmt.Errorf("call hash value: %w", err)
	}
	enc, err := callHashArgs.Pack(in.IsStatic, in.SourceAddress, in.SourceRollupID, in.TargetAddress,
		in.TargetRollupID, in.Value, in.CallGas, in.Data)
	if err != nil {
		return common.Hash{}, fmt.Errorf("call hash: %w", err)
	}
	return crypto.Keccak256Hash(enc), nil
}
