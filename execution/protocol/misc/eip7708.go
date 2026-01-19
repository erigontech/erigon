// Copyright 2026 The go-ethereum Authors
// (original work)
// Copyright 2026 The Erigon Authors
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

package misc

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// EthTransferLog creates and ETH transfer log according to EIP-7708.
// Specification: https://eips.ethereum.org/EIPS/eip-7708
func EthTransferLog(from, to accounts.Address, amount uint256.Int) *types.Log {
	amount32 := amount.Bytes32()
	return &types.Log{
		Address: params.SystemAddress.Value(),
		Topics: []common.Hash{
			params.EthTransferLogEvent,
			from.Value().Hash(),
			to.Value().Hash(),
		},
		Data: amount32[:],
	}
}

// EthSelfDestructLog creates and ETH self-destruct burn log according to EIP-7708.
// Specification: https://eips.ethereum.org/EIPS/eip-7708
func EthSelfDestructLog(from accounts.Address, amount uint256.Int) *types.Log {
	amount32 := amount.Bytes32()
	return &types.Log{
		Address: params.SystemAddress.Value(),
		Topics: []common.Hash{
			params.EthSelfDestructLogEvent,
			from.Value().Hash(),
		},
		Data: amount32[:],
	}
}

// Transfer subtracts amount from sender and adds amount to recipient using the given Db
func Transfer(db evmtypes.IntraBlockState, sender, recipient accounts.Address, amount uint256.Int, bailout bool, rules *chain.Rules) error {
	if !bailout {
		err := db.SubBalance(sender, amount, tracing.BalanceChangeTransfer)
		if err != nil {
			return err
		}
	}
	err := db.AddBalance(recipient, amount, tracing.BalanceChangeTransfer)
	if err != nil {
		return err
	}
	if rules.IsAmsterdam && !amount.IsZero() {
		db.AddLog(EthTransferLog(sender, recipient, amount))
	}
	return nil
}
