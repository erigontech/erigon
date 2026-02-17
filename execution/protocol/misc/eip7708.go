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
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

var (
	// keccak256('Transfer(address,address,uint256)')
	EthTransferLogEvent = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	// TODO: This is the latest spec event, but as of 5.1.0 the bal hive tests still use the legacy event naming
	// this should be updated once the hive tests are upto the latest spec 
	// keccak256('Burn(address,uint256)')
	//EthBurnLogEvent = common.HexToHash("0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5")
	
	// keccak256('Selfdestruct(address,uint256)')
	EthBurnLogEvent = common.HexToHash("0x4bfaba3443c1a1836cd362418edc679fc96cae8449cbefccb6457cdf2c943083")
)

// EthTransferLog creates and ETH transfer log according to EIP-7708.
// Specification: https://eips.ethereum.org/EIPS/eip-7708
func EthTransferLog(from, to common.Address, amount uint256.Int) *types.Log {
	amount32 := amount.Bytes32()
	return &types.Log{
		Address: params.SystemAddress.Value(),
		Topics: []common.Hash{
			EthTransferLogEvent,
			from.Hash(),
			to.Hash(),
		},
		Data: amount32[:],
	}
}

// EthBurnLog creates an ETH burn log according to EIP-7708.
// Specification: https://eips.ethereum.org/EIPS/eip-7708
func EthBurnLog(from common.Address, amount uint256.Int) *types.Log {
	amount32 := amount.Bytes32()
	return &types.Log{
		Address: params.SystemAddress.Value(),
		Topics: []common.Hash{
			EthBurnLogEvent,
			from.Hash(),
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
	if rules.IsAmsterdam && !amount.IsZero() && sender != recipient { // EIP-7708
		db.AddLog(EthTransferLog(sender.Value(), recipient.Value(), amount))
	}
	return nil
}

func LogSelfDestructedAccounts(ibs evmtypes.IntraBlockState, sender accounts.Address, coinbase accounts.Address, result *evmtypes.ExecutionResult, rules *chain.Rules) {
	if !rules.IsAmsterdam {
		return
	}
	// Emit burn logs where accounts with non-empty balances have been deleted.
	// See case (2) in https://eips.ethereum.org/EIPS/eip-7708#selfdestruct-processing
	//
	// Prefer the pre-computed list from execution time: in the parallel executor the
	// IBS passed here is reconstructed from VersionedWrites and its journal is empty,
	// so ibs.GetRemovedAccountsWithBalance() would return nothing.
	// result.SelfDestructedWithBalance was captured before SoftFinalise cleared the
	// journal in the original execution IBS.
	var removedWithBalance []evmtypes.AddressAndBalance
	if result != nil && result.SelfDestructedWithBalance != nil {
		removedWithBalance = result.SelfDestructedWithBalance
	} else {
		removedWithBalance = ibs.GetRemovedAccountsWithBalance()
	}
	if removedWithBalance != nil {
		sort.Slice(removedWithBalance, func(i, j int) bool {
			return removedWithBalance[i].Address.Cmp(removedWithBalance[j].Address) < 0
		})
		for _, sd := range removedWithBalance {
			ibs.AddLog(EthBurnLog(sd.Address, sd.Balance))
		}
	}
}
