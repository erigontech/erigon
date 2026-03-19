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

	// keccak256('Burn(address,uint256)')
	EthBurnLogEvent = common.HexToHash("0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5")
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
	// Emit burn logs for selfdestructed accounts that hold a positive balance at
	// finalization time (EIP-7708 case 2: funded after selfdestruct).
	//
	// Two sources of residual balance are unioned:
	//
	//  1. result.SelfDestructedWithBalance — execution-time residuals captured
	//     before SoftFinalise cleared the journal. Needed because the parallel
	//     executor's finalization IBS may not see these accounts when their
	//     balance writes are stripped from VersionedWrites.
	//
	//  2. ibs.GetRemovedAccountsWithBalance() — the finalized balance of
	//     selfdestructed accounts in the current IBS. This is the most up-to-date
	//     value: it includes the execution-time residual plus any funds added
	//     during finalization (e.g. priority fee credited to a coinbase that
	//     selfdestructed). When present, it supersedes source 1.
	combined := make(map[common.Address]uint256.Int)
	if result != nil {
		for _, ab := range result.SelfDestructedWithBalance {
			combined[ab.Address] = ab.Balance
		}
	}
	finalizeList := ibs.GetRemovedAccountsWithBalance()
	for _, ab := range finalizeList {
		// Always prefer the finalized balance — it already encompasses the
		// execution-time residual and any additions during finalization.
		combined[ab.Address] = ab.Balance
	}
	if len(combined) == 0 {
		return
	}
	addrs := make([]common.Address, 0, len(combined))
	for addr := range combined {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i].Cmp(addrs[j]) < 0
	})
	for _, addr := range addrs {
		bal := combined[addr]
		ibs.AddLog(EthBurnLog(addr, bal))
	}
}
