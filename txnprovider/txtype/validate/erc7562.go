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

package validate

import (
	"github.com/erigontech/erigon/execution/vm"
)

// erc7562CallOpcodes is the set of call variants that may legally follow a GAS
// opcode in an ERC-7562 validation frame.
// Ref: ERC-7562 §3 [OP-031] "GAS opcode is allowed only if followed by a *CALL"
var erc7562CallOpcodes = map[vm.OpCode]bool{
	vm.CALL:         true,
	vm.CALLCODE:     true,
	vm.DELEGATECALL: true,
	vm.STATICCALL:   true,
}

// erc7562BalanceRule rejects the BALANCE opcode.
//
// BALANCE probes the ETH balance of an arbitrary address.  In a validation
// frame this allows the validation result to depend on mutable external account
// state, undermining mempool consistency.
//
// Ref: ERC-7562 §3 [OP-011] "may not use BALANCE opcode"
func erc7562BalanceRule(insns []Insn, idx int) (Violation, bool) {
	if insns[idx].Opcode != vm.BALANCE {
		return Violation{}, false
	}
	return Violation{
		PC:   insns[idx].PC,
		Op:   vm.BALANCE,
		Kind: ViolBannedOpcode,
		Msg:  "BALANCE: reveals external account balance, non-deterministic (ERC-7562 [OP-011])",
	}, true
}

// erc7562GasRule rejects a GAS opcode that is not immediately followed by a
// *CALL instruction.
//
// GAS is used legitimately to forward "all remaining gas" to a sub-call.
// Using it for any other purpose (e.g. branching on the remaining gas) lets
// validation behaviour vary between nodes or across time, breaking mempool
// safety.  The rule permits GAS only when the very next decoded instruction is
// one of CALL / CALLCODE / DELEGATECALL / STATICCALL.
//
// Ref: ERC-7562 §3 [OP-031] "GAS opcode is allowed only if followed by a *CALL"
func erc7562GasRule(insns []Insn, idx int) (Violation, bool) {
	if insns[idx].Opcode != vm.GAS {
		return Violation{}, false
	}
	// GAS is the last instruction, or the next is not a call variant → violation.
	if idx+1 >= len(insns) || !erc7562CallOpcodes[insns[idx+1].Opcode] {
		return Violation{
			PC:   insns[idx].PC,
			Op:   vm.GAS,
			Kind: ViolGasNotFollowedByCall,
			Msg:  "GAS must be immediately followed by CALL/CALLCODE/DELEGATECALL/STATICCALL (ERC-7562 [OP-031])",
		}, true
	}
	return Violation{}, false
}

// ERC7562Rules is the complete static rule set for RIP-7560 (AA type 5)
// validation frames.  It extends BaseRules with the two ERC-7562-specific
// checks: the BALANCE ban and the GAS-before-CALL constraint.
//
// Rules that require execution context (CALL-with-value, storage slot
// restrictions, out-of-gas revert ban) are enforced at runtime by
// ValidationRulesTracer and are not repeated here.
var ERC7562Rules = BaseRules.Extend(erc7562BalanceRule, erc7562GasRule)
