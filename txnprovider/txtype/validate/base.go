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

// baseEnvironmentOpcodes lists opcodes that expose mutable block/environment
// state and are banned in all validation contexts (both ERC-7562 and EIP-8141).
//
// Using any of these in a validation frame would make the validation result
// non-deterministic across nodes or across time, breaking mempool safety.
//
// Sources:
//   - ERC-7562 §3 "Opcode Rules" — OP-011 through OP-014
//   - EIP-8141 §4.1 "VERIFY frame restrictions" (draft)
var baseEnvironmentOpcodes = map[vm.OpCode]string{
	vm.ORIGIN:       "ORIGIN: reveals transaction origin, non-deterministic across callers",
	vm.GASPRICE:     "GASPRICE: reveals gas price, varies per transaction",
	vm.BLOCKHASH:    "BLOCKHASH: reveals block hash, varies per block",
	vm.COINBASE:     "COINBASE: reveals block producer address, varies per block",
	vm.TIMESTAMP:    "TIMESTAMP: reveals block timestamp, varies per block",
	vm.NUMBER:       "NUMBER: reveals block number, varies per block",
	vm.DIFFICULTY:   "DIFFICULTY/PREVRANDAO: reveals randomness beacon, varies per block",
	vm.GASLIMIT:     "GASLIMIT: reveals block gas limit, varies per block",
	vm.BASEFEE:      "BASEFEE: reveals base fee, varies per block",
	vm.BLOBHASH:     "BLOBHASH: reveals blob versioned hash, varies per transaction",
	vm.BLOBBASEFEE:  "BLOBBASEFEE: reveals blob base fee, varies per block",
	vm.INVALID:      "INVALID: always reverts, disallowed in validation",
	vm.SELFDESTRUCT: "SELFDESTRUCT: destroys account, disallowed in validation",
	vm.SELFBALANCE:  "SELFBALANCE: reveals contract balance, non-deterministic",
}

// baseBannedOpcodeRule rejects any opcode in baseEnvironmentOpcodes.
func baseBannedOpcodeRule(insns []Insn, idx int) (Violation, bool) {
	op := insns[idx].Opcode
	if msg, banned := baseEnvironmentOpcodes[op]; banned {
		return Violation{
			PC:   insns[idx].PC,
			Op:   op,
			Kind: ViolBannedOpcode,
			Msg:  msg,
		}, true
	}
	return Violation{}, false
}

// BaseRules is the minimal rule set shared by all validation frame types.
// Both ERC7562Rules and EIP8141VerifyRules extend this.
var BaseRules = RuleSet{baseBannedOpcodeRule}
