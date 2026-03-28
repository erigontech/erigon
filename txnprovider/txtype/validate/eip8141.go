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

// eip8141SstoreRule rejects the SSTORE opcode in an EIP-8141 VERIFY frame.
//
// VERIFY frames are responsible solely for validating the transaction's
// authorisation (signature or custom logic).  They must not modify persistent
// state; any SSTORE in a VERIFY frame indicates a mis-designed contract that
// would alter world state as a side-effect of signature checking.
//
// NOTE: EIP-8141 is still in draft.  This rule tracks §4.1 of the current
// draft text.  Only VERIFY frames (FrameKindVerify) are subject to this rule;
// SENDER and DEFAULT frames execute under normal EVM rules.
//
// Ref: EIP-8141 §4.1 "VERIFY frame restrictions" (draft)
func eip8141SstoreRule(insns []Insn, idx int) (Violation, bool) {
	if insns[idx].Opcode != vm.SSTORE {
		return Violation{}, false
	}
	return Violation{
		PC:   insns[idx].PC,
		Op:   vm.SSTORE,
		Kind: ViolSstoreInVerify,
		Msg:  "SSTORE is not permitted in a VERIFY frame — VERIFY frames must be read-only (EIP-8141 §4.1 draft)",
	}, true
}

// EIP8141VerifyRules is the static rule set for EIP-8141 (Frame type 6) VERIFY
// frames.  It extends BaseRules with the SSTORE prohibition.
//
// SENDER and DEFAULT frames are not subject to these restrictions and should
// not use this rule set.
//
// NOTE: EIP-8141 is still in draft; this rule set will be updated as the spec
// evolves.
var EIP8141VerifyRules = BaseRules.Extend(eip8141SstoreRule)
