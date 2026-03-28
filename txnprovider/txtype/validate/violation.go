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
	"fmt"

	"github.com/erigontech/erigon/execution/vm"
)

// ViolationKind classifies the type of static-analysis violation.
type ViolationKind uint8

const (
	// ViolBannedOpcode is set when bytecode contains an outright banned opcode.
	ViolBannedOpcode ViolationKind = iota

	// ViolGasNotFollowedByCall is set when a GAS opcode is not immediately
	// followed by a *CALL instruction.
	// Ref: ERC-7562 [OP-031]
	ViolGasNotFollowedByCall

	// ViolSstoreInVerify is set when a SSTORE appears in an EIP-8141 VERIFY
	// frame, which must be read-only.
	// Ref: EIP-8141 §4.1 (draft)
	ViolSstoreInVerify
)

// Violation describes a single static-analysis rule failure.
type Violation struct {
	PC   uint64
	Op   vm.OpCode
	Kind ViolationKind
	Msg  string
}

func (v Violation) Error() string {
	return fmt.Sprintf("bytecode violation at pc=%d op=%s: %s", v.PC, v.Op, v.Msg)
}
