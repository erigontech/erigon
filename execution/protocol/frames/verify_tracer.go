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

package frames

import (
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/vm"
)

// VerifyFrameTracer observes APPROVE opcode execution during EIP-8141 VERIFY
// frames.
//
// EIP-8141 §4.2 (draft): a VERIFY frame MUST execute the APPROVE opcode to
// signal that it authorises the enclosing frame transaction.  If the frame
// completes without executing APPROVE (or reverts), the transaction is
// rejected.
//
// Call Hooks() to obtain a *tracing.Hooks to install on the EVM.
// Call Reset() between VERIFY frames when reusing the tracer.
type VerifyFrameTracer struct {
	Approved bool
}

// Hooks returns a *tracing.Hooks with an OnOpcode hook that sets Approved when
// APPROVE (0xF8) is executed.
func (t *VerifyFrameTracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnOpcode: func(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
			if vm.OpCode(op) == vm.APPROVE {
				t.Approved = true
			}
		},
	}
}

// Reset clears Approved so the tracer can be reused for a subsequent VERIFY frame.
func (t *VerifyFrameTracer) Reset() {
	t.Approved = false
}
