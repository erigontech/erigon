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

package txtype

import (
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
	"github.com/erigontech/erigon/txnprovider/txtype/validate"
)

// FrameHandler handles EIP-8141 frame transactions (type 6).
//
// Frame transactions contain an ordered sequence of execution frames:
//   - VERIFY frame: custom signature/authorization validation (uses APPROVE opcode)
//   - SENDER frame: determines the effective transaction sender
//   - DEFAULT frame: the actual user operation
//
// Construct with NewFrameHandler to enable bytecode validation.  The zero value
// (FrameHandler{}) has a nil reader and skips validation — safe for contexts
// without state access (tests, the global registry).
//
// NOTE: EIP-8141 is still in draft status. Rule sets and frame semantics will
// be updated as the spec evolves.
type FrameHandler struct {
	DefaultHandler
	reader validate.CodeReader
}

// NewFrameHandler creates a FrameHandler with the given CodeReader for static
// EIP-8141 bytecode validation of VERIFY frames at pool admission.
func NewFrameHandler(reader validate.CodeReader) FrameHandler {
	return FrameHandler{reader: reader}
}

func (FrameHandler) TypeByte() byte { return types.FrameTxType }
func (FrameHandler) Name() string   { return "frame" }

// ForkRequired returns true only when frame transactions are explicitly enabled.
func (FrameHandler) ForkRequired(forks ForkState) bool { return forks.AllowFrameTx }

// CanCreate returns false — frame transactions cannot deploy contracts directly.
func (FrameHandler) CanCreate() bool { return false }

// ValidateTx applies static EIP-8141 §4.1 bytecode rules to every VERIFY frame.
//
// VERIFY frames must be read-only (no SSTORE) and must not use banned
// environment opcodes (inherited from BaseRules).  SENDER and DEFAULT frames
// execute under normal EVM rules and are not checked here.
//
// If no CodeReader was provided (nil reader), or if a code read fails,
// validation is skipped (permissive — EIP-8141 is still a draft spec).
func (h FrameHandler) ValidateTx(tx types.Transaction, _ bool, _ *txpoolcfg.Config) txpoolcfg.DiscardReason {
	if h.reader == nil {
		return txpoolcfg.NotSet
	}
	frameTx, ok := tx.(*types.FrameTransaction)
	if !ok {
		return txpoolcfg.NotSet
	}
	for _, frame := range frameTx.Frames {
		if frame.Kind != types.FrameKindVerify {
			continue
		}
		code, err := h.reader.Code(frame.To)
		if err != nil {
			// Permissive on read error: spec is still a draft and the contract
			// may legitimately not exist yet (SENDER frame deploys it).
			continue
		}
		if v := validate.EIP8141VerifyRules.Validate(code); v != nil {
			return txpoolcfg.InvalidAA // reuse until FrameTx has its own discard reason
		}
	}
	return txpoolcfg.NotSet
}
