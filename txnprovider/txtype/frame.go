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
)

// FrameHandler handles EIP-8141 frame transactions (type 6).
//
// Frame transactions contain an ordered sequence of execution frames:
//   - VERIFY frame: custom signature/authorization validation (uses APPROVE opcode)
//   - SENDER frame: determines the effective transaction sender
//   - DEFAULT frame: the actual user operation
//
// NOTE: EIP-8141 is still in draft status. This handler provides the registry
// registration and fork-gating scaffolding. Full validation, parsing, and
// execution will be completed when the spec is finalized (Phase 5 completion).
type FrameHandler struct{ DefaultHandler }

func (FrameHandler) TypeByte() byte { return types.FrameTxType }
func (FrameHandler) Name() string   { return "frame" }

// ForkRequired returns true only when frame transactions are explicitly enabled.
// Frame transactions are gated by the AllowFrameTx flag (set when the EIP-8141
// hardfork is active) rather than a named fork, since the opcode number and
// activation block are not yet assigned.
func (FrameHandler) ForkRequired(forks ForkState) bool { return forks.AllowFrameTx }

// CanCreate returns false — frame transactions cannot deploy contracts.
// (Contract deployment happens implicitly via SENDER frame logic.)
func (FrameHandler) CanCreate() bool { return false }

// ValidateTx is a stub — full EIP-8141 frame sequence validation (well-formedness,
// VERIFY/SENDER/DEFAULT ordering, opcode restrictions) will be added when the
// spec is finalized.
func (FrameHandler) ValidateTx(_ types.Transaction, _ bool, _ *txpoolcfg.Config) txpoolcfg.DiscardReason {
	return txpoolcfg.NotSet
}
