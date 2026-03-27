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

// SetCodeHandler handles EIP-7702 set-code transactions (type 4).
//
// SetCode transactions differ from standard types in several ways:
//   - Require Prague fork activation
//   - Cannot be used for contract creation
//   - Must carry at least one authorization (non-empty authorization list)
//   - Track (authority, nonce) reservations in the pool to prevent conflicts
//     (OnAdd registers authorities; OnRemove releases them)
type SetCodeHandler struct{ DefaultHandler }

func (SetCodeHandler) TypeByte() byte { return types.SetCodeTxType }
func (SetCodeHandler) Name() string   { return "set-code" }

// ForkRequired returns true only when Prague is active.
func (SetCodeHandler) ForkRequired(forks ForkState) bool { return forks.IsPrague }

// CanCreate returns false — set-code transactions cannot deploy contracts.
func (SetCodeHandler) CanCreate() bool { return false }

// ValidateTx is a stub in Phase 1/3.
//
// The authorizations non-empty check uses TxnSlot.AuthAndNonces (recovered
// signers) rather than types.Transaction.GetAuthorizations() (raw structs),
// so it stays in pool.go until Phase 3b adds recovered-authority plumbing.
// Additional self-authorization nonce and duplicate authority checks also
// remain in addLocked where the metaTxn reference is available.
func (SetCodeHandler) ValidateTx(_ types.Transaction, _ bool, _ *txpoolcfg.Config) txpoolcfg.DiscardReason {
	return txpoolcfg.NotSet
}

// OnAdd and OnRemove are no-ops for SetCode transactions.
//
// Authority reservations require recovered signers (TxnSlot.AuthAndNonces),
// which are pool-internal and not accessible through the types.Transaction
// interface. Reservation management therefore stays explicit in addLocked and
// discardLocked, using TxPool.AuthReserve / TxPool.AuthRelease directly.
func (SetCodeHandler) OnAdd(_ types.Transaction, _ PoolMutation)    {}
func (SetCodeHandler) OnRemove(_ types.Transaction, _ PoolMutation) {}
