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

// ValidateTx checks that the authorization list is non-empty.
// The pool enforces self-authorization nonce rules and duplicate authority
// checks separately before calling this handler.
func (SetCodeHandler) ValidateTx(txn types.Transaction, _ bool, _ *txpoolcfg.Config) txpoolcfg.DiscardReason {
	if len(txn.GetAuthorizations()) == 0 {
		return txpoolcfg.NoAuthorizations
	}
	return txpoolcfg.NotSet
}

// OnAdd and OnRemove manage (authority, nonce) reservations in the pool.
// The "authority" is the recovered signer of each Authorization, not
// Authorization.Address (which is the delegate target). Recovered authorities
// are computed by the pool and stored in TxnSlot.AuthAndNonces.
//
// Phase 3 wiring will extend the call site to pass recovered authorities here.
// Until then, these are no-ops — authority reservation logic stays in pool.go.
func (SetCodeHandler) OnAdd(_ types.Transaction, _ PoolMutation)    {}
func (SetCodeHandler) OnRemove(_ types.Transaction, _ PoolMutation) {}
