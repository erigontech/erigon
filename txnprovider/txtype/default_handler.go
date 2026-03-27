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

// DefaultHandler provides sensible defaults for the Validator and PoolPolicy
// interfaces. Concrete handlers embed DefaultHandler and override only what
// their type needs.
//
// Default behavior matches legacy/access-list/dynamic-fee transactions
// (types 0–2): always active, can create, standard price bumps, no special
// pool state tracking.
type DefaultHandler struct{}

// --- Validator defaults ---

// ForkRequired returns true — legacy types are always active.
func (DefaultHandler) ForkRequired(_ ForkState) bool { return true }

// ValidateTx returns NotSet (success) — no type-specific checks beyond the
// generic ones already performed by the pool.
func (DefaultHandler) ValidateTx(_ types.Transaction, _ bool, _ *txpoolcfg.Config) txpoolcfg.DiscardReason {
	return txpoolcfg.NotSet
}

// CanCreate returns true — standard types allow contract deployment.
func (DefaultHandler) CanCreate() bool { return true }

// IntrinsicGasFlags returns the zero value — no special flags for standard types.
func (DefaultHandler) IntrinsicGasFlags() IntrinsicGasFlags { return IntrinsicGasFlags{} }

// --- PoolPolicy defaults ---

// PriceBumpPercent returns cfg.PriceBump (default 10%).
func (DefaultHandler) PriceBumpPercent(cfg *txpoolcfg.Config) uint64 { return cfg.PriceBump }

// CanReplace returns true for standard types replacing other standard types.
// Non-blob transactions cannot replace blob transactions (the blob tx replacement
// rule is asymmetric: blobs can only be replaced by blobs).
func (DefaultHandler) CanReplace(existingType byte) bool {
	return existingType != types.BlobTxType
}

// AccountLimit returns cfg.AccountSlots (default 16).
func (DefaultHandler) AccountLimit(cfg *txpoolcfg.Config) uint64 { return cfg.AccountSlots }

// EvictOnNonceGap returns false — standard types tolerate nonce gaps up to
// cfg.MaxNonceGap.
func (DefaultHandler) EvictOnNonceGap() bool { return false }

// PromotionCheck returns true — no extra fee conditions for standard types.
func (DefaultHandler) PromotionCheck(_, _ uint64) bool { return true }

// OnAdd is a no-op — standard types have no extra pool state to update.
func (DefaultHandler) OnAdd(_ types.Transaction, _ PoolMutation) {}

// OnRemove is a no-op — standard types have no extra pool state to undo.
func (DefaultHandler) OnRemove(_ types.Transaction, _ PoolMutation) {}
