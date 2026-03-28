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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// ForkState captures which EIPs/forks are active at the point of validation.
// Populated from the pool's fork-checker methods.
type ForkState struct {
	IsShanghai   bool // EIP-3860 init code size
	IsCancun     bool // EIP-4844 blobs
	IsPrague     bool // EIP-7702 set-code
	IsAmsterdam  bool // EIP-7825 gas cap, EIP-8037
	AllowFrameTx bool // EIP-8141 frame transactions
}

// IntrinsicGasFlags carries type-specific flags for intrinsic gas calculation.
type IntrinsicGasFlags struct{}

// PoolMutation is a minimal interface the pool exposes to handlers for
// type-specific state updates when transactions are added or removed.
// The pool passes a live implementation; handlers call only what they need.
type PoolMutation interface {
	// BlobAdded increments the pool-wide blob count by n.
	BlobAdded(n int)
	// BlobRemoved decrements the pool-wide blob count by n.
	BlobRemoved(n int)
	// AuthReserve marks the (authority, nonce) pair as reserved by a SetCode tx.
	AuthReserve(authority common.Address, nonce uint64)
	// AuthRelease removes a (authority, nonce) reservation.
	AuthRelease(authority common.Address, nonce uint64)
}

// Validator defines how a transaction type is validated in the mempool.
type Validator interface {
	// ForkRequired returns true when this type is active under the given forks.
	// Returns false if the required fork is not yet active; the pool discards
	// the tx with TypeNotActivated.
	ForkRequired(forks ForkState) bool

	// ValidateTx performs type-specific validation beyond the generic checks
	// (fee cap, nonce, gas limit, balance). Returns NotSet on success.
	// Called after generic validation; receives the decoded transaction.
	ValidateTx(txn types.Transaction, isLocal bool, cfg *txpoolcfg.Config) txpoolcfg.DiscardReason

	// CanCreate returns whether this type allows contract creation (zero To address).
	// Blob (type 3) and SetCode (type 4) prohibit creation.
	CanCreate() bool

	// IntrinsicGasFlags returns type-specific flags for intrinsic gas calculation.
	IntrinsicGasFlags() IntrinsicGasFlags
}

// PoolPolicy defines how a transaction type behaves in the pool regarding
// replacement, eviction, account limits, and promotion.
type PoolPolicy interface {
	// PriceBumpPercent returns the minimum percentage by which both tip and
	// feeCap must increase for a replacement transaction to be accepted.
	// Default is cfg.PriceBump (10%). Blobs use cfg.BlobPriceBump (100%).
	PriceBumpPercent(cfg *txpoolcfg.Config) uint64

	// CanReplace returns true if a transaction of this type can replace an
	// existing transaction of existingType at the same (sender, nonce).
	// Blob transactions can only be replaced by other blob transactions.
	CanReplace(existingType byte) bool

	// AccountLimit returns the per-sender maximum for this type.
	// Standard types use cfg.AccountSlots; blobs use cfg.BlobSlots.
	AccountLimit(cfg *txpoolcfg.Config) uint64

	// EvictOnNonceGap returns true if any nonce gap causes immediate eviction.
	// Blobs require strict nonce ordering; standard types tolerate gaps up to
	// cfg.MaxNonceGap.
	EvictOnNonceGap() bool

	// PromotionCheck returns true if this transaction is eligible for promotion
	// to the pending sub-pool given current pool conditions.
	// Blob transactions check that their blobFeeCap >= pendingBlobFee.
	PromotionCheck(blobFeeCap uint64, pendingBlobFee uint64) bool

	// OnAdd is called when a transaction of this type is added to the pool.
	// Handlers update type-specific pool state: blob counts, auth reservations.
	OnAdd(txn types.Transaction, pm PoolMutation)

	// OnRemove is called when a transaction of this type is removed from the pool.
	// Handlers undo the updates made in OnAdd.
	OnRemove(txn types.Transaction, pm PoolMutation)
}
