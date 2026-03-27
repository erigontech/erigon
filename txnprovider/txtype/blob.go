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
	execparams "github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// BlobHandler handles EIP-4844 blob transactions (type 3).
//
// Blob transactions differ from standard types in several ways:
//   - Require Cancun fork activation
//   - Cannot be used for contract creation
//   - Can only replace other blob transactions (not standard types)
//   - Use a higher price bump (cfg.BlobPriceBump, default 100%) for replacement
//   - Use cfg.BlobSlots for per-sender limits instead of cfg.AccountSlots
//   - Any nonce gap causes immediate eviction (strict ordering)
//   - Promotion requires blobFeeCap >= pendingBlobFee
//   - Track total blob count in the pool (OnAdd/OnRemove)
type BlobHandler struct{ DefaultHandler }

func (BlobHandler) TypeByte() byte { return types.BlobTxType }
func (BlobHandler) Name() string   { return "blob" }

// ForkRequired returns true only when Cancun is active.
func (BlobHandler) ForkRequired(forks ForkState) bool { return forks.IsCancun }

// CanCreate returns false — blob transactions cannot deploy contracts.
func (BlobHandler) CanCreate() bool { return false }

// CanReplace returns true only when the existing transaction is also a blob.
// Blob transactions cannot replace non-blob transactions at the same nonce.
func (BlobHandler) CanReplace(existingType byte) bool { return existingType == types.BlobTxType }

// PriceBumpPercent returns cfg.BlobPriceBump (default 100%) for blob replacements.
func (BlobHandler) PriceBumpPercent(cfg *txpoolcfg.Config) uint64 { return cfg.BlobPriceBump }

// AccountLimit returns cfg.BlobSlots (tracked in total blobs, not transactions).
func (BlobHandler) AccountLimit(cfg *txpoolcfg.Config) uint64 { return cfg.BlobSlots }

// EvictOnNonceGap returns true — blob transactions require strict nonce ordering.
// Any gap between the queued blob nonce and the sender's on-chain nonce causes
// eviction of the blob transaction.
func (BlobHandler) EvictOnNonceGap() bool { return true }

// PromotionCheck returns true only when the transaction's blob fee cap meets
// the current pending blob base fee.
func (BlobHandler) PromotionCheck(blobFeeCap uint64, pendingBlobFee uint64) bool {
	return blobFeeCap >= pendingBlobFee
}

// OnAdd increments the pool-wide blob count by the number of blobs in txn.
func (BlobHandler) OnAdd(txn types.Transaction, pm PoolMutation) {
	pm.BlobAdded(len(txn.GetBlobHashes()))
}

// OnRemove decrements the pool-wide blob count by the number of blobs in txn.
func (BlobHandler) OnRemove(txn types.Transaction, pm PoolMutation) {
	pm.BlobRemoved(len(txn.GetBlobHashes()))
}

// ValidateTx performs blob-specific validation: checks that the transaction
// carries at least one blob hash and no more than the per-tx blob limit.
// KZG proof verification and pool-wide blob limits are checked by the pool
// before calling this handler.
func (BlobHandler) ValidateTx(txn types.Transaction, _ bool, _ *txpoolcfg.Config) txpoolcfg.DiscardReason {
	hashes := txn.GetBlobHashes()
	if len(hashes) == 0 {
		return txpoolcfg.NoBlobs
	}
	// Per-transaction blob count limit (protects against oversized txns).
	if len(hashes) > execparams.MaxBlobsPerTxn {
		return txpoolcfg.TooManyBlobs
	}
	return txpoolcfg.NotSet
}
