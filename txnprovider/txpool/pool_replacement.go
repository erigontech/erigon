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

package txpool

import (
	"math/big"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type replacementDecision struct {
	reason      txpoolcfg.DiscardReason
	rebroadcast bool
}

// checkReplacement requires the pool lock and leaves pool state unchanged.
func (p *TxPool) checkReplacement(replacement, previous *metaTxn) replacementDecision {
	newTxn, oldTxn := replacement.TxnSlot, previous.TxnSlot
	if newTxn.TxType() == FrameTxnType || oldTxn.TxType() == FrameTxnType {
		if reason := validateFrameReplacement(oldTxn, newTxn, p.cfg.PriceBump, p.cfg.BlobPriceBump); reason != txpoolcfg.Success {
			return replacementDecision{reason: reason}
		}
	}
	if oldTxn.HasBlobs() && !newTxn.HasBlobs() {
		return replacementDecision{reason: txpoolcfg.BlobTxReplace}
	}
	priceBump := p.cfg.PriceBump

	if newTxn.HasBlobs() {
		// Blob txn threshold checks for replace txn
		if newTxn.TxType() != FrameTxnType {
			priceBump = p.cfg.BlobPriceBump
		}
		blobFeeThreshold, overflow := (&uint256.Int{}).MulDivOverflow(
			oldTxn.GetBlobFeeCap(),
			uint256.NewInt(100+p.cfg.BlobPriceBump),
			uint256.NewInt(100),
		)
		if newTxn.GetBlobFeeCap().Lt(blobFeeThreshold) && !overflow {
			return replacementDecision{reason: txpoolcfg.ReplaceUnderpriced}
		}
	}

	// Regular txn threshold checks
	tipThreshold := uint256.NewInt(0)
	tipThreshold = tipThreshold.Mul(oldTxn.GetTipCap(), uint256.NewInt(100+priceBump))
	tipThreshold.Div(tipThreshold, &u256.N100)
	feecapThreshold := uint256.NewInt(0)
	feecapThreshold.Mul(oldTxn.GetFeeCap(), uint256.NewInt(100+priceBump))
	feecapThreshold.Div(feecapThreshold, &u256.N100)

	if newTxn.GetValue().Cmp(oldTxn.GetValue()) > 0 {
		// Potential latent overdraft attack
		tipThreshold.Mul(tipThreshold, uint256.NewInt(uint64(p.all.count(newTxn.SenderID))))
	}
	if newTxn.GetTipCap().Cmp(tipThreshold) < 0 || newTxn.GetFeeCap().Cmp(feecapThreshold) < 0 {
		rebroadcast := replacement.subPool&IsLocal != 0 && (previous.currentSubPool == PendingSubPool || previous.currentSubPool == BaseFeeSubPool)
		return replacementDecision{
			reason:      txpoolcfg.NotReplaced,
			rebroadcast: rebroadcast,
		}
	}
	return replacementDecision{}
}

// Frame replacements must keep the same nonce keys and strictly increase fees,
// even when the configured percentage would round down to no increase.
func validateFrameReplacement(previous, replacement *TxnSlot, priceBump, blobPriceBump uint64) txpoolcfg.DiscardReason {
	previousKeys, replacementKeys := []uint256.Int{{}}, []uint256.Int{{}}
	if frame, ok := previous.Txn.(*types.FrameTransaction); ok {
		previousKeys = frame.NonceKeys
	}
	if frame, ok := replacement.Txn.(*types.FrameTransaction); ok {
		replacementKeys = frame.NonceKeys
	}
	if !slices.Equal(previousKeys, replacementKeys) {
		return txpoolcfg.InvalidFrame
	}
	if !frameReplacementFee(previous.GetTipCap(), replacement.GetTipCap(), priceBump) || !frameReplacementFee(previous.GetFeeCap(), replacement.GetFeeCap(), priceBump) {
		return txpoolcfg.NotReplaced
	}
	if replacement.HasBlobs() && previous.HasBlobs() && !frameReplacementFee(previous.GetBlobFeeCap(), replacement.GetBlobFeeCap(), blobPriceBump) {
		return txpoolcfg.ReplaceUnderpriced
	}
	return txpoolcfg.Success
}

func frameReplacementFee(previous, replacement *uint256.Int, bump uint64) bool {
	if replacement.Cmp(previous) <= 0 {
		return false
	}
	// Cross-products preserve the full percentage bump without division rounding
	// or overflow at the uint256 boundary.
	percent := new(big.Int).Add(new(big.Int).SetUint64(bump), big.NewInt(100))
	required := new(big.Int).Mul(previous.ToBig(), percent)
	offered := new(big.Int).Mul(replacement.ToBig(), big.NewInt(100))
	return offered.Cmp(required) >= 0
}
