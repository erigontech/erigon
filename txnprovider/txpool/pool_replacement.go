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
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv/kvcache"
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
	if newTxn.TxType() == FrameTxnType && oldTxn.TxType() == FrameTxnType {
		// Frame replacements compare total budgets, not derived per-gas caps.
		return replacementDecision{}
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

	if newTxn.GetTipCap().Cmp(tipThreshold) < 0 || newTxn.GetFeeCap().Cmp(feecapThreshold) < 0 {
		rebroadcast := replacement.subPool&IsLocal != 0 && (previous.currentSubPool == PendingSubPool || previous.currentSubPool == BaseFeeSubPool)
		return replacementDecision{
			reason:      txpoolcfg.NotReplaced,
			rebroadcast: rebroadcast,
		}
	}
	return replacementDecision{}
}

// fundedPrefix is the run of a sender's transactions from the account nonce up to end that its
// balance covers, and what the run costs.
type fundedPrefix struct {
	end   uint64
	spent uint256.Int
}

// keepsSenderFunded reports whether the sender's balance still covers its funded prefix once replacement
// takes the place of previous. A batch of additions records the prefix in prefixes at its first costlier
// replacement for the sender and updates it with each replacement it accepts, so the batch walks the
// sender's transactions once. Transactions that the batch adds later are not covered. The caller must
// replace previous when it returns true.
func (p *TxPool) keepsSenderFunded(cacheView kvcache.CacheView, prefixes map[uint64]*fundedPrefix, replacement *TxnSlot, previous *metaTxn) (bool, error) {
	oldCost, newCost := requiredBalance(previous.TxnSlot), requiredBalance(replacement)
	costlier := newCost.Gt(oldCost)
	prefix := prefixes[replacement.SenderID]
	if prefix == nil && !costlier {
		return true, nil
	}
	senderNonce, balance, _, err := p.senders.info(cacheView, replacement.SenderID)
	if err != nil {
		return false, err
	}
	// As in onSenderStateChange, frame reservations come out of the balance first.
	sender := p.senders.senderID2Addr[replacement.SenderID]
	reserved := p.frameReservations[sender].cost
	if prefix == nil {
		prefix = &fundedPrefix{end: senderNonce}
		funds := unreservedFunds(balance, reserved)
		p.all.ascend(replacement.SenderID, func(mt *metaTxn) bool {
			if mt.keyedNonce || mt.TxnSlot.Nonce < senderNonce {
				return true
			}
			if mt.TxnSlot.Nonce != prefix.end {
				return false
			}
			var spent uint256.Int
			if _, overflow := spent.AddOverflow(&prefix.spent, requiredBalance(mt.TxnSlot)); overflow || spent.Gt(&funds) {
				return false
			}
			prefix.spent = spent
			prefix.end++
			return true
		})
		prefixes[replacement.SenderID] = prefix
	}
	if previous.keyedNonce || previous.TxnSlot.Nonce < senderNonce || previous.TxnSlot.Nonce >= prefix.end {
		return true, nil
	}
	var spent uint256.Int
	spent.Sub(&prefix.spent, oldCost)
	_, overflow := spent.AddOverflow(&spent, newCost)
	if costlier {
		// A replaced frame releases its own reservation.
		if old := previous.TxnSlot.frameValidation; old != nil && old.Payer == sender {
			reserved.Sub(&reserved, frameMaxFee(previous.TxnSlot))
		}
		if funds := unreservedFunds(balance, reserved); overflow || spent.Gt(&funds) {
			return false, nil
		}
	}
	prefix.spent = spent
	return true, nil
}

func unreservedFunds(balance, reserved uint256.Int) uint256.Int {
	var funds uint256.Int
	if balance.Gt(&reserved) {
		funds.Sub(&balance, &reserved)
	}
	return funds
}

// validateFrameReplacement checks replacements involving at least one frame.
// Fees must increase even when the configured percentage would round down to no increase.
func validateFrameReplacement(previous, replacement *TxnSlot, priceBump, blobPriceBump uint64) txpoolcfg.DiscardReason {
	oldFrame, oldIsFrame := previous.Txn.(*types.FrameTransaction)
	newFrame, newIsFrame := replacement.Txn.(*types.FrameTransaction)
	bothHaveBlobs := previous.HasBlobs() && replacement.HasBlobs()
	var oldCap, newCap *uint256.Int
	switch {
	case oldIsFrame && newIsFrame:
		oldCap, newCap = &oldFrame.Fees.MaxFee, &newFrame.Fees.MaxFee
	case bothHaveBlobs:
		// Comparing a frame's budget with each type-3 fee cap separately would
		// count the same wei toward both the execution and blob fee bumps.
		return txpoolcfg.BlobReplacementTypeMismatch
	default:
		oldCap, newCap = previous.GetFeeCap(), replacement.GetFeeCap()
	}
	if !frameReplacementFee(previous.GetTipCap(), replacement.GetTipCap(), priceBump) || !frameReplacementFee(oldCap, newCap, priceBump) {
		return txpoolcfg.NotReplaced
	}
	if bothHaveBlobs && !frameReplacementFee(oldCap, newCap, blobPriceBump) {
		return txpoolcfg.ReplaceUnderpriced
	}
	return txpoolcfg.Success
}

func frameReplacementFee(previous, replacement *uint256.Int, bump uint64) bool {
	if replacement.Cmp(previous) <= 0 {
		return false
	}
	var percent, threshold, remainder uint256.Int
	percent.SetUint64(bump)
	percent.AddUint64(&percent, 100)
	_, _, overflow := threshold.MulDivOverflowRem(previous, &percent, &u256.N100, &remainder)
	cmp := replacement.Cmp(&threshold)
	// A fractional threshold requires a strictly higher fee. Comparing instead
	// of rounding up also rejects a threshold just above MaxUint256.
	return !overflow && (cmp > 0 || cmp == 0 && remainder.IsZero())
}
