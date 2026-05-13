// Copyright 2024 The Erigon Authors
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

import "github.com/holiman/uint256"

func newMetaTxn(slot *TxnSlot, isLocal bool, timestamp uint64) *metaTxn {
	mt := &metaTxn{TxnSlot: slot, worstIndex: -1, bestIndex: -1, timestamp: timestamp}
	if isLocal {
		mt.subPool = IsLocal
	}
	return mt
}

// metaTxn holds transaction and some metadata
type metaTxn struct {
	TxnSlot                   *TxnSlot
	minFeeCap                 uint256.Int
	nonceDistance             uint64 // how far their nonces are from the state's nonce for the sender
	cumulativeBalanceDistance uint64 // how far their cumulativeRequiredBalance are from the state's balance for the sender
	minTip                    uint64
	bestIndex                 int
	worstIndex                int
	timestamp                 uint64 // when it was added to pool
	subPool                   SubPoolMarker
	currentSubPool            SubPoolType
	minedBlockNum             uint64
}

// effectiveTip computes min(feeCap - baseFee, tip), or zero if feeCap < baseFee.
func effectiveTip(minFeeCap, pendingBaseFee *uint256.Int, minTip uint64) uint256.Int {
	if minFeeCap.Cmp(pendingBaseFee) < 0 {
		return uint256.Int{}
	}
	var diff uint256.Int
	diff.Sub(minFeeCap, pendingBaseFee)
	tip := uint256.NewInt(minTip)
	if diff.Cmp(tip) <= 0 {
		return diff
	}
	return *tip
}

// Returns true if the txn "mt" is better than the parameter txn "than"
// it first compares the subpool markers of the two meta txns, then,
// (since they have the same subpool marker, and thus same pool)
// depending on the pool - pending (P), basefee (B), queued (Q) -
// it compares the effective tip (for P), nonceDistance (for both P,Q)
// minFeeCap (for B), and cumulative balance distance (for P, Q)
func (mt *metaTxn) better(than *metaTxn, pendingBaseFee uint256.Int) bool {
	subPool := mt.subPool
	thanSubPool := than.subPool
	if mt.minFeeCap.Cmp(&pendingBaseFee) >= 0 {
		subPool |= EnoughFeeCapBlock
	}
	if than.minFeeCap.Cmp(&pendingBaseFee) >= 0 {
		thanSubPool |= EnoughFeeCapBlock
	}
	if subPool != thanSubPool {
		return subPool > thanSubPool
	}

	switch mt.currentSubPool {
	case PendingSubPool:
		mtTip := effectiveTip(&mt.minFeeCap, &pendingBaseFee, mt.minTip)
		thanTip := effectiveTip(&than.minFeeCap, &pendingBaseFee, than.minTip)
		if mtTip.Cmp(&thanTip) != 0 {
			return mtTip.Cmp(&thanTip) > 0
		}
		// Compare nonce and cumulative balance. Just as a side note, it doesn't
		// matter if they're from same sender or not because we're comparing
		// nonce distance of the sender from state's nonce and not the actual
		// value of nonce.
		if mt.nonceDistance != than.nonceDistance {
			return mt.nonceDistance < than.nonceDistance
		}
		if mt.cumulativeBalanceDistance != than.cumulativeBalanceDistance {
			return mt.cumulativeBalanceDistance < than.cumulativeBalanceDistance
		}
	case BaseFeeSubPool:
		if mt.minFeeCap.Cmp(&than.minFeeCap) != 0 {
			return mt.minFeeCap.Cmp(&than.minFeeCap) > 0
		}
	case QueuedSubPool:
		if mt.nonceDistance != than.nonceDistance {
			return mt.nonceDistance < than.nonceDistance
		}
		if mt.cumulativeBalanceDistance != than.cumulativeBalanceDistance {
			return mt.cumulativeBalanceDistance < than.cumulativeBalanceDistance
		}
	}
	return mt.timestamp < than.timestamp
}

func (mt *metaTxn) worse(than *metaTxn, pendingBaseFee uint256.Int) bool {
	subPool := mt.subPool
	thanSubPool := than.subPool
	if mt.minFeeCap.Cmp(&pendingBaseFee) >= 0 {
		subPool |= EnoughFeeCapBlock
	}
	if than.minFeeCap.Cmp(&pendingBaseFee) >= 0 {
		thanSubPool |= EnoughFeeCapBlock
	}
	if subPool != thanSubPool {
		return subPool < thanSubPool
	}

	switch mt.currentSubPool {
	case PendingSubPool:
		if mt.minFeeCap != than.minFeeCap {
			return mt.minFeeCap.Cmp(&than.minFeeCap) < 0
		}
		if mt.nonceDistance != than.nonceDistance {
			return mt.nonceDistance > than.nonceDistance
		}
		if mt.cumulativeBalanceDistance != than.cumulativeBalanceDistance {
			return mt.cumulativeBalanceDistance > than.cumulativeBalanceDistance
		}
	case BaseFeeSubPool, QueuedSubPool:
		if mt.nonceDistance != than.nonceDistance {
			return mt.nonceDistance > than.nonceDistance
		}
		if mt.cumulativeBalanceDistance != than.cumulativeBalanceDistance {
			return mt.cumulativeBalanceDistance > than.cumulativeBalanceDistance
		}
	}
	return mt.timestamp > than.timestamp
}

func SortByNonceLess(a, b *metaTxn) bool {
	if a.TxnSlot.SenderID != b.TxnSlot.SenderID {
		return a.TxnSlot.SenderID < b.TxnSlot.SenderID
	}
	return a.TxnSlot.Nonce < b.TxnSlot.Nonce
}
