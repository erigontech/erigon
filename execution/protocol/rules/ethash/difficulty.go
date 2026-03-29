// Copyright 2020 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package ethash

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

const (
	// frontierDurationLimit is for Frontier:
	// The decision boundary on the blocktime duration used to determine
	// whether difficulty should go up or down.
	frontierDurationLimit = 13
	// minimumDifficulty The minimum that the difficulty may ever be.
	minimumDifficulty = 131072
	// expDiffPeriod is the exponential difficulty period
	expDiffPeriod = 100000
	// difficultyBoundDivisorBitShift is the bound divisor of the difficulty (2048),
	// This constant is the right-shifts to use for the division.
	difficultyBoundDivisor = 11
)

// calcDifficultyFrontier is the difficulty adjustment algorithm. It returns the
// difficulty that a new block should have when created at time given the parent
// block's time and difficulty. The calculation uses the Frontier rules.
func calcDifficultyFrontier(time, parentTime uint64, parentDifficulty uint256.Int, parentNumber uint64) uint256.Int {
	/*
		Algorithm
		block_diff = pdiff + pdiff / 2048 * (1 if time - ptime < 13 else -1) + int(2^((num // 100000) - 2))

		Where:
		- pdiff  = parent.difficulty
		- ptime = parent.time
		- time = block.timestamp
		- num = block.number
	*/

	pDiff := parentDifficulty.Clone()
	adjust := pDiff.Clone()
	adjust.Rsh(adjust, difficultyBoundDivisor) // adjust: pDiff / 2048

	if time-parentTime < frontierDurationLimit {
		pDiff.Add(pDiff, adjust)
	} else {
		pDiff.Sub(pDiff, adjust)
	}
	if pDiff.LtUint64(minimumDifficulty) {
		pDiff.SetUint64(minimumDifficulty)
	}
	// 'pdiff' now contains:
	// pdiff + pdiff / 2048 * (1 if time - ptime < 13 else -1)

	if periodCount := (parentNumber + 1) / expDiffPeriod; periodCount > 1 {
		// diff = diff + 2^(periodCount - 2)
		expDiff := adjust.SetOne()
		expDiff.Lsh(expDiff, uint(periodCount-2)) // expdiff: 2 ^ (periodCount -2)
		pDiff.Add(pDiff, expDiff)
	}
	return *pDiff
}

// calcDifficultyHomestead is the difficulty adjustment algorithm. It returns
// the difficulty that a new block should have when created at time given the
// parent block's time and difficulty. The calculation uses the Homestead rules.
func calcDifficultyHomestead(time, parentTime uint64, parentDifficulty uint256.Int, parentNumber uint64) uint256.Int {
	/*
		https://github.com/ethereum/EIPs/blob/master/EIPS/eip-2.md
		Algorithm:
		block_diff = pdiff + pdiff / 2048 * max(1 - (time - ptime) / 10, -99) + 2 ^ int((num / 100000) - 2))

		Our modification, to use unsigned ints:
		block_diff = pdiff - pdiff / 2048 * max((time - ptime) / 10 - 1, 99) + 2 ^ int((num / 100000) - 2))

		Where:
		- pdiff  = parent.difficulty
		- ptime = parent.time
		- time = block.timestamp
		- num = block.number
	*/

	pDiff := parentDifficulty.Clone()
	adjust := pDiff.Clone()
	adjust.Rsh(adjust, difficultyBoundDivisor) // adjust: pDiff / 2048

	x := (time - parentTime) / 10 // (time - ptime) / 10)
	var neg = true
	if x == 0 {
		x = 1
		neg = false
	} else if x >= 100 {
		x = 99
	} else {
		x = x - 1
	}
	z := new(uint256.Int).SetUint64(x)
	adjust.Mul(adjust, z) // adjust: (pdiff / 2048) * max((time - ptime) / 10 - 1, 99)
	if neg {
		pDiff.Sub(pDiff, adjust) // pdiff - pdiff / 2048 * max((time - ptime) / 10 - 1, 99)
	} else {
		pDiff.Add(pDiff, adjust) // pdiff + pdiff / 2048 * max((time - ptime) / 10 - 1, 99)
	}
	if pDiff.LtUint64(minimumDifficulty) {
		pDiff.SetUint64(minimumDifficulty)
	}
	// for the exponential factor, a.k.a "the bomb"
	// diff = diff + 2^(periodCount - 2)
	if periodCount := (1 + parentNumber) / expDiffPeriod; periodCount > 1 {
		expFactor := adjust.Lsh(adjust.SetOne(), uint(periodCount-2))
		pDiff.Add(pDiff, expFactor)
	}
	return *pDiff
}

// makeDifficultyCalculator creates a difficultyCalculator with the given bomb-delay.
// the difficulty is calculated with Byzantium rules, which differs from Homestead in
// how uncles affect the calculation
func makeDifficultyCalculator(bombDelay uint64) func(time, parentTime uint64, parentDifficulty uint256.Int, parentNumber uint64, parentUncleHash common.Hash) uint256.Int {
	// Note, the calculations below looks at the parent number, which is 1 below
	// the block number. Thus we remove one from the delay given
	bombDelayFromParent := bombDelay - 1
	return func(time, parentTime uint64, pDiff uint256.Int, pNum uint64, parentUncleHash common.Hash) uint256.Int {
		/*
			https://github.com/ethereum/EIPs/issues/100
			pDiff = parent.difficulty
			BLOCK_DIFF_FACTOR = 9
			a = pDiff + (pDiff // BLOCK_DIFF_FACTOR) * adj_factor
			b = min(parent.difficulty, MIN_DIFF)
			child_diff = max(a,b )
		*/
		x := (time - parentTime) / 9 // (block_timestamp - parent_timestamp) // 9
		c := uint64(1)               // if parent.unclehash == emptyUncleHashHash
		if parentUncleHash != empty.UncleHash {
			c = 2
		}
		xNeg := x >= c
		if xNeg {
			// x is now _negative_ adjustment factor
			x = x - c // - ( (t-p)/p -( 2 or 1) )
		} else {
			x = c - x // (2 or 1) - (t-p)/9
		}
		if x > 99 {
			x = 99 // max(x, 99)
		}
		// parent_diff + (parent_diff / 2048 * max((2 if len(parent.uncles) else 1) - ((timestamp - parent.timestamp) // 9), -99))
		y := pDiff.Clone()
		z := new(uint256.Int).SetUint64(x) //z : +-adj_factor (either pos or negative)
		y.Rsh(y, difficultyBoundDivisor)   // y: p__diff / 2048
		z.Mul(y, z)                        // z: (p_diff / 2048 ) * (+- adj_factor)

		if xNeg {
			y.Sub(&pDiff, z) // y: parent_diff + parent_diff/2048 * adjustment_factor
		} else {
			y.Add(&pDiff, z) // y: parent_diff + parent_diff/2048 * adjustment_factor
		}
		// minimum difficulty can ever be (before exponential factor)
		if y.LtUint64(minimumDifficulty) {
			y.SetUint64(minimumDifficulty)
		}
		// calculate a fake block number for the ice-age delay
		// Specification: https://eips.ethereum.org/EIPS/eip-1234
		if pNum >= bombDelayFromParent {
			if fakeBlockNumber := pNum - bombDelayFromParent; fakeBlockNumber >= 2*expDiffPeriod {
				z.SetOne()
				z.Lsh(z, uint(fakeBlockNumber/expDiffPeriod-2))
				y.Add(z, y)
			}
		}
		return *y
	}
}
