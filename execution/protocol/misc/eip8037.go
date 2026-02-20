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

package misc

import (
	"math"
	"math/bits"

	"github.com/erigontech/erigon/execution/protocol/params"
)

func CostPerStateByte(gasLimit uint64) uint64 {
	//raw = ceil((gas_limit * 2_628_000) / (2 * TARGET_STATE_GROWTH_PER_YEAR))
	//shifted = raw + CPSB_OFFSET
	//shift = max(bit_length(shifted) - CPSB_SIGNIFICANT_BITS, 0)
	//cost_per_state_byte = max(((shifted >> shift) << shift) - CPSB_OFFSET, 1)
	raw := uint64(math.Ceil(float64(gasLimit*2_628_000) / float64(2*params.TargetStateGrowthPerYear)))
	shifted := raw + params.CpsbOffset
	shift := max(bits.Len64(shifted)-params.CpsbSignificantBits, 0)
	return max(((shifted>>shift)<<shift)-params.CpsbOffset, uint64(1))
}
