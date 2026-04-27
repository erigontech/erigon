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

// CostPerStateByte derives the per-byte price for new state using the block
// gas limit as per EIP-8037.
//
//	raw = ceil((gas_limit * BLOCKS_PER_YEAR) / (2 * TARGET_STATE_GROWTH_PER_YEAR))
//	shifted = raw + CPSB_OFFSET
//	shift = max(bit_length(shifted) - CPSB_SIGNIFICANT_BITS, 0)
//	quantized = (shifted >> shift) << shift
//	cost_per_state_byte = quantized - CPSB_OFFSET, floored at 1
func CostPerStateByte(gasLimit uint64) uint64 {
	//
	// TODO clean up all changes related to the dynamic nature of this. Simplify to static val references.
	//
	// it was decided to stick to static gas
	return 1174
	//denominator := 2 * params.TargetStateGrowthPerYear
	//raw := (gasLimit*params.BlocksPerYear + denominator - 1) / denominator
	//shifted := raw + params.CpsbOffset
	//shift := max(bits.Len64(shifted)-params.CpsbSignificantBits, 0)
	//quantized := (shifted >> shift) << shift
	//if quantized <= params.CpsbOffset {
	//	return 1
	//}
	//return quantized - params.CpsbOffset
}
