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

package services

// IsGasLimitTargetCompatible checks whether the bid's gas_limit is consistent with the
// proposer's target given the parent's gas_limit, per EIP-1559 transition rules (consensus-specs PR #5236).
func IsGasLimitTargetCompatible(parentGasLimit, gasLimit, targetGasLimit uint64) bool {
	var maxGasLimitDifference uint64
	if parentGasLimit/1024 > 1 {
		maxGasLimitDifference = parentGasLimit/1024 - 1
	}

	minGasLimit := parentGasLimit - maxGasLimitDifference
	maxGasLimit := parentGasLimit + maxGasLimitDifference

	if targetGasLimit >= minGasLimit && targetGasLimit <= maxGasLimit {
		return gasLimit == targetGasLimit
	}
	if targetGasLimit > maxGasLimit {
		return gasLimit == maxGasLimit
	}
	return gasLimit == minGasLimit
}
