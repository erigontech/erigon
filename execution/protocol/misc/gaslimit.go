// Copyright 2021 The go-ethereum Authors
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

package misc

import (
	"fmt"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

// VerifyGaslimit verifies the header gas limit according increase/decrease
// in relation to the parent gas limit.
func VerifyGaslimit(parentGasLimit, headerGasLimit uint64) error {
	// Verify that the gas limit remains within allowed bounds
	diff := int64(parentGasLimit) - int64(headerGasLimit)
	if diff < 0 {
		diff *= -1
	}
	limit := parentGasLimit / params.GasLimitBoundDivisor
	if uint64(diff) >= limit {
		return fmt.Errorf("invalid gas limit: have %d, want %d +-= %d", headerGasLimit, parentGasLimit, limit-1)
	}
	if headerGasLimit < params.MinBlockGasLimit {
		return fmt.Errorf("invalid gas limit below %d", params.MinBlockGasLimit)
	}
	return nil
}

// MaxGasLimit is the largest gas limit VerifyParentGasLimit accepts for a header after parent, or
// the parent's gas limit when it accepts none.
func MaxGasLimit(config *chain.Config, parent, header *types.Header) uint64 {
	gasLimit := parentGasLimit(config, parent, header)
	delta := gasLimit / params.GasLimitBoundDivisor
	if delta == 0 {
		return gasLimit
	}
	return gasLimit + delta - 1
}

// VerifyParentGasLimit checks the parent-relative gas limit rule (±1/1024).
func VerifyParentGasLimit(config *chain.Config, parent, header *types.Header) error {
	return VerifyGaslimit(parentGasLimit(config, parent, header), header.GasLimit)
}

// parentGasLimit applies the elasticity multiplier at the London transition boundary.
func parentGasLimit(config *chain.Config, parent, header *types.Header) uint64 {
	if config.IsLondon(header.Number.Uint64()) && !config.IsLondon(parent.Number.Uint64()) {
		return parent.GasLimit * params.ElasticityMultiplier
	}
	return parent.GasLimit
}

// CalcGasLimit computes the gas limit of the next block after parent. It aims
// to keep the baseline gas close to the provided target, and increase it towards
// the target if the baseline gas is lower.
func CalcGasLimit(parentGasLimit, desiredLimit uint64) uint64 {
	delta := parentGasLimit/params.GasLimitBoundDivisor - 1
	limit := parentGasLimit
	desiredLimit = max(desiredLimit, params.MinBlockGasLimit)
	// If we're outside our allowed gas range, we try to hone towards them
	if limit < desiredLimit {
		return min(parentGasLimit+delta, desiredLimit)
	}
	if limit > desiredLimit {
		return max(parentGasLimit-delta, desiredLimit)
	}
	return limit
}
