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

package misc

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/types"
)

// Constants for The Merge as specified by EIP-3675: Upgrade consensus to Proof-of-Stake
var (
	ProofOfStakeDifficulty = common.Big0        // PoS block's difficulty is always 0
	ProofOfStakeNonce      = types.BlockNonce{} // PoS block's have all-zero nonces
)

// IsPoSHeader reports the header belongs to the PoS-stage with some special fields.
// This function is not suitable for a part of APIs like Prepare or CalcDifficulty
// because the header difficulty is not set yet.
func IsPoSHeader(header *types.Header) bool {
	if header.Difficulty == nil {
		panic("IsPoSHeader called with invalid difficulty")
	}
	return header.Difficulty.Cmp(ProofOfStakeDifficulty) == 0
}
