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

package merge

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
)

type FakeMerge struct {
	*Merge
}

// NewFaker creates a ethash rules engine with a fake PoW scheme that accepts
// all blocks' seal as valid, though they still have to conform to the Ethereum
// consensus rules.
func NewFaker(eth1Fake *ethash.FakeEthash) *FakeMerge {
	return &FakeMerge{
		Merge: New(eth1Fake),
	}
}

func (s *FakeMerge) CalcDifficulty(chain rules.ChainHeaderReader, time, parentTime uint64, parentDifficulty uint256.Int, parentNumber uint64, parentHash, parentUncleHash common.Hash, parentAuRaStep uint64) uint256.Int {
	return *ProofOfStakeDifficulty
}
