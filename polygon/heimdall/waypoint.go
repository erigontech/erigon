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

package heimdall

import (
	"fmt"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
)

type Waypoint interface {
	Entity
	fmt.Stringer
	StartBlock() *big.Int
	EndBlock() *big.Int
	RootHash() libcommon.Hash
	Timestamp() uint64
	Length() uint64
	CmpRange(n uint64) int
}

type WaypointFields struct {
	Proposer   libcommon.Address `json:"proposer"`
	StartBlock *big.Int          `json:"start_block"`
	EndBlock   *big.Int          `json:"end_block"`
	RootHash   libcommon.Hash    `json:"root_hash"`
	ChainID    string            `json:"bor_chain_id"`
	Timestamp  uint64            `json:"timestamp"`
}

func (a *WaypointFields) Length() uint64 {
	return a.EndBlock.Uint64() - a.StartBlock.Uint64() + 1
}

func (a *WaypointFields) CmpRange(n uint64) int {
	num := new(big.Int).SetUint64(n)
	if num.Cmp(a.StartBlock) < 0 {
		return -1
	}
	if num.Cmp(a.EndBlock) > 0 {
		return 1
	}
	return 0
}

type Waypoints []Waypoint
