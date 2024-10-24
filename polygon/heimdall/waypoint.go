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

func (wf *WaypointFields) Length() uint64 {
	return wf.EndBlock.Uint64() - wf.StartBlock.Uint64() + 1
}

func (wf *WaypointFields) CmpRange(n uint64) int {
	return cmpBlockRange(wf.StartBlock.Uint64(), wf.EndBlock.Uint64(), n)
}

type Waypoints []Waypoint
