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
	"math/big"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/polygon/heimdall/heimdalltest"
)

func TestMilestoneJsonMarshall(t *testing.T) {
	heimdalltest.AssertJsonMarshalUnmarshal(t, makeMilestone(10, 100))
}

func makeMilestone(start uint64, len uint) *Milestone {
	m := Milestone{
		Fields: WaypointFields{
			StartBlock: new(big.Int).SetUint64(start),
			EndBlock:   new(big.Int).SetUint64(start + uint64(len) - 1),
			RootHash:   common.BytesToHash(crypto.Keccak256([]byte("ROOT"))),
			Timestamp:  uint64(time.Now().Unix()),
		},
	}
	return &m
}
