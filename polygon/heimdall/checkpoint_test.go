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
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/polygon/heimdall/heimdalltest"
	"github.com/stretchr/testify/require"
)

func TestCheckpointListResponse(t *testing.T) {
	output := []byte(`{"checkpoint_list": [{"id": "1","proposer": "0x20d8433dc819af087336a725422df4cfbef29710","start_block": "0","end_block": "255","root_hash": "0+EHMZn8iUFWtDMO6qn37fcLQqNUQVbEfL4d7HYrx8E=","bor_chain_id": "2756","timestamp": "1750522061"},{"id": "2","proposer": "0x735de19a997ef33a090c873d1ac27f99d77b843c","start_block": "256","end_block": "511","root_hash": "Hl4oZROMThMttqdzXFvWuELaOI8C3pPiFkQVN0vWUtc=","bor_chain_id": "2756","timestamp": "1750522660"}]}`)

	var v CheckpointListResponseV2

	err := json.Unmarshal(output, &v)
	require.Nil(t, err)

	list, err := v.ToList()
	require.Nil(t, err)
	require.Len(t, list, 2)
}

func TestCheckpointJsonMarshall(t *testing.T) {
	heimdalltest.AssertJsonMarshalUnmarshal(t, makeCheckpoint(10, 100))
}

func makeCheckpoint(start uint64, len uint) *Checkpoint {
	return &Checkpoint{
		Fields: WaypointFields{
			StartBlock: new(big.Int).SetUint64(start),
			EndBlock:   new(big.Int).SetUint64(start + uint64(len) - 1),
			RootHash:   common.BytesToHash(crypto.Keccak256([]byte("ROOT"))),
			Timestamp:  uint64(time.Now().Unix()),
		},
	}
}
