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

package receipts

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// DeriveFields must fill Bloom: finalize-produced and cache-read receipts
// arrive without one, and the notification path publishes the set as-is.
func TestDeriveFieldsSetsBloom(t *testing.T) {
	withLogs := &types.Receipt{
		Logs: []*types.Log{{
			Address: common.HexToAddress("0x01"),
			Topics:  []common.Hash{common.HexToHash("0x02")},
		}},
	}
	noLogs := &types.Receipt{}
	blockHash := common.HexToHash("0xabc")

	DeriveFields(types.Receipts{withLogs, noLogs}, blockHash)

	assert.NotEqual(t, types.Bloom{}, withLogs.Bloom)
	assert.Equal(t, types.CreateBloom(types.Receipts{withLogs}), withLogs.Bloom)
	assert.Equal(t, types.Bloom{}, noLogs.Bloom)
	assert.Equal(t, blockHash, withLogs.BlockHash)
	assert.Equal(t, blockHash, noLogs.BlockHash)
}

// Replay-produced receipts arrive with Bloom already computed; DeriveFields
// must reuse it instead of hashing the same logs again.
func TestDeriveFieldsPreservesExistingBloom(t *testing.T) {
	var preset types.Bloom
	preset[0] = 0x80
	preset[types.BloomByteLength-1] = 0x01
	withPresetBloom := &types.Receipt{
		Bloom: preset,
		Logs: []*types.Log{{
			Address: common.HexToAddress("0x01"),
			Topics:  []common.Hash{common.HexToHash("0x02")},
		}},
	}

	DeriveFields(types.Receipts{withPresetBloom}, common.HexToHash("0xabc"))

	assert.Equal(t, preset, withPresetBloom.Bloom)
}
