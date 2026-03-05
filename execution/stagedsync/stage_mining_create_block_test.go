// Copyright 2025 The Erigon Authors
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

package stagedsync

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

// Reported in https://audits.sherlock.xyz/contests/1140/voting/70
func TestMiningBlock_AvailableRlpSpace_BugReproduction(t *testing.T) {
	// Simulate post-Osaka scenario: block number < timestamp, but timestamp > Osaka activation
	header := &types.Header{
		Number: big.NewInt(27500000), // Block number (smaller than Osaka timestamp)
		Time:   1764800001,           // Timestamp (greater than Osaka activation time)
	}

	mb := &MiningBlock{
		Header:      header,
		Uncles:      []*types.Header{},
		Withdrawals: nil,
	}

	config := &chain.Config{
		OsakaTime: big.NewInt(1764800000),
	}

	// See EIP-7934: EIP-7934: RLP Execution Block Size Limit
	availableSpace := mb.AvailableRlpSpace(config)
	assert.Less(t, availableSpace, params.MaxRlpBlockSize)
}
