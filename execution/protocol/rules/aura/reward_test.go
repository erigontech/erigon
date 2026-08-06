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

package aura

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

func TestApplyRewardsZeroAmsterdamNoAccess(t *testing.T) {
	engine := &AuRa{cfg: AuthorityRoundParams{
		BlockReward: BlockRewardList{{
			amount: uint256.NewInt(0),
		}},
	}}
	ibs := state.NewWithVersionMap(state.NewNoopReader(), state.NewVersionMap(nil))
	ibs.SetTxContext(1, 0)
	ibs.StartAccessRecording()
	t.Cleanup(func() { ibs.Release(false) })

	amsterdam := uint64(0)
	err := engine.applyRewards(
		&chain.Config{AmsterdamTime: &amsterdam},
		&types.Header{Coinbase: common.HexToAddress("0x1111111111111111111111111111111111111111")},
		ibs,
		nil,
	)
	require.NoError(t, err)

	writes, err := ibs.FinalizedWrites(&chain.Rules{
		IsSpuriousDragon: true,
		IsAmsterdam:      true,
	})
	require.NoError(t, err)
	io := state.NewVersionedIO(1)
	ibs.MergeTxIOInto(io, writes)
	require.Empty(t, io.AsBlockAccessList())
}
