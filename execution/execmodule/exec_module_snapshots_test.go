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

package execmodule_test

import (
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestExecModuleProducesE2Snapshots(t *testing.T) {
	ctx := t.Context()
	retireStep := uint64(10)
	chainLen := int(retireStep) + 2
	emt := execmoduletester.New(
		t,
		execmoduletester.WithChainConfig(chain.AllProtocolChanges),
		execmoduletester.WithE2RetireStep(retireStep),
		execmoduletester.WithMaxReorgDepth(1),
	)
	cp, err := emt.GenerateChain(chainLen, func(i int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(
				gen.TxNonce(emt.Address),
				common.Address{1},
				uint256.NewInt(10_000),
				params.TxGas,
				uint256.NewInt(emt.Genesis.BaseFee().Uint64()),
				nil,
			),
			*types.LatestSignerForChainID(emt.ChainConfig.ChainID),
			emt.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	retirementDoneSub, retirementDoneSubClose := emt.Notifications.Events.AddRetirementDoneSubscription()
	t.Cleanup(retirementDoneSubClose)
	status, err := emt.InsertBlocks(ctx, cp.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := emt.UpdateForkChoice(ctx, cp.Blocks[len(cp.Blocks)-2].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	result, err = emt.UpdateForkChoice(ctx, cp.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	// wait for 2 retirement loops to be done since there are 2 UFCs
	timeoutC := time.After(time.Minute)
	for range 2 {
		select {
		case <-retirementDoneSub:
		case <-timeoutC:
			t.Fatal("retirement done timed out")
		}
	}
	err = emt.BlockSnapshots.OpenFolder()
	require.NoError(t, err)
	require.Equal(t, uint64(9), emt.BlockSnapshots.BlocksAvailable())
}
