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

package jsonrpc

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/rpc"
)

type chainWitnesses struct {
	Legacy     [][]byte
	Canonical  [][]byte
	GetWitness []hexutil.Bytes
}

func collectChainWitnesses(t *testing.T, commitmentV3 bool) chainWitnesses {
	previousAssert, previousSchema, previousV3 := dbg.AssertEnabled, statecfg.Schema, statecfg.ExperimentalCommitmentV3
	t.Cleanup(func() {
		dbg.AssertEnabled, statecfg.Schema, statecfg.ExperimentalCommitmentV3 = previousAssert, previousSchema, previousV3
	})
	dbg.AssertEnabled = true
	statecfg.ExperimentalCommitmentV3 = commitmentV3
	if commitmentV3 {
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	}
	statecfg.EnableHistoricalCommitment()

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))
	var latest uint64
	require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
		var err error
		latest, err = stages.GetStageProgress(tx, stages.Execution)
		return err
	}))
	require.Positive(t, latest)

	debugAPI := newDebugApiForTest(m)
	ethAPI := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	var out chainWitnesses
	for n := uint64(1); n <= latest; n++ {
		bn := rpc.BlockNumber(n)
		at := rpc.BlockNumberOrHash{BlockNumber: &bn}
		for _, mode := range []string{"legacy", "canonical"} {
			result, err := debugAPI.ExecutionWitness(ctx, at, &mode)
			require.NoError(t, err, "block %d mode %s", n, mode)
			encoded, err := json.Marshal(result)
			require.NoError(t, err)
			if mode == "legacy" {
				out.Legacy = append(out.Legacy, encoded)
			} else {
				out.Canonical = append(out.Canonical, encoded)
			}
		}
		witness, err := ethAPI.GetWitness(ctx, at)
		require.NoError(t, err, "block %d eth_getWitness", n)
		out.GetWitness = append(out.GetWitness, witness)
	}
	return out
}

func TestWitnessesMatchHPHUnderCommitmentV3(t *testing.T) {
	var hph, v3 chainWitnesses
	t.Run("hph", func(t *testing.T) { hph = collectChainWitnesses(t, false) })
	t.Run("v3", func(t *testing.T) { v3 = collectChainWitnesses(t, true) })
	require.NotEmpty(t, hph.Legacy)
	for n := range hph.Legacy {
		require.Equal(t, string(hph.Legacy[n]), string(v3.Legacy[n]), "debug_executionWitness legacy, block %d", n+1)
		require.Equal(t, string(hph.Canonical[n]), string(v3.Canonical[n]), "debug_executionWitness canonical, block %d", n+1)
		require.Equal(t, hph.GetWitness[n], v3.GetWitness[n], "eth_getWitness, block %d", n+1)
	}
}
