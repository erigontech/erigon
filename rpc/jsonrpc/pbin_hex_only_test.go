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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// eth_getProof rebuilds proofs with the hex trie, so it must refuse a bin datadir
// instead of reading bit-path branch records as hex ones.
func TestPBinGetProofRefusesBin(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	cfg := &rpccfg.EthApiConfig{
		GasCap:                      5000000,
		FeeCap:                      ethconfig.Defaults.RPCTxFeeCap,
		ReturnDataLimit:             100_000,
		MaxGetProofRewindBlockCount: 1,
		SubscribeLogsChannelSize:    128,
		RpcTxSyncDefaultTimeout:     20 * time.Second,
		RpcTxSyncMaxTimeout:         1 * time.Minute,
	}
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, cfg, log.New())

	// The chain above is built on the hex trie; only the proof call runs under bin.
	orig := statecfg.ExperimentalBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment = orig })
	statecfg.ExperimentalBinCommitment = true

	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	_, err := api.GetProof(t.Context(), common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"), nil, &latest)
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)

	req := SimulationRequest{BlockStateCalls: []SimulatedBlock{{Calls: []ethapi.CallArgs{{}}}}}
	_, err = api.SimulateV1(t.Context(), req, latest)
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
}
