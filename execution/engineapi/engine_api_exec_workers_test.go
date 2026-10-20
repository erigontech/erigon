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

package engineapi_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/node/ethconfig"
)

// An embedded caller configures its executor via ethconfig.Sync.ExecWorkerCount
// without touching dbg.Exec3Workers; eth.New must carry that resolved count into
// the node config so OpenDatabase floors the read-tx semaphore on the workers
// this node actually runs.
func TestEngineApiNodeConfigCarriesExecWorkerCount(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)

	workers := dbg.Exec3Workers + 7 // any value the process global cannot equal
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(cfg *ethconfig.Config) {
			cfg.Sync.ExecWorkerCount = workers
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	require.Equal(t, workers, eat.Node.Config().ExecWorkerCount)
}
