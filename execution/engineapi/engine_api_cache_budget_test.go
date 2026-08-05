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

	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

// Node close must return every cache reservation to the process-wide envelope;
// otherwise each per-fixture node in a test binary leaks its slice of
// cachebudget.Global and later caches size against phantom concurrency.
func TestEngineApiNodeCloseReleasesCacheBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)

	usedBefore := cachebudget.Global.Used()
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	require.Greater(t, cachebudget.Global.Used(), usedBefore,
		"a running node must hold cache-budget reservations")

	require.NoError(t, eat.Close())
	require.Equal(t, usedBefore, cachebudget.Global.Used(),
		"node close must release every cache-budget reservation")
}
