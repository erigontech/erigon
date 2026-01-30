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

package executiontests

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
)

func TestBenchmarkEngineX(t *testing.T) {
	if !dbg.EnvBool("BENCH_ENGINE_X_MANUAL_ALLOW", false) {
		t.Skip("benchmark engine x tests are for manual use; enable via BENCH_ENGINE_X_MANUAL_ALLOW=true")
	}
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	logger := testlog.Logger(t, log.LvlDebug)
	engineXDir := filepath.Join(eestDir, "benchmark", "blockchain_tests_engine_x")
	preAllocDir := filepath.Join(engineXDir, "pre_alloc")
	engineXRunner, err := NewEngineXTestRunner(t, logger, preAllocDir)
	require.NoError(t, err)
	testsDir := filepath.Join(engineXDir, "benchmark")
	tm := new(testMatcher)
	// if wanting to run only a particular test, use this, e.g.:
	//tm.whitelist(".*log\\.json")
	//tm.whitelist(".*codecopy\\.json")
	tm.walk(t, testsDir, func(t *testing.T, name string, test EngineXTestDefinition) {
		err := engineXRunner.Run(t.Context(), test)
		require.NoError(t, err)
	})
}
