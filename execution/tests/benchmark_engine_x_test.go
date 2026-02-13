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

func TestBenchmarkEngineXInstruction(t *testing.T) {
	// if wanting to run only a particular test, pass a whitelist, e.g.
	//benchmarkCategory(t, "instruction", ".*log\\.json", nil)
	//benchmarkCategory(t, "instruction", ".*codecopy\\.json", nil)
	benchmarkCategory(t, "instruction", "", nil)
}

func TestBenchmarkEngineXPrecompile(t *testing.T) {
	benchmarkCategory(t, "precompile", "", nil)
}

func TestBenchmarkEngineXScenario(t *testing.T) {
	benchmarkCategory(t, "scenario", "", nil)
}

func benchmarkCategory(t *testing.T, category string, whitelist string, skipload []string) {
	if !dbg.EnvBool("BENCH_ENGINE_X_MANUAL_ALLOW", false) {
		t.Skip("benchmark engine x tests are for manual use; enable via BENCH_ENGINE_X_MANUAL_ALLOW=true")
	}
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	logger := testlog.Logger(t, log.LvlDebug)
	engineXDir := filepath.Join(eestDir, "benchmark", "blockchain_tests_engine_x")
	testsDir := filepath.Join(engineXDir, "benchmark", "compute", category)
	preAllocDir := filepath.Join(engineXDir, "pre_alloc")
	engineXRunner, err := NewEngineXTestRunner(t, logger, preAllocDir)
	require.NoError(t, err)
	tm := testMatcher{
		// we re-use the same engine for tests,
		// and we want to do sequential new payloads
		// without getting SYNCING responses
		noparallel: true,
	}
	if whitelist != "" {
		tm.whitelist(whitelist)
	}
	for _, s := range skipload {
		tm.skipLoad(s)
	}
	tm.walk(t, testsDir, func(t *testing.T, name string, test EngineXTestDefinition) {
		err := engineXRunner.Run(t.Context(), test)
		require.NoError(t, err)
	})
}
