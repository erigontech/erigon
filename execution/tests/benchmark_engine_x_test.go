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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
)

var engineXDir = filepath.Join(eestDir, "benchmark", "blockchain_tests_engine_x")

var eestPreAllocDir = filepath.Join(engineXDir, "pre_alloc")

// BenchmarkEngineXInstruction measures payload execution time per instruction category,
// excluding one-time setup (genesis write, node startup, DB init).
// Usage: BENCH_ENGINE_X_MANUAL_ALLOW=true go test -run='^$' -bench BenchmarkEngineXInstruction -benchtime=1x -timeout 60m ./execution/tests/
func BenchmarkEngineXInstruction(b *testing.B) {
	benchmarkEngineX(b, eestPreAllocDir, filepath.Join(engineXDir, "benchmark", "compute", "instruction"))
}

func BenchmarkEngineXPrecompile(b *testing.B) {
	benchmarkEngineX(b, eestPreAllocDir, filepath.Join(engineXDir, "benchmark", "compute", "precompile"))
}

func BenchmarkEngineXScenario(b *testing.B) {
	benchmarkEngineX(b, eestPreAllocDir, filepath.Join(engineXDir, "benchmark", "compute", "scenario"))
}

// BenchmarkEngineXExtraFixtures runs locally stored fixtures from benchmark-fixtures/.
// Usage: BENCH_ENGINE_X_MANUAL_ALLOW=true go test -run='^$' -bench BenchmarkEngineXExtraFixtures -benchtime=1x -timeout 60m ./execution/tests/
func BenchmarkEngineXExtraFixtures(b *testing.B) {
	benchmarkEngineX(b, filepath.Join("benchmark-fixtures", "pre_alloc"), filepath.Join("benchmark-fixtures", "benchmark"))
}

type testEntry struct {
	name string
	def  EngineXTestDefinition
}

// benchmarkEngineX walks the given directories for engine-x JSON fixtures,
// groups them by first-level subdirectory into sub-benchmarks, and executes them.
func benchmarkEngineX(b *testing.B, preAllocDir, testsDir string) {
	if !dbg.EnvBool("BENCH_ENGINE_X_MANUAL_ALLOW", false) {
		b.Skip("benchmark engine x tests are for manual use; enable via BENCH_ENGINE_X_MANUAL_ALLOW=true")
	}

	logger := testlog.Logger(b, log.LvlDebug)
	runner, err := NewEngineXTestRunner(b, logger, preAllocDir)
	require.NoError(b, err)

	subcategories := make(map[string][]testEntry)
	err = filepath.WalkDir(testsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var tests map[string]EngineXTestDefinition
		if err := json.Unmarshal(data, &tests); err != nil {
			return nil
		}
		rel := filepath.ToSlash(strings.TrimPrefix(path, testsDir+string(filepath.Separator)))
		subcat := strings.SplitN(rel, "/", 2)[0]
		for name, def := range tests {
			subcategories[subcat] = append(subcategories[subcat], testEntry{name: name, def: def})
		}
		return nil
	})
	require.NoError(b, err)

	// Pre-create all testers (not timed).
	seen := make(map[[2]string]bool)
	for _, entries := range subcategories {
		for _, e := range entries {
			k := [2]string{string(e.def.Fork), string(e.def.PreAllocHash)}
			if !seen[k] {
				seen[k] = true
				require.NoError(b, runner.EnsureTester(e.def))
			}
		}
	}

	b.ResetTimer()
	for subcat, entries := range subcategories {
		entries := entries
		var totalGas uint64
		for _, e := range entries {
			totalGas += e.def.GasUsed()
		}
		b.Run(subcat, func(b *testing.B) {
			for _, e := range entries {
				e := e
				gasUsed := e.def.GasUsed()
				b.Run(e.name, func(b *testing.B) {
					start := time.Now()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						require.NoError(b, runner.Execute(b.Context(), e.def), e.name)
					}
					b.StopTimer()
					elapsed := uint64(time.Since(start))
					if elapsed > 0 && gasUsed > 0 {
						mgasps := (100 * 1000 * gasUsed * uint64(b.N)) / elapsed
						b.ReportMetric(float64(mgasps)/100, "Mgas/s")
					}
				})
			}
			if totalGas > 0 {
				b.ReportMetric(float64(totalGas)/float64(b.Elapsed().Nanoseconds())*1e3, "Mgas/s")
			}
		})
	}
}
