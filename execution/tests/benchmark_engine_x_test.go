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
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

// BenchmarkEngineXInstruction measures payload execution time per instruction category,
// excluding one-time setup (genesis write, node startup, DB init).
// Usage: BENCH_ENGINE_X_MANUAL_ALLOW=true go test -run='^$' -bench BenchmarkEngineXInstruction -benchtime=1x -timeout 60m ./execution/tests/
func BenchmarkEngineXInstruction(b *testing.B) {
	benchmarkEngineX(b, "instruction")
}

func BenchmarkEngineXPrecompile(b *testing.B) {
	benchmarkEngineX(b, "precompile")
}

func BenchmarkEngineXScenario(b *testing.B) {
	benchmarkEngineX(b, "scenario")
}

func benchmarkEngineX(b *testing.B, category string) {
	if !dbg.EnvBool("BENCH_ENGINE_X_MANUAL_ALLOW", false) {
		b.Skip("benchmark engine x tests are for manual use; enable via BENCH_ENGINE_X_MANUAL_ALLOW=true")
	}

	ctx := b.Context()
	logger := testlog.Logger(b, log.LvlDebug)
	engineXDir := filepath.Join("..", "..", "test-fixtures-cache", "eest_benchmark", "fixtures", "blockchain_tests_engine_x")
	preAllocDir := filepath.Join(engineXDir, "pre_alloc")

	// Layout: blockchain_tests_engine_x/for_osaka_at_<NNNN>M/compute/<category>/<subcat>/<file>.json
	gasEntries, err := os.ReadDir(engineXDir)
	require.NoError(b, err)
	var gasLimits []string
	for _, e := range gasEntries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "for_") {
			gasLimits = append(gasLimits, e.Name())
		}
	}
	sort.Strings(gasLimits)
	require.NotEmpty(b, gasLimits, "no for_*_at_<gas>M dirs under %s", engineXDir)

	runner, err := engineapitester.NewEngineXTestRunner(ctx, logger, preAllocDir)
	require.NoError(b, err)
	b.Cleanup(func() {
		err := runner.Close()
		require.NoError(b, err)
	})

	type testEntry struct {
		name string
		def  engineapitester.EngineXTestDefinition
	}
	type subcatEntries struct {
		name    string
		entries []testEntry
	}
	allTests := make(map[string][]subcatEntries)
	for _, gas := range gasLimits {
		testsDir := filepath.Join(engineXDir, gas, "compute", category)
		bySubcat := make(map[string][]testEntry)
		err = filepath.WalkDir(testsDir, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var tests map[string]engineapitester.EngineXTestDefinition
			if err := json.Unmarshal(data, &tests); err != nil {
				return nil
			}
			rel := filepath.ToSlash(strings.TrimPrefix(path, testsDir+string(filepath.Separator)))
			subcat := strings.SplitN(rel, "/", 2)[0]
			for name, def := range tests {
				bySubcat[subcat] = append(bySubcat[subcat], testEntry{name: name, def: def})
			}
			return nil
		})
		require.NoError(b, err)
		subcatNames := make([]string, 0, len(bySubcat))
		for k := range bySubcat {
			subcatNames = append(subcatNames, k)
		}
		sort.Strings(subcatNames)
		ordered := make([]subcatEntries, 0, len(subcatNames))
		for _, k := range subcatNames {
			ordered = append(ordered, subcatEntries{name: k, entries: bySubcat[k]})
		}
		allTests[gas] = ordered
	}

	// Pre-create all testers (not timed).
	seen := make(map[[2]string]bool)
	for _, ordered := range allTests {
		for _, sc := range ordered {
			for _, e := range sc.entries {
				k := [2]string{string(e.def.Fork), string(e.def.PreAllocHash)}
				if !seen[k] {
					seen[k] = true
					require.NoError(b, runner.EnsureTester(e.def))
				}
			}
		}
	}

	b.ResetTimer()
	for _, gas := range gasLimits {
		ordered := allTests[gas]
		b.Run(gas, func(b *testing.B) {
			for _, sc := range ordered {
				sc := sc
				b.Run(sc.name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						for _, e := range sc.entries {
							require.NoError(b, runner.Execute(ctx, e.def), "%s/%s/%s", gas, sc.name, e.name)
						}
					}
				})
			}
		})
	}
}
