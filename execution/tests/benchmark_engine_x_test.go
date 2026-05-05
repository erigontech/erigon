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
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
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

	logger := testlog.Logger(b, log.LvlDebug)

	const (
		preAllocPrefix = "fixtures/blockchain_tests_engine_x/pre_alloc/"
		computePrefix  = "fixtures/blockchain_tests_engine_x/benchmark/compute/"
	)
	categoryPrefix := computePrefix + category + "/"

	type testEntry struct {
		name string
		def  engineapitester.EngineXTestDefinition
	}
	preAllocs := make(map[engineapitester.PreAllocHash]*engineapitester.PreAlloc)
	subcategories := make(map[string][]testEntry)

	tarPath := filepath.Join(eestDir, "fixtures_benchmark.tar.gz")
	f, err := os.Open(tarPath)
	require.NoError(b, err)
	defer f.Close()
	gzr, err := gzip.NewReader(f)
	require.NoError(b, err)
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(b, err)
		if hdr.Typeflag != tar.TypeReg || filepath.Ext(hdr.Name) != ".json" {
			continue
		}
		switch {
		case strings.HasPrefix(hdr.Name, preAllocPrefix):
			data, err := io.ReadAll(tr)
			require.NoError(b, err)
			var preAlloc engineapitester.PreAlloc
			require.NoError(b, json.Unmarshal(data, &preAlloc))
			base := filepath.Base(hdr.Name)
			hash := strings.TrimSuffix(base, filepath.Ext(base))
			preAllocs[engineapitester.PreAllocHash(hash)] = &preAlloc
		case strings.HasPrefix(hdr.Name, categoryPrefix):
			data, err := io.ReadAll(tr)
			require.NoError(b, err)
			var tests map[string]engineapitester.EngineXTestDefinition
			if err := json.Unmarshal(data, &tests); err != nil {
				continue
			}
			rel := strings.TrimPrefix(hdr.Name, categoryPrefix)
			subcat := strings.SplitN(rel, "/", 2)[0]
			for name, def := range tests {
				subcategories[subcat] = append(subcategories[subcat], testEntry{name: name, def: def})
			}
		}
	}

	runner := engineapitester.NewEngineXTestRunner(b, logger, preAllocs)

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
		b.Run(subcat, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, e := range entries {
					require.NoError(b, runner.Execute(b.Context(), e.def), "%s/%s", subcat, e.name)
				}
			}
		})
	}
}
