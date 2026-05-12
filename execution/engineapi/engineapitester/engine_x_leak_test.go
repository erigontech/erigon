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

//go:build leak

package engineapitester_test

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

// TestEngineXLeakLoop creates and evicts a single tester repeatedly and
// samples per-iteration memory + goroutine + mmap counts. If any of those
// grow with iteration count, the cleanup chain is leaking that resource.
//
// Run with:
//
//	ENGINEX_PRE_ALLOC_DIR=/tmp/eest-fixtures/fixtures/blockchain_tests_engine_x/pre_alloc \
//	ENGINEX_PRE_ALLOC_HASH=0x1d04680031171f51 \
//	ENGINEX_FORK=Prague \
//	ENGINEX_LEAK_ITERS=20 \
//	go test -tags leak -run TestEngineXLeakLoop -v -count=1 -timeout 30m \
//	  ./execution/engineapi/engineapitester/
func TestEngineXLeakLoop(t *testing.T) {
	preAllocDir := os.Getenv("ENGINEX_PRE_ALLOC_DIR")
	if preAllocDir == "" {
		t.Skip("set ENGINEX_PRE_ALLOC_DIR to a directory containing the pre-alloc JSON for the chosen hash")
	}
	hashStr := os.Getenv("ENGINEX_PRE_ALLOC_HASH")
	if hashStr == "" {
		t.Skip("set ENGINEX_PRE_ALLOC_HASH (e.g. 0x1d04680031171f51)")
	}
	forkStr := os.Getenv("ENGINEX_FORK")
	if forkStr == "" {
		forkStr = "Prague"
	}
	iterations := 20
	if v := os.Getenv("ENGINEX_LEAK_ITERS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)
		iterations = n
	}

	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlCrit)

	runner, err := engineapitester.NewEngineXTestRunner(ctx, logger, preAllocDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := runner.Close()
		require.NoError(t, err)
	})

	def := engineapitester.EngineXTestDefinition{
		Fork:         engineapitester.Fork(forkStr),
		PreAllocHash: engineapitester.PreAllocHash(hashStr),
	}

	fmt.Printf("iter,rss_mb,vm_gb,maps_lines,goroutines,heap_alloc_mb\n")
	printSample(t, -1) // baseline before any tester

	for i := 0; i < iterations; i++ {
		err := runner.EnsureTester(def)
		require.NoError(t, err)
		err = runner.Evict(def.Fork, def.PreAllocHash)
		require.NoError(t, err)
		printSample(t, i)
	}

	// Dump full goroutine stacks so the leaked ones (which appear N times,
	// once per iteration) stand out. Top-of-stack functions repeated many
	// times point at the goroutines that aren't being terminated.
	dumpDir := os.Getenv("ENGINEX_LEAK_DUMP_DIR")
	if dumpDir == "" {
		dumpDir = os.TempDir()
	}
	buf := make([]byte, 1<<22) // 4 MiB
	n := runtime.Stack(buf, true)
	dumpPath := dumpDir + "/enginex-leak-goroutines.txt"
	err = os.WriteFile(dumpPath, buf[:n], 0o644)
	require.NoError(t, err)
	fmt.Printf("\nGoroutine dump written to %s (%d bytes, %d goroutines)\n", dumpPath, n, runtime.NumGoroutine())
}

func printSample(t *testing.T, iter int) {
	t.Helper()
	// Give recently signalled goroutines time to actually exit before counting.
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	statBytes, err := os.ReadFile("/proc/self/status")
	require.NoError(t, err)
	rssKb := procStatusKb(string(statBytes), "VmRSS:")
	vmKb := procStatusKb(string(statBytes), "VmSize:")

	mapsBytes, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	mapsLines := bytes.Count(mapsBytes, []byte{'\n'})

	fmt.Printf("%d,%d,%d,%d,%d,%d\n",
		iter,
		rssKb/1024,
		vmKb/1024/1024,
		mapsLines,
		runtime.NumGoroutine(),
		ms.HeapAlloc/(1024*1024),
	)
}

func procStatusKb(s, key string) int {
	for _, line := range strings.Split(s, "\n") {
		if !strings.HasPrefix(line, key) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		n, _ := strconv.Atoi(fields[1])
		return n
	}
	return 0
}
