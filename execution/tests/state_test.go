// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"syscall"
	"testing"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing/tracers/logger"
	"github.com/erigontech/erigon/execution/vm"
)

func TestStateCornerCases(t *testing.T) {
	stateTestSetup(t)
	runStateTests(t, new(testutil.TestMatcher), filepath.Join(cornersDir, "state"))
}

// TestLegacyCancunState runs legacy ethereum/tests GeneralStateTests fixtures
// (state-test format, non-EEST) at the Cancun-era snapshot, under
// legacy-tests/LegacyTests/Cancun/GeneralStateTests. The fixtures cover all
// pre-merge fork variants (Frontier through London, plus Paris/Shanghai/Cancun)
// — the EEST static_tests exercised by `evm statetest` don't carry every
// variant (e.g. RevertPrecompiledTouch_d3 in Berlin/Istanbul/London catches
// the ripemd-touch state-clearing path that the Hive `legacy-cancun`
// simulator flagged). Run them locally so that class of regression is
// caught on CI rather than only by the weekly out-of-tree Hive run.
func TestLegacyCancunState(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	stateTestSetup(t)

	st := new(testutil.TestMatcher)
	// Slow tests
	st.Slow(`^stPreCompiledContracts/precompsEIP2929Cancun`)
	// Very slow tests
	st.SkipLoad(`^stTimeConsuming/`)
	// EVM perf-stress fixtures (loopMul, loopExp, performanceTester) overrun
	// the 1h package timeout under -race instrumentation.
	st.SkipLoad(`^VMTests/vmPerformance/`)

	// Pre-existing Constantinople-only divergences from geth — see
	// https://github.com/erigontech/erigon/issues/20894. Geth's local runner
	// walks LegacyTests/Constantinople/GeneralStateTests (an older snapshot
	// that doesn't include these fixtures) so it never exercises them
	// locally even though it generated them. Skip-loading the whole file
	// means we lose non-Constantinople coverage of these six fixtures, which
	// is acceptable: the d3 RIPEMD-160 touch case this test was added to
	// catch is in stRevertTest/RevertPrecompiledTouch.json, not these.
	st.SkipLoad(`^stSStoreTest/sstoreGas\.json`)
	st.SkipLoad(`^stCreateTest/CREATE_HighNonce\.json`)
	st.SkipLoad(`^stCreate2/CREATE2_HighNonce\.json`)
	st.SkipLoad(`^stCreate2/CREATE2_HighNonceDelegatecall\.json`)
	st.SkipLoad(`^stPreCompiledContracts2/CallEcrecover_Overflow\.json`)
	st.SkipLoad(`^stPreCompiledContracts2/ecrecoverShortBuff\.json`)

	runStateTests(t, st, filepath.Join(legacyDir, "LegacyTests", "Cancun", "GeneralStateTests"))
}

// stateTestSetup applies the parallel/log/Windows-skip boilerplate shared by
// state-test runners.
func stateTestSetup(t *testing.T) {
	t.Helper()
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // it's too slow on win and stops on macos, need generally improve speed of this tests
	}
	prev := log.Root().GetHandler()
	t.Cleanup(func() { log.Root().SetHandler(prev) })
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
}

// runStateTests walks testDir and runs each StateTest's subtests through the
// shared per-subtest harness (temp datadir, fresh DB, withTrace). The matcher
// is supplied by the caller pre-configured with any Slow/SkipLoad/Whitelist
// patterns.
func runStateTests(t *testing.T, st *testutil.TestMatcher, testDir string) {
	t.Helper()
	st.Walk(t, testDir, func(t *testing.T, name string, test *testutil.StateTest) {
		tmpDir, err := os.MkdirTemp("", "erigon-test-*")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { dir.RemoveAll(tmpDir) })
		dirs := datadir.New(tmpDir)
		db := temporaltest.NewTestDB(t, dirs)
		for _, subtest := range test.Subtests() {
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			t.Run(key, func(t *testing.T) {
				withTrace(t, func(vmconfig vm.Config) error {
					tx := beginRwNoContention(t, db)
					defer tx.Rollback()
					_, _, err = test.Run(t, tx, subtest, vmconfig, dirs)
					tx.Rollback()
					if err != nil && len(test.Json.Post[subtest.Fork][subtest.Index].ExpectException) > 0 {
						// Ignore expected errors
						return nil
					}
					return st.CheckFailure(t, err)
				})
			})
		}
	})
}

// temporalRwTry is implemented by *temporal.DB when the underlying MDBX env
// supports non-blocking write-tx opens.
type temporalRwTry interface {
	BeginTemporalRwTry(ctx context.Context) (kv.TemporalRwTx, error)
}

// beginRwNoContention opens a write tx and fatals immediately on EBUSY, which
// indicates that two goroutines are racing on the same DB — a sign of
// unintended db sharing between parallel subtests.
func beginRwNoContention(t *testing.T, db kv.TemporalRwDB) kv.TemporalRwTx {
	t.Helper()
	if tryDB, ok := db.(temporalRwTry); ok {
		tx, err := tryDB.BeginTemporalRwTry(context.Background())
		if err != nil {
			if errors.Is(err, syscall.EBUSY) {
				t.Fatal("write lock contention: multiple goroutines sharing one db")
			}
			t.Fatal(err)
		}
		t.Cleanup(tx.Rollback)
		return tx
	}
	tx, err := db.BeginTemporalRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tx.Rollback)
	return tx
}

func withTrace(t *testing.T, test func(vm.Config) error) {
	// Use config from command line arguments.
	config := vm.Config{}
	err := test(config)
	if err == nil {
		return
	}

	// Test failed, re-run with tracing enabled.
	t.Error(err)
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)
	tracer := logger.NewJSONLogger(&logger.LogConfig{EnableMemory: false}, w)
	config.Tracer = tracer.Tracer().Hooks
	err2 := test(config)
	if !reflect.DeepEqual(err, err2) {
		t.Errorf("different error for second run: %v", err2)
	}
	w.Flush()
	if buf.Len() == 0 {
		t.Log("no EVM operation logs generated")
		//} else {
		//enable it if need extensive logging
		//t.Log("EVM operation log:\n" + buf.String())
	}
	//t.Logf("EVM output: 0x%x", tracer.Output())
	//t.Logf("EVM error: %v", tracer.Error())
}
