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
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing/tracers/logger"
	"github.com/erigontech/erigon/execution/vm"
)

func TestStateCornerCases(t *testing.T) {
	t.Parallel()

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // it's too slow on win and stops on macos, need generally improve speed of this tests
	}

	st := new(testMatcher)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	st.walk(t, cornersDir, func(t *testing.T, name string, test *testutil.StateTest) {
		for _, subtest := range test.Subtests() {
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			t.Run(key, func(t *testing.T) {
				withTrace(t, func(vmconfig vm.Config) error {
					tx, err := db.BeginTemporalRw(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					_, _, err = test.Run(t, tx, subtest, vmconfig, dirs)
					tx.Rollback()
					if err != nil && len(test.Json.Post[subtest.Fork][subtest.Index].ExpectException) > 0 {
						// Ignore expected errors
						return nil
					}
					return st.checkFailure(t, err)
				})
			})
		}
	})
}

func TestState(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // it's too slow on win and stops on macos, need generally improve speed of this tests
	}
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	st := new(testMatcher)
	// Corresponds to GeneralStateTests from ethereum/tests:
	// see https://github.com/ethereum/execution-spec-tests/releases/tag/v5.0.0
	dir := filepath.Join(eestDir, "state_tests", "static", "state_tests")

	// Slow tests
	st.slow(`^stPreCompiledContracts/precompsEIP2929Cancun`)

	// Very slow tests
	st.skipLoad(`^stTimeConsuming/`)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	st.walk(t, dir, func(t *testing.T, name string, test *testutil.StateTest) {
		for _, subtest := range test.Subtests() {
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			t.Run(key, func(t *testing.T) {
				withTrace(t, func(vmconfig vm.Config) error {
					tx, err := db.BeginTemporalRw(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					_, _, err = test.Run(t, tx, subtest, vmconfig, dirs)
					tx.Rollback()
					if err != nil && len(test.Json.Post[subtest.Fork][subtest.Index].ExpectException) > 0 {
						// Ignore expected errors
						return nil
					}
					return st.checkFailure(t, err)
				})
			})
		}
	})
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
	tracer := logger.NewJSONLogger(&logger.LogConfig{DisableMemory: true}, w)
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
