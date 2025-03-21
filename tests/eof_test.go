// Copyright 2024 The Erigon Authors
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

package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/vm"
)

func TestEOFValidation(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	et := new(testMatcher)

	dir := filepath.Join(".", "osaka-eof/eof_tests/osaka/eip7692_eof_v1")

	et.walk(t, dir, func(t *testing.T, name string, test *EOFTest) {
		// import pre accounts & construct test genesis block & state root
		if err := et.checkFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
		fmt.Println("---------------------------------")
	})
}

func TestEOFStateTest(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	st := new(testMatcher)

	dir := filepath.Join(".", "osaka-eof/state_tests/osaka/eip7692_eof_v1")

	dirs := datadir.New(t.TempDir())
	db, _ := temporaltest.NewTestDB(t, dirs)
	st.walk(t, dir, func(t *testing.T, name string, test *StateTest) {
		for _, subtest := range test.Subtests() {
			subtest := subtest
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			t.Run(key, func(t *testing.T) {
				withTrace(t, func(vmconfig vm.Config) error {
					tx, err := db.BeginRw(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					_, _, err = test.Run(tx, subtest, vmconfig, dirs)
					tx.Rollback()
					if err != nil && len(test.json.Post[subtest.Fork][subtest.Index].ExpectException) > 0 {
						// Ignore expected errors
						return nil
					}
					return st.checkFailure(t, err)
				})
			})
		}
	})
}

func TestEOFBlockchain(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testMatcher)

	dir := filepath.Join(".", "osaka-eof/blockchain_tests/osaka/eip7692_eof_v1")

	bt.walk(t, dir, func(t *testing.T, name string, test *BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t, true)); err != nil {
			t.Error(err)
		}
		fmt.Println("---------------------------------")
	})
}
