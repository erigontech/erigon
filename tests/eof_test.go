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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
)

func TestEOFValidation(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	et := new(testMatcher)

	dir := filepath.Join(".", "execution-spec-tests", "eof_tests")

	et.walk(t, dir, func(t *testing.T, name string, test *EOFTest) {
		// import pre accounts & construct test genesis block & state root
		if err := et.checkFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
		fmt.Println("---------------------------------")
	})
}
