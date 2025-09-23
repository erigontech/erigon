// Copyright 2017 The go-ethereum Authors
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
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

func TestDifficulty(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	dt := new(testMatcher)
	dir := filepath.Join(legacyDir, "DifficultyTests")

	dt.walk(t, dir, func(t *testing.T, name string, superTest map[string]json.RawMessage) {
		for fork, rawTests := range superTest {
			if fork == "_info" {
				continue
			}
			var tests map[string]testutil.DifficultyTest
			if err := json.Unmarshal(rawTests, &tests); err != nil {
				t.Error(err)
				continue
			}

			cfg, ok := testforks.Forks[fork]
			if !ok {
				t.Error(testforks.UnsupportedForkError{Name: fork})
				continue
			}

			for subname, subtest := range tests {
				key := fmt.Sprintf("%s/%s", fork, subname)
				t.Run(key, func(t *testing.T) {
					if err := dt.checkFailure(t, subtest.Run(cfg)); err != nil {
						t.Error(err)
					}
				})
			}
		}
	})
}
