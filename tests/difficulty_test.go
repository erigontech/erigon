// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

//go:build integration

package tests

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestDifficulty(t *testing.T) {
	//t.Parallel()

	dt := new(testMatcher)

	dt.walk(t, difficultyTestDir, func(t *testing.T, name string, superTest map[string]json.RawMessage) {
		for fork, rawTests := range superTest {
			if fork == "_info" {
				continue
			}
			var tests map[string]DifficultyTest
			if err := json.Unmarshal(rawTests, &tests); err != nil {
				t.Error(err)
				continue
			}

			cfg, ok := Forks[fork]
			if !ok {
				t.Error(UnsupportedForkError{fork})
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
