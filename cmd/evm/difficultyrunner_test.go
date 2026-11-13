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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunDifficultyTest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "difficulty.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"difficultyFrontier": {
			"Frontier": {
				"DifficultyTest1": {
					"currentBlockNumber": "0x0186a0",
					"currentDifficulty": "0x6a8d5758858f3fb6",
					"currentTimestamp": "0x75311a08b",
					"parentDifficulty": "0x6a8007579a9bec39",
					"parentTimestamp": "0x75311a08a",
					"parentUncles": "0x00"
				}
			}
		}
	}`), 0o600))
	filter, err := compileTestFilter(".*", nil)
	require.NoError(t, err)

	results, err := runDifficultyTest(path, filter)
	require.NoError(t, err)
	require.Equal(t, []testResult{{
		Name: "difficultyFrontier/Frontier/DifficultyTest1",
		Pass: true,
	}}, results)
}
