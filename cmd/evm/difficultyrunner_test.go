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

func TestRunBasicDifficultyTest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "difficulty.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"preExpDiffIncrease": {
			"parentTimestamp": "42",
			"parentDifficulty": "1000000",
			"currentTimestamp": "43",
			"currentBlockNumber": "42",
			"currentDifficulty": "1000488",
			"parentUncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
		}
	}`), 0o600))
	filter, err := compileTestFilter(".*", nil)
	require.NoError(t, err)

	results, err := runDifficultyTest(path, filter)
	require.NoError(t, err)
	require.Equal(t, []testResult{{
		Name: "difficulty/preExpDiffIncrease",
		Pass: true,
	}}, results)
}

func TestBasicDifficultyConfig(t *testing.T) {
	u64 := func(value uint64) *uint64 {
		return &value
	}
	for _, test := range []struct {
		file           string
		homestead      *uint64
		byzantium      *uint64
		constantinople *uint64
	}{
		{"difficulty.json", u64(1_150_000), u64(4_370_000), u64(7_280_000)},
		{"difficultyCustomHomestead.json", u64(0), nil, nil},
		{"difficultyCustomMainNetwork.json", u64(1_150_000), u64(4_370_000), u64(7_280_000)},
		{"difficultyMainNetwork.json", u64(1_150_000), u64(4_370_000), u64(7_280_000)},
		{"difficultyRopsten.json", u64(0), u64(1_700_000), u64(4_230_000)},
	} {
		t.Run(test.file, func(t *testing.T) {
			config, ok := basicDifficultyConfig(test.file)
			require.True(t, ok)
			require.Equal(t, test.homestead, config.HomesteadBlock)
			require.Equal(t, test.byzantium, config.ByzantiumBlock)
			require.Equal(t, test.constantinople, config.ConstantinopleBlock)
		})
	}
}
