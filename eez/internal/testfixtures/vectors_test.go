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

package testfixtures

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestLoadRollingHashVectors(t *testing.T) {
	v, err := LoadRollingHashVectors()
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, v.L1Seed)
	require.NotEqual(t, v.L1Seed, v.L2Seed)
}

func TestLoadPublicInputsVectors(t *testing.T) {
	v, err := LoadPublicInputsVectors()
	require.NoError(t, err)
	names := make([]string, 0, len(v.Vectors))
	for _, c := range v.Vectors {
		names = append(names, c.Name)
	}
	require.Equal(t, []string{
		"empty_entry_single_rollup_single_ps",
		"full_entry_and_calldata_single_rollup_single_ps",
		"multi_rollup_multi_ps",
		"all_shared_input_dimensions",
	}, names)
}

// mutateHeader rewrites one top-level field of a vendored vector file.
func mutateHeader(t *testing.T, file, field string, value any) []byte {
	t.Helper()
	data, err := os.ReadFile(Path(vectorsDir, file))
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(data, &doc))
	doc[field] = value
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	return out
}

func TestVectorHeadersAreChecked(t *testing.T) {
	parsers := map[string]func([]byte) error{
		rollingHashVectorsFile:  func(b []byte) error { _, err := ParseRollingHashVectors(b); return err },
		publicInputsVectorsFile: func(b []byte) error { _, err := ParsePublicInputsVectors(b); return err },
	}
	for file, parse := range parsers {
		for _, tc := range []struct {
			field string
			value any
		}{
			{"schemaVersion", 2},
			{"protocolCommit", "6ae4110"},
			{"solidityOracle", "contracts/test/Other.t.sol"},
			{"unexpectedField", true},
		} {
			t.Run(file+"/"+tc.field, func(t *testing.T) {
				err := parse(mutateHeader(t, file, tc.field, tc.value))
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), file), err.Error())
			})
		}
	}
}

func TestPublicInputsVectorConsistencyIsChecked(t *testing.T) {
	data, err := os.ReadFile(Path(vectorsDir, publicInputsVectorsFile))
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(data, &doc))
	vectors := doc["vectors"].([]any)
	vectors[0].(map[string]any)["proofSystemCount"] = 2
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	_, err = ParsePublicInputsVectors(out)
	require.ErrorContains(t, err, "expected hashes")
}
