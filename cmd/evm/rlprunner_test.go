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

func TestRunRLPTest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rlp.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"small": {"in": 1, "out": "0x01"},
		"big": {
			"in": "#115792089237316195423570985008687907853269984665640564039457584007913129639936",
			"out": "0xa1010000000000000000000000000000000000000000000000000000000000000000"
		}
	}`), 0o600))
	filter, err := compileTestFilter(".*", nil)
	require.NoError(t, err)

	results, err := runRLPTest(path, filter)
	require.NoError(t, err)
	require.Equal(t, []testResult{
		{Name: "big", Pass: true},
		{Name: "small", Pass: true},
	}, results)
}
