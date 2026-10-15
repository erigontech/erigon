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
package recording_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/eez/l1/recording"
)

// TestRecordingConformance is T-L1-1 against a capture of the Erigon L1 the
// RPC reader is tested on.
func TestRecordingConformance(t *testing.T) {
	_, model := l1test.ErigonL1(t)
	dir := t.TempDir()
	require.NoError(t, recording.Write(dir, model))
	recorded, err := recording.Load(dir)
	require.NoError(t, err)
	l1test.Conformance(t, recorded, model, l1test.SampleFilters)
}

func TestRecordingKeepsOrphansAndFinality(t *testing.T) {
	model := l1test.SampleChain(t)
	dir := t.TempDir()
	require.NoError(t, recording.Write(dir, model))
	recorded, err := recording.Load(dir)
	require.NoError(t, err)
	require.Len(t, recorded.Orphans(), len(model.Orphans()))
	l1test.Conformance(t, recorded, model, l1test.SampleFilters)
}

func TestRecordingRejects(t *testing.T) {
	model := l1test.SampleChain(t)
	for _, tc := range []struct {
		name, file, old, new string
	}{
		{"unknown field", recording.HeadersFile, `"chainId"`, `"chain_id"`},
		{"broken chain", recording.HeadersFile, `"parentHash": "0x`, `"parentHash": "0x00`},
		{"log outside the blocks", recording.LogsFile, `"blockNumber": "0x1"`, `"blockNumber": "0x2"`},
		{"transaction of another position", recording.TxsFile, `"index": "0x0"`, `"index": "0x1"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, recording.Write(dir, model))
			path := filepath.Join(dir, tc.file)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Contains(t, string(data), tc.old)
			require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(data), tc.old, tc.new, 1)), 0o644))
			_, err = recording.Load(dir)
			require.Error(t, err)
		})
	}
}
