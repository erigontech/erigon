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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	commondir "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/eez/l1"
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

// TestRecordingWithoutState: a recording without the state file loads and
// answers no state read, and rewriting a chain without state reads removes
// the file.
func TestRecordingWithoutState(t *testing.T) {
	ctx := context.Background()
	model := l1test.SampleChain(t)
	dir := t.TempDir()
	require.NoError(t, recording.Write(dir, model))
	require.NoError(t, commondir.RemoveFile(filepath.Join(dir, recording.StateFile)))
	recorded, err := recording.Load(dir)
	require.NoError(t, err)
	head, err := recorded.Head(ctx)
	require.NoError(t, err)
	_, err = recorded.CodeAt(ctx, l1test.Registry, head.Hash)
	require.ErrorIs(t, err, l1.ErrNotFound)
	_, err = recorded.CallAt(ctx, l1test.RollupsCall, head.Hash)
	require.ErrorIs(t, err, l1.ErrNotFound)

	dir = t.TempDir()
	require.NoError(t, recording.Write(dir, model))
	require.FileExists(t, filepath.Join(dir, recording.StateFile))
	require.NoError(t, recording.Write(dir, recorded))
	require.NoFileExists(t, filepath.Join(dir, recording.StateFile))
	_, err = recording.Load(dir)
	require.NoError(t, err)
}

func TestRecordingRejects(t *testing.T) {
	model := l1test.SampleChain(t)
	genesis := model.Canonical()[0].Header.Hash
	for _, tc := range []struct {
		name, file, old, new string
	}{
		{"unknown field", recording.HeadersFile, `"chainId"`, `"chain_id"`},
		{"broken chain", recording.HeadersFile, `"parentHash": "0x`, `"parentHash": "0x00`},
		{"log outside the blocks", recording.LogsFile, `"blockNumber": "0x1"`, `"blockNumber": "0x2"`},
		{"transaction of another position", recording.TxsFile, `"index": "0x0"`, `"index": "0x1"`},
		{"unknown state field", recording.StateFile, `"return"`, `"result"`},
		{"state of an unrecorded block", recording.StateFile, `"blockHash": "` + genesis.Hex() + `"`, `"blockHash": "` + common.Hash{0xde, 0xad}.Hex() + `"`},
		{"code recorded twice", recording.StateFile, `"address": "0x00000000000000000000000000000000000000cc"`, `"address": "0x00000000000000000000000000000000000000aa"`},
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
