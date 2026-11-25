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

package commitment

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrieTraceSaveLoad(t *testing.T) {
	t.Parallel()

	tt := &TrieTrace{
		Branches: map[string]string{
			"0a0b": "deadbeef",
		},
		Accounts: map[string]string{
			"68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9": "050100000000000004",
		},
		Storages: map[string]string{
			"8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed": "100204010000",
		},
		Updates: []TraceKeyUpdate{
			{
				PlainKey: "68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9",
				Update:   "050100000000000004",
			},
			{
				PlainKey: "8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed",
				Update:   "100204010000",
			},
		},
	}

	tracePath := filepath.Join(t.TempDir(), "test_trace.toml")
	err := tt.Save(tracePath)
	require.NoError(t, err)

	loaded, err := LoadTrieTrace(tracePath)
	require.NoError(t, err)

	require.Equal(t, tt.Branches, loaded.Branches)
	require.Equal(t, tt.Accounts, loaded.Accounts)
	require.Equal(t, tt.Storages, loaded.Storages)
	require.Equal(t, len(tt.Updates), len(loaded.Updates))
	for i := range tt.Updates {
		require.Equal(t, tt.Updates[i].PlainKey, loaded.Updates[i].PlainKey)
		require.Equal(t, tt.Updates[i].Update, loaded.Updates[i].Update)
	}
}

func TestTrieTraceNewFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	tt := &TrieTrace{
		BlockNum:  12345,
		TxNum:     67890,
		Error:     "test error: something went wrong",
		TrieState: "deadbeefcafebabe",
		Branches: map[string]string{
			"0a0b": "deadbeef",
		},
		Accounts: map[string]string{
			"68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9": "050100000000000004",
		},
		Storages: map[string]string{},
		Updates: []TraceKeyUpdate{
			{
				PlainKey: "68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9",
				Update:   "050100000000000004",
			},
		},
		PutBranches: map[string]TraceBranchWrite{
			"0a0b": {
				PrevData: "",
				NewData:  "cafebabe",
			},
		},
	}

	tracePath := filepath.Join(t.TempDir(), "new_fields_trace.toml")
	err := tt.Save(tracePath)
	require.NoError(t, err)

	loaded, err := LoadTrieTrace(tracePath)
	require.NoError(t, err)

	require.Equal(t, tt.BlockNum, loaded.BlockNum)
	require.Equal(t, tt.TxNum, loaded.TxNum)
	require.Equal(t, tt.Error, loaded.Error)
	require.Equal(t, tt.TrieState, loaded.TrieState)
	require.Equal(t, tt.Branches, loaded.Branches)
	require.Equal(t, tt.Accounts, loaded.Accounts)
	require.Equal(t, len(tt.PutBranches), len(loaded.PutBranches))
	require.Equal(t, tt.PutBranches["0a0b"].NewData, loaded.PutBranches["0a0b"].NewData)
}

func TestTrieTraceBackwardsCompatibility(t *testing.T) {
	t.Parallel()

	oldFormatTOML := `[branches]
"0a0b" = "deadbeef"

[accounts]
"68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9" = "050100000000000004"

[storages]

[[updates]]
plain_key = "68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9"
update = "050100000000000004"
`
	tracePath := filepath.Join(t.TempDir(), "old_format_trace.toml")
	err := os.WriteFile(tracePath, []byte(oldFormatTOML), 0o600)
	require.NoError(t, err)

	loaded, err := LoadTrieTrace(tracePath)
	require.NoError(t, err)

	require.Equal(t, uint64(0), loaded.BlockNum)
	require.Equal(t, uint64(0), loaded.TxNum)
	require.Empty(t, loaded.Error)
	require.Empty(t, loaded.TrieState)
	require.Empty(t, loaded.PutBranches)

	require.Equal(t, "deadbeef", loaded.Branches["0a0b"])
	require.Equal(t, "050100000000000004", loaded.Accounts["68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9"])
	require.Len(t, loaded.Updates, 1)
}

func TestErrorTracePath(t *testing.T) {
	t.Parallel()

	require.Equal(t, "trace.error.toml", ErrorTracePath("trace.toml"))
	require.Equal(t, "/tmp/trie-trace-block-42.error.toml", ErrorTracePath("/tmp/trie-trace-block-42.toml"))
	require.Equal(t, "some-file.error", ErrorTracePath("some-file"))
}
