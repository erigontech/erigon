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

package commitment

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// LoadTrieTraceIntoMockState loads a TOML trace file, populates a MockState
// with the branches/accounts/storages, and returns the plainKeys and Updates
// decoded from the trace's update list.
func LoadTrieTraceIntoMockState(t testing.TB, path string) (*MockState, [][]byte, []Update) {
	t.Helper()

	tt, err := LoadTrieTrace(path)
	require.NoError(t, err)

	ms := NewMockState(t)

	// Load branches into ms.cm
	for hexKey, hexVal := range tt.Branches {
		key, err := hex.DecodeString(hexKey)
		require.NoError(t, err)
		val, err := hex.DecodeString(hexVal)
		require.NoError(t, err)
		ms.cm[string(key)] = BranchData(val)
	}

	// Load accounts into ms.sm
	for hexKey, hexVal := range tt.Accounts {
		key, err := hex.DecodeString(hexKey)
		require.NoError(t, err)
		val, err := hex.DecodeString(hexVal)
		require.NoError(t, err)
		ms.sm[string(key)] = val
	}

	// Load storages into ms.sm
	for hexKey, hexVal := range tt.Storages {
		key, err := hex.DecodeString(hexKey)
		require.NoError(t, err)
		val, err := hex.DecodeString(hexVal)
		require.NoError(t, err)
		ms.sm[string(key)] = val
	}

	// Decode updates
	plainKeys := make([][]byte, len(tt.Updates))
	updates := make([]Update, len(tt.Updates))
	for i, tu := range tt.Updates {
		key, err := hex.DecodeString(tu.PlainKey)
		require.NoError(t, err)
		plainKeys[i] = key

		buf, err := hex.DecodeString(tu.Update)
		require.NoError(t, err)
		pos, err := updates[i].Decode(buf, 0)
		require.NoError(t, err)
		require.Equal(t, len(buf), pos, "leftover bytes decoding update for key %s", tu.PlainKey)
	}

	return ms, plainKeys, updates
}

func TestTrieTraceRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Build some account and storage updates
	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 42).
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e",
			"24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Build()

	// Phase 1: Process with RecordingContext and capture trace
	state1 := NewMockState(t)
	err := state1.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	trie1 := NewHexPatriciaHashed(length.Addr, state1)
	recorder := NewRecordingContext(state1)
	trie1.ResetContext(recorder)

	upds1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	rootHash1, err := trie1.Process(ctx, upds1, "", nil, WarmupConfig{})
	require.NoError(t, err)
	upds1.Close()

	// Build trace and save to temp file
	trace, err := BuildTrieTrace(recorder)
	require.NoError(t, err)

	tracePath := filepath.Join(t.TempDir(), "trace.toml")
	err = trace.Save(tracePath)
	require.NoError(t, err)

	// Phase 2: Load trace into fresh MockState and replay
	state2, replayKeys, replayUpdates := LoadTrieTraceIntoMockState(t, tracePath)

	trie2 := NewHexPatriciaHashed(length.Addr, state2)

	upds2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, replayKeys, replayUpdates)
	rootHash2, err := trie2.Process(ctx, upds2, "", nil, WarmupConfig{})
	require.NoError(t, err)
	upds2.Close()

	require.Equal(t, rootHash1, rootHash2, "root hash from replay should match original")
}

func TestTrieTraceEmptyUpdates(t *testing.T) {
	t.Parallel()

	// A RecordingContext with no reads should produce an empty trace
	// that round-trips correctly.
	ms := NewMockState(t)
	rc := NewRecordingContext(ms)

	trace, err := BuildTrieTrace(rc)
	require.NoError(t, err)
	require.Empty(t, trace.Branches)
	require.Empty(t, trace.Accounts)
	require.Empty(t, trace.Storages)
	require.Empty(t, trace.Updates)

	// Save and reload
	tracePath := filepath.Join(t.TempDir(), "empty_trace.toml")
	err = trace.Save(tracePath)
	require.NoError(t, err)

	loaded, err := LoadTrieTrace(tracePath)
	require.NoError(t, err)
	require.Empty(t, loaded.Updates)
}

func TestTrieTraceAccountOnlyRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Build account-only updates (no storage)
	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Build()

	state1 := NewMockState(t)
	err := state1.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	trie1 := NewHexPatriciaHashed(length.Addr, state1)
	recorder := NewRecordingContext(state1)
	trie1.ResetContext(recorder)

	upds1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	rootHash1, err := trie1.Process(ctx, upds1, "", nil, WarmupConfig{})
	require.NoError(t, err)
	upds1.Close()

	trace, err := BuildTrieTrace(recorder)
	require.NoError(t, err)
	require.Empty(t, trace.Storages, "account-only trace should have no storages")
	require.NotEmpty(t, trace.Accounts, "account-only trace should have accounts")

	tracePath := filepath.Join(t.TempDir(), "account_only_trace.toml")
	err = trace.Save(tracePath)
	require.NoError(t, err)

	// Replay
	state2, replayKeys, replayUpdates := LoadTrieTraceIntoMockState(t, tracePath)
	trie2 := NewHexPatriciaHashed(length.Addr, state2)
	upds2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, replayKeys, replayUpdates)
	rootHash2, err := trie2.Process(ctx, upds2, "", nil, WarmupConfig{})
	require.NoError(t, err)
	upds2.Close()

	require.Equal(t, rootHash1, rootHash2, "account-only replay root hash should match")
}

func TestTrieTraceStorageOnlyRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Create an account first, then do storage-only updates.
	// We need an existing account for storage to make sense in the trie.
	plainKeys, updates := NewUpdateBuilder().
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1).
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e",
			"24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e",
			"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ff01").
		Build()

	state1 := NewMockState(t)
	err := state1.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	trie1 := NewHexPatriciaHashed(length.Addr, state1)
	recorder := NewRecordingContext(state1)
	trie1.ResetContext(recorder)

	upds1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	rootHash1, err := trie1.Process(ctx, upds1, "", nil, WarmupConfig{})
	require.NoError(t, err)
	upds1.Close()

	trace, err := BuildTrieTrace(recorder)
	require.NoError(t, err)
	require.NotEmpty(t, trace.Storages, "storage trace should have storages")

	tracePath := filepath.Join(t.TempDir(), "storage_trace.toml")
	err = trace.Save(tracePath)
	require.NoError(t, err)

	// Replay
	state2, replayKeys, replayUpdates := LoadTrieTraceIntoMockState(t, tracePath)
	trie2 := NewHexPatriciaHashed(length.Addr, state2)
	upds2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, replayKeys, replayUpdates)
	rootHash2, err := trie2.Process(ctx, upds2, "", nil, WarmupConfig{})
	require.NoError(t, err)
	upds2.Close()

	require.Equal(t, rootHash1, rootHash2, "storage replay root hash should match")
}

func TestTrieTraceSaveLoad(t *testing.T) {
	t.Parallel()

	// Build a TrieTrace with known data
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

	// Verify all fields round-trip correctly
	require.Equal(t, tt.Branches, loaded.Branches)
	require.Equal(t, tt.Accounts, loaded.Accounts)
	require.Equal(t, tt.Storages, loaded.Storages)
	require.Equal(t, len(tt.Updates), len(loaded.Updates))
	for i := range tt.Updates {
		require.Equal(t, tt.Updates[i].PlainKey, loaded.Updates[i].PlainKey)
		require.Equal(t, tt.Updates[i].Update, loaded.Updates[i].Update)
	}

	// Verify hex values are valid
	for hexKey, hexVal := range loaded.Branches {
		_, err := hex.DecodeString(hexKey)
		require.NoError(t, err, "branch key should be valid hex")
		_, err = hex.DecodeString(hexVal)
		require.NoError(t, err, "branch value should be valid hex")
	}
	for hexKey, hexVal := range loaded.Accounts {
		_, err := hex.DecodeString(hexKey)
		require.NoError(t, err, "account key should be valid hex")
		_, err = hex.DecodeString(hexVal)
		require.NoError(t, err, "account value should be valid hex")
	}
	for hexKey, hexVal := range loaded.Storages {
		_, err := hex.DecodeString(hexKey)
		require.NoError(t, err, "storage key should be valid hex")
		_, err = hex.DecodeString(hexVal)
		require.NoError(t, err, "storage value should be valid hex")
	}
}
