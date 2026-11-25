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

package v2

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/v2/branch"
)

func LoadTrieTraceIntoMockState(t testing.TB, path string) (*MockState, [][]byte, []commitment.Update, []byte) {
	t.Helper()

	tt, err := commitment.LoadTrieTrace(path)
	require.NoError(t, err)

	ms := NewMockState(t)

	for hexKey, hexVal := range tt.Branches {
		key, err := hex.DecodeString(hexKey)
		require.NoError(t, err)
		val, err := hex.DecodeString(hexVal)
		require.NoError(t, err)
		ms.cm[string(key)] = branch.BranchData(val)
	}

	for hexKey, hexVal := range tt.Accounts {
		key, err := hex.DecodeString(hexKey)
		require.NoError(t, err)
		val, err := hex.DecodeString(hexVal)
		require.NoError(t, err)
		ms.sm[string(key)] = val
	}

	for hexKey, hexVal := range tt.Storages {
		key, err := hex.DecodeString(hexKey)
		require.NoError(t, err)
		val, err := hex.DecodeString(hexVal)
		require.NoError(t, err)
		ms.sm[string(key)] = val
	}

	plainKeys := make([][]byte, len(tt.Updates))
	updates := make([]commitment.Update, len(tt.Updates))
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

	trieState, err := tt.DecodeTrieState()
	require.NoError(t, err)

	return ms, plainKeys, updates, trieState
}

func recordTrace(t *testing.T, ms *MockState, trie *HexPatriciaHashed, plainKeys [][]byte, updates []commitment.Update) (*commitment.TrieTrace, []byte) {
	t.Helper()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))

	recorder := commitment.NewRecordingContext(ms)
	trie.ResetContext(recorder)

	upds := WrapKeyUpdates(t, commitment.ModeDirect, commitment.KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()
	root, err := trie.Process(context.Background(), upds, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)

	trace, err := commitment.BuildTrieTrace(recorder, nil, nil)
	require.NoError(t, err)
	return trace, root
}

func saveTrace(t *testing.T, trace *commitment.TrieTrace) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "trace.toml")
	require.NoError(t, trace.Save(path))
	return path
}

func replayTraceFromFile(t *testing.T, path string) []byte {
	t.Helper()
	state, replayKeys, replayUpdates, _ := LoadTrieTraceIntoMockState(t, path)
	trie := NewHexPatriciaHashed(length.Addr, state, commitment.DefaultTrieConfig())
	upds := WrapKeyUpdates(t, commitment.ModeDirect, commitment.KeyToHexNibbleHash, replayKeys, replayUpdates)
	defer upds.Close()
	root, err := trie.Process(context.Background(), upds, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	return root
}

func TestTrieTraceRoundTrip(t *testing.T) {
	t.Parallel()

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 42).
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e",
			"24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Build()

	state1 := NewMockState(t)
	trie1 := NewHexPatriciaHashed(length.Addr, state1, commitment.DefaultTrieConfig())
	trace, rootHash1 := recordTrace(t, state1, trie1, plainKeys, updates)

	rootHash2 := replayTraceFromFile(t, saveTrace(t, trace))
	require.Equal(t, rootHash1, rootHash2, "root hash from replay should match original")
}

func TestTrieTraceEmptyUpdates(t *testing.T) {
	t.Parallel()

	ms := NewMockState(t)
	rc := commitment.NewRecordingContext(ms)

	trace, err := commitment.BuildTrieTrace(rc, nil, nil)
	require.NoError(t, err)
	require.Empty(t, trace.Branches)
	require.Empty(t, trace.Accounts)
	require.Empty(t, trace.Storages)
	require.Empty(t, trace.Updates)

	tracePath := filepath.Join(t.TempDir(), "empty_trace.toml")
	err = trace.Save(tracePath)
	require.NoError(t, err)

	loaded, err := commitment.LoadTrieTrace(tracePath)
	require.NoError(t, err)
	require.Empty(t, loaded.Updates)
}

func TestTrieTraceAccountOnlyRoundTrip(t *testing.T) {
	t.Parallel()

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Build()

	state1 := NewMockState(t)
	trie1 := NewHexPatriciaHashed(length.Addr, state1, commitment.DefaultTrieConfig())
	trace, rootHash1 := recordTrace(t, state1, trie1, plainKeys, updates)
	require.Empty(t, trace.Storages, "account-only trace should have no storages")
	require.NotEmpty(t, trace.Accounts, "account-only trace should have accounts")

	rootHash2 := replayTraceFromFile(t, saveTrace(t, trace))
	require.Equal(t, rootHash1, rootHash2, "account-only replay root hash should match")
}

func TestTrieTraceStorageOnlyRoundTrip(t *testing.T) {
	t.Parallel()

	plainKeys, updates := NewUpdateBuilder().
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1).
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e",
			"24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e",
			"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ff01").
		Build()

	state1 := NewMockState(t)
	trie1 := NewHexPatriciaHashed(length.Addr, state1, commitment.DefaultTrieConfig())
	trace, rootHash1 := recordTrace(t, state1, trie1, plainKeys, updates)
	require.NotEmpty(t, trace.Storages, "storage trace should have storages")

	rootHash2 := replayTraceFromFile(t, saveTrace(t, trace))
	require.Equal(t, rootHash1, rootHash2, "storage replay root hash should match")
}

type errorAccountState struct {
	commitment.PatriciaContext
	errorKey    string
	errToReturn error
}

func (e *errorAccountState) Account(plainKey []byte) (*commitment.Update, error) {
	if string(plainKey) == e.errorKey {
		return nil, e.errToReturn
	}
	return e.PatriciaContext.Account(plainKey)
}

func TestTrieTraceErrorRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Build()

	state := NewMockState(t)
	err := state.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	corruptKey := string(plainKeys[0])
	accountErr := errors.New("corrupt account: invalid encoding")
	errState := &errorAccountState{
		PatriciaContext: state,
		errorKey:        corruptKey,
		errToReturn:     accountErr,
	}

	trie1 := NewHexPatriciaHashed(length.Addr, state, commitment.DefaultTrieConfig())
	recorder := commitment.NewRecordingContext(errState)
	trie1.ResetContext(recorder)

	upds1 := WrapKeyUpdates(t, commitment.ModeDirect, commitment.KeyToHexNibbleHash, plainKeys, updates)
	_, processErr := trie1.Process(ctx, upds1, "", nil, commitment.WarmupConfig{})
	upds1.Close()
	require.Error(t, processErr, "Process should have failed due to corrupt account")
	require.Contains(t, processErr.Error(), "corrupt account")

	trace, err := commitment.BuildTrieTrace(recorder, nil, nil)
	require.NoError(t, err)

	trace.Error = processErr.Error()
	trace.BlockNum = 42
	trace.TxNum = 100

	tracePath := filepath.Join(t.TempDir(), "trace.toml")
	errorPath := commitment.ErrorTracePath(tracePath)
	require.True(t, strings.HasSuffix(errorPath, ".error.toml"), "error path should end with .error.toml")

	err = trace.Save(errorPath)
	require.NoError(t, err)

	_, err = os.Stat(errorPath)
	require.NoError(t, err, "error trace file should exist")

	loaded, err := commitment.LoadTrieTrace(errorPath)
	require.NoError(t, err)
	require.NotEmpty(t, loaded.Error, "loaded trace should contain error message")
	require.Contains(t, loaded.Error, "corrupt account")
	require.Equal(t, uint64(42), loaded.BlockNum)
	require.Equal(t, uint64(100), loaded.TxNum)

	state2, _, _, _ := LoadTrieTraceIntoMockState(t, errorPath)

	err = state2.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	errState2 := &errorAccountState{
		PatriciaContext: state2,
		errorKey:        corruptKey,
		errToReturn:     accountErr,
	}

	trie2 := NewHexPatriciaHashed(length.Addr, state2, commitment.DefaultTrieConfig())
	trie2.ResetContext(errState2)
	upds2 := WrapKeyUpdates(t, commitment.ModeDirect, commitment.KeyToHexNibbleHash, plainKeys, updates)
	_, replayErr := trie2.Process(ctx, upds2, "", nil, commitment.WarmupConfig{})
	upds2.Close()
	require.Error(t, replayErr, "replay should produce the same error")
	require.Contains(t, replayErr.Error(), "corrupt account")
}

func TestTrieTracePartialRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Build()

	state := NewMockState(t)
	err := state.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	corruptKey := string(plainKeys[len(plainKeys)-1])
	accountErr := errors.New("corrupt account: partial trace test")
	errState := &errorAccountState{
		PatriciaContext: state,
		errorKey:        corruptKey,
		errToReturn:     accountErr,
	}

	recorder := commitment.NewRecordingContext(errState)

	trie := NewHexPatriciaHashed(length.Addr, state, commitment.DefaultTrieConfig())
	trie.ResetContext(recorder)

	upds := WrapKeyUpdates(t, commitment.ModeDirect, commitment.KeyToHexNibbleHash, plainKeys, updates)
	_, processErr := trie.Process(ctx, upds, "", nil, commitment.WarmupConfig{})
	upds.Close()

	require.Error(t, processErr, "Process should fail due to corrupt account")

	trace, err := commitment.BuildTrieTrace(recorder, nil, nil)
	require.NoError(t, err)

	trace.Error = processErr.Error()

	totalRecords := len(trace.Accounts) + len(trace.Storages) + len(trace.Branches)
	require.Greater(t, totalRecords, 0, "partial trace should contain some recorded data")

	tracePath := filepath.Join(t.TempDir(), "partial_trace.toml")
	err = trace.Save(tracePath)
	require.NoError(t, err)

	loaded, err := commitment.LoadTrieTrace(tracePath)
	require.NoError(t, err)

	require.Equal(t, len(trace.Branches), len(loaded.Branches))
	require.Equal(t, len(trace.Accounts), len(loaded.Accounts))
	require.Equal(t, len(trace.Storages), len(loaded.Storages))
	require.Equal(t, len(trace.Updates), len(loaded.Updates))
	require.Contains(t, loaded.Error, "corrupt account")
}

func TestTrieTracePutBranchRecording(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Build()

	state := NewMockState(t)
	err := state.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	trie1 := NewHexPatriciaHashed(length.Addr, state, commitment.DefaultTrieConfig())
	recorder := commitment.NewRecordingContext(state)
	trie1.ResetContext(recorder)

	upds := WrapKeyUpdates(t, commitment.ModeDirect, commitment.KeyToHexNibbleHash, plainKeys, updates)
	_, err = trie1.Process(ctx, upds, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	upds.Close()

	require.NotEmpty(t, recorder.PutBranches, "PutBranch calls should have been recorded")

	trace, err := commitment.BuildTrieTrace(recorder, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, trace.PutBranches, "trace should contain PutBranches")

	for hexPrefix, bw := range trace.PutBranches {
		require.NotEmpty(t, hexPrefix, "PutBranch prefix should not be empty")
		require.NotEmpty(t, bw.NewData, "PutBranch NewData should not be empty")
	}

	tracePath := filepath.Join(t.TempDir(), "putbranch_trace.toml")
	err = trace.Save(tracePath)
	require.NoError(t, err)

	loaded, err := commitment.LoadTrieTrace(tracePath)
	require.NoError(t, err)
	require.Equal(t, len(trace.PutBranches), len(loaded.PutBranches))
	for k, v := range trace.PutBranches {
		loadedV, ok := loaded.PutBranches[k]
		require.True(t, ok, "PutBranch key %s should exist in loaded trace", k)
		require.Equal(t, v.PrevData, loadedV.PrevData)
		require.Equal(t, v.NewData, loadedV.NewData)
	}
}

// capture a trace with ERIGON_TRIE_TRACE_BLOCK=<N> on the production side, replay it here.
func TestTrieTraceReplayFromFile(t *testing.T) {
	tracePath := os.Getenv("ERIGON_TRIE_TRACE_FILE")
	if tracePath == "" {
		t.Skip("set ERIGON_TRIE_TRACE_FILE=<path to .toml trace> to run this manual debugging test")
	}

	ctx := context.Background()
	state, plainKeys, replayUpdates, trieState := LoadTrieTraceIntoMockState(t, tracePath)

	trie := NewHexPatriciaHashed(length.Addr, state, commitment.DefaultTrieConfig())
	trie.ResetContext(state)
	if len(trieState) > 0 {
		err := trie.SetState(trieState)
		require.NoError(t, err)
	}

	upds := WrapKeyUpdates(t, commitment.ModeDirect, commitment.KeyToHexNibbleHash, plainKeys, replayUpdates)
	defer upds.Close()

	rootHash, err := trie.Process(ctx, upds, "", nil, commitment.WarmupConfig{})
	if err != nil {
		t.Logf("Process returned error (expected if replaying .error.toml): %v", err)
		tt, loadErr := commitment.LoadTrieTrace(tracePath)
		require.NoError(t, loadErr)
		if tt.Error != "" {
			t.Logf("Original error from trace: %s", tt.Error)
		}
		return
	}
	t.Logf("Root hash: %x", rootHash)
}

func TestTrieTraceNonEmptyStateRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	plainKeys1, updates1 := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Build()

	state1 := NewMockState(t)
	trie1 := NewHexPatriciaHashed(length.Addr, state1, commitment.DefaultTrieConfig())
	processBatch(t, state1, trie1, plainKeys1, updates1)

	trieState, err := trie1.EncodeCurrentState(nil)
	require.NoError(t, err)
	require.NotEmpty(t, trieState, "trie state should be non-empty after Process")

	plainKeys2, updates2 := NewUpdateBuilder().
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 42).
		Build()

	err = state1.applyPlainUpdates(plainKeys2, updates2)
	require.NoError(t, err)

	recorder := commitment.NewRecordingContext(state1)
	trie1.ResetContext(recorder)

	upds2 := WrapKeyUpdates(t, commitment.ModeDirect, commitment.KeyToHexNibbleHash, plainKeys2, updates2)
	rootHash1, err := trie1.Process(ctx, upds2, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	upds2.Close()

	trace, err := commitment.BuildTrieTrace(recorder, nil, trieState)
	require.NoError(t, err)
	require.NotEmpty(t, trace.TrieState, "trace should contain trie state")

	tracePath := filepath.Join(t.TempDir(), "stateful_trace.toml")
	err = trace.Save(tracePath)
	require.NoError(t, err)

	state2, replayKeys, replayUpdates, loadedState := LoadTrieTraceIntoMockState(t, tracePath)
	require.NotEmpty(t, loadedState, "loaded trie state should be non-empty")

	trie2 := NewHexPatriciaHashed(length.Addr, state2, commitment.DefaultTrieConfig())
	trie2.ResetContext(state2)
	err = trie2.SetState(loadedState)
	require.NoError(t, err)

	upds3 := WrapKeyUpdates(t, commitment.ModeDirect, commitment.KeyToHexNibbleHash, replayKeys, replayUpdates)
	rootHash2, err := trie2.Process(ctx, upds3, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	upds3.Close()

	require.Equal(t, rootHash1, rootHash2, "replay with restored state should match original root hash")
}

func TestTrieTraceDeleteRoundTrip(t *testing.T) {
	t.Parallel()

	plainKeys1, updates1 := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Build()

	state1 := NewMockState(t)
	trie1 := NewHexPatriciaHashed(length.Addr, state1, commitment.DefaultTrieConfig())
	processBatch(t, state1, trie1, plainKeys1, updates1)

	plainKeys2, updates2 := NewUpdateBuilder().
		Delete("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40").
		Build()

	trace, rootHash1 := recordTrace(t, state1, trie1, plainKeys2, updates2)

	require.NotEmpty(t, trace.Updates, "trace must include delete update")
	foundDelete := false
	for _, u := range trace.Updates {
		key, decErr := hex.DecodeString(u.PlainKey)
		require.NoError(t, decErr)
		if bytes.Equal(key, plainKeys2[0]) {
			foundDelete = true
			buf, decErr := hex.DecodeString(u.Update)
			require.NoError(t, decErr)
			var upd commitment.Update
			_, decErr = upd.Decode(buf, 0)
			require.NoError(t, decErr)
			require.True(t, upd.Deleted(), "update for deleted key must have DeleteUpdate flag")
		}
	}
	require.True(t, foundDelete, "deleted key must appear in trace Updates")
	deletedHexKey := hex.EncodeToString(plainKeys2[0])
	_, inAccountsMap := trace.Accounts[deletedHexKey]
	require.False(t, inAccountsMap, "deleted key must not appear in trace Accounts map")

	rootHash2 := replayTraceFromFile(t, saveTrace(t, trace))
	require.Equal(t, rootHash1, rootHash2, "replay with delete should match original root hash")
}
