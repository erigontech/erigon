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
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"

	toml "github.com/pelletier/go-toml/v2"
)

const (
	// ErrorTraceSuffix is appended to the trace filename when Process returns an error.
	ErrorTraceSuffix = ".error"
)

// TrieTrace holds a self-contained snapshot of trie state captured during
// Process. All keys and values are hex-encoded strings for TOML readability.
type TrieTrace struct {
	BlockNum    uint64                      `toml:"block_num,omitempty"`
	TxNum       uint64                      `toml:"tx_num,omitempty"`
	Error       string                      `toml:"error,omitempty"`
	TrieState   string                      `toml:"trie_state,omitempty"`
	Branches    map[string]string           `toml:"branches"`
	Accounts    map[string]string           `toml:"accounts"`
	Storages    map[string]string           `toml:"storages"`
	Updates     []TraceKeyUpdate            `toml:"updates"`
	PutBranches map[string]TraceBranchWrite `toml:"put_branches,omitempty"`
}

// TraceBranchWrite stores a single PutBranch write with both prev and new data hex-encoded.
type TraceBranchWrite struct {
	PrevData string `toml:"prev_data"`
	NewData  string `toml:"new_data"`
}

// TraceKeyUpdate stores a single input update that was processed.
// PlainKey and Update are hex-encoded binary data.
type TraceKeyUpdate struct {
	PlainKey string `toml:"plain_key"`
	Update   string `toml:"update"`
}

// BuildTrieTrace converts the data recorded by a RecordingContext into a
// TrieTrace. All recorded branches, accounts, and storages are included in the
// state maps (for MockState population). The Updates list is filtered to only
// include keys present in inputKeys — this prevents fold-time context reads
// (for neighboring cells) from being treated as input updates during replay.
// When inputKeys is nil, all recorded accounts and storages are included as
// updates (useful in tests where the trie starts empty).
func BuildTrieTrace(rc *RecordingContext, inputKeys map[string]struct{}, trieState []byte) (*TrieTrace, error) {
	tt := &TrieTrace{
		Branches: make(map[string]string, len(rc.branches)),
		Accounts: make(map[string]string, len(rc.accounts)),
		Storages: make(map[string]string, len(rc.storages)),
		Updates:  make([]TraceKeyUpdate, 0, len(rc.accounts)+len(rc.storages)),
	}

	if len(trieState) > 0 {
		tt.TrieState = hex.EncodeToString(trieState)
	}

	for k, v := range rc.branches {
		tt.Branches[hex.EncodeToString([]byte(k))] = hex.EncodeToString(v)
	}
	for k, v := range rc.accounts {
		hexKey := hex.EncodeToString([]byte(k))
		isDelete := len(v) > 0 && UpdateFlags(v[0])&DeleteUpdate != 0
		// Delete entries go into Updates only — MockState returns DeleteUpdate
		// for missing keys automatically, so they must not appear in the state map.
		if !isDelete {
			tt.Accounts[hexKey] = hex.EncodeToString(v)
		}
		if inputKeys == nil || containsKey(inputKeys, k) {
			tt.Updates = append(tt.Updates, TraceKeyUpdate{
				PlainKey: hexKey,
				Update:   hex.EncodeToString(v),
			})
		}
	}
	for k, v := range rc.storages {
		hexKey := hex.EncodeToString([]byte(k))
		isDelete := len(v) > 0 && UpdateFlags(v[0])&DeleteUpdate != 0
		if !isDelete {
			tt.Storages[hexKey] = hex.EncodeToString(v)
		}
		if inputKeys == nil || containsKey(inputKeys, k) {
			tt.Updates = append(tt.Updates, TraceKeyUpdate{
				PlainKey: hexKey,
				Update:   hex.EncodeToString(v),
			})
		}
	}

	// Sort updates by plain key for deterministic output.
	sort.Slice(tt.Updates, func(i, j int) bool {
		return tt.Updates[i].PlainKey < tt.Updates[j].PlainKey
	})

	// Include PutBranch writes if any were recorded.
	if len(rc.putBranches) > 0 {
		tt.PutBranches = make(map[string]TraceBranchWrite, len(rc.putBranches))
		for k, bw := range rc.putBranches {
			tt.PutBranches[hex.EncodeToString([]byte(k))] = TraceBranchWrite{
				PrevData: hex.EncodeToString(bw.PrevData),
				NewData:  hex.EncodeToString(bw.NewData),
			}
		}
	}

	return tt, nil
}

// containsKey checks if the key (as raw bytes stored in a string) exists in the set.
func containsKey(keys map[string]struct{}, k string) bool {
	_, ok := keys[k]
	return ok
}

// Save marshals the TrieTrace to TOML and writes it to the given path.
// When the trace contains an error, the caller should use ErrorTracePath
// to compute the appropriate filename with the .error suffix.
func (tt *TrieTrace) Save(path string) error {
	data, err := toml.Marshal(tt)
	if err != nil {
		return fmt.Errorf("marshal trie trace: %w", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write trie trace to %s: %w", path, err)
	}
	return nil
}

// ErrorTracePath inserts the ".error" suffix before the file extension.
// For example, "trace.toml" becomes "trace.error.toml".
func ErrorTracePath(path string) string {
	if strings.HasSuffix(path, ".toml") {
		return strings.TrimSuffix(path, ".toml") + ErrorTraceSuffix + ".toml"
	}
	return path + ErrorTraceSuffix
}

// LoadTrieTrace reads a TOML file and unmarshals it into a TrieTrace.
func LoadTrieTrace(path string) (*TrieTrace, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read trie trace from %s: %w", path, err)
	}
	var tt TrieTrace
	if err := toml.Unmarshal(data, &tt); err != nil {
		return nil, fmt.Errorf("unmarshal trie trace: %w", err)
	}
	return &tt, nil
}

// DecodeTrieState returns the decoded trie state bytes, or nil if no state
// was captured (empty trie or old trace format).
func (tt *TrieTrace) DecodeTrieState() ([]byte, error) {
	if tt.TrieState == "" {
		return nil, nil
	}
	return hex.DecodeString(tt.TrieState)
}
