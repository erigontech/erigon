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
	"encoding/hex"
	"fmt"
	"os"
	"sort"

	toml "github.com/pelletier/go-toml/v2"
)

// TrieTrace holds a self-contained snapshot of trie state captured during
// Process. All keys and values are hex-encoded strings for TOML readability.
type TrieTrace struct {
	Branches map[string]string `toml:"branches"`
	Accounts map[string]string `toml:"accounts"`
	Storages map[string]string `toml:"storages"`
	Updates  []TraceKeyUpdate  `toml:"updates"`
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
func BuildTrieTrace(rc *RecordingContext, inputKeys map[string]struct{}) (*TrieTrace, error) {
	tt := &TrieTrace{
		Branches: make(map[string]string, len(rc.branches)),
		Accounts: make(map[string]string, len(rc.accounts)),
		Storages: make(map[string]string, len(rc.storages)),
		Updates:  make([]TraceKeyUpdate, 0, len(rc.accounts)+len(rc.storages)),
	}

	for k, v := range rc.branches {
		tt.Branches[hex.EncodeToString([]byte(k))] = hex.EncodeToString(v)
	}
	for k, v := range rc.accounts {
		hexKey := hex.EncodeToString([]byte(k))
		tt.Accounts[hexKey] = hex.EncodeToString(v)
		if inputKeys == nil || containsKey(inputKeys, k) {
			tt.Updates = append(tt.Updates, TraceKeyUpdate{
				PlainKey: hexKey,
				Update:   hex.EncodeToString(v),
			})
		}
	}
	for k, v := range rc.storages {
		hexKey := hex.EncodeToString([]byte(k))
		tt.Storages[hexKey] = hex.EncodeToString(v)
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

	return tt, nil
}

// containsKey checks if the key (as raw bytes stored in a string) exists in the set.
func containsKey(keys map[string]struct{}, k string) bool {
	_, ok := keys[k]
	return ok
}

// Save marshals the TrieTrace to TOML and writes it to the given path.
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
