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

package downloader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func validParentCut() *ParentCut {
	return &ParentCut{
		Schema:             ParentCutSchemaVersion,
		ParentChain:        "mainnet",
		ParentChainID:      1,
		CutBlock:           23760000,
		CutBlockHash:       common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
		CutBlockTimestamp:  1735689600,
		CutBlockParentHash: common.HexToHash("0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"),
		ParentManifestName: "chain.v2.7900fbf8f3411de0.a0aba4ce58f94ee2.toml",
		ParentManifestHash: "20004fef6f6b652bde5f7c20e67e33cbc3e059d3",
		Source:             CaptureLive,
		SourceRef:          "https://mainnet.example/rpc",
		CapturedAt:         1735689600,
	}
}

func TestParentCut_ValidateAcceptsWellFormed(t *testing.T) {
	require.NoError(t, validParentCut().Validate())
}

func TestParentCut_ValidateRejectsMissingFields(t *testing.T) {
	cases := map[string]func(*ParentCut){
		"wrong schema":             func(p *ParentCut) { p.Schema = 999 },
		"empty parent_chain":       func(p *ParentCut) { p.ParentChain = "" },
		"zero parent_chain_id":     func(p *ParentCut) { p.ParentChainID = 0 },
		"zero cut_block":           func(p *ParentCut) { p.CutBlock = 0 },
		"zero cut_block_hash":      func(p *ParentCut) { p.CutBlockHash = common.Hash{} },
		"zero cut_block_timestamp": func(p *ParentCut) { p.CutBlockTimestamp = 0 },
		"empty source":             func(p *ParentCut) { p.Source = "" },
		"unknown source":           func(p *ParentCut) { p.Source = ParentCutCaptureSource("magic") },
		"malformed manifest hash":  func(p *ParentCut) { p.ParentManifestHash = "not-hex" },
		"short manifest hash":      func(p *ParentCut) { p.ParentManifestHash = "deadbeef" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := validParentCut()
			mutate(p)
			require.Error(t, p.Validate())
		})
	}
}

func TestParentCut_ValidateAcceptsEmptyParentManifestHash(t *testing.T) {
	// A pre-Phase-1 root chain has no V2 manifest to reference; the
	// fork-from utility tolerates this and just doesn't populate the
	// downstream chain.Config.ParentManifestHash.
	p := validParentCut()
	p.ParentManifestHash = ""
	p.ParentManifestName = ""
	require.NoError(t, p.Validate())
}

func TestParentCut_NilValidateErrors(t *testing.T) {
	var p *ParentCut
	require.Error(t, p.Validate())
}

func TestParentCut_MarshalCanonicalIsDeterministic(t *testing.T) {
	p := validParentCut()
	a, err := p.MarshalCanonical()
	require.NoError(t, err)
	b, err := p.MarshalCanonical()
	require.NoError(t, err)
	require.Equal(t, a, b, "two canonical marshals of the same struct must produce identical bytes")
}

func TestParentCut_MarshalCanonicalKeysAreSorted(t *testing.T) {
	p := validParentCut()
	bytes, err := p.MarshalCanonical()
	require.NoError(t, err)
	out := string(bytes)
	// First keys in alphabetical order: "captured_at" before "cut_block".
	capturedAt := indexOfKey(t, out, "captured_at")
	cutBlock := indexOfKey(t, out, "cut_block")
	require.Less(t, capturedAt, cutBlock,
		"canonical marshal must emit keys in alphabetical order")
}

func TestParentCut_SaveAndLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "parent-cut.json")

	original := validParentCut()
	require.NoError(t, SaveParentCut(path, original))

	loaded, err := LoadParentCut(path)
	require.NoError(t, err)
	require.Equal(t, original, loaded, "save → load must round-trip exactly")
}

func TestParentCut_LoadRejectsInvalidFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "parent-cut.json")
	// Write a file missing required fields — schema-only.
	require.NoError(t, SaveParentCut(path, validParentCut()))
	// Corrupt: rewrite with wrong schema.
	bad := validParentCut()
	bad.Schema = 999
	bytes, err := bad.MarshalCanonical()
	require.NoError(t, err)
	require.NoError(t, writeFile(path, bytes))

	_, err = LoadParentCut(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported schema version")
}

func TestParentCut_LoadRejectsMissingFile(t *testing.T) {
	_, err := LoadParentCut(filepath.Join(t.TempDir(), "does-not-exist.json"))
	require.Error(t, err)
}

func TestParentCut_SaveRejectsInvalidStruct(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "parent-cut.json")
	bad := validParentCut()
	bad.ParentChain = "" // invalidates
	require.Error(t, SaveParentCut(path, bad))
}

func TestStripURLCredentials(t *testing.T) {
	cases := map[string]string{
		"https://user:pass@mainnet.example/rpc": "https://mainnet.example/rpc",
		"https://mainnet.example/rpc":           "https://mainnet.example/rpc",
		"http://api.example:8545":               "http://api.example:8545",
		"no-scheme":                             "no-scheme",
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			require.Equal(t, want, stripURLCredentials(in))
		})
	}
}

// --- helpers ---

func indexOfKey(t *testing.T, jsonStr, key string) int {
	t.Helper()
	idx := -1
	for i := 0; i+len(key)+1 <= len(jsonStr); i++ {
		if jsonStr[i] == '"' && i+1+len(key)+1 <= len(jsonStr) &&
			jsonStr[i+1:i+1+len(key)] == key &&
			jsonStr[i+1+len(key)] == '"' {
			idx = i
			break
		}
	}
	require.GreaterOrEqual(t, idx, 0, "key %q not found in canonical output", key)
	return idx
}

func writeFile(path string, contents []byte) error {
	return os.WriteFile(path, contents, 0o644)
}
