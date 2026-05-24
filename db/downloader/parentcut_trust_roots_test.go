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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
)

func TestTrustRootsToEntries_NilAndEmpty(t *testing.T) {
	require.Nil(t, trustRootsToEntries(nil),
		"nil input produces nil output so the optional TOML field is omitted")
	require.Nil(t, trustRootsToEntries([]chain.ParentTrustRoot{}),
		"empty slice produces nil output")
}

func TestTrustRootsToEntries_HexEncodesPubkey(t *testing.T) {
	pk := []byte{0x02, 0x03, 0x04, 0x05}
	roots := []chain.ParentTrustRoot{
		{Kind: "did", Pubkey: pk, DID: "did:key:z6Mk"},
		{Kind: "enr", Pubkey: []byte{0x03, 0xff}},
	}
	entries := trustRootsToEntries(roots)
	require.Len(t, entries, 2)
	require.Equal(t, "did", entries[0].Kind)
	require.Equal(t, "02030405", entries[0].Pubkey)
	require.Equal(t, "did:key:z6Mk", entries[0].DID)
	require.Equal(t, "enr", entries[1].Kind)
	require.Equal(t, "03ff", entries[1].Pubkey)
	require.Empty(t, entries[1].DID)
}

func TestEntriesToTrustRoots_IsInverseOfTrustRootsToEntries(t *testing.T) {
	pk1 := []byte{0xde, 0xad, 0xbe, 0xef}
	pk2 := []byte{0xfe, 0xed, 0xfa, 0xce}
	original := []chain.ParentTrustRoot{
		{Kind: "did", Pubkey: pk1, DID: "did:key:z6Mk"},
		{Kind: "enr", Pubkey: pk2},
	}
	round := EntriesToTrustRoots(trustRootsToEntries(original))
	require.Equal(t, original, round, "round-trip through TOML form preserves identity")
}

func TestEntriesToTrustRoots_InvalidHexLeavesPubkeyNil(t *testing.T) {
	// Defensive: malformed hex in a manifest's pubkey field yields an
	// entry with nil Pubkey. The verifier rejects nil-pubkey roots on
	// subsequent equality check; this prevents a malformed manifest
	// from crashing the parse path.
	entries := []ParentTrustRootEntry{
		{Kind: "did", Pubkey: "not-hex-at-all", DID: "did:key:bogus"},
	}
	roots := EntriesToTrustRoots(entries)
	require.Len(t, roots, 1)
	require.Equal(t, "did", roots[0].Kind)
	require.Nil(t, roots[0].Pubkey,
		"malformed hex yields nil pubkey rather than parse error")
	require.Equal(t, "did:key:bogus", roots[0].DID)
}

func TestEntriesToTrustRoots_NilAndEmpty(t *testing.T) {
	require.Nil(t, EntriesToTrustRoots(nil))
	require.Nil(t, EntriesToTrustRoots([]ParentTrustRootEntry{}))
}

func TestParentSectionFromCut_PropagatesTrustRoots(t *testing.T) {
	cut := &ParentCut{
		Schema:             1,
		ParentChain:        "mainnet",
		ParentChainID:      1,
		CutBlock:           20_000_000,
		CutBlockHash:       common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		CutBlockTimestamp:  1_700_000_000,
		CutBlockParentHash: common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222"),
		ParentManifestName: "chain.v2.deadbeef.somegen.toml",
		ParentManifestHash: "1234567890abcdef1234567890abcdef12345678",
		Source:             "live",
		SourceRef:          "https://mainnet.example/rpc",
		CapturedAt:         1_700_000_001,
	}
	clValidatorsRoot := [32]byte{0xaa}
	clForkVersion := [32]byte{0xbb}
	trustRoots := []chain.ParentTrustRoot{
		{Kind: "did", Pubkey: []byte{0x02, 0x03}, DID: "did:key:zfoo"},
		{Kind: "enr", Pubkey: []byte{0x03, 0xff}},
	}

	section, err := ParentSectionFromCut(cut, 23760000, clValidatorsRoot, clForkVersion, "msf-0", trustRoots)
	require.NoError(t, err)
	require.NotNil(t, section)

	require.Len(t, section.ValidParentTrustRoots, 2)
	require.Equal(t, "did", section.ValidParentTrustRoots[0].Kind)
	require.Equal(t, "0203", section.ValidParentTrustRoots[0].Pubkey)
	require.Equal(t, "did:key:zfoo", section.ValidParentTrustRoots[0].DID)
	require.Equal(t, "enr", section.ValidParentTrustRoots[1].Kind)
	require.Equal(t, "03ff", section.ValidParentTrustRoots[1].Pubkey)
}

func TestParentSectionFromCut_NilTrustRootsOmitField(t *testing.T) {
	// Operator that doesn't supply --valid-parent-trust-roots passes
	// nil; the generated section emits no field (optional TOML).
	cut := &ParentCut{
		Schema:             1,
		ParentChain:        "mainnet",
		ParentChainID:      1,
		CutBlock:           20_000_000,
		CutBlockHash:       common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		CutBlockTimestamp:  1_700_000_000,
		CutBlockParentHash: common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222"),
		ParentManifestName: "chain.v2.deadbeef.somegen.toml",
		ParentManifestHash: "1234567890abcdef1234567890abcdef12345678",
		Source:             "live",
		SourceRef:          "https://mainnet.example/rpc",
		CapturedAt:         1_700_000_001,
	}
	section, err := ParentSectionFromCut(cut, 23760000, [32]byte{}, [32]byte{}, "", nil)
	require.NoError(t, err)
	require.Nil(t, section.ValidParentTrustRoots,
		"nil trust roots → nil field → TOML omits the section")
}
