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
	"strings"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"
)

func hashOf(b byte) metainfo.Hash {
	var h metainfo.Hash
	for i := range h {
		h[i] = b
	}
	return h
}

// TestAtomicSetGate_DropsWholeCoordinateWhenAccessorUnservable pins the
// publish half of the atomic-set rule: a primary and the accessors built
// from it are one unit, so a coordinate is advertised whole or not at all.
//
// A rebuilt accessor gets a new info-hash, which leaves the old one
// unservable. Advertising the surviving primary on its own invites a
// consumer to pair it with an accessor from the previous generation, whose
// offsets index past the end of the new primary.
func TestAtomicSetGate_DropsWholeCoordinateWhenAccessorUnservable(t *testing.T) {
	kvHash, kviHash := hashOf(0x11), hashOf(0x22)
	otherKV, otherKVI := hashOf(0x33), hashOf(0x44)

	manifest := &ChainTomlV2{
		Version: ChainTomlV2Version,
		Domains: map[string]*DomainManifest{
			"commitment": {
				Coverage: [2]uint64{328, 331},
				Files: []DomainFileEntry{
					{Name: "domain/v2.2-commitment.330-331.kv", Range: [2]uint64{330, 331}, Kind: KindKVName, Hash: kvHash.HexString()},
					{Name: "domain/v2.1-commitment.330-331.kvi", Range: [2]uint64{330, 331}, Kind: KindAccessorName, Hash: kviHash.HexString()},
					{Name: "domain/v2.2-commitment.328-330.kv", Range: [2]uint64{328, 330}, Kind: KindKVName, Hash: otherKV.HexString()},
					{Name: "domain/v2.1-commitment.328-330.kvi", Range: [2]uint64{328, 330}, Kind: KindAccessorName, Hash: otherKVI.HexString()},
				},
			},
		},
	}

	// The 330-331 accessor was rebuilt: its old hash is no longer servable.
	servable := map[metainfo.Hash]struct{}{
		kvHash:   {},
		otherKV:  {},
		otherKVI: {},
	}

	dropped := FilterManifestByAtomicSet(manifest, servable)

	names := map[string]bool{}
	for _, f := range manifest.Domains["commitment"].Files {
		names[f.Name] = true
	}
	require.False(t, names["domain/v2.1-commitment.330-331.kvi"],
		"the unservable accessor must go")
	require.False(t, names["domain/v2.2-commitment.330-331.kv"],
		"its primary must go with it — advertised alone, a consumer pairs it "+
			"with the previous generation's accessor and reads past the end of the file")
	require.True(t, names["domain/v2.2-commitment.328-330.kv"],
		"an intact coordinate must be untouched")
	require.True(t, names["domain/v2.1-commitment.328-330.kvi"],
		"an intact coordinate must be untouched")
	require.Equal(t, 2, dropped)
}

// TestAtomicSetGate_SpansBlockList covers a coordinate whose members sit in
// the manifest's block list — where every state file the inventory cannot
// attribute to a domain ends up.
func TestAtomicSetGate_SpansBlockList(t *testing.T) {
	efHash, efiHash := hashOf(0x55), hashOf(0x66)
	manifest := &ChainTomlV2{
		Version: ChainTomlV2Version,
		Blocks: []BlockFileEntry{
			{Name: "idx/v3.0-logaddrs.330-331.ef", Range: [2]uint64{330, 331}, Hash: efHash.HexString()},
			{Name: "accessor/v2.1-logaddrs.330-331.efi", Range: [2]uint64{330, 331}, Hash: efiHash.HexString()},
		},
	}

	dropped := FilterManifestByAtomicSet(manifest, map[metainfo.Hash]struct{}{efHash: {}})

	require.Empty(t, manifest.Blocks,
		"an index and its accessor are one coordinate; neither is advertisable alone")
	require.Equal(t, 2, dropped)
}

// TestAtomicSetGate_NilServableDisablesGate matches FilterManifestByServable:
// callers with no torrent client (tests, the free publish helper) opt out.
func TestAtomicSetGate_NilServableDisablesGate(t *testing.T) {
	manifest := &ChainTomlV2{
		Version: ChainTomlV2Version,
		Blocks: []BlockFileEntry{
			{Name: "idx/v3.0-logaddrs.330-331.ef", Range: [2]uint64{330, 331}, Hash: strings.Repeat("ab", 20)},
		},
	}
	require.Zero(t, FilterManifestByAtomicSet(manifest, nil))
	require.Len(t, manifest.Blocks, 1)
}
