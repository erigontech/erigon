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

package manifest_exchange

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestV2ToPeerManifest_AccessorKind pins that an accessor entry in a V2
// manifest converts to a peer FileEntry tagged KindAccessor — not
// KindKV, which would mistake it for a domain primary.
func TestV2ToPeerManifest_AccessorKind(t *testing.T) {
	hash := strings.Repeat("ab", 20)
	m := &downloader.ChainTomlV2{
		Version: downloader.ChainTomlV2Version,
		Domains: map[string]*downloader.DomainManifest{
			"accounts": {
				Coverage: [2]uint64{0, 2048},
				Files: []downloader.DomainFileEntry{
					{Name: "v1.0-accounts.0-2048.kv", Range: [2]uint64{0, 2048}, Kind: downloader.KindKVName, Hash: hash, Trust: "verified"},
					{Name: "v1.0-accounts.0-2048.bt", Range: [2]uint64{0, 2048}, Kind: downloader.KindAccessorName, Hash: hash, Trust: "verified"},
				},
			},
		},
	}

	out := v2ToPeerManifest("peer-1", m)

	entries := out.Domains[snapshot.DomainAccounts]
	require.Len(t, entries, 2)

	byName := map[string]snapshot.FileKind{}
	for _, e := range entries {
		byName[e.Name] = e.Kind
	}
	require.Equal(t, snapshot.KindKV, byName["v1.0-accounts.0-2048.kv"])
	require.Equal(t, snapshot.KindAccessor, byName["v1.0-accounts.0-2048.bt"],
		"accessor entry must convert to KindAccessor, not be mistaken for a primary")
}

// TestAccessor_EndToEndRoundTrip drives an inventory containing domain
// and block accessors through the full producer→consumer chain —
// GenerateV2 → MarshalV2 → ParseV2 → v2ToPeerManifest — and checks the
// accessors arrive as peer FileEntries tagged KindAccessor. Each hop has
// its own unit test; this pins the seams between them.
func TestAccessor_EndToEndRoundTrip(t *testing.T) {
	inv := snapshot.NewInventory()
	add := func(name string, h byte) {
		require.NoError(t, inv.AddFile(&snapshot.FileEntry{
			Name: name, TorrentHash: [20]byte{h}, Local: true, Trust: snapshot.TrustVerified,
		}))
	}
	add("v1.0-accounts.0-2048.kv", 0x10)
	add("v1.0-accounts.0-2048.bt", 0x11)
	add("v1.0-accounts.0-2048.kvi", 0x12)
	add("v1.0-accounts.0-2048.kvei", 0x13)
	add("v1.0-000000-000500-headers.seg", 0x20)
	add("v1.0-000000-000500-headers.idx", 0x21)

	data, err := downloader.MarshalV2(downloader.GenerateV2(inv))
	require.NoError(t, err)
	parsed, err := downloader.ParseV2(data)
	require.NoError(t, err)
	peer := v2ToPeerManifest("peer", parsed)

	kinds := map[string]snapshot.FileKind{}
	for _, e := range peer.Domains[snapshot.DomainAccounts] {
		kinds[e.Name] = e.Kind
	}
	require.Equal(t, snapshot.KindKV, kinds["v1.0-accounts.0-2048.kv"])
	require.Equal(t, snapshot.KindAccessor, kinds["v1.0-accounts.0-2048.bt"])
	require.Equal(t, snapshot.KindAccessor, kinds["v1.0-accounts.0-2048.kvi"])
	require.Equal(t, snapshot.KindAccessor, kinds["v1.0-accounts.0-2048.kvei"])

	blockNames := map[string]bool{}
	for _, e := range peer.Blocks {
		blockNames[e.Name] = true
	}
	require.True(t, blockNames["v1.0-000000-000500-headers.seg"])
	require.True(t, blockNames["v1.0-000000-000500-headers.idx"], "block .idx accessor must survive the round trip")
}
