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
