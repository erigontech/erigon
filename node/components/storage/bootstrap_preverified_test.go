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

package storage

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestEntryFromPreverifiedItem pins the translation contract: a
// preverified registry entry becomes a snapshot.FileEntry that the
// orchestrator handles identically to one received from a V2 peer
// manifest. Without this, the bootstrap path would silently dispatch
// the wrong bucket (e.g. salt into Blocks) and consumers would miss
// files.
func TestEntryFromPreverifiedItem(t *testing.T) {
	t.Parallel()

	// Mainnet-shaped sample of every distinct file kind preverified
	// contains. Hex hashes are arbitrary 40-char strings — translation
	// doesn't validate against torrent registries, just decodes.
	const sampleHash = "0123456789abcdef0123456789abcdef01234567"
	want, _ := hex.DecodeString(sampleHash)
	var wantBytes [20]byte
	copy(wantBytes[:], want)

	cases := []struct {
		name       string
		item       preverified.Item
		wantNil    bool
		wantKind   snapshot.FileKind
		wantDomain snapshot.Domain
	}{
		{
			name:     "state primary kv routes to KindKV + Domain",
			item:     preverified.Item{Name: "v1.0-accounts.0-1024.kv", Hash: sampleHash},
			wantKind: snapshot.KindKV, wantDomain: snapshot.DomainAccounts,
		},
		{
			name:     "history .v routes to KindHistory + Domain",
			item:     preverified.Item{Name: "v1.0-accountsHistory.0-1024.v", Hash: sampleHash},
			wantKind: snapshot.KindHistory, wantDomain: snapshot.DomainAccounts,
		},
		{
			name:     "inverted-index .ef routes to KindIdx + Domain",
			item:     preverified.Item{Name: "v1.0-accountsIdx.0-1024.ef", Hash: sampleHash},
			wantKind: snapshot.KindIdx, wantDomain: snapshot.DomainAccounts,
		},
		{
			name:     "block headers .seg routes to KindKV (no Domain)",
			item:     preverified.Item{Name: "v1.0-000000-000500-headers.seg", Hash: sampleHash},
			wantKind: snapshot.KindKV, wantDomain: "",
		},
		{
			name:     "caplin/ prefix routes to KindCaplin",
			item:     preverified.Item{Name: "caplin/v1.1-000000-000010-beaconblocks.seg", Hash: sampleHash},
			wantKind: snapshot.KindCaplin, wantDomain: "",
		},
		{
			name:     "erigondb.toml routes to KindMeta",
			item:     preverified.Item{Name: "erigondb.toml", Hash: sampleHash},
			wantKind: snapshot.KindMeta, wantDomain: "",
		},
		{
			name:     "salt-*.txt routes to KindSalt",
			item:     preverified.Item{Name: "salt-state.txt", Hash: sampleHash},
			wantKind: snapshot.KindSalt, wantDomain: "",
		},
		{
			name:    "malformed hash drops entry",
			item:    preverified.Item{Name: "v1.0-accounts.0-1024.kv", Hash: "not-hex"},
			wantNil: true,
		},
		{
			name:    "short hash drops entry",
			item:    preverified.Item{Name: "v1.0-accounts.0-1024.kv", Hash: "abcd"},
			wantNil: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := entryFromPreverifiedItem(c.item)
			if c.wantNil {
				require.Nil(t, got, "malformed hash must drop entry, not silently corrupt it")
				return
			}
			require.NotNil(t, got)
			require.Equal(t, c.item.Name, got.Name)
			require.Equal(t, wantBytes, got.TorrentHash, "torrent hash must round-trip from hex")
			require.Equal(t, snapshot.TrustNone, got.Trust,
				"bootstrap entries start at TrustNone — promoted to TrustVerified on DownloadComplete")
			require.Equal(t, c.wantKind, got.Kind, "Kind must match name pattern")
			require.Equal(t, c.wantDomain, got.Domain, "Domain must match name pattern")
		})
	}
}
