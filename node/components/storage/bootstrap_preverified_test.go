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

	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/components/storage/flow"
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

// TestBuildBootstrapManifest_RespectsPruneMode pins the L2 contract:
// buildBootstrapManifest must (1) apply the prune-mode filter via
// snapshotsync.FilterPreverifiedByPruneMode and (2) route the
// surviving entries into the correct PeerManifestReceived buckets. A
// regression in either piece is the bug-N class — a publisher under
// --prune.mode=minimal pulling the full archive history (1.3 TB+ of
// .v files) onto disk.
//
// Integration sentinel linking L1's filter contract to the observable
// manifest the orchestrator receives.
func TestBuildBootstrapManifest_RespectsPruneMode(t *testing.T) {
	t.Parallel()

	const sampleHash = "0123456789abcdef0123456789abcdef01234567"
	mkItem := func(name string) preverified.Item {
		return preverified.Item{Name: name, Hash: sampleHash}
	}
	items := snapcfg.PreverifiedItems{
		mkItem("domain/v1.0-accounts.0-1024.kv"),
		mkItem("domain/v1.0-storage.0-1024.kv"),
		mkItem("domain/v1.0-commitment.0-1024.kv"),
		mkItem("history/v1.0-accountsHistory.0-1024.v"),
		mkItem("idx/v1.0-accountsIdx.0-1024.ef"),
		mkItem("accessor/v1.0-history.0-1024.vi"),
		mkItem("v1.0-000000-000500-headers.seg"),
		mkItem("v1.0-000000-000500-bodies.seg"),
		mkItem("caplin/v1.1-000000-000010-beaconblocks.seg"),
		mkItem("salt-state.txt"),
		mkItem("erigondb.toml"),
	}

	mergeHeight := uint64(15_000_000)
	cc := &chain.Config{MergeHeight: &mergeHeight}

	gather := func(m flow.PeerManifestReceived) map[string]struct{} {
		set := make(map[string]struct{})
		for _, e := range m.Blocks {
			set[e.Name] = struct{}{}
		}
		for _, e := range m.Caplin {
			set[e.Name] = struct{}{}
		}
		for _, e := range m.Meta {
			set[e.Name] = struct{}{}
		}
		for _, e := range m.Salt {
			set[e.Name] = struct{}{}
		}
		for _, list := range m.Domains {
			for _, e := range list {
				set[e.Name] = struct{}{}
			}
		}
		return set
	}

	t.Run("archive mode keeps every entry; bucket routing intact", func(t *testing.T) {
		m := buildBootstrapManifest(items, cc, prune.ArchiveMode, nil)
		require.Equal(t, "bootstrap-preverified", m.PeerID,
			"PeerID sentinel must be stable — orchestrator peer-attribution depends on it")

		got := gather(m)
		require.Len(t, got, len(items), "archive must keep every preverified entry")

		require.NotEmpty(t, m.Domains[snapshot.DomainAccounts],
			"state-domain entries must land in Domains[<domain>]")
		blockNames := make([]string, len(m.Blocks))
		for i, e := range m.Blocks {
			blockNames[i] = e.Name
		}
		require.Contains(t, blockNames, "v1.0-000000-000500-headers.seg",
			"block files must land in Blocks")
		caplinNames := make([]string, len(m.Caplin))
		for i, e := range m.Caplin {
			caplinNames[i] = e.Name
		}
		require.Contains(t, caplinNames, "caplin/v1.1-000000-000010-beaconblocks.seg",
			"caplin/ entries must land in Caplin")
		metaNames := make([]string, len(m.Meta))
		for i, e := range m.Meta {
			metaNames[i] = e.Name
		}
		require.Contains(t, metaNames, "erigondb.toml",
			"meta entries must land in Meta")
		saltNames := make([]string, len(m.Salt))
		for i, e := range m.Salt {
			saltNames[i] = e.Name
		}
		require.Contains(t, saltNames, "salt-state.txt",
			"salt entries must land in Salt")
	})

	t.Run("minimal mode drops state history; bucket routing intact for survivors", func(t *testing.T) {
		m := buildBootstrapManifest(items, cc, prune.MinimalMode, nil)
		got := gather(m)

		// Bug-N regression sentinel: history/idx/accessor + caplin
		// archive must NOT appear under minimal mode. State history
		// is ~1.3 TB on mainnet; caplin archive is ~150 GB. Both
		// inflated the filled disk that the original bug-N report
		// hit (1.9 TB total).
		require.NotContains(t, got, "history/v1.0-accountsHistory.0-1024.v",
			"minimal must drop state history (.v in history/) — this is the file kind that filled the 1.3 TB disk in bug N")
		require.NotContains(t, got, "idx/v1.0-accountsIdx.0-1024.ef",
			"minimal must drop state-history indexes (.ef in idx/)")
		require.NotContains(t, got, "accessor/v1.0-history.0-1024.vi",
			"minimal must drop state-history accessors (.vi in accessor/)")
		require.NotContains(t, got, "caplin/v1.1-000000-000010-beaconblocks.seg",
			"minimal must drop caplin/ archive — operator opts into it via --caplin.archive, not by being a publisher")

		// State primaries + blocks + config still present.
		require.Contains(t, got, "domain/v1.0-accounts.0-1024.kv")
		require.Contains(t, got, "v1.0-000000-000500-headers.seg")
		require.Contains(t, got, "salt-state.txt")
		require.Contains(t, got, "erigondb.toml")
	})

	t.Run("uninitialised prune mode is a defensive no-op pass-through", func(t *testing.T) {
		var zero prune.Mode
		m := buildBootstrapManifest(items, cc, zero, nil)
		got := gather(m)
		require.Len(t, got, len(items),
			"uninitialised prune.Mode must not silently filter; callers wire an initialised mode when they want filtering")
	})
}
