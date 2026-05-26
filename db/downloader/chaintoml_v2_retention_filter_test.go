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
)

func TestFilterManifestByRetentionFloor_NilAndZeroAreNoOps(t *testing.T) {
	require.NotPanics(t, func() {
		FilterManifestByRetentionFloor(nil, 1000)
	})

	m := &ChainTomlV2{
		Domains: map[string]*DomainManifest{
			"accounts": {
				Coverage: [2]uint64{0, 4096},
				Files: []DomainFileEntry{
					{Name: "v2.0-accounts.0-2048.kv", Range: [2]uint64{0, 2048}, Hash: "aa"},
					{Name: "v2.0-accounts.2048-4096.kv", Range: [2]uint64{2048, 4096}, Hash: "bb"},
				},
			},
		},
	}
	FilterManifestByRetentionFloor(m, 0)
	require.Len(t, m.Domains["accounts"].Files, 2,
		"floor=0 (full-history publisher) preserves every file")
	require.Equal(t, [2]uint64{0, 4096}, m.Domains["accounts"].Coverage)
}

func TestFilterManifestByRetentionFloor_DropsFilesEndingAtOrBelowFloor(t *testing.T) {
	m := &ChainTomlV2{
		Domains: map[string]*DomainManifest{
			"accounts": {
				Coverage: [2]uint64{0, 4096},
				Files: []DomainFileEntry{
					{Name: "v2.0-accounts.0-1024.kv", Range: [2]uint64{0, 1024}, Hash: "aa"},       // ends at floor → drop
					{Name: "v2.0-accounts.1024-2048.kv", Range: [2]uint64{1024, 2048}, Hash: "bb"}, // straddles → keep
					{Name: "v2.0-accounts.2048-4096.kv", Range: [2]uint64{2048, 4096}, Hash: "cc"}, // above → keep
				},
			},
		},
	}
	FilterManifestByRetentionFloor(m, 1024)

	require.Len(t, m.Domains["accounts"].Files, 2,
		"file ending exactly at the floor is dropped (its content is below)")
	require.Equal(t, "v2.0-accounts.1024-2048.kv", m.Domains["accounts"].Files[0].Name)
	require.Equal(t, [2]uint64{1024, 4096}, m.Domains["accounts"].Coverage,
		"Coverage[0] is pinned to the floor regardless of the lowest surviving file")
}

func TestFilterManifestByRetentionFloor_DropsEmptyDomains(t *testing.T) {
	m := &ChainTomlV2{
		Domains: map[string]*DomainManifest{
			"old": {
				Coverage: [2]uint64{0, 1024},
				Files: []DomainFileEntry{
					{Name: "v2.0-old.0-1024.kv", Range: [2]uint64{0, 1024}, Hash: "aa"},
				},
			},
			"current": {
				Coverage: [2]uint64{2048, 4096},
				Files: []DomainFileEntry{
					{Name: "v2.0-current.2048-4096.kv", Range: [2]uint64{2048, 4096}, Hash: "bb"},
				},
			},
		},
	}
	FilterManifestByRetentionFloor(m, 1500)
	require.NotContains(t, m.Domains, "old",
		"domain whose every file falls below the floor is removed entirely")
	require.Contains(t, m.Domains, "current")
	require.Equal(t, [2]uint64{1500, 4096}, m.Domains["current"].Coverage)
}

func TestFilterManifestByRetentionFloor_LeavesNonDomainBucketsAlone(t *testing.T) {
	// Block / meta / salt / caplin entries don't carry step ranges and
	// aren't filtered by the step floor. Their own retention semantics
	// (block-unit) are handled elsewhere.
	m := &ChainTomlV2{
		Blocks: blocksFromMap(map[string]string{
			"v1.0-000000-001000-headers.seg": "aa",
		}),
		Meta:   map[string]string{"erigondb.toml": "bb"},
		Salt:   map[string]string{"salt-state.txt": "cc"},
		Caplin: []CaplinFileEntry{{Name: "v1.0-000000-001000-caplin.seg", Hash: "dd"}},
	}
	FilterManifestByRetentionFloor(m, 1_000_000)
	require.Len(t, m.Blocks, 1, "block entries are not step-filtered")
	require.Len(t, m.Meta, 1, "meta entries are chain-wide")
	require.Len(t, m.Salt, 1, "salt entries are chain-wide")
	require.Len(t, m.Caplin, 1, "caplin entries follow their own retention")
}
