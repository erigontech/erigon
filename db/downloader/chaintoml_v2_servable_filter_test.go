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

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"
)

func servableSet(hexes ...string) map[metainfo.Hash]struct{} {
	out := make(map[metainfo.Hash]struct{}, len(hexes))
	for _, h := range hexes {
		var ih metainfo.Hash
		if err := ih.FromHexString(h); err != nil {
			panic(err)
		}
		out[ih] = struct{}{}
	}
	return out
}

const (
	hashA = "aa00000000000000000000000000000000000000"
	hashB = "bb00000000000000000000000000000000000000"
	hashC = "cc00000000000000000000000000000000000000"
)

// TestFilterManifestByServable_NilSetIsNoOp pins the opt-out: callers
// without a torrent client (tests, the free publish helper) pass nil
// and every entry survives, preserving pre-gate behaviour.
func TestFilterManifestByServable_NilSetIsNoOp(t *testing.T) {
	require.NotPanics(t, func() { FilterManifestByServable(nil, nil) })

	m := &ChainTomlV2{
		Blocks: []BlockFileEntry{{Name: "v1.1-000000-000500-headers.seg", Hash: hashA}},
	}
	FilterManifestByServable(m, nil)
	require.Len(t, m.Blocks, 1, "nil servable set must not filter anything")
}

// TestFilterManifestByServable_DropsUnservableAcrossEverySection pins
// the gate V1 has had since "validate before advertise" was written.
// An entry whose info-hash is not loaded in the torrent client cannot
// be served, so advertising it hands peers a dead hash.
func TestFilterManifestByServable_DropsUnservableAcrossEverySection(t *testing.T) {
	m := &ChainTomlV2{
		Blocks: []BlockFileEntry{
			{Name: "servable.seg", Hash: hashA},
			{Name: "dropped.seg", Hash: hashB},
		},
		Meta:   map[string]string{"servable-meta": hashA, "dropped-meta": hashB},
		Salt:   map[string]string{"servable-salt": hashA, "dropped-salt": hashB},
		Caplin: []CaplinFileEntry{{Name: "servable-cl.seg", Hash: hashA}, {Name: "dropped-cl.seg", Hash: hashB}},
		Domains: map[string]*DomainManifest{
			"accounts": {
				Coverage: [2]uint64{0, 4096},
				Files: []DomainFileEntry{
					{Name: "servable.kv", Range: [2]uint64{0, 2048}, Hash: hashA},
					{Name: "dropped.kv", Range: [2]uint64{2048, 4096}, Hash: hashB},
				},
			},
		},
	}

	FilterManifestByServable(m, servableSet(hashA))

	require.Len(t, m.Blocks, 1)
	require.Equal(t, "servable.seg", m.Blocks[0].Name)
	require.Equal(t, map[string]string{"servable-meta": hashA}, m.Meta)
	require.Equal(t, map[string]string{"servable-salt": hashA}, m.Salt)
	require.Len(t, m.Caplin, 1)
	require.Equal(t, "servable-cl.seg", m.Caplin[0].Name)
	require.Len(t, m.Domains["accounts"].Files, 1)
	require.Equal(t, "servable.kv", m.Domains["accounts"].Files[0].Name)
}

// TestFilterManifestByServable_DropsDomainWhenNoFileSurvives pins that
// an emptied domain section is removed rather than left advertising a
// coverage range backed by nothing.
func TestFilterManifestByServable_DropsDomainWhenNoFileSurvives(t *testing.T) {
	m := &ChainTomlV2{
		Domains: map[string]*DomainManifest{
			"accounts": {
				Coverage: [2]uint64{0, 2048},
				Files:    []DomainFileEntry{{Name: "gone.kv", Range: [2]uint64{0, 2048}, Hash: hashB}},
			},
			"storage": {
				Coverage: [2]uint64{0, 2048},
				Files:    []DomainFileEntry{{Name: "kept.kv", Range: [2]uint64{0, 2048}, Hash: hashA}},
			},
		},
	}

	FilterManifestByServable(m, servableSet(hashA, hashC))

	require.NotContains(t, m.Domains, "accounts",
		"domain with no servable file must be dropped, not left advertising empty coverage")
	require.Contains(t, m.Domains, "storage")
}

// TestFilterManifestByServable_UnparsableHashIsDropped pins the
// conservative direction for a malformed hash: it cannot be matched
// against the servable set, so it cannot be served, so it must not be
// advertised.
func TestFilterManifestByServable_UnparsableHashIsDropped(t *testing.T) {
	m := &ChainTomlV2{
		Blocks: []BlockFileEntry{
			{Name: "good.seg", Hash: hashA},
			{Name: "malformed.seg", Hash: "not-hex"},
			{Name: "empty.seg", Hash: ""},
		},
	}

	FilterManifestByServable(m, servableSet(hashA))

	require.Len(t, m.Blocks, 1)
	require.Equal(t, "good.seg", m.Blocks[0].Name)
}
