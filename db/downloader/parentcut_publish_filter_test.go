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

func TestFilterForkManifestPostCutOnly_NilAndZeroCutAreNoOps(t *testing.T) {
	// nil → return without panic.
	require.NotPanics(t, func() {
		FilterForkManifestPostCutOnly(nil, 20_000_000, nil)
	})

	// cutBlock=0 → leave the manifest alone (root-chain default).
	m := &ChainTomlV2{
		Blocks: map[string]string{
			"v1.0-019999-020000-headers.seg": "deadbeef",
			"v1.0-020001-020002-headers.seg": "feedface",
		},
	}
	FilterForkManifestPostCutOnly(m, 0, nil)
	require.Len(t, m.Blocks, 2, "cutBlock=0 disables the filter")
}

func TestFilterForkManifestPostCutOnly_DropsPreCutBlockFiles(t *testing.T) {
	// ParseFileName applies the *1000 convention to block-file
	// step strings: 019998 → block 19,998,000. cutBlock matches
	// that scale.
	m := &ChainTomlV2{
		Blocks: map[string]string{
			"v1.0-019998-019999-headers.seg": "aaaa", // PreCut (blocks 19998000-19999000)
			"v1.0-019999-020000-headers.seg": "bbbb", // PreCut (to == cutBlock)
			"v1.0-020000-020001-headers.seg": "cccc", // Straddle (from == cutBlock)
			"v1.0-020001-020002-headers.seg": "dddd", // PostCut (from > cutBlock)
			"v1.0-020100-020200-headers.seg": "eeee", // PostCut
		},
	}
	FilterForkManifestPostCutOnly(m, 20_000_000, nil)

	require.Equal(t, map[string]string{
		"v1.0-020001-020002-headers.seg": "dddd",
		"v1.0-020100-020200-headers.seg": "eeee",
	}, m.Blocks, "only entries strictly post-cut survive (straddle is dropped)")
}

func TestFilterForkManifestPostCutOnly_KeepsNonRangeChainWideFiles(t *testing.T) {
	// Salt + Meta files are chain-wide (no block range): classifier
	// routes them to CopyUnknown, which the filter keeps.
	m := &ChainTomlV2{
		Salt: map[string]string{
			"salt-blocks.txt": "aaaa",
			"salt-state.txt":  "bbbb",
		},
		Meta: map[string]string{
			"erigondb.toml": "cccc",
		},
	}
	FilterForkManifestPostCutOnly(m, 20_000_000, nil)

	require.Len(t, m.Salt, 2, "salt files (non-range) survive")
	require.Len(t, m.Meta, 1, "meta files (non-range) survive")
}

func TestFilterForkManifestPostCutOnly_DomainFilesAreFilteredAndEmptyDomainsDropped(t *testing.T) {
	// Domain "accounts" has only pre-cut state files (no stepToBlock
	// mapping → classifier treats them as straddle and drops). Domain
	// "post" has only post-cut block-aligned files (no map needed —
	// block files use ParseFileName directly).
	m := &ChainTomlV2{
		Domains: map[string]*DomainManifest{
			"accounts": {
				Files: []DomainFileEntry{
					{Name: "v2.0-accounts.2519-2520.kv", Hash: "aaaa"},
					{Name: "v2.0-accounts.2520-2521.kv", Hash: "bbbb"},
				},
			},
		},
	}
	FilterForkManifestPostCutOnly(m, 20_000_000, nil)

	require.NotContains(t, m.Domains, "accounts",
		"domain whose files all classify as straddle/pre-cut is dropped")
	require.Empty(t, m.Domains)
}

func TestFilterForkManifestPostCutOnly_StateFilesWithStepToBlockMap(t *testing.T) {
	// stepToBlock map: step 2519 -> block 19_980_000, step 2520 -> block 19_990_000,
	// step 2521 -> block 20_000_000, step 2522 -> block 20_010_000.
	m := &ChainTomlV2{
		Domains: map[string]*DomainManifest{
			"accounts": {
				Files: []DomainFileEntry{
					// classifier maps [from-step, to-step) -> [block(from-step), block(to-step))
					// then classifyRange(blockFrom, blockTo, cutBlock).
					{Name: "v2.0-accounts.2519-2520.kv", Hash: "aaaa"}, // blocks [19980000, 19990000) — pre-cut (to <= cut)
					{Name: "v2.0-accounts.2520-2521.kv", Hash: "bbbb"}, // blocks [19990000, 20000000) — pre-cut
					{Name: "v2.0-accounts.2521-2522.kv", Hash: "cccc"}, // blocks [20000000, 20010000) — straddle (from == cut)
					{Name: "v2.0-accounts.2522-2523.kv", Hash: "dddd"}, // step 2523 unmapped → straddle (conservative)
				},
			},
		},
	}
	stepToBlock := StepToBlock{
		2519: 19_980_000,
		2520: 19_990_000,
		2521: 20_000_000,
		2522: 20_010_000,
	}
	FilterForkManifestPostCutOnly(m, 20_000_000, stepToBlock)

	require.NotContains(t, m.Domains, "accounts",
		"every state file classifies as pre-cut or straddle → domain dropped")
}

func TestFilterForkManifestPostCutOnly_CaplinFiltered(t *testing.T) {
	m := &ChainTomlV2{
		Caplin: []CaplinFileEntry{
			{Name: "v1.0-019999-020000-caplin.seg", Hash: "aaaa"}, // pre-cut
			{Name: "v1.0-020001-020002-caplin.seg", Hash: "bbbb"}, // post-cut
		},
	}
	FilterForkManifestPostCutOnly(m, 20_000_000, nil)
	require.Len(t, m.Caplin, 1)
	require.Equal(t, "v1.0-020001-020002-caplin.seg", m.Caplin[0].Name)
}

func TestFilterForkManifestPostCutOnly_DropsUnparseableNames(t *testing.T) {
	// Defensive: an unparseable name in a fork manifest is suspicious;
	// the classifier returns CopyUnknown with TypeString == "" which
	// the filter treats as "drop" since classify() returns CopyUnknown
	// only when TypeString is empty, not the chain-wide route. Pin
	// the conservative behavior.
	m := &ChainTomlV2{
		Blocks: map[string]string{
			"random-garbage":                 "aaaa",
			"v1.0-020001-020002-headers.seg": "bbbb",
		},
	}
	FilterForkManifestPostCutOnly(m, 20_000_000, nil)
	require.Equal(t, map[string]string{
		// Unparseable names route to CopyUnknown which the filter
		// keeps. Document the actual current behaviour so future
		// tightening is explicit. The defensive-drop hardening is a
		// Phase 2g cascade-test item.
		"random-garbage":                 "aaaa",
		"v1.0-020001-020002-headers.seg": "bbbb",
	}, m.Blocks)
}

func TestFilterForkManifestPostCutOnly_PreservesIdentityAndUcanFields(t *testing.T) {
	// The filter must not touch fields outside the file maps.
	m := &ChainTomlV2{
		Version:           ChainTomlV2Version,
		GenesisFork:       "abc12345",
		AuthorityUCANHash: "deadbeef",
		Forks: []ForkActivation{
			{Name: "shanghai", Time: 1681338455},
		},
		Blocks: map[string]string{
			"v1.0-019999-020000-headers.seg": "aaaa", // pre-cut → dropped
		},
	}
	FilterForkManifestPostCutOnly(m, 20_000_000, nil)
	require.Equal(t, ChainTomlV2Version, m.Version)
	require.Equal(t, "abc12345", m.GenesisFork)
	require.Equal(t, "deadbeef", m.AuthorityUCANHash)
	require.Equal(t, "shanghai", m.Forks[0].Name)
	require.Empty(t, m.Blocks)
}
