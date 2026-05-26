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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Consumer-side cascade -ve tests for fork manifests. Mirrors
// FilterForkManifestPostCutOnly (producer side): where the producer
// drops, the consumer rejects. A fork-follower running this
// validator on a received fork manifest must reject any entry the
// fork operator's authority cannot legitimately speak about.
//
// Per memory/fork-trust-cascade-ve-tests-2026-05-24, this closes the
// "Fork manifest contains a pre-cut entry" + "Fork manifest contains
// a straddle entry" rows of the read-side cascade.

func TestValidateForkManifestPostCutOnly_NilAndZeroCutAreNoOps(t *testing.T) {
	require.NoError(t, ValidateForkManifestPostCutOnly(nil, 20_000_000, nil),
		"nil manifest → nil (no-op consistent with the filter)")

	// cutBlock=0 → no fork rules apply; manifest passes regardless.
	m := &ChainTomlV2{
		Blocks: blocksFromMap(map[string]string{
			"v1.0-019999-020000-headers.seg": "aaaa",
		}),
	}
	require.NoError(t, ValidateForkManifestPostCutOnly(m, 0, nil),
		"cutBlock=0 disables the validator (non-fork manifest)")
}

func TestValidateForkManifestPostCutOnly_AcceptsCleanPostCutManifest(t *testing.T) {
	// Honest fork-publisher emission: post-cut block files + chain-
	// wide salt + meta. No pre-cut, no straddle, no unparseable.
	m := &ChainTomlV2{
		Blocks: blocksFromMap(map[string]string{
			"v1.0-020001-020002-headers.seg": "aaaa",
			"v1.0-020100-020200-headers.seg": "bbbb",
		}),
		Salt: map[string]string{
			"salt-blocks.txt": "cccc",
			"salt-state.txt":  "dddd",
		},
		Meta: map[string]string{
			"erigondb.toml": "eeee",
		},
	}
	require.NoError(t, ValidateForkManifestPostCutOnly(m, 20_000_000, nil))
}

func TestValidateForkManifestPostCutOnly_RejectsPreCutBlockFile(t *testing.T) {
	m := &ChainTomlV2{
		Blocks: blocksFromMap(map[string]string{
			"v1.0-020001-020002-headers.seg": "aaaa", // post-cut
			"v1.0-019998-019999-headers.seg": "bbbb", // pre-cut
		}),
	}
	err := ValidateForkManifestPostCutOnly(m, 20_000_000, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrForkManifestContainsForbiddenEntry)
	require.Contains(t, err.Error(), "v1.0-019998-019999-headers.seg")
	require.Contains(t, err.Error(), "pre-cut",
		"error must name which classification rule rejected the entry")
}

func TestValidateForkManifestPostCutOnly_RejectsStraddleBlockFile(t *testing.T) {
	// from == cutBlock → spans (Straddle).
	m := &ChainTomlV2{
		Blocks: blocksFromMap(map[string]string{
			"v1.0-020000-020001-headers.seg": "aaaa",
		}),
	}
	err := ValidateForkManifestPostCutOnly(m, 20_000_000, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrForkManifestContainsForbiddenEntry)
	require.Contains(t, err.Error(), "spans cut block")
}

func TestValidateForkManifestPostCutOnly_RejectsPreCutCaplinFile(t *testing.T) {
	m := &ChainTomlV2{
		Caplin: []CaplinFileEntry{
			{Name: "v1.0-020001-020002-caplin.seg", Hash: "aaaa"}, // post-cut
			{Name: "v1.0-019998-019999-caplin.seg", Hash: "bbbb"}, // pre-cut
		},
	}
	err := ValidateForkManifestPostCutOnly(m, 20_000_000, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrForkManifestContainsForbiddenEntry)
	require.Contains(t, err.Error(), "v1.0-019998-019999-caplin.seg")
}

func TestValidateForkManifestPostCutOnly_RejectsPreCutDomainFile(t *testing.T) {
	// Step→block map places step 2519 at block 19,990,000 (pre-cut).
	m := &ChainTomlV2{
		Domains: map[string]*DomainManifest{
			"accounts": {
				Files: []DomainFileEntry{
					{Name: "v2.0-accounts.2519-2520.kv", Hash: "aaaa"},
				},
			},
		},
	}
	stepToBlock := StepToBlock{
		2519: 19_990_000,
		2520: 20_000_000,
	}
	err := ValidateForkManifestPostCutOnly(m, 20_000_000, stepToBlock)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrForkManifestContainsForbiddenEntry)
	require.Contains(t, err.Error(), "v2.0-accounts.2519-2520.kv")
}

func TestValidateForkManifestPostCutOnly_RejectsStraddleStateFileWithUnmappedStep(t *testing.T) {
	// Step 2521 unmapped → classifier treats as straddle (conservative).
	m := &ChainTomlV2{
		Domains: map[string]*DomainManifest{
			"accounts": {
				Files: []DomainFileEntry{
					{Name: "v2.0-accounts.2521-2522.kv", Hash: "aaaa"},
				},
			},
		},
	}
	err := ValidateForkManifestPostCutOnly(m, 20_000_000, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrForkManifestContainsForbiddenEntry)
}

func TestValidateForkManifestPostCutOnly_RejectsUnparseableName(t *testing.T) {
	// Defense-in-depth: an honest publisher's manifest never carries
	// unparseable filenames. The consumer must reject suspicious input
	// even when the producer-side filter is conservative-keeping.
	m := &ChainTomlV2{
		Blocks: blocksFromMap(map[string]string{
			"random-garbage": "aaaa",
		}),
	}
	err := ValidateForkManifestPostCutOnly(m, 20_000_000, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrForkManifestContainsForbiddenEntry)
	require.Contains(t, err.Error(), "unparseable")
}

func TestValidateForkManifestPostCutOnly_DeterministicFirstReject(t *testing.T) {
	// Two pre-cut entries; the validator names the
	// alphabetically-first one (sorted key iteration). Pin the
	// determinism so failure messages stay stable across runs.
	m := &ChainTomlV2{
		Blocks: blocksFromMap(map[string]string{
			"v1.0-019999-020000-headers.seg": "zzzz", // pre-cut, sorts later
			"v1.0-019997-019998-headers.seg": "aaaa", // pre-cut, sorts first
		}),
	}
	err := ValidateForkManifestPostCutOnly(m, 20_000_000, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "v1.0-019997-019998-headers.seg",
		"sorted iteration must name the alphabetically-first violating entry")
}

func TestValidateForkManifestPostCutOnly_PreservesProducerEmittedManifestOK(t *testing.T) {
	// Round-trip: a manifest that has passed FilterForkManifestPostCutOnly
	// must always pass ValidateForkManifestPostCutOnly. Two halves of
	// the same trust rule. If this ever fails, producer + consumer
	// have drifted apart.
	m := &ChainTomlV2{
		Blocks: blocksFromMap(map[string]string{
			"v1.0-019998-019999-headers.seg": "aaaa", // pre-cut → filtered out
			"v1.0-020000-020001-headers.seg": "bbbb", // straddle → filtered out
			"v1.0-020001-020002-headers.seg": "cccc", // post-cut → kept
		}),
		Salt: map[string]string{
			"salt-blocks.txt": "dddd",
		},
		Domains: map[string]*DomainManifest{
			"accounts": {
				Files: []DomainFileEntry{
					{Name: "v2.0-accounts.2519-2520.kv", Hash: "eeee"}, // pre-cut → filtered
				},
			},
		},
	}
	stepToBlock := StepToBlock{2519: 19_990_000, 2520: 20_000_000}

	FilterForkManifestPostCutOnly(m, 20_000_000, stepToBlock)
	require.NoError(t, ValidateForkManifestPostCutOnly(m, 20_000_000, stepToBlock),
		"producer-filtered manifest must validate clean")

	// Sanity: the filter actually trimmed.
	require.Len(t, m.Blocks, 1)
	require.NotContains(t, m.Domains, "accounts")
}

func TestValidateForkManifestPostCutOnly_DomainWithNilEntryIsTolerated(t *testing.T) {
	// Defensive: a nil DomainManifest pointer (impossible from a
	// well-formed publisher, but possible from a malformed peer) must
	// not nil-deref.
	m := &ChainTomlV2{
		Domains: map[string]*DomainManifest{
			"accounts": nil,
		},
	}
	require.NotPanics(t, func() {
		_ = ValidateForkManifestPostCutOnly(m, 20_000_000, nil)
	})
}

func TestValidateForkManifestPostCutOnly_ErrorIsUnwrappable(t *testing.T) {
	// The sentinel ErrForkManifestContainsForbiddenEntry is the
	// stable contract callers (cascade tests, production code that
	// will gate fork-manifest acceptance) check against. errors.Is
	// must match.
	m := &ChainTomlV2{
		Blocks: blocksFromMap(map[string]string{
			"v1.0-019998-019999-headers.seg": "aaaa", // pre-cut
		}),
	}
	err := ValidateForkManifestPostCutOnly(m, 20_000_000, nil)
	require.True(t, errors.Is(err, ErrForkManifestContainsForbiddenEntry),
		"sentinel must be unwrappable from the returned error")
}
