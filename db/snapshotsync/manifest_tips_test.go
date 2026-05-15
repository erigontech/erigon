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

package snapshotsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/snapcfg"
)

// TestDeriveManifestTips_AlignedFullManifest pins the canonical happy
// path: headers, bodies, transactions all extend to the same block-file
// range, commitment goes to a single highest step. The tips reflect
// that aligned state cleanly.
func TestDeriveManifestTips_AlignedFullManifest(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		// State files — commitment is the state-tip anchor.
		// Step naming uses domain/v<VER>-NAME.FROM-TO.kv.
		preverified.Item{Name: "domain/v2.0-accounts.0-8192.kv", Hash: "0001"},
		preverified.Item{Name: "domain/v2.0-accounts.8192-8704.kv", Hash: "0002"},
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "0003"},
		preverified.Item{Name: "domain/v2.0-commitment.8192-8704.kv", Hash: "0004"},

		// Block files — naming uses v<VER>-FROM-TO-KIND.seg where
		// FROM/TO are in thousands of blocks. snapshotsync.go:137
		// comment documents this convention.
		preverified.Item{Name: "v1.1-000000-000500-headers.seg", Hash: "1001"},
		preverified.Item{Name: "v1.1-000500-001000-headers.seg", Hash: "1002"},
		preverified.Item{Name: "v1.1-000000-000500-bodies.seg", Hash: "2001"},
		preverified.Item{Name: "v1.1-000500-001000-bodies.seg", Hash: "2002"},
		preverified.Item{Name: "v1.1-000000-000500-transactions.seg", Hash: "3001"},
		preverified.Item{Name: "v1.1-000500-001000-transactions.seg", Hash: "3002"},

		// Non-tip-contributing entries (must be ignored).
		preverified.Item{Name: "salt-state.txt", Hash: "9001"},
		preverified.Item{Name: "erigondb.toml", Hash: "9002"},
		preverified.Item{Name: "accessor/v1.1-accounts.0-8192.bt", Hash: "9003"},
	}

	tips := DeriveManifestTips(items)

	// All three block-types extend to file ToBlock=1000 (in K-units).
	// Convert to inclusive block number: 1000 * 1000 - 1 = 999_999.
	require.Equal(t, uint64(999_999), tips.BlockTip,
		"BlockTip is the inclusive highest block where all three of (headers, bodies, transactions) are advertised")

	// Highest commitment step end is 8704.
	require.Equal(t, uint64(8704), tips.StateTipStep,
		"StateTipStep is the highest commitment.kv To-step")
}

// TestDeriveManifestTips_MinOfMaxes_ShortBodies pins the multi-type
// alignment rule: if bodies don't extend as far as headers and
// transactions, BlockTip collapses to the lowest common bound.
//
// This catches a producer bug where a publisher republishes a chain.v2
// missing some bodies — consumers see the tip at the lowest-common
// point, not at the highest headers point.
func TestDeriveManifestTips_MinOfMaxes_ShortBodies(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		// Headers + transactions go to To=2000; bodies stop at To=1500.
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "1001"},
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "1002"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "2001"},
		preverified.Item{Name: "v1.1-001000-001500-bodies.seg", Hash: "2002"},
		preverified.Item{Name: "v1.1-000000-001000-transactions.seg", Hash: "3001"},
		preverified.Item{Name: "v1.1-001000-002000-transactions.seg", Hash: "3002"},
	}

	tips := DeriveManifestTips(items)

	// Bodies max is 1500, others 2000. Min-of-maxes is 1500.
	// Inclusive block: 1500 * 1000 - 1 = 1_499_999.
	require.Equal(t, uint64(1_499_999), tips.BlockTip,
		"BlockTip uses the minimum of per-type max-To so the tip is the highest block with all three types present")
}

// TestDeriveManifestTips_MissingType pins that BlockTip is 0 when any
// of the three required types is absent. Headers + bodies present, no
// transactions → no tip.
//
// This is the right behaviour: a manifest that doesn't advertise
// transactions cannot define a block-tip for execution, since blocks
// can't be executed without their transactions.
func TestDeriveManifestTips_MissingType(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "1001"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "2001"},
		// no transactions
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "0003"},
	}

	tips := DeriveManifestTips(items)
	require.Equal(t, uint64(0), tips.BlockTip,
		"BlockTip is 0 when any of headers/bodies/transactions is absent — manifest is incomplete for execution")
	require.Equal(t, uint64(8192), tips.StateTipStep,
		"StateTipStep independent of block-tip completeness")
}

// TestDeriveManifestTips_CLDataIgnored pins that beaconblocks /
// blobsidecars / caplin-prefixed entries do not contribute to block-
// tip. CL data covers different ranges with different semantics; the
// EL-side tips are EL-only.
func TestDeriveManifestTips_CLDataIgnored(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "1001"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "2001"},
		preverified.Item{Name: "v1.1-000000-001000-transactions.seg", Hash: "3001"},

		// CL entries with much higher "To" values must NOT influence
		// BlockTip — they're not EL block files.
		preverified.Item{Name: "v1.1-013500-014000-beaconblocks.seg", Hash: "c001"},
		preverified.Item{Name: "v1.1-013500-014000-blobsidecars.seg", Hash: "c002"},
		preverified.Item{Name: "caplin/v1.1-013500-014000-ActiveValidatorIndicies.seg", Hash: "c003"},
	}

	tips := DeriveManifestTips(items)
	require.Equal(t, uint64(999_999), tips.BlockTip,
		"BlockTip ignores CL entries (caplin/, beaconblocks, blobsidecars) — they're separately filtered upstream and must not contaminate EL tip")
}

// TestDeriveManifestTips_EmptyManifest pins the zero case: no entries,
// no tips.
func TestDeriveManifestTips_EmptyManifest(t *testing.T) {
	t.Parallel()

	tips := DeriveManifestTips(snapcfg.PreverifiedItems{})
	require.Equal(t, uint64(0), tips.BlockTip)
	require.Equal(t, uint64(0), tips.StateTipStep)
}

// TestDeriveManifestTips_StateOnlyNoBlocks pins that a manifest with
// only state-domain entries (no block files) returns a state-tip but
// zero block-tip. This shape can arise during early bootstrap
// synthesis or in a malformed manifest.
func TestDeriveManifestTips_StateOnlyNoBlocks(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "0001"},
		preverified.Item{Name: "domain/v2.0-accounts.0-8192.kv", Hash: "0002"},
	}

	tips := DeriveManifestTips(items)
	require.Equal(t, uint64(0), tips.BlockTip)
	require.Equal(t, uint64(8192), tips.StateTipStep)
}

// TestDeriveManifestTips_NonCommitmentDomainsIgnored pins that only
// commitment.kv files contribute to StateTipStep. Accounts/storage/
// code/receipt step-ends may legitimately lag commitment by one step
// during retire — so they're not authoritative for state-tip
// purposes. The commitment domain is the canonical state-root anchor.
func TestDeriveManifestTips_NonCommitmentDomainsIgnored(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		// Accounts goes to step 16000 — much higher than commitment.
		preverified.Item{Name: "domain/v2.0-accounts.0-8192.kv", Hash: "0001"},
		preverified.Item{Name: "domain/v2.0-accounts.8192-16000.kv", Hash: "0002"},
		// But commitment only goes to step 8192.
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "0003"},
	}

	tips := DeriveManifestTips(items)
	require.Equal(t, uint64(8192), tips.StateTipStep,
		"StateTipStep follows commitment.kv only — accounts/storage/code/receipt step-ends are not authoritative")
}
