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

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/stretchr/testify/require"
)

func TestDetectVersion(t *testing.T) {
	// V1: no version field.
	v1 := []byte(`"v1.0-000000-000500-headers.seg" = "abc123"` + "\n")
	require.Equal(t, 1, DetectVersion(v1))

	// V2: has version field.
	v2 := []byte("version = 2\n[blocks]\n")
	require.Equal(t, 2, DetectVersion(v2))

	// Garbage.
	require.Equal(t, 1, DetectVersion([]byte("not valid toml {{{")))
}

func TestGenerateV2FromInventory(t *testing.T) {
	inv := snapshotinv.NewInventory()

	// Add block files.
	inv.AddFile(&snapshotinv.FileEntry{
		Name:        "v1.0-000000-000500-headers.seg",
		TorrentHash: [20]byte{0xab, 0xcd},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})

	// Add domain files — mix of canonical and non-canonical.
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    0,
		ToStep:      2048,
		Name:        "v1.0-accounts.0-2048.kv",
		TorrentHash: [20]byte{0x11, 0x22},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    2048,
		ToStep:      3072,
		Name:        "v1.0-accounts.2048-3072.kv",
		TorrentHash: [20]byte{0x33, 0x44},
		Local:       true,
		Trust:       snapshotinv.TrustConsensus,
	})
	// Non-canonical file (size 100, not power-of-2) — should be excluded.
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    3072,
		ToStep:      3172,
		Name:        "v1.0-accounts.3072-3172.kv",
		TorrentHash: [20]byte{0x55, 0x66},
		Local:       true,
		Trust:       snapshotinv.TrustNone,
	})

	manifest := GenerateV2(inv)

	require.Equal(t, ChainTomlV2Version, manifest.Version)
	require.Len(t, manifest.Blocks, 1)
	require.Contains(t, manifest.Blocks, "v1.0-000000-000500-headers.seg")

	// Domain: only canonical files included.
	acc := manifest.Domains["accounts"]
	require.NotNil(t, acc)
	require.Len(t, acc.Files, 2) // 0-2048 and 2048-3072 (both canonical), 3072-3172 excluded (non-pow2)
	require.Equal(t, "v1.0-accounts.0-2048.kv", acc.Files[0].Name)
	require.Equal(t, "verified", acc.Files[0].Trust)
	require.Equal(t, "v1.0-accounts.2048-3072.kv", acc.Files[1].Name)
	require.Equal(t, "consensus", acc.Files[1].Trust)
}

func TestV2MarshalRoundTrip(t *testing.T) {
	inv := snapshotinv.NewInventory()
	inv.AddFile(&snapshotinv.FileEntry{
		Name:        "v1.0-000000-000500-headers.seg",
		TorrentHash: [20]byte{0xab},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    0,
		ToStep:      4096,
		Name:        "v1.0-accounts.0-4096.kv",
		TorrentHash: [20]byte{0xcd},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})

	original := GenerateV2(inv)

	data, err := MarshalV2(original)
	require.NoError(t, err)

	parsed, err := ParseV2(data)
	require.NoError(t, err)

	require.Equal(t, original.Version, parsed.Version)
	require.Equal(t, original.Blocks, parsed.Blocks)
	require.Len(t, parsed.Domains, 1)
	require.Len(t, parsed.Domains["accounts"].Files, 1)
	require.Equal(t, original.Domains["accounts"].Files[0].Name, parsed.Domains["accounts"].Files[0].Name)
}

func TestParseV2RejectsV1(t *testing.T) {
	v1 := []byte(`"v1.0-headers.seg" = "abc123"` + "\n")
	_, err := ParseV2(v1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "version")
}

// TestV2FullScopeRoundTrip exercises every section a V2 manifest can carry —
// blocks, meta, salt, domains with mixed kv/history/idx files, and caplin.
// Roundtrip must preserve names, hashes, kinds, and per-domain coverage.
func TestV2FullScopeRoundTrip(t *testing.T) {
	inv := snapshotinv.NewInventory()

	inv.AddFile(&snapshotinv.FileEntry{
		Name:        "v1.1-000000-000100-headers.seg",
		TorrentHash: [20]byte{0xb1}, Local: true, Trust: snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Kind: snapshotinv.KindMeta, Name: "erigondb.toml",
		TorrentHash: [20]byte{0x1e}, Local: true, Trust: snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Kind: snapshotinv.KindSalt, Name: "salt-blocks.txt",
		TorrentHash: [20]byte{0x5a}, Local: true, Trust: snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Kind: snapshotinv.KindCaplin, Name: "v1.1-000000-000010-beaconblocks.seg",
		TorrentHash: [20]byte{0xca}, Local: true, Trust: snapshotinv.TrustVerified,
	})
	for _, kind := range []snapshotinv.FileKind{snapshotinv.KindKV, snapshotinv.KindHistory, snapshotinv.KindIdx} {
		ext := map[snapshotinv.FileKind]string{snapshotinv.KindKV: "kv", snapshotinv.KindHistory: "v", snapshotinv.KindIdx: "ef"}[kind]
		inv.AddFile(&snapshotinv.FileEntry{
			Domain: snapshotinv.DomainAccounts, Kind: kind,
			FromStep: 0, ToStep: 128,
			Name:        "v1.1-accounts.0-128." + ext,
			TorrentHash: [20]byte{0xa0 + byte(len(ext))},
			Local:       true, Trust: snapshotinv.TrustVerified,
		})
	}

	original := GenerateV2(inv)
	data, err := MarshalV2(original)
	require.NoError(t, err)

	parsed, err := ParseV2(data)
	require.NoError(t, err)

	require.Equal(t, original.Blocks, parsed.Blocks)
	require.Equal(t, original.Meta, parsed.Meta)
	require.Equal(t, original.Salt, parsed.Salt)
	require.Equal(t, original.Caplin, parsed.Caplin)

	require.Len(t, parsed.Domains, 1)
	files := parsed.Domains["accounts"].Files
	require.Len(t, files, 3)
	gotKinds := map[string]bool{}
	for _, f := range files {
		gotKinds[f.Kind] = true
		require.Equal(t, [2]uint64{0, 128}, f.Range)
	}
	require.True(t, gotKinds["kv"] && gotKinds["history"] && gotKinds["idx"],
		"every kind must roundtrip; got %v", gotKinds)

	// Coverage is computed from kv only — even with history+idx present.
	require.Equal(t, [2]uint64{0, 128}, parsed.Domains["accounts"].Coverage)

	// Determinism: marshalling the same inventory twice must produce
	// byte-identical output.
	again, err := MarshalV2(GenerateV2(inv))
	require.NoError(t, err)
	require.Equal(t, data, again, "GenerateV2 + MarshalV2 must be deterministic")
}

func TestDomainManifestStepRanges(t *testing.T) {
	dm := &DomainManifest{
		Coverage: [2]uint64{0, 4096},
		Files: []DomainFileEntry{
			{Name: "a.kv", Range: [2]uint64{0, 2048}, Hash: "aa", Trust: "verified"},
			{Name: "b.kv", Range: [2]uint64{2048, 4096}, Hash: "bb", Trust: "consensus"},
		},
	}

	ranges := dm.StepRanges()
	require.True(t, ranges.IsComplete(0, 4096))
	require.Equal(t, uint64(4096), ranges.Coverage())
}

func TestDomainManifestFilesAtTrust(t *testing.T) {
	dm := &DomainManifest{
		Files: []DomainFileEntry{
			{Name: "a.kv", Trust: "verified"},
			{Name: "b.kv", Trust: "consensus"},
			{Name: "c.kv", Trust: "none"},
		},
	}

	require.Len(t, dm.FilesAtTrust(snapshotinv.TrustNone), 3)
	require.Len(t, dm.FilesAtTrust(snapshotinv.TrustConsensus), 2)
	require.Len(t, dm.FilesAtTrust(snapshotinv.TrustVerified), 1)
}

func TestIsCanonicalFile(t *testing.T) {
	// Canonical: power-of-2, aligned.
	require.True(t, isCanonicalFile(snapshotinv.StepRange{From: 0, To: 2048}))
	require.True(t, isCanonicalFile(snapshotinv.StepRange{From: 2048, To: 4096}))
	require.True(t, isCanonicalFile(snapshotinv.StepRange{From: 4096, To: 5120}))

	// Non-canonical: not power-of-2.
	require.False(t, isCanonicalFile(snapshotinv.StepRange{From: 0, To: 3000}))
	require.False(t, isCanonicalFile(snapshotinv.StepRange{From: 0, To: 100}))

	// Non-canonical: misaligned.
	require.False(t, isCanonicalFile(snapshotinv.StepRange{From: 100, To: 1124}))
}

// TestV2StepHeaderRoundTrip verifies that the step-header anchors
// (ProofRoot / AtBlock / AtTxNum / IsPartialBlock) on a FileEntry flow
// into the manifest, survive marshal+parse, and stay zero/empty for
// files that the validator hasn't populated yet.
func TestV2StepHeaderRoundTrip(t *testing.T) {
	inv := snapshotinv.NewInventory()

	// Block file — no step-header anchors apply (lives in Blocks section, not Domains).
	inv.AddFile(&snapshotinv.FileEntry{
		Name:        "v1.1-000000-000500-headers.seg",
		TorrentHash: [20]byte{0x01},
		Local:       true, Trust: snapshotinv.TrustVerified,
	})

	// Commitment file with the validator-populated step-header anchors —
	// represents a typical post-validation state.
	commitmentRoot := [32]byte{0xd1, 0xe5, 0x56, 0x19, 0xbb, 0x21, 0x05, 0xd1}
	inv.AddFile(&snapshotinv.FileEntry{
		Domain: snapshotinv.DomainCommitment, FromStep: 0, ToStep: 256,
		Name:        "v2.0-commitment.0-256.kv",
		TorrentHash: [20]byte{0x02},
		Local:       true, Trust: snapshotinv.TrustVerified,
		Anchors: snapshotinv.Anchors{
			Root: commitmentRoot, AtBlock: 12345, AtTxNum: 99999999, IsPartialBlock: true,
		},
	})

	// Accounts file — no step-header anchors yet (the state-trie
	// validator only records on the commitment domain in this iteration).
	inv.AddFile(&snapshotinv.FileEntry{
		Domain: snapshotinv.DomainAccounts, FromStep: 0, ToStep: 256,
		Name:        "v2.0-accounts.0-256.kv",
		TorrentHash: [20]byte{0x03},
		Local:       true, Trust: snapshotinv.TrustVerified,
	})

	manifest := GenerateV2(inv)
	bytes, err := MarshalV2(manifest)
	require.NoError(t, err)

	parsed, err := ParseV2(bytes)
	require.NoError(t, err)

	// Find the commitment file in the parsed manifest.
	commitmentDM := parsed.Domains["commitment"]
	require.NotNil(t, commitmentDM, "commitment domain should be in manifest")
	require.Len(t, commitmentDM.Files, 1)
	cf := commitmentDM.Files[0]

	// Step-header anchors round-tripped intact.
	require.Equal(t, "d1e55619bb2105d1000000000000000000000000000000000000000000000000", cf.ProofRoot)
	require.Equal(t, uint64(12345), cf.AtBlock)
	require.Equal(t, uint64(99999999), cf.AtTxNum)
	require.True(t, cf.IsPartialBlock, "commitment file should be marked partial-block")

	// Accounts file in the same domain group should have NO step-header
	// anchors — empty/zero on round-trip, not bogus all-zero hashes.
	accountsDM := parsed.Domains["accounts"]
	require.NotNil(t, accountsDM)
	require.Len(t, accountsDM.Files, 1)
	require.Empty(t, accountsDM.Files[0].ProofRoot, "accounts file without populated root must marshal to empty ProofRoot, not zero hash")
	require.Zero(t, accountsDM.Files[0].AtBlock)
	require.Zero(t, accountsDM.Files[0].AtTxNum)
	require.False(t, accountsDM.Files[0].IsPartialBlock)
}

// TestParseChainTomlAutoV1V2 verifies that the consumer's auto-detect
// parser (parseChainTomlAuto) routes V1 + V2 input into the same flat
// name -> hash map shape the merge path consumes. Step-header anchors
// are intentionally dropped at this layer — they need an inventory-aware
// application path to land on local FileEntries.
func TestParseChainTomlAutoV1V2(t *testing.T) {
	v1 := []byte("\"v1.1-000000-000500-headers.seg\" = \"aabb\"\n\"v1.1-000000-000500-bodies.seg\" = \"ccdd\"\n")
	out, err := parseChainTomlAuto(v1)
	require.NoError(t, err)
	require.Equal(t, "aabb", out["v1.1-000000-000500-headers.seg"])
	require.Equal(t, "ccdd", out["v1.1-000000-000500-bodies.seg"])

	inv := snapshotinv.NewInventory()
	inv.AddFile(&snapshotinv.FileEntry{
		Name: "v1.1-000000-000500-headers.seg", TorrentHash: [20]byte{0x01},
		Local: true, Trust: snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Domain: snapshotinv.DomainCommitment, FromStep: 0, ToStep: 256,
		Name: "v2.0-commitment.0-256.kv", TorrentHash: [20]byte{0x02},
		Local: true, Trust: snapshotinv.TrustVerified,
	})
	v2bytes, err := MarshalV2(GenerateV2(inv))
	require.NoError(t, err)

	out2, err := parseChainTomlAuto(v2bytes)
	require.NoError(t, err)
	require.Equal(t, "01"+strings.Repeat("00", 19), out2["v1.1-000000-000500-headers.seg"])
	require.Equal(t, "02"+strings.Repeat("00", 19), out2["v2.0-commitment.0-256.kv"])
}

// TestApplyV2AnchorsToInventory verifies the consumer-side anchor
// plumbing: a V2 manifest's step-header anchors land on matching local
// FileEntries via SetAnchors, and the optional cross-check rejects
// entries whose ProofRoot disagrees with the chain header's stateRoot.
func TestApplyV2AnchorsToInventory(t *testing.T) {
	publisher := snapshotinv.NewInventory()
	publisherRoot := [32]byte{0xd1, 0xe5, 0x56, 0x19}
	require.NoError(t, publisher.AddFile(&snapshotinv.FileEntry{
		Domain: snapshotinv.DomainCommitment, FromStep: 0, ToStep: 256,
		Name: "v2.0-commitment.0-256.kv", TorrentHash: [20]byte{0x02},
		Local: true, Trust: snapshotinv.TrustVerified,
		Anchors: snapshotinv.Anchors{
			Root: publisherRoot, AtBlock: 12345, AtTxNum: 99999, IsPartialBlock: true,
		},
	}))
	manifest := GenerateV2(publisher)

	// Local consumer inventory has the matching file but no anchors yet.
	consumer := snapshotinv.NewInventory()
	require.NoError(t, consumer.AddFile(&snapshotinv.FileEntry{
		Domain: snapshotinv.DomainCommitment, FromStep: 0, ToStep: 256,
		Name: "v2.0-commitment.0-256.kv", TorrentHash: [20]byte{0x02},
		Local: true, Trust: snapshotinv.TrustVerified,
	}))

	// No cross-check: anchors apply unconditionally.
	applied, mismatches, err := ApplyV2AnchorsToInventory(consumer, manifest, nil)
	require.NoError(t, err)
	require.Empty(t, mismatches)
	require.Equal(t, 1, applied)

	files := consumer.AllDomainFiles(snapshotinv.DomainCommitment)
	require.Len(t, files, 1)
	require.Equal(t, publisherRoot, files[0].Anchors.Root)
	require.Equal(t, uint64(12345), files[0].Anchors.AtBlock)
	require.Equal(t, uint64(99999), files[0].Anchors.AtTxNum)
	require.True(t, files[0].Anchors.IsPartialBlock)

	// With matching cross-check: still applies (idempotent).
	applied2, mismatches2, err := ApplyV2AnchorsToInventory(consumer, manifest, func(b uint64) ([32]byte, error) {
		require.Equal(t, uint64(12345), b)
		return publisherRoot, nil
	})
	require.NoError(t, err)
	require.Empty(t, mismatches2)
	require.Equal(t, 0, applied2, "second pass with same values is a no-op")

	// Mismatch: chain reports a different stateRoot — entry rejected.
	consumer2 := snapshotinv.NewInventory()
	require.NoError(t, consumer2.AddFile(&snapshotinv.FileEntry{
		Domain: snapshotinv.DomainCommitment, FromStep: 0, ToStep: 256,
		Name: "v2.0-commitment.0-256.kv", TorrentHash: [20]byte{0x02},
		Local: true, Trust: snapshotinv.TrustVerified,
	}))
	chainRoot := [32]byte{0xff, 0xff, 0xff, 0xff}
	applied3, mismatches3, err := ApplyV2AnchorsToInventory(consumer2, manifest, func(b uint64) ([32]byte, error) {
		return chainRoot, nil
	})
	require.NoError(t, err)
	require.Equal(t, 0, applied3, "mismatched anchor must not be applied")
	require.Len(t, mismatches3, 1)
	require.Equal(t, "v2.0-commitment.0-256.kv", mismatches3[0].Name)
	require.Equal(t, uint64(12345), mismatches3[0].AtBlock)
	require.Equal(t, publisherRoot, mismatches3[0].Manifest)
	require.Equal(t, chainRoot, mismatches3[0].ChainState)

	files2 := consumer2.AllDomainFiles(snapshotinv.DomainCommitment)
	require.Len(t, files2, 1)
	require.True(t, files2[0].Anchors.IsZero(), "rejected entry must keep zero anchors")
}

// TestFindPartialBlockCommitmentsWithoutCoverage mirrors the
// publisher-side defensive pause: a partial-block commitment is only
// usable as a snapshot tip when the same manifest carries the block
// .seg covering AtBlock. The consumer refuses to load entries the
// publisher hasn't bundled with their block data.
func TestFindPartialBlockCommitmentsWithoutCoverage(t *testing.T) {
	t.Parallel()

	makeManifest := func(commitFile DomainFileEntry, blockFiles map[string]string) *ChainTomlV2 {
		m := &ChainTomlV2{
			Version: ChainTomlV2Version,
			Blocks:  blockFiles,
			Domains: map[string]*DomainManifest{
				"commitment": {Files: []DomainFileEntry{commitFile}},
			},
		}
		return m
	}

	commitPartial := DomainFileEntry{
		Name: "v2.0-commitment.0-256.kv", Hash: "ab", Trust: "verified",
		Range: [2]uint64{0, 256}, Kind: KindKVName,
		ProofRoot: "ab", AtBlock: 25049601, AtTxNum: 99, IsPartialBlock: true,
	}
	commitWhole := DomainFileEntry{
		Name: "v2.0-commitment.0-256.kv", Hash: "ab", Trust: "verified",
		Range: [2]uint64{0, 256}, Kind: KindKVName,
		ProofRoot: "ab", AtBlock: 25049601, AtTxNum: 99, IsPartialBlock: false,
	}

	t.Run("partial_with_block_coverage_passes", func(t *testing.T) {
		m := makeManifest(commitPartial, map[string]string{
			"v1.1-025040-025050-headers.seg": "h",
			"v1.1-025040-025050-bodies.seg":  "b",
		})
		require.Empty(t, FindPartialBlockCommitmentsWithoutCoverage(m))
	})

	t.Run("partial_without_block_coverage_flagged", func(t *testing.T) {
		m := makeManifest(commitPartial, map[string]string{
			"v1.1-025030-025040-headers.seg": "h",
		})
		got := FindPartialBlockCommitmentsWithoutCoverage(m)
		require.Len(t, got, 1)
		require.Equal(t, "v2.0-commitment.0-256.kv", got[0].Name)
		require.Equal(t, uint64(25049601), got[0].AtBlock)
	})

	t.Run("non_partial_never_flagged", func(t *testing.T) {
		// Even with no block coverage at all, a non-partial commitment
		// is fine — block-aligned commitments don't need replay.
		m := makeManifest(commitWhole, map[string]string{})
		require.Empty(t, FindPartialBlockCommitmentsWithoutCoverage(m))
	})

	t.Run("nil_manifest_safe", func(t *testing.T) {
		require.Nil(t, FindPartialBlockCommitmentsWithoutCoverage(nil))
	})
}
