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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// hashHex is a hex-decoding helper for the fixture setup: takes a
// 64-char lower-case hex string and returns the common.Hash it
// decodes to. Fails the test if the input isn't well-formed.
func hashHex(t *testing.T, s string) common.Hash {
	t.Helper()
	var out common.Hash
	_, err := hex.Decode(out[:], []byte(s))
	require.NoError(t, err)
	return out
}

func TestExpectedParentIdentityForChain_MainnetReturnsRegistryData(t *testing.T) {
	got, err := ExpectedParentIdentityForChain("mainnet", 0)
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, got.GenesisHash, "mainnet genesis must not be zero")
	require.NotEmpty(t, got.HeightForks, "mainnet has height-based forks")
}

func TestExpectedParentIdentityForChain_UnknownChainReturnsError(t *testing.T) {
	_, err := ExpectedParentIdentityForChain("no-such-chain-ever", 0)
	require.Error(t, err)
}

func TestValidateParentIdentity_NilSectionIsNoOp(t *testing.T) {
	require.NoError(t, ValidateParentIdentity(nil, ExpectedParentIdentity{}))
}

func TestValidateParentIdentity_GenesisHashMatches(t *testing.T) {
	const genesisHex = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3" // mainnet
	section := &ParentSection{
		Chain:             "mainnet",
		CutBlock:          20_000_000,
		ParentGenesisHash: genesisHex,
	}
	expected := ExpectedParentIdentity{
		GenesisHash: hashHex(t, genesisHex),
	}
	require.NoError(t, ValidateParentIdentity(section, expected))
}

func TestValidateParentIdentity_GenesisHashMismatchRejects(t *testing.T) {
	const manifestGenesis = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"
	const localGenesis = "0000000000000000000000000000000000000000000000000000000000000001"
	section := &ParentSection{
		Chain:             "mainnet",
		CutBlock:          20_000_000,
		ParentGenesisHash: manifestGenesis,
	}
	expected := ExpectedParentIdentity{
		GenesisHash: hashHex(t, localGenesis),
	}
	err := ValidateParentIdentity(section, expected)
	require.ErrorIs(t, err, ErrParentGenesisHashMismatch)
}

func TestValidateParentIdentity_GenesisHashMalformedHexRejects(t *testing.T) {
	section := &ParentSection{
		Chain:             "mainnet",
		CutBlock:          20_000_000,
		ParentGenesisHash: "not-hex!!not-hex!!not-hex!!not-hex!!not-hex!!not-hex!!not-hex!!aa",
	}
	err := ValidateParentIdentity(section, ExpectedParentIdentity{})
	require.ErrorIs(t, err, ErrParentGenesisHashMalformed)
}

func TestValidateParentIdentity_GenesisHashOmittedSkipsCheck(t *testing.T) {
	// A manifest without the ParentGenesisHash field skips the check
	// (permissive for early fork-from CLI invocations that predate
	// the E.2 hardening). Callers who want strict validation must
	// check the field is populated before invoking.
	section := &ParentSection{
		Chain:    "mainnet",
		CutBlock: 20_000_000,
	}
	require.NoError(t, ValidateParentIdentity(section, ExpectedParentIdentity{
		GenesisHash: hashHex(t, "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"),
	}))
}

func TestValidateParentIdentity_ForkIDMatchesOnIdenticalSchedules(t *testing.T) {
	const genesisHex = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"
	section := &ParentSection{
		Chain:             "mainnet",
		CutBlock:          20_000_000,
		ParentGenesisHash: genesisHex,
		ParentForks: []ForkActivation{
			{Name: "TangerineWhistle", Block: 2_463_000},
			{Name: "Byzantium", Block: 4_370_000},
			{Name: "Constantinople", Block: 7_280_000},
			{Name: "ShanghaiTime", Time: 1_681_338_455},
			{Name: "CancunTime", Time: 1_710_338_135},
		},
	}
	expected := ExpectedParentIdentity{
		GenesisHash: hashHex(t, genesisHex),
		HeightForks: []uint64{2_463_000, 4_370_000, 7_280_000},
		TimeForks:   []uint64{1_681_338_455, 1_710_338_135},
	}
	require.NoError(t, ValidateParentIdentity(section, expected))
}

func TestValidateParentIdentity_ForkIDMismatchOnDivergentHeights(t *testing.T) {
	const genesisHex = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"
	section := &ParentSection{
		Chain:             "mainnet",
		CutBlock:          20_000_000,
		ParentGenesisHash: genesisHex,
		ParentForks: []ForkActivation{
			{Name: "Byzantium", Block: 100},
		},
	}
	expected := ExpectedParentIdentity{
		GenesisHash: hashHex(t, genesisHex),
		HeightForks: []uint64{4_370_000},
	}
	err := ValidateParentIdentity(section, expected)
	require.ErrorIs(t, err, ErrParentForkIDMismatch)
}

func TestValidateParentIdentity_ForkIDToleratesExtraPostCutHeightFork(t *testing.T) {
	// F.4 tolerance for height forks: parent added a NEW height fork
	// after fork creation, activating past CutBlock. NewIDFromForks
	// with headHeight=CutBlock naturally excludes post-cut activations
	// from the CRC, so the fork-ID cross-check still passes. Callers
	// don't need to trim their local schedule for this shape.
	const genesisHex = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"
	section := &ParentSection{
		ParentGenesisHash: genesisHex,
		CutBlock:          20_000_000,
		ParentForks: []ForkActivation{
			{Block: 2_463_000},
			{Block: 4_370_000},
		},
	}
	expected := ExpectedParentIdentity{
		GenesisHash: hashHex(t, genesisHex),
		HeightForks: []uint64{2_463_000, 4_370_000, 30_000_000},
	}
	require.NoError(t, ValidateParentIdentity(section, expected))
}

func TestValidateParentIdentity_ForkIDMismatchOnExtraLocalTimeFork(t *testing.T) {
	// Time forks are all included in the CRC regardless of headHeight
	// (NewIDFromForks uses headTime=MaxUint64 in ValidateParentIdentity).
	// A time-fork the local registry knows that the manifest does not
	// therefore differs the CRC and fails — callers who want F.4
	// tolerance on time-forks must trim their expected.TimeForks to
	// the at-cut snapshot before calling.
	const genesisHex = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"
	section := &ParentSection{
		ParentGenesisHash: genesisHex,
		CutBlock:          20_000_000,
		ParentForks: []ForkActivation{
			{Time: 1_681_338_455},
		},
	}
	expected := ExpectedParentIdentity{
		GenesisHash: hashHex(t, genesisHex),
		TimeForks:   []uint64{1_681_338_455, 1_710_338_135},
	}
	err := ValidateParentIdentity(section, expected)
	require.ErrorIs(t, err, ErrParentForkIDMismatch)
}

func TestValidateParentIdentity_ForkIDSkippedWhenParentForksEmpty(t *testing.T) {
	// Empty ParentForks skips the ForkID check (permissive for early
	// manifests that don't carry the snapshot). Genesis check still runs.
	const genesisHex = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"
	section := &ParentSection{
		ParentGenesisHash: genesisHex,
		CutBlock:          20_000_000,
	}
	expected := ExpectedParentIdentity{
		GenesisHash: hashHex(t, genesisHex),
		HeightForks: []uint64{2_463_000, 4_370_000, 7_280_000},
	}
	require.NoError(t, ValidateParentIdentity(section, expected))
}

func TestSplitForkActivations_SplitsByBlockOrTime(t *testing.T) {
	forks := []ForkActivation{
		{Name: "Byzantium", Block: 4_370_000},
		{Name: "ShanghaiTime", Time: 1_681_338_455},
		{Name: "Constantinople", Block: 7_280_000},
	}
	h, ti := splitForkActivations(forks)
	require.Equal(t, []uint64{4_370_000, 7_280_000}, h)
	require.Equal(t, []uint64{1_681_338_455}, ti)
}

func TestSplitForkActivations_HeightWinsOnBothSet(t *testing.T) {
	// Defensive against malformed input carrying both Block and Time.
	forks := []ForkActivation{
		{Block: 100, Time: 200},
	}
	h, ti := splitForkActivations(forks)
	require.Equal(t, []uint64{100}, h)
	require.Empty(t, ti)
}

func TestSplitForkActivations_SortsAscending(t *testing.T) {
	forks := []ForkActivation{
		{Block: 7_280_000},
		{Block: 2_463_000},
		{Block: 4_370_000},
	}
	h, _ := splitForkActivations(forks)
	require.Equal(t, []uint64{2_463_000, 4_370_000, 7_280_000}, h)
}
