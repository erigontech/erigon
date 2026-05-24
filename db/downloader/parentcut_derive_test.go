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
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
)

// parentConfigForDerive returns a chain.Config shaped like mainnet:
// pre-merge fork blocks set to actual mainnet values, post-merge fork
// times set to actual mainnet values, plus a future-fork
// (AmsterdamTime) far in the future to exercise the drop-post-cut
// logic.
func parentConfigForDerive() *chain.Config {
	homestead := uint64(1_150_000)
	tangerine := uint64(2_463_000)
	spurious := uint64(2_675_000)
	byzantium := uint64(4_370_000)
	merge := uint64(15_537_394)
	shanghai := uint64(1_681_338_455)
	cancun := uint64(1_710_338_135)
	prague := uint64(1_746_612_311)
	amsterdam := uint64(9_999_999_999) // far future — must be dropped on a cut at Prague era
	return &chain.Config{
		ChainName:             "mainnet",
		ChainID:               big.NewInt(1),
		HomesteadBlock:        &homestead,
		TangerineWhistleBlock: &tangerine,
		SpuriousDragonBlock:   &spurious,
		ByzantiumBlock:        &byzantium,
		MergeHeight:           &merge,
		ShanghaiTime:          &shanghai,
		CancunTime:            &cancun,
		PragueTime:            &prague,
		AmsterdamTime:         &amsterdam,
	}
}

func cutForDerive(block, ts uint64) *ParentCut {
	return &ParentCut{
		Schema:             ParentCutSchemaVersion,
		ParentChain:        "mainnet",
		ParentChainID:      1,
		CutBlock:           block,
		CutBlockHash:       common.HexToHash("0xaaaa"),
		CutBlockTimestamp:  ts,
		CutBlockParentHash: common.HexToHash("0xbbbb"),
		ParentManifestName: "chain.v2.7900fbf8f3411de0.a0aba4ce58f94ee2.toml",
		ParentManifestHash: "20004fef6f6b652bde5f7c20e67e33cbc3e059d3",
		Source:             CaptureLive,
		CapturedAt:         1735689600,
	}
}

func TestDeriveForkChainConfig_KeepsPreCutDropsPostCut(t *testing.T) {
	parent := parentConfigForDerive()
	// Cut at a Prague-era block + Prague-era timestamp. Forks at or
	// before this should survive; AmsterdamTime (post-cut) should drop.
	cut := cutForDerive(20_000_000, 1_746_612_311)

	derived, err := DeriveForkChainConfig(parent, cut, "mainnet-fork-20000000")
	require.NoError(t, err)

	// Pre-merge height forks all preserved.
	require.NotNil(t, derived.HomesteadBlock)
	require.NotNil(t, derived.ByzantiumBlock)
	require.NotNil(t, derived.MergeHeight)
	// Pre-cut time forks preserved.
	require.NotNil(t, derived.ShanghaiTime)
	require.NotNil(t, derived.CancunTime)
	require.NotNil(t, derived.PragueTime, "PragueTime equals CutBlockTimestamp; AT the cut must be preserved")
	// Post-cut time fork DROPPED.
	require.Nil(t, derived.AmsterdamTime, "AmsterdamTime is after the cut and must be dropped")
}

func TestDeriveForkChainConfig_PreservesChainIDAndPopulatesLineage(t *testing.T) {
	parent := parentConfigForDerive()
	cut := cutForDerive(20_000_000, 1_746_612_311)

	derived, err := DeriveForkChainConfig(parent, cut, "mainnet-fork-20000000")
	require.NoError(t, err)

	require.Equal(t, parent.ChainID.Uint64(), derived.ChainID.Uint64(),
		"fork's EL ChainID stays = parent for replay protection")
	require.Equal(t, "mainnet-fork-20000000", derived.ChainName)
	require.Equal(t, "mainnet", derived.Parent)
	require.Equal(t, uint64(20_000_000), derived.CutBlock)

	decoded, _ := hex.DecodeString(cut.ParentManifestHash)
	var want [20]byte
	copy(want[:], decoded)
	require.Equal(t, want, derived.ParentManifestHash)
}

func TestDeriveForkChainConfig_IsDeepCopy(t *testing.T) {
	parent := parentConfigForDerive()
	cut := cutForDerive(20_000_000, 1_746_612_311)

	derived, err := DeriveForkChainConfig(parent, cut, "mainnet-fork-20000000")
	require.NoError(t, err)

	// Mutating the parent's HomesteadBlock pointer-target must not
	// affect the derived config — it's a deep copy.
	originalHomestead := *parent.HomesteadBlock
	*parent.HomesteadBlock = 999
	require.Equal(t, originalHomestead, *derived.HomesteadBlock,
		"derived config holds its own copy of *Block values")
	// Restore so subsequent tests see clean state.
	*parent.HomesteadBlock = originalHomestead
}

func TestDeriveForkChainConfig_TolratesEmptyParentManifestHash(t *testing.T) {
	parent := parentConfigForDerive()
	cut := cutForDerive(20_000_000, 1_746_612_311)
	cut.ParentManifestHash = ""
	cut.ParentManifestName = ""

	derived, err := DeriveForkChainConfig(parent, cut, "mainnet-fork-20000000")
	require.NoError(t, err)
	require.Equal(t, [20]byte{}, derived.ParentManifestHash,
		"empty parent_manifest_hash leaves the derived field zero (pre-Phase-1 root parent)")
}

func TestDeriveForkChainConfig_RejectsBadInputs(t *testing.T) {
	good := parentConfigForDerive()
	cut := cutForDerive(20_000_000, 1_746_612_311)

	t.Run("nil parent", func(t *testing.T) {
		_, err := DeriveForkChainConfig(nil, cut, "fork")
		require.Error(t, err)
	})
	t.Run("nil cut", func(t *testing.T) {
		_, err := DeriveForkChainConfig(good, nil, "fork")
		require.Error(t, err)
	})
	t.Run("empty fork name", func(t *testing.T) {
		_, err := DeriveForkChainConfig(good, cut, "")
		require.Error(t, err)
	})
	t.Run("fork name same as parent", func(t *testing.T) {
		_, err := DeriveForkChainConfig(good, cut, good.ChainName)
		require.Error(t, err)
	})
	t.Run("nil ChainID", func(t *testing.T) {
		p := parentConfigForDerive()
		p.ChainID = nil
		_, err := DeriveForkChainConfig(p, cut, "fork")
		require.Error(t, err)
	})
	t.Run("ChainID mismatch with cut", func(t *testing.T) {
		c := cutForDerive(20_000_000, 1_746_612_311)
		c.ParentChainID = 999 // mismatches parent.ChainID=1
		_, err := DeriveForkChainConfig(good, c, "fork")
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not match")
	})
}
