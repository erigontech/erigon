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
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
)

func TestIsForkConfig(t *testing.T) {
	require.False(t, IsForkConfig(nil), "nil is not a fork")
	require.False(t, IsForkConfig(&chain.Config{ChainName: "mainnet"}), "root chain is not a fork")
	require.True(t, IsForkConfig(&chain.Config{ChainName: "fork", Parent: "mainnet"}), "Parent != empty → fork")
}

func TestBuildForkBootstrapPlan_NilForRootChain(t *testing.T) {
	plan, err := BuildForkBootstrapPlan(&chain.Config{ChainName: "mainnet"})
	require.NoError(t, err)
	require.Nil(t, plan, "root chain.Config produces nil plan (no bootstrap required)")
}

func TestBuildForkBootstrapPlan_NilForNilConfig(t *testing.T) {
	plan, err := BuildForkBootstrapPlan(nil)
	require.NoError(t, err)
	require.Nil(t, plan)
}

func TestBuildForkBootstrapPlan_PopulatesFromForkConfig(t *testing.T) {
	hash := [20]byte{0x20, 0x00, 0x4f, 0xef, 0x6f, 0x6b, 0x65, 0x2b, 0xde, 0x5f, 0x7c, 0x20, 0xe6, 0x7e, 0x33, 0xcb, 0xc3, 0xe0, 0x59, 0xd3}
	cfg := &chain.Config{
		ChainName:          "mainnet-fork-20000000",
		ChainID:            big.NewInt(1),
		Parent:             "mainnet",
		CutBlock:           20_000_000,
		ParentManifestHash: hash,
	}
	plan, err := BuildForkBootstrapPlan(cfg)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, "mainnet", plan.Parent)
	require.Equal(t, uint64(20_000_000), plan.CutBlock)
	require.Equal(t, hash, plan.ParentManifestHash)
}

func TestBuildForkBootstrapPlan_RejectsForkWithZeroCutBlock(t *testing.T) {
	cfg := &chain.Config{
		ChainName: "fork",
		Parent:    "mainnet",
		// CutBlock missing — malformed
	}
	plan, err := BuildForkBootstrapPlan(cfg)
	require.Error(t, err)
	require.Nil(t, plan)
	require.Contains(t, err.Error(), "CutBlock=0")
	require.Contains(t, err.Error(), "malformed")
}

func TestBuildForkBootstrapPlan_TolratesEmptyParentManifestHash(t *testing.T) {
	// A fork created from a pre-Phase-1 root parent has no V2 manifest
	// to pin against. ParentManifestHash stays zero; bootstrap plan
	// still builds, fork-follower falls back to direct file downloads.
	cfg := &chain.Config{
		ChainName: "fork-of-old-chain",
		Parent:    "legacy-root",
		CutBlock:  100,
		// ParentManifestHash is zero
	}
	plan, err := BuildForkBootstrapPlan(cfg)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, [20]byte{}, plan.ParentManifestHash, "zero hash preserved")
}
