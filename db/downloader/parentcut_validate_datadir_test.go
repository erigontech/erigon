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
	"github.com/holiman/uint256"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
)

func forkChainConfigForValidate(t *testing.T, cutBlock, mergeHeight uint64) *chain.Config {
	t.Helper()
	mh := mergeHeight
	return &chain.Config{
		ChainName:   "mainnet-fork-20000000",
		ChainID:     uint256.NewInt(1),
		Parent:      "mainnet",
		CutBlock:    cutBlock,
		MergeHeight: &mh,
	}
}

func TestValidateForkDatadir_RootChainConfigIsNoOp(t *testing.T) {
	cfg := &chain.Config{
		ChainName: "mainnet",
		ChainID:   uint256.NewInt(1),
		// Parent == "" → not a fork; no validation required.
	}
	require.NoError(t, ValidateForkDatadir(cfg, t.TempDir()))
	require.NoError(t, ValidateForkDatadir(cfg, ""))
	require.NoError(t, ValidateForkDatadir(cfg, "/non/existent/path"))
}

func TestValidateForkDatadir_RejectsPreMergeCut(t *testing.T) {
	// Mainnet's actual merge block is 15_537_394. A cut at block 1M
	// would be pre-merge.
	cfg := forkChainConfigForValidate(t, 1_000_000, 15_537_394)
	err := ValidateForkDatadir(cfg, t.TempDir())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrForkPreMergeCut)
}

func TestValidateForkDatadir_AcceptsAtMergeCut(t *testing.T) {
	// A cut AT the merge block is the earliest legal post-merge cut.
	cfg := forkChainConfigForValidate(t, 15_537_394, 15_537_394)
	require.NoError(t, ValidateForkDatadir(cfg, t.TempDir()))
}

func TestValidateForkDatadir_TolratesNoMergeHeight(t *testing.T) {
	// Without MergeHeight populated (older config form), we can't
	// detect pre-merge; trust the upstream fork-from validation +
	// pass.
	cfg := &chain.Config{
		ChainName: "fork",
		Parent:    "mainnet",
		CutBlock:  1_000_000,
		// MergeHeight is nil
	}
	require.NoError(t, ValidateForkDatadir(cfg, t.TempDir()))
}

func TestValidateForkDatadir_AcceptsEmptySnapDir(t *testing.T) {
	cfg := forkChainConfigForValidate(t, 20_000_000, 15_537_394)
	require.NoError(t, ValidateForkDatadir(cfg, t.TempDir()))
}

func TestValidateForkDatadir_AcceptsMissingSnapDir(t *testing.T) {
	cfg := forkChainConfigForValidate(t, 20_000_000, 15_537_394)
	require.NoError(t, ValidateForkDatadir(cfg, filepath.Join(t.TempDir(), "does-not-exist")))
}

func TestValidateForkDatadir_AcceptsOnlyPreCutFiles(t *testing.T) {
	// Fresh fork-from output: only pre-cut snap files + non-range
	// chain-wide files (salt). Should pass.
	cfg := forkChainConfigForValidate(t, 20_000_000, 15_537_394)
	snapDir := t.TempDir()
	for _, name := range []string{
		"v1.0-019998-019999-headers.seg", // PreCut
		"v1.0-019999-020000-headers.seg", // PreCut (to == CutBlock)
		"salt-blocks.txt",                // non-range
		"salt-state.txt",                 // non-range
	} {
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, name), []byte{}, 0o644))
	}
	require.NoError(t, ValidateForkDatadir(cfg, snapDir))
}

func TestValidateForkDatadir_RejectsStraddleFile(t *testing.T) {
	cfg := forkChainConfigForValidate(t, 20_000_000, 15_537_394)
	snapDir := t.TempDir()
	// File straddles the cut → parent-lineage post-cut data on disk.
	require.NoError(t, os.WriteFile(
		filepath.Join(snapDir, "v1.0-019999-020001-headers.seg"), []byte{}, 0o644))

	err := ValidateForkDatadir(cfg, snapDir)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrForkDatadirHasPostCutData)
	require.Contains(t, err.Error(), "v1.0-019999-020001-headers.seg")
	require.Contains(t, err.Error(), "snapshots fork-from")
}

func TestValidateForkDatadir_RejectsPostCutFile(t *testing.T) {
	cfg := forkChainConfigForValidate(t, 20_000_000, 15_537_394)
	snapDir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(snapDir, "v1.0-020001-020002-headers.seg"), []byte{}, 0o644))

	err := ValidateForkDatadir(cfg, snapDir)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrForkDatadirHasPostCutData)
	require.Contains(t, err.Error(), "v1.0-020001-020002-headers.seg")
}

func TestValidateForkDatadir_RejectsNilConfig(t *testing.T) {
	err := ValidateForkDatadir(nil, t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil chain.Config")
}
