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

// TestValidateForkDatadirWithStepMap_PreCutStateFilesAccepted pins the
// fix for the Tier 3c restart-transition gap: after debug_setFork's
// in-process mode-B trim leaves state files whose step range covers
// only pre-cut blocks, the validator with an empty step map treats
// them all as straddle and refuses --chain=<fork> boot. With a real
// step→block map (built from rawdbv3.TxNums via
// BuildStepToBlockFromMaxTxNum in storage.Initialize), those known-
// pre-cut files classify correctly and the boot succeeds.
func TestValidateForkDatadirWithStepMap_PreCutStateFilesAccepted(t *testing.T) {
	// Step 2520 = block 19_990_000, step 2521 = block 20_010_000
	// (straddles cut), step 2522 = block 20_020_000 (post-cut).
	stepToBlock := StepToBlock{
		2519: 19_980_000,
		2520: 19_990_000,
		2521: 20_010_000,
		2522: 20_020_000,
	}
	cfg := forkChainConfigForValidate(t, 20_000_000 /* cut */, 15_000_000 /* merge */)

	snapDir := t.TempDir()
	// Pre-cut state file — must be accepted with the real step map.
	preCutFile := "v2.0-accounts.2519-2520.kv"
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, preCutFile), []byte("x"), 0o644))

	// Empty-map behavior: state file classifies as straddle → error.
	err := ValidateForkDatadirWithStepMap(cfg, snapDir, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "straddles cut")

	// Real-map behavior: same file classifies as pre-cut → accepted.
	require.NoError(t, ValidateForkDatadirWithStepMap(cfg, snapDir, stepToBlock),
		"pre-cut state file must classify correctly when the step map is supplied")

	// Sanity: post-cut file still rejected even with the real map.
	postCutFile := "v2.0-accounts.2521-2522.kv"
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, postCutFile), []byte("x"), 0o644))
	err = ValidateForkDatadirWithStepMap(cfg, snapDir, stepToBlock)
	require.Error(t, err)
	require.Contains(t, err.Error(), postCutFile)
}
