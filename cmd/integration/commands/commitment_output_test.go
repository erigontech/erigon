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

package commands

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

const (
	testSourceStepSize          = 1562500
	testSourceStepsInFrozenFile = 64
)

func sourceDatadirFixture(t *testing.T) datadir.Dirs {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	for _, name := range []string{
		"v1.0-accounts.0-64.kv", "v1.0-accounts.0-64.bt", "v1.0-accounts.0-64.kvei",
		"v1.0-storage.0-64.kv", "v1.0-storage.0-64.bt",
		"v1.0-code.0-64.kv", "v1.0-code.0-64.bt",
		"v1.0-commitment.0-64.kv", "v1.0-commitment.0-64.kvi",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, name), []byte(name), 0o644))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapHistory, "v1.0-accounts.0-64.v"), []byte("acc-hist"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapHistory, "v1.0-commitment.0-64.v"), []byte("com-hist"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapIdx, "v1.0-commitment.0-64.ef"), []byte("com-idx"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapAccessors, "v1.0-commitment.0-64.vi"), []byte("com-vi"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapAccessors, "v1.0-commitment.0-64.efi"), []byte("com-efi"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-state.txt"), []byte("salt"), 0o644))
	refs := true
	require.NoError(t, dbstate.WriteErigonDBSettings(dirs, &dbstate.ErigonDBSettings{
		StepSize: testSourceStepSize, StepsInFrozenFile: testSourceStepsInFrozenFile, ReferencesInCommitmentBranches: &refs,
	}))
	return dirs
}

func hexBinSourceDatadirFixture(t *testing.T) datadir.Dirs {
	t.Helper()
	dirs := sourceDatadirFixture(t)
	refs := false
	variant, hash := dbstate.TrieVariantHexBin, commitment.PBinHashBlake3
	require.NoError(t, dbstate.WriteErigonDBSettings(dirs, &dbstate.ErigonDBSettings{
		StepSize: testSourceStepSize, StepsInFrozenFile: testSourceStepsInFrozenFile, ReferencesInCommitmentBranches: &refs,
		TrieVariant: &variant, TrieHash: &hash,
	}))
	return dirs
}

func hexTarget(t *testing.T) dbstate.RebuildTarget {
	t.Helper()
	target, err := dbstate.RebuildTarget{Variant: commitment.VariantHexPatriciaTrie}.Resolve()
	require.NoError(t, err)
	return target
}

func withBinCommitmentProcess(t *testing.T, hash string) {
	t.Helper()
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.BinCommitmentHash = hash
	statecfg.ExperimentalParallelCommitment = false
	if hash != "" {
		require.NoError(t, commitment.SetPBinHashSuite(hash))
	}
}

func snapshotTree(t *testing.T, root string) map[string]string {
	t.Helper()
	got := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		got[rel] = string(data)
		return nil
	}))
	return got
}

func domainFileNames(t *testing.T, snapDomain string) []string {
	t.Helper()
	entries, err := os.ReadDir(snapDomain)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

func TestResolveCommitmentRebuildTargetUsesProcessFlags(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = false
	for _, test := range []struct {
		name     string
		parallel bool
		v3       bool
		variant  commitment.TrieVariant
	}{
		{name: "flagless", variant: commitment.VariantHexPatriciaTrie},
		{name: "parallel", parallel: true, variant: commitment.VariantParallelHexPatricia},
		{name: "v3", v3: true, variant: commitment.VariantCommitmentV3},
	} {
		t.Run(test.name, func(t *testing.T) {
			statecfg.ExperimentalParallelCommitment = test.parallel
			statecfg.ExperimentalCommitmentV3 = test.v3
			target, err := resolveCommitmentRebuildTarget()
			require.NoError(t, err)
			require.Equal(t, test.variant, target.Variant)
		})
	}
}

func TestCommitmentRebuildDomainUsesCommitmentDomain(t *testing.T) {
	require.Equal(t, kv.CommitmentDomain, commitmentRebuildDomain(dbstate.RebuildTarget{}, []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain}))
}

func TestStageRebuildOutputLinksInputsAndOmitsCommitment(t *testing.T) {
	src := sourceDatadirFixture(t)
	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), hexTarget(t), false, log.New())
	require.NoError(t, err)
	require.Equal(t, []string{
		"v1.0-accounts.0-64.bt", "v1.0-accounts.0-64.kv", "v1.0-accounts.0-64.kvei",
		"v1.0-code.0-64.bt", "v1.0-code.0-64.kv", "v1.0-storage.0-64.bt", "v1.0-storage.0-64.kv",
	}, domainFileNames(t, out.dirs.SnapDomain))
	for _, name := range []string{"v1.0-accounts.0-64.kv", "v1.0-storage.0-64.kv", "v1.0-code.0-64.kv"} {
		sourceInfo, statErr := os.Stat(filepath.Join(src.SnapDomain, name))
		require.NoError(t, statErr)
		outputInfo, statErr := os.Stat(filepath.Join(out.dirs.SnapDomain, name))
		require.NoError(t, statErr)
		require.True(t, os.SameFile(sourceInfo, outputInfo))
	}
	_, err = os.Stat(filepath.Join(out.dirs.SnapHistory, "v1.0-accounts.0-64.v"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(out.dirs.SnapIdx, "v1.0-commitment.0-64.ef"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStageRebuildOutputLeavesSourceIntact(t *testing.T) {
	src := sourceDatadirFixture(t)
	before := snapshotTree(t, src.Snap)
	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), hexTarget(t), false, log.New())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(out.dirs.SnapDomain, "v1.0-commitment.0-64.kv"), []byte("rebuilt"), 0o644))
	require.Equal(t, before, snapshotTree(t, src.Snap))
}

func TestStageRebuildOutputResumeAndRefusals(t *testing.T) {
	src := sourceDatadirFixture(t)
	outPath := filepath.Join(t.TempDir(), "out")
	out, err := stageRebuildOutput(src, outPath, hexTarget(t), false, log.New())
	require.NoError(t, err)
	rebuilt := filepath.Join(out.dirs.SnapDomain, "v1.0-commitment.0-64.kv")
	require.NoError(t, os.WriteFile(rebuilt, []byte("rebuilt"), 0o644))
	_, err = stageRebuildOutput(src, outPath, hexTarget(t), false, log.New())
	require.ErrorContains(t, err, "--resume")
	_, err = stageRebuildOutput(src, outPath, hexTarget(t), true, log.New())
	require.NoError(t, err)
}

func TestStageRebuildOutputRefusesOverlappingPaths(t *testing.T) {
	src := sourceDatadirFixture(t)
	_, err := stageRebuildOutput(src, src.DataDir, hexTarget(t), false, log.New())
	require.ErrorContains(t, err, "overlaps")
	_, err = stageRebuildOutput(src, filepath.Join(src.Snap, "out"), hexTarget(t), false, log.New())
	require.ErrorContains(t, err, "overlaps")
}

func TestStageRebuildOutputRefusesNonRegularSourceFile(t *testing.T) {
	src := sourceDatadirFixture(t)
	require.NoError(t, os.Symlink(filepath.Join(src.SnapDomain, "v1.0-accounts.0-64.kv"), filepath.Join(src.SnapDomain, "v1.0-storage.64-128.kv")))
	_, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), hexTarget(t), false, log.New())
	require.ErrorContains(t, err, "not a regular file")
}

func TestStageRebuildOutputRefusesSymlinkedOutput(t *testing.T) {
	src := sourceDatadirFixture(t)
	outPath := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.Symlink(src.DataDir, outPath))
	_, err := stageRebuildOutput(src, outPath, hexTarget(t), false, log.New())
	require.ErrorContains(t, err, "overlaps the source datadir")
}

func TestStageRebuildOutputResumeRefusesUnrelatedExistingFile(t *testing.T) {
	src := sourceDatadirFixture(t)
	outPath := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.MkdirAll(filepath.Join(outPath, "snapshots"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(outPath, "snapshots", "stale"), []byte("stale"), 0o644))
	_, err := stageRebuildOutput(src, outPath, hexTarget(t), true, log.New())
	require.ErrorContains(t, err, "unexpected file in resumed output")
}

func TestStageRebuildOutputRefusesNonEmptyOutput(t *testing.T) {
	src := sourceDatadirFixture(t)
	outPath := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.MkdirAll(filepath.Join(outPath, "snapshots"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(outPath, "stale"), []byte("stale"), 0o644))
	_, err := stageRebuildOutput(src, outPath, hexTarget(t), false, log.New())
	require.ErrorContains(t, err, "is not empty")
}

func TestRebuildOutputSettingsKeepSourceScheme(t *testing.T) {
	src := sourceDatadirFixture(t)
	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), hexTarget(t), false, log.New())
	require.NoError(t, err)
	settings, err := dbstate.ReadErigonDBSettings(out.dirs)
	require.NoError(t, err)
	require.Nil(t, settings.TrieVariant)
	require.Nil(t, settings.TrieHash)
	require.True(t, settings.RefsInCommitmentBranches())
}

func TestStageRebuildOutputDoesNotCreateSourceMigrations(t *testing.T) {
	src := sourceDatadirFixture(t)
	require.NoError(t, dir.RemoveFile(src.Migrations))
	_, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), hexTarget(t), false, log.New())
	require.NoError(t, err)
	_, err = os.Stat(src.Migrations)
	require.ErrorIs(t, err, os.ErrNotExist)
}
