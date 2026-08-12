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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

const (
	testSourceStepSize          = 1562500
	testSourceStepsInFrozenFile = 64
)

// sourceDatadirFixture builds a hex datadir holding one step range of account,
// storage, code and commitment files plus the settings that describe them.
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
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapIdx, "v1.0-commitment.0-64.ef"), []byte("com-idx"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-state.txt"), []byte("salt"), 0o644))

	refs := true
	require.NoError(t, dbstate.WriteErigonDBSettings(dirs, &dbstate.ErigonDBSettings{
		StepSize:                       testSourceStepSize,
		StepsInFrozenFile:              testSourceStepsInFrozenFile,
		ReferencesInCommitmentBranches: &refs,
	}))
	return dirs
}

func binTarget(t *testing.T) dbstate.RebuildTarget {
	t.Helper()
	target, err := dbstate.RebuildTarget{
		Variant:  commitment.VariantBinPatriciaTrie,
		HashName: commitment.PBinHashBlake3,
	}.Resolve()
	require.NoError(t, err)
	return target
}

// snapshotTree records every regular file under root with its content, so a
// later call can prove the source datadir was not written to.
func snapshotTree(t *testing.T, root string) map[string]string {
	t.Helper()
	got := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
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
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func TestRequireRebuildOutputForBinTarget(t *testing.T) {
	require.Error(t, requireRebuildOutput(binTarget(t), ""))
	require.NoError(t, requireRebuildOutput(binTarget(t), t.TempDir()))

	hex, err := dbstate.RebuildTarget{Variant: commitment.VariantHexPatriciaTrie}.Resolve()
	require.NoError(t, err)
	require.NoError(t, requireRebuildOutput(hex, ""))
}

func TestStageRebuildOutputLinksInputsAndOmitsCommitment(t *testing.T) {
	src := sourceDatadirFixture(t)
	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), binTarget(t), false, log.New())
	require.NoError(t, err)

	require.Equal(t, []string{
		"v1.0-accounts.0-64.bt", "v1.0-accounts.0-64.kv", "v1.0-accounts.0-64.kvei",
		"v1.0-code.0-64.bt", "v1.0-code.0-64.kv",
		"v1.0-storage.0-64.bt", "v1.0-storage.0-64.kv",
	}, domainFileNames(t, out.dirs.SnapDomain))

	for _, name := range []string{"v1.0-accounts.0-64.kv", "v1.0-storage.0-64.kv", "v1.0-code.0-64.kv"} {
		srcFi, err := os.Stat(filepath.Join(src.SnapDomain, name))
		require.NoError(t, err)
		outFi, err := os.Stat(filepath.Join(out.dirs.SnapDomain, name))
		require.NoError(t, err)
		require.True(t, os.SameFile(srcFi, outFi), "%s must be a hardlink, not a copy", name)
	}

	// The rest of the snapshot tree travels too, minus the commitment history.
	_, err = os.Stat(filepath.Join(out.dirs.Snap, "salt-state.txt"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(out.dirs.SnapHistory, "v1.0-accounts.0-64.v"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(out.dirs.SnapIdx, "v1.0-commitment.0-64.ef"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStageRebuildOutputLeavesSourceIntact(t *testing.T) {
	src := sourceDatadirFixture(t)
	before := snapshotTree(t, src.Snap)

	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), binTarget(t), false, log.New())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(out.dirs.SnapDomain, "v1.0-commitment.0-64.kv"), []byte("rebuilt"), 0o644))

	require.Equal(t, before, snapshotTree(t, src.Snap))
}

func TestStageRebuildOutputRefusesExistingCommitmentFiles(t *testing.T) {
	src := sourceDatadirFixture(t)
	outPath := filepath.Join(t.TempDir(), "out")

	out, err := stageRebuildOutput(src, outPath, binTarget(t), false, log.New())
	require.NoError(t, err)
	rebuilt := filepath.Join(out.dirs.SnapDomain, "v1.0-commitment.0-64.kv")
	require.NoError(t, os.WriteFile(rebuilt, []byte("rebuilt"), 0o644))

	_, err = stageRebuildOutput(src, outPath, binTarget(t), false, log.New())
	require.ErrorContains(t, err, "--resume")

	_, err = stageRebuildOutput(src, outPath, binTarget(t), true, log.New())
	require.NoError(t, err)
	data, err := os.ReadFile(rebuilt)
	require.NoError(t, err)
	require.Equal(t, "rebuilt", string(data))
}

func TestStageRebuildOutputRefusesSourceAsOutput(t *testing.T) {
	src := sourceDatadirFixture(t)
	_, err := stageRebuildOutput(src, src.DataDir, binTarget(t), false, log.New())
	require.Error(t, err)
}

// Staging creates the output tree before it walks the source, so an output nested
// in the source would have the walk descend into what it is writing.
func TestStageRebuildOutputRefusesNestedOutput(t *testing.T) {
	src := sourceDatadirFixture(t)

	_, err := stageRebuildOutput(src, filepath.Join(src.Snap, "out"), binTarget(t), false, log.New())
	require.ErrorContains(t, err, "overlaps the source datadir")

	outer := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(outer, "inner"), 0o755))
	nestedSrc := datadir.New(filepath.Join(outer, "inner"))
	_, err = stageRebuildOutput(nestedSrc, outer, binTarget(t), false, log.New())
	require.ErrorContains(t, err, "overlaps the source datadir")
}

func TestRebuildOutputSettingsDescribeProducedScheme(t *testing.T) {
	src := sourceDatadirFixture(t)
	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), binTarget(t), false, log.New())
	require.NoError(t, err)

	final, err := dbstate.ReadErigonDBSettings(out.dirs)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantBin, final.TrieVariantName())
	require.Equal(t, commitment.PBinHashBlake3, final.TrieHashName())
	require.Equal(t, uint64(testSourceStepSize), final.StepSize)
	require.Equal(t, uint64(testSourceStepsInFrozenFile), final.StepsInFrozenFile)
	// The bin trie cannot read referenced branch keys, so the output says so
	// even though the source datadir was built with them.
	require.False(t, final.RefsInCommitmentBranches())
}

func TestRebuildOutputSettingsHexTargetCarriesSourceRefs(t *testing.T) {
	src := sourceDatadirFixture(t)
	hex, err := dbstate.RebuildTarget{Variant: commitment.VariantHexPatriciaTrie}.Resolve()
	require.NoError(t, err)

	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), hex, false, log.New())
	require.NoError(t, err)

	final, err := dbstate.ReadErigonDBSettings(out.dirs)
	require.NoError(t, err)
	require.Nil(t, final.TrieVariant)
	require.Nil(t, final.TrieHash)
	require.True(t, final.RefsInCommitmentBranches())
}

// The output directory on its own is what a node is started on, so the settings
// resolver must accept it under the bin flag that the source datadir refuses.
func TestRebuildOutputStartsUnderTheBinFlag(t *testing.T) {
	src := sourceDatadirFixture(t)
	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), binTarget(t), false, log.New())
	require.NoError(t, err)
	withBinCommitmentProcess(t, commitment.PBinHashBlake3)

	settings, err := dbstate.ResolveErigonDBSettings(out.dirs, log.New(), true)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantBin, settings.TrieVariantName())
	require.Equal(t, commitment.PBinHashBlake3, settings.TrieHashName())
	require.Equal(t, commitment.PBinHashBlake3, commitment.PBinHashSuiteName())

	_, err = dbstate.ResolveErigonDBSettings(src, log.New(), true)
	require.Error(t, err, "the hex source is what a separate output directory exists to avoid")
}

// withBinCommitmentProcess puts the process into the state a bin target implies:
// nothing but --experimental.bin-commitment makes DefaultRebuildTarget pick bin.
func withBinCommitmentProcess(t *testing.T, hash string) {
	t.Helper()
	bin, prevHash, suite := statecfg.ExperimentalBinCommitment, statecfg.BinCommitmentHash, commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment, statecfg.BinCommitmentHash = bin, prevHash
		require.NoError(t, commitment.SetPBinHashSuite(suite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.BinCommitmentHash = hash
}

// The rebuild reopens the staged directory as a datadir before it writes a single
// file into it, so the settings resolver has to accept it under the same bin flags
// that made the target bin in the first place.
func TestStagedRebuildOutputOpensUnderTheBinFlag(t *testing.T) {
	src := sourceDatadirFixture(t)
	withBinCommitmentProcess(t, commitment.PBinHashBlake3)

	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), binTarget(t), false, log.New())
	require.NoError(t, err)

	settings, err := dbstate.ResolveErigonDBSettings(out.dirs, log.New(), false)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantBin, settings.TrieVariantName())
	require.Equal(t, commitment.PBinHashBlake3, settings.TrieHashName())
}

// An interrupted run leaves bin commitment files behind. The directory must still
// describe them, or the next start reads them as hex.
func TestStagedRebuildOutputDescribesBinBeforeItFinishes(t *testing.T) {
	src := sourceDatadirFixture(t)
	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), binTarget(t), false, log.New())
	require.NoError(t, err)

	staged, err := dbstate.ReadErigonDBSettings(out.dirs)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantBin, staged.TrieVariantName())
	require.Equal(t, commitment.PBinHashBlake3, staged.TrieHashName())
}

func TestStageRebuildOutputLeavesProcessConfigUnmodified(t *testing.T) {
	src := sourceDatadirFixture(t)
	bin, hash, suite := statecfg.ExperimentalBinCommitment, statecfg.BinCommitmentHash, commitment.PBinHashSuiteName()

	_, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), binTarget(t), false, log.New())
	require.NoError(t, err)

	require.Equal(t, bin, statecfg.ExperimentalBinCommitment)
	require.Equal(t, hash, statecfg.BinCommitmentHash)
	require.Equal(t, suite, commitment.PBinHashSuiteName())
}
