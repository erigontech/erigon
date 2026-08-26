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
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapHistory, "v1.0-commitment.0-64.v"), []byte("com-hist"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapIdx, "v1.0-commitment.0-64.ef"), []byte("com-idx"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapAccessors, "v1.0-commitment.0-64.vi"), []byte("com-vi"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapAccessors, "v1.0-commitment.0-64.efi"), []byte("com-efi"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-state.txt"), []byte("salt"), 0o644))

	refs := true
	require.NoError(t, dbstate.WriteErigonDBSettings(dirs, &dbstate.ErigonDBSettings{
		StepSize:                       testSourceStepSize,
		StepsInFrozenFile:              testSourceStepsInFrozenFile,
		ReferencesInCommitmentBranches: &refs,
	}))
	return dirs
}

// binSourceDatadirFixture is a source datadir that records the bin trie. It
// carries no commitment history: convert-format rewrites domain files only.
func binSourceDatadirFixture(t *testing.T) datadir.Dirs {
	t.Helper()
	dirs := sourceDatadirFixture(t)
	for _, p := range []string{
		filepath.Join(dirs.SnapHistory, "v1.0-commitment.0-64.v"),
		filepath.Join(dirs.SnapIdx, "v1.0-commitment.0-64.ef"),
		filepath.Join(dirs.SnapAccessors, "v1.0-commitment.0-64.vi"),
		filepath.Join(dirs.SnapAccessors, "v1.0-commitment.0-64.efi"),
	} {
		require.NoError(t, dir.RemoveFile(p))
	}
	refs := false
	variant, hash := dbstate.TrieVariantBin, commitment.PBinHashBlake3
	require.NoError(t, dbstate.WriteErigonDBSettings(dirs, &dbstate.ErigonDBSettings{
		StepSize:                       testSourceStepSize,
		StepsInFrozenFile:              testSourceStepsInFrozenFile,
		ReferencesInCommitmentBranches: &refs,
		TrieVariant:                    &variant,
		TrieHash:                       &hash,
	}))
	return dirs
}

func hexTarget(t *testing.T) dbstate.RebuildTarget {
	t.Helper()
	target, err := dbstate.RebuildTarget{Variant: commitment.VariantHexPatriciaTrie}.Resolve()
	require.NoError(t, err)
	return target
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

func TestRequireConvertFormatOutput(t *testing.T) {
	require.ErrorContains(t, requireConvertFormatOutput(""), "--output.datadir")
	require.NoError(t, requireConvertFormatOutput(t.TempDir()))
}

func TestConvertFormatRegistersOutputFlags(t *testing.T) {
	for _, name := range []string{"output.datadir", "resume", "verify.sample"} {
		require.NotNil(t, cmdCommitmentConvertFormat.Flags().Lookup(name), name)
	}
}

func TestConvertFormatHelpDescribesOutputDatadirModel(t *testing.T) {
	help := cmdCommitmentConvertFormat.Long
	require.Contains(t, help, "--output.datadir")
	require.Contains(t, help, "--resume")
	require.Contains(t, help, "--verify.sample")
	require.Contains(t, help, "datadir remains unchanged")
	require.NotContains(t, help, "backup/domains")
	require.NotContains(t, help, "--restore")
	require.NotContains(t, help, "--continue")
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
	_, err = os.Stat(filepath.Join(out.dirs.SnapHistory, "v1.0-commitment.0-64.v"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(out.dirs.SnapAccessors, "v1.0-commitment.0-64.vi"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(out.dirs.SnapAccessors, "v1.0-commitment.0-64.efi"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestLinkCommitmentSnapshotsLinksAllCommitmentFiles(t *testing.T) {
	src := sourceDatadirFixture(t)
	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), dbstate.RebuildTarget{}, false, log.New(), preserveSourceSettings)
	require.NoError(t, err)

	linked, err := linkCommitmentSnapshots(src.Snap, out.dirs.Snap)
	require.NoError(t, err)
	require.Equal(t, 6, linked)
	require.NoError(t, filepath.WalkDir(src.Snap, func(srcPath string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || entry.Name() == dbstate.ERIGONDB_SETTINGS_FILE {
			return err
		}
		require.True(t, entry.Type().IsRegular(), srcPath)
		rel, err := filepath.Rel(src.Snap, srcPath)
		require.NoError(t, err)
		srcInfo, err := os.Stat(srcPath)
		require.NoError(t, err)
		outInfo, err := os.Stat(filepath.Join(out.dirs.Snap, rel))
		require.NoError(t, err)
		require.True(t, os.SameFile(srcInfo, outInfo), "%s must be a hardlink", rel)
		return nil
	}))

	for _, name := range []string{
		"domain/v1.0-commitment.0-64.kv",
		"domain/v1.0-commitment.0-64.kvi",
		"history/v1.0-commitment.0-64.v",
		"idx/v1.0-commitment.0-64.ef",
		"accessor/v1.0-commitment.0-64.vi",
		"accessor/v1.0-commitment.0-64.efi",
	} {
		srcPath := filepath.Join(src.Snap, name)
		outPath := filepath.Join(out.dirs.Snap, name)
		srcInfo, err := os.Stat(srcPath)
		require.NoError(t, err)
		outInfo, err := os.Stat(outPath)
		require.NoError(t, err)
		require.True(t, os.SameFile(srcInfo, outInfo), "%s must be a hardlink", name)
	}
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

func TestStageRebuildOutputRefusesExistingNonCommitmentFiles(t *testing.T) {
	src := sourceDatadirFixture(t)
	outPath := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.MkdirAll(filepath.Join(outPath, "snapshots"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(outPath, "stale"), []byte("stale"), 0o644))

	_, err := stageRebuildOutput(src, outPath, binTarget(t), false, log.New())
	require.ErrorContains(t, err, "is not empty")
	require.ErrorContains(t, err, "--resume")
}

func TestStageRebuildOutputResumeRefusesUnrelatedExistingFile(t *testing.T) {
	src := sourceDatadirFixture(t)
	outPath := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.MkdirAll(filepath.Join(outPath, "snapshots"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(outPath, "snapshots", "stale"), []byte("stale"), 0o644))

	_, err := stageRebuildOutput(src, outPath, binTarget(t), true, log.New())
	require.ErrorContains(t, err, "unexpected file in resumed output")
}

func TestStageRebuildOutputRefusesSourceAsOutput(t *testing.T) {
	src := sourceDatadirFixture(t)
	_, err := stageRebuildOutput(src, src.DataDir, binTarget(t), false, log.New())
	require.Error(t, err)
}

func TestStageRebuildOutputRefusesSymlinkedOutput(t *testing.T) {
	src := sourceDatadirFixture(t)
	outPath := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.Symlink(src.DataDir, outPath))

	_, err := stageRebuildOutput(src, outPath, binTarget(t), false, log.New())
	require.ErrorContains(t, err, "overlaps the source datadir")
}

// Staging creates the output tree before it walks the source, so an output nested
// in the source would have the walk descend into what it is writing.
func TestStageRebuildOutputRefusesNestedOutput(t *testing.T) {
	src := sourceDatadirFixture(t)

	_, err := stageRebuildOutput(src, filepath.Join(src.Snap, "out"), binTarget(t), false, log.New())
	require.ErrorContains(t, err, "overlaps the source datadir")
	_, err = os.Stat(filepath.Join(src.Snap, "out"))
	require.ErrorIs(t, err, os.ErrNotExist, "a refused output must not be created inside the source")

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

func TestConvertFormatOutputPreservesSourceSettings(t *testing.T) {
	src := binSourceDatadirFixture(t)
	settingsPath := filepath.Join(src.Snap, dbstate.ERIGONDB_SETTINGS_FILE)
	sourceSettings, err := os.ReadFile(settingsPath)
	require.NoError(t, err)

	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), dbstate.RebuildTarget{}, false, log.New(), preserveSourceSettings)
	require.NoError(t, err)

	outputSettings, err := os.ReadFile(filepath.Join(out.dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE))
	require.NoError(t, err)
	require.Equal(t, sourceSettings, outputSettings)

	sourceInfo, err := os.Stat(settingsPath)
	require.NoError(t, err)
	outputInfo, err := os.Stat(filepath.Join(out.dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE))
	require.NoError(t, err)
	require.False(t, os.SameFile(sourceInfo, outputInfo))
}

func TestConvertFormatStagingLeavesSourceSnapshotsUnchanged(t *testing.T) {
	src := binSourceDatadirFixture(t)
	before := snapshotTree(t, src.Snap)

	_, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), dbstate.RebuildTarget{}, false, log.New(), preserveSourceSettings)
	require.NoError(t, err)

	require.Equal(t, before, snapshotTree(t, src.Snap))
}

func TestConvertFormatRequiresBinarySource(t *testing.T) {
	require.ErrorContains(t, requireConvertFormatSource(sourceDatadirFixture(t)), "requires a binary-trie")
	require.NoError(t, requireConvertFormatSource(binSourceDatadirFixture(t)))
}

func TestConvertFormatRefusesCommitmentHistory(t *testing.T) {
	for _, planted := range []struct {
		dir  func(datadir.Dirs) string
		name string
	}{
		{func(d datadir.Dirs) string { return d.SnapHistory }, "v1.0-commitment.0-64.v"},
		{func(d datadir.Dirs) string { return d.SnapIdx }, "v1.0-commitment.0-64.ef"},
		{func(d datadir.Dirs) string { return d.SnapAccessors }, "v1.0-commitment.0-64.vi"},
	} {
		src := binSourceDatadirFixture(t)
		require.NoError(t, os.WriteFile(filepath.Join(planted.dir(src), planted.name), []byte{}, 0o644))
		require.ErrorContains(t, requireConvertFormatSource(src), "commitment history", planted.name)
	}
}

func TestStageRebuildOutputDoesNotCreateSourceMigrations(t *testing.T) {
	src := sourceDatadirFixture(t)
	require.NoError(t, dir.RemoveFile(src.Migrations))

	_, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), binTarget(t), false, log.New())
	require.NoError(t, err)

	_, err = os.Stat(src.Migrations)
	require.ErrorIs(t, err, os.ErrNotExist)
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
	parallel := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment, statecfg.BinCommitmentHash = bin, prevHash
		statecfg.ExperimentalParallelCommitment = parallel
		require.NoError(t, commitment.SetPBinHashSuite(suite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.BinCommitmentHash = hash
	// The settings resolver refuses bin together with parallel, so a process-wide
	// parallel default would make every bin case here fail on the combination.
	statecfg.ExperimentalParallelCommitment = false
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

// The target is resolved from the flags before the datadir is opened, so nothing
// in the run has read the source's scheme yet. A commitment .kv records no trie
// variant, so hex files written into a bin datadir are read back as bin.
func TestRebuildRefusesHexTargetOnBinSource(t *testing.T) {
	binSrc := binSourceDatadirFixture(t)
	require.ErrorContains(t, refuseRebuildIntoBinSource(hexTarget(t), binSrc), "bin commitment trie")
	require.NoError(t, refuseRebuildIntoBinSource(binTarget(t), binSrc))
	require.NoError(t, refuseRebuildIntoBinSource(hexTarget(t), sourceDatadirFixture(t)))
	// A datadir with no erigondb.toml predates the file and is hex.
	require.NoError(t, refuseRebuildIntoBinSource(hexTarget(t), datadir.New(t.TempDir())))
}

// --resume keeps the commitment files the interrupted run wrote. Continuing under
// a different scheme leaves one directory holding two sets of them, which nothing
// downstream can tell apart.
func TestStageRebuildOutputResumeRefusesADifferentTarget(t *testing.T) {
	src := sourceDatadirFixture(t)
	outPath := filepath.Join(t.TempDir(), "out")

	out, err := stageRebuildOutput(src, outPath, binTarget(t), false, log.New())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(out.dirs.SnapDomain, "v1.0-commitment.0-64.kv"), []byte("rebuilt"), 0o644))

	keccak, err := dbstate.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, HashName: commitment.PBinHashKeccak}.Resolve()
	require.NoError(t, err)
	_, err = stageRebuildOutput(src, outPath, keccak, true, log.New())
	require.ErrorContains(t, err, commitment.PBinHashKeccak)

	_, err = stageRebuildOutput(src, outPath, hexTarget(t), true, log.New())
	require.ErrorContains(t, err, dbstate.TrieVariantHex)

	staged, err := dbstate.ReadErigonDBSettings(out.dirs)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantBin, staged.TrieVariantName())
	require.Equal(t, commitment.PBinHashBlake3, staged.TrieHashName())
}

// A source file the walk cannot hardlink would leave the output missing an input
// the rebuild then derives commitment without.
func TestStageRebuildOutputRefusesNonRegularSourceFile(t *testing.T) {
	src := sourceDatadirFixture(t)
	require.NoError(t, os.Symlink(
		filepath.Join(src.SnapDomain, "v1.0-accounts.0-64.kv"),
		filepath.Join(src.SnapDomain, "v1.0-storage.64-128.kv")))

	_, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), binTarget(t), false, log.New())
	require.ErrorContains(t, err, "not a regular file")
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
