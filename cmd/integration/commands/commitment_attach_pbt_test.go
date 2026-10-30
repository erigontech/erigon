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
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
	"github.com/stretchr/testify/require"
)

func TestValidatePBTAttachFilesRequiresBothCommitmentDomains(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	require.NoError(t, validatePBTAttachFiles(node, published, 8, 7))
	require.NoError(t, validatePBTAttachFiles(node, published, 8, 6))
	require.ErrorContains(t, validatePBTAttachFiles(node, published, 8, 8), "do not cover conversion txNum")
	require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-1.kv")))
	require.ErrorContains(t, validatePBTAttachFiles(node, published, 8, 7), "commitment-bin")
}

func TestValidatePBTAttachFilesAcceptsStaleNodeBinFiles(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	for _, dirs := range []datadir.Dirs{node, published} {
		for _, name := range []string{"v1.0-accounts.1-2.kv", "v1.0-storage.1-2.kv", "v1.0-code.1-2.kv", "v3.0-commitment.1-2.kv"} {
			require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, name), []byte("step"), 0o644))
		}
	}
	require.NoError(t, os.WriteFile(filepath.Join(published.SnapDomain, "v1.0-commitment-bin.1-2.kv"), []byte("published"), 0o644))
	require.NoError(t, validatePBTAttachFiles(node, published, 8, 15))
}

func TestPBTAttachVisibleFilesPrefersMergedRanges(t *testing.T) {
	files := []pbtAttachFile{
		{domain: kv.AccountsDomain, from: 0, to: 1, path: "v2.0-accounts.0-1.kv", data: true},
		{domain: kv.AccountsDomain, from: 1, to: 2, path: "v2.0-accounts.1-2.kv", data: true},
		{domain: kv.AccountsDomain, from: 0, to: 2, path: "v2.0-accounts.0-2.kv", data: true},
		{domain: kv.AccountsDomain, from: 0, to: 1, path: "v2.0-accounts.0-1.bt"},
		{domain: kv.AccountsDomain, from: 1, to: 2, path: "v2.0-accounts.1-2.bt"},
		{domain: kv.AccountsDomain, from: 0, to: 2, path: "v2.0-accounts.0-2.bt"},
	}
	visible := pbtAttachVisibleFiles(files)
	require.Len(t, visible, 2)
	require.ElementsMatch(t, []string{"v2.0-accounts.0-2.kv", "v2.0-accounts.0-2.bt"}, pbtAttachFilePaths(visible))
}

func pbtAttachFilePaths(files []pbtAttachFile) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.path)
	}
	return paths
}

func TestValidatePBTAttachSaltsIgnoresBlockSalt(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	require.NoError(t, os.WriteFile(filepath.Join(node.Snap, "salt-state.txt"), []byte{1}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(published.Snap, "salt-state.txt"), []byte{1}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(node.Snap, "salt-blocks.txt"), []byte{2}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(published.Snap, "salt-blocks.txt"), []byte{3}, 0o644))
	require.NoError(t, validatePBTAttachSalts(node, published))
}

func TestAttachPBTUsesLegacyStepSizeWithoutNodeSettings(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	hash := commitment.PBinHashBlake3
	variant := state.TrieVariantHexBin
	refs := false
	blockNum, txNum := uint64(2), uint64(7)
	require.NoError(t, state.WriteErigonDBSettings(published, &state.ErigonDBSettings{
		StepSize:                       config3.LegacyStepSize,
		StepsInFrozenFile:              config3.LegacyStepsInFrozenFile,
		ReferencesInCommitmentBranches: &refs,
		TrieVariant:                    &variant,
		TrieHash:                       &hash,
		ConversionBlockNum:             &blockNum,
		ConversionTxNum:                &txNum,
	}))
	require.NoError(t, os.WriteFile(filepath.Join(node.Snap, "salt-state.txt"), []byte{1}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(published.Snap, "salt-state.txt"), []byte{1}, 0o644))
	commitmentflags.Restore(t)
	statecfg.BinCommitmentHash = hash
	before := snapshotTree(t, node.DataDir)
	err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
	require.ErrorContains(t, err, "step size")
	require.Equal(t, before, snapshotTree(t, node.DataDir))
}

func TestValidatePBTAttachFilesRejectsUncutStateHistory(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	require.NoError(t, os.MkdirAll(node.SnapHistory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapHistory, "v1.0-accounts.0-2.v"), []byte("history"), 0o644))
	require.ErrorContains(t, validatePBTAttachFiles(node, published, 8, 7), "spans conversion txNum 7")
}

func TestValidatePBTAttachFilesRejectsHistoryBelowStateFrontier(t *testing.T) {
	files := []pbtAttachFile{
		{path: "accounts.0-2.kv", domain: kv.AccountsDomain, from: 0, to: 2, data: true},
		{path: "accounts.0-1.v", domain: kv.AccountsDomain, from: 0, to: 1},
	}
	err := validatePBTAttachHistoryFrontier(files, 8, 10)
	require.ErrorContains(t, err, "accounts.0-1.v")
	require.ErrorContains(t, err, "ends at txNum 8")
}

func TestValidatePBTAttachHistoryFrontierChecksEachFileExtension(t *testing.T) {
	files := []pbtAttachFile{
		{path: "accounts.0-2.kv", domain: kv.AccountsDomain, from: 0, to: 2, data: true},
		{path: "accounts.0-2.v", domain: kv.AccountsDomain, from: 0, to: 2},
		{path: "accounts.0-1.efi", domain: kv.AccountsDomain, from: 0, to: 1},
	}
	err := validatePBTAttachHistoryFrontier(files, 8, 10)
	require.ErrorContains(t, err, "accounts.0-1.efi")
}

func TestAttachPBTRejectsHistorySpanningPointWithoutMutation(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	require.NoError(t, os.MkdirAll(source.SnapHistory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(source.SnapHistory, "v1.0-accounts.0-2.v"), []byte("history"), 0o644))
	setExecutionProgress(t, source.Chaindata, 1)
	before := snapshotTree(t, source.DataDir)
	err := attachPBT(t.Context(), source.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "spans conversion txNum 7")
	require.Equal(t, before, snapshotTree(t, source.DataDir))
}

func TestAdoptPBTFilesRemovesSupersededNodeFiles(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	superseded := []string{"v1.0-commitment-bin.0-4.kv", "v1.0-commitment-bin.0-2.kv", "v1.0-accounts.0-4.kv", "v1.0-accounts.0-2.kv"}
	for _, name := range superseded {
		require.NoError(t, os.WriteFile(filepath.Join(node.SnapDomain, name), []byte("node"), 0o644))
	}
	require.NoError(t, adoptPBTFiles(node, published, 8, 7))
	for _, name := range superseded {
		_, err := os.Stat(filepath.Join(node.SnapDomain, name))
		require.ErrorIs(t, err, os.ErrNotExist, name)
	}
}

func TestAdoptPBTFilesReplacesStateAndCommitmentFiles(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapDomain, "v1.0-accounts.1-2.kv"), []byte("past"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapDomain, "v1.0-receipt.2-3.kv"), []byte("past"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapHistory, "v1.0-receipt.2-3.v"), []byte("past"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapHistory, "v1.0-rcache.2-3.v"), []byte("past"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapIdx, "v1.0-logtopics.2-3.ef"), []byte("past"), 0o644))
	require.NoError(t, adoptPBTFiles(node, published, 8, 7))
	_, err := os.Stat(filepath.Join(node.SnapDomain, "v1.0-accounts.1-2.kv"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(node.SnapDomain, "v1.0-receipt.2-3.kv"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(node.SnapHistory, "v1.0-receipt.2-3.v"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(node.SnapHistory, "v1.0-rcache.2-3.v"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(node.SnapIdx, "v1.0-logtopics.2-3.ef"))
	require.ErrorIs(t, err, os.ErrNotExist)
	for _, name := range []string{
		"v1.0-accounts.0-1.kv",
		"v1.0-storage.0-1.kv",
		"v1.0-code.0-1.kv",
		"v3.0-commitment.0-1.kv",
		"v1.0-commitment-bin.0-1.kv",
	} {
		got, readErr := os.ReadFile(filepath.Join(node.SnapDomain, name))
		require.NoError(t, readErr)
		require.Equal(t, []byte("published"), got)
	}
}

func TestConfiguredPBTNodeHashUsesPersistedSuite(t *testing.T) {
	hash := commitment.PBinHashBlake3
	variant := state.TrieVariantHexBin
	settings := &state.ErigonDBSettings{TrieVariant: &variant, TrieHash: &hash}
	require.Equal(t, hash, configuredPBTNodeHash(settings))
}

func TestAttachPBTResetsAndReopensAtPublishedRoot(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, wantRoot := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	corruptPBTBinTable(t, source.Chaindata)
	require.NoError(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}))
	settings, err := state.ReadErigonDBSettings(datadir.Open(source.DataDir))
	require.NoError(t, err)
	require.Equal(t, state.TrieVariantHexBin, settings.TrieVariantName())
	require.Zero(t, readExecutionStageProgress(t, source.Chaindata))
	genesisDB := dbCfg(dbcfg.ChainDB, source.Chaindata).MustOpen()
	var genesisHash common.Hash
	require.NoError(t, genesisDB.View(t.Context(), func(tx kv.Tx) error {
		var err error
		genesisHash, err = rawdb.ReadCanonicalHash(tx, 0)
		return err
	}))
	require.NotEqual(t, [32]byte{}, genesisHash)
	genesisDB.Close()
	manifest, err := state.ReadPBTManifest(filepath.Join(datadir.Open(source.DataDir).Snap, "pbt-manifest.toml"))
	require.NoError(t, err)
	blockNum, txNum, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	blockHash, _, blockEnd, _, _, err := pbtAttachBlockEnd(t.Context(), datadir.Open(source.DataDir), blockNum, txNum)
	require.NoError(t, err)
	if blockEnd {
		attachedDB := dbCfg(dbcfg.ChainDB, source.Chaindata).MustOpen()
		var shadow []byte
		require.NoError(t, attachedDB.View(t.Context(), func(tx kv.Tx) error {
			var err error
			shadow, err = rawdb.ReadShadowStateRoot(tx, blockHash, blockNum)
			return err
		}))
		manifestRoot := common.HexToHash(manifest.PBTRoot)
		require.Equal(t, manifestRoot[:], shadow)
		attachedDB.Close()
	}
	root, _ := reopenBinSource(t, source.DataDir, source.Chaindata)
	require.Equal(t, wantRoot[:], root)
}

type pbtLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *pbtLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *pbtLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestPBTProgressCoversConvertAndAttach(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	previousHandler := log.Root().GetHandler()
	var logs pbtLogBuffer
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	t.Cleanup(func() { log.Root().SetHandler(previousHandler) })
	previousInterval := state.PBTProgressInterval
	state.PBTProgressInterval = time.Nanosecond
	t.Cleanup(func() { state.PBTProgressInterval = previousInterval })
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	require.NoError(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}))
	for _, phase := range []string{
		"source leaf walk",
		"leaf load",
		"source end verify",
		"pbin-ops file write",
		"fold batches",
		"range leaf stream",
		"range 0-1 .kv",
		"range 0-1 .idt",
		"range 0-1 .kvi",
		"domain root verify",
		"output identity verify",
		"attach published bin verify",
		"attach adopt",
	} {
		pattern := regexp.MustCompile(`phase="` + regexp.QuoteMeta(phase) + `(?:"|\s).*done=(\d+)(?:.*total=(\d+))?`)
		var found, intermediate, increasing bool
		var previous uint64
		for line := range strings.SplitSeq(logs.String(), "\n") {
			match := pattern.FindStringSubmatch(line)
			if len(match) == 0 {
				continue
			}
			found = true
			require.Contains(t, line, "rate=")
			done, err := strconv.ParseUint(match[1], 10, 64)
			require.NoError(t, err)
			if match[2] != "" {
				total, err := strconv.ParseUint(match[2], 10, 64)
				require.NoError(t, err)
				intermediate = intermediate || done < total
			} else if done > previous {
				increasing = true
			}
			previous = done
		}
		require.True(t, found, "missing progress phase %q in logs:\n%s", phase, logs.String())
		require.True(t, intermediate || increasing, "phase %q has no intermediate progress in logs:\n%s", phase, logs.String())
	}
}

func TestAttachPBTSkipVerifyAdoptsWithoutLeafPass(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, wantRoot := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	require.NoError(t, corruptPBTDomain(t.Context(), datadir.Open(published), kv.CommitmentBinDomain))
	setExecutionProgress(t, source.Chaindata, 1)
	hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, skipVerify: true}
	require.NoError(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks))
	settings, err := state.ReadErigonDBSettings(datadir.Open(source.DataDir))
	require.NoError(t, err)
	require.Equal(t, state.TrieVariantHexBin, settings.TrieVariantName())
	require.Zero(t, readExecutionStageProgress(t, source.Chaindata))
	root, _ := reopenBinSource(t, source.DataDir, source.Chaindata)
	require.Equal(t, wantRoot[:], root)
}

func TestAttachPBTAcceptsEmptyConversion(t *testing.T) {
	for _, skipVerify := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip-verify=%t", skipVerify), func(t *testing.T) {
			commitmentflags.Restore(t)
			statecfg.ExperimentalCommitmentV3 = true
			statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
			statecfg.BinCommitmentHash = ""
			require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
			source := newPBTEmptyConversionSource(t)
			published := filepath.Join(t.TempDir(), "published")
			require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
			setExecutionProgress(t, source.Chaindata, 1)
			require.NoError(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis, skipVerify: skipVerify}))
		})
	}
}

func TestAttachPBTRejectsMissingRootForNonEmptyConversion(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	require.NoError(t, removePBTGlobalRootDomain(t.Context(), datadir.Open(published)))
	setExecutionProgress(t, source.Chaindata, 1)
	err := attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis, skipVerify: true})
	require.ErrorContains(t, err, "published binary root record")
}

func TestAttachPBTManifestRefusesTamperingBeforeAdoption(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string)
		want   string
	}{
		{name: "changed file", mutate: func(t *testing.T, published string) {
			path := pbtAttachManifestStateFile(t, published)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			data[0] ^= 1
			require.NoError(t, os.WriteFile(path, data, 0o644))
		}, want: "digest mismatch"},
		{name: "missing file", mutate: func(t *testing.T, published string) {
			require.NoError(t, dir.RemoveFile(pbtAttachManifestStateFile(t, published)))
		}, want: "pbt manifest"},
		{name: "extra file", mutate: func(t *testing.T, published string) {
			dirs := datadir.Open(published)
			require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-2.kv"), []byte("extra"), 0o644))
		}, want: "pbt manifest"},
		{name: "wrong pbt root", mutate: func(t *testing.T, published string) {
			rewritePBTAttachManifest(t, published, func(manifest *state.PBTManifest) { manifest.PBTRoot = common.HexToHash("0x09").Hex() })
		}, want: "binary root record"},
		{name: "wrong hex root", mutate: func(t *testing.T, published string) {
			rewritePBTAttachManifest(t, published, func(manifest *state.PBTManifest) { manifest.HexRoot = common.HexToHash("0x09").Hex() })
		}, want: "published hex root"},
		{name: "missing manifest", mutate: func(t *testing.T, published string) {
			dirs := datadir.Open(published)
			require.NoError(t, dir.RemoveFile(filepath.Join(dirs.Snap, "pbt-manifest.toml")))
		}, want: "re-run convert-pbt with this binary"},
	} {
		t.Run(test.name, func(t *testing.T) {
			commitmentflags.Restore(t)
			statecfg.ExperimentalCommitmentV3 = true
			statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
			statecfg.BinCommitmentHash = ""
			require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
			source, _ := newPBTConversionSource(t)
			published := filepath.Join(t.TempDir(), "published")
			require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
			test.mutate(t, published)
			before := snapshotTreeDigest(t, source.Snap)
			setExecutionProgress(t, source.Chaindata, 1)
			err := attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis})
			require.ErrorContains(t, err, test.want)
			require.Equal(t, before, snapshotTreeDigest(t, source.Snap))
		})
	}
}

func TestAttachPBTSkipVerifyChecksRootsButSkipsDigests(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string)
		want   string
	}{
		{name: "changed file", mutate: func(t *testing.T, published string) {
			path := pbtAttachManifestStateFile(t, published)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			data[len(data)-1] ^= 1
			require.NoError(t, os.WriteFile(path, data, 0o644))
		}, want: ""},
		{name: "wrong pbt root", mutate: func(t *testing.T, published string) {
			rewritePBTAttachManifest(t, published, func(manifest *state.PBTManifest) { manifest.PBTRoot = common.HexToHash("0x09").Hex() })
		}, want: "binary root record"},
		{name: "wrong hex root", mutate: func(t *testing.T, published string) {
			rewritePBTAttachManifest(t, published, func(manifest *state.PBTManifest) { manifest.HexRoot = common.HexToHash("0x09").Hex() })
		}, want: "published hex root"},
	} {
		t.Run(test.name, func(t *testing.T) {
			commitmentflags.Restore(t)
			statecfg.ExperimentalCommitmentV3 = true
			statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
			statecfg.BinCommitmentHash = ""
			require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
			source, _ := newPBTConversionSource(t)
			published := filepath.Join(t.TempDir(), "published")
			require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
			test.mutate(t, published)
			setExecutionProgress(t, source.Chaindata, 1)
			err := attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis, skipVerify: true})
			if test.want == "" {
				require.NoError(t, err)
				_, statErr := os.Stat(filepath.Join(datadir.Open(source.DataDir).Snap, "pbt-manifest.toml"))
				require.NoError(t, statErr)
			} else {
				require.ErrorContains(t, err, test.want)
			}
		})
	}
}

func TestAttachPBTRemovesCorruptedCopiedFile(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	sourcePath := pbtAttachManifestStateFile(t, published)
	publishedDirs := datadir.Open(published)
	sourceDirs := datadir.Open(source.DataDir)
	rel, err := filepath.Rel(publishedDirs.Snap, sourcePath)
	require.NoError(t, err)
	destinationPath := filepath.Join(sourceDirs.Snap, rel)
	hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, afterCopy: func() error {
		data, err := os.ReadFile(destinationPath)
		if err != nil {
			return err
		}
		data[0] ^= 1
		return os.WriteFile(destinationPath, data, 0o644)
	}}
	err = attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks)
	require.ErrorContains(t, err, "digest mismatch")
	_, statErr := os.Stat(destinationPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func pbtAttachManifestStateFile(t *testing.T, published string) string {
	t.Helper()
	sources, err := pbtAttachManifestSources(datadir.Open(published))
	require.NoError(t, err)
	for _, source := range sources {
		if strings.Contains(source.Name, "accounts") && strings.HasSuffix(source.Name, ".kv") {
			info, statErr := os.Stat(source.Path)
			require.NoError(t, statErr)
			if info.Size() != 0 {
				return source.Path
			}
		}
	}
	t.Fatal("no accounts state file")
	return ""
}

func snapshotTreeDigest(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	tree := snapshotTree(t, root)
	got := make(map[string][32]byte, len(tree))
	for path, data := range tree {
		got[path] = sha256.Sum256([]byte(data))
	}
	return got
}

func rewritePBTAttachManifest(t *testing.T, published string, mutate func(*state.PBTManifest)) {
	t.Helper()
	dirs := datadir.Open(published)
	path := filepath.Join(dirs.Snap, "pbt-manifest.toml")
	manifest, err := state.ReadPBTManifest(path)
	require.NoError(t, err)
	mutate(&manifest)
	require.NoError(t, state.WritePBTManifest(path, manifest))
}

func TestAttachPBTRejectsDifferentStateSalt(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	require.NoError(t, os.WriteFile(filepath.Join(source.Snap, "salt-state.txt"), []byte{9, 9, 9, 9}, 0o644))
	publishedDirs := datadir.Open(published)
	require.NoError(t, dir.RemoveFile(filepath.Join(publishedDirs.Snap, "salt-state.txt")))
	require.NoError(t, os.WriteFile(filepath.Join(publishedDirs.Snap, "salt-state.txt"), []byte{8, 8, 8, 8}, 0o644))
	err := attachPBT(t.Context(), source.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "salt-state.txt")
	require.ErrorContains(t, err, "node=09090909")
}

func TestAttachPBTRemovesOutputSettingsRefusalCases(t *testing.T) {
	t.Run("missing settings", func(t *testing.T) {
		node := datadir.New(t.TempDir())
		published := datadir.New(t.TempDir())
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "no erigondb.toml")
	})
	t.Run("missing node settings leaves datadir unchanged", func(t *testing.T) {
		commitmentflags.Restore(t)
		statecfg.BinCommitmentHash = commitment.PBinHashBlake3
		node, published := newPBTAttachFileTrees(t, true)
		refs := false
		variant := state.TrieVariantHexBin
		hash := commitment.PBinHashKeccak
		require.NoError(t, state.WriteErigonDBSettings(published, &state.ErigonDBSettings{
			StepSize: config3.DefaultStepSize, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs,
			TrieVariant: &variant, TrieHash: &hash, ConversionBlockNum: uint64Ptr(1), ConversionTxNum: uint64Ptr(7),
		}))
		before := snapshotTree(t, node.DataDir)
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "trie_hash")
		require.Equal(t, before, snapshotTree(t, node.DataDir))
	})
	t.Run("history spanning conversion point", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, os.MkdirAll(node.SnapHistory, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(node.SnapHistory, "v1.0-accounts.0-2.v"), []byte("history"), 0o644))
		before := snapshotTree(t, node.DataDir)
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "spans conversion txNum 7")
		require.Equal(t, before, snapshotTree(t, node.DataDir))
	})
	t.Run("frozen torrent sidecar does not set the history frontier", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, os.MkdirAll(node.SnapHistory, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(node.SnapHistory, "v1.0-accounts.0-0.kvei.torrent"), []byte("torrent"), 0o644))
		files, err := pbtAttachFiles(node)
		require.NoError(t, err)
		require.NoError(t, validatePBTAttachHistoryFrontier(files, 8, 7))
	})
	t.Run("hash", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashKeccak, 1, 7)
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "trie_hash")
	})
	t.Run("variant", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		refs := false
		variant := state.TrieVariantHex
		require.NoError(t, state.WriteErigonDBSettings(published, &state.ErigonDBSettings{StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs, TrieVariant: &variant, ConversionBlockNum: uint64Ptr(1), ConversionTxNum: uint64Ptr(7)}))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "hex+bin")
	})
	t.Run("ranges", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-accounts.0-1.kv")))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.EqualError(t, err, "commitment attach-pbt: published files are missing domain accounts through txNum 7")
	})
	t.Run("conversion point", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 9)
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "do not cover conversion txNum")
	})
	t.Run("published bin range", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, false)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		oldPath := filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-1.kv")
		newPath := filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-0.kv")
		require.NoError(t, os.Rename(oldPath, newPath))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "commitment-bin ranges do not match commitment ranges")
	})
	t.Run("missing bin", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-1.kv")))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "commitment-bin")
	})
	t.Run("missing hex", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v3.0-commitment.0-1.kv")))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "commitment")
	})
	t.Run("node behind", func(t *testing.T) {
		commitmentflags.Restore(t)
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, os.WriteFile(filepath.Join(node.Snap, "salt-state.txt"), []byte{0, 0, 0, 1}, 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(published.Snap, "salt-state.txt"), []byte{0, 0, 0, 1}, 0o644))
		require.NoError(t, os.MkdirAll(node.Chaindata, 0o755))
		rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(node.Chaindata).MustOpen()
		rawDB.Close()
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "node hex state is missing")
	})
}

func TestPBTAttachPointRemedies(t *testing.T) {
	blockEnd := pbtAttachNodePointError("/node", "hoodi", 10, 20, 10, 5, 13, true)
	require.ErrorContains(t, blockEnd, "--unwind=5")
	require.ErrorContains(t, blockEnd, "--block=5")
	midBlock := pbtAttachNodePointError("/node", "hoodi", 10, 20, 10, 5, 13, false)
	require.ErrorContains(t, midBlock, "--reset")
	require.NotContains(t, midBlock.Error(), "--unwind=")
	behind := pbtAttachNodePointError("/node", "hoodi", 4, 10, 4, 5, 13, true)
	require.ErrorContains(t, behind, "behind conversion point")
	require.NotContains(t, behind.Error(), "--unwind=")
	dev := pbtAttachNodePointError("/node", "dev", 10, 20, 10, 5, 13, true)
	require.Error(t, dev)
	require.NotContains(t, dev.Error(), "stage_exec")
	require.ErrorContains(t, dev, "cannot be loaded by integration")
	emptyChain := pbtAttachNodePointError("/node", "", 10, 20, 10, 5, 13, true)
	require.Error(t, emptyChain)
	require.NotContains(t, emptyChain.Error(), "stage_exec")
	require.ErrorContains(t, emptyChain, "cannot be loaded by integration")

	for name, err := range map[string]error{
		"behind":      pbtAttachNodePointError("/node", "dev", 4, 10, 4, 5, 13, true),
		"mid-block":   pbtAttachNodePointError("/node", "dev", 10, 20, 10, 5, 13, false),
		"at-point":    pbtAttachNodePointError("/node", "dev", 10, 13, 5, 5, 13, true),
		"files-ahead": pbtAttachFilesAheadError("/node", "dev", 3, 8, 10, 2, 17),
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, err)
			require.NotContains(t, err.Error(), "stage_exec")
			require.ErrorContains(t, err, "cannot be loaded by integration")
		})
	}
}

func TestPBTAttachFilesAheadOfPointRemedy(t *testing.T) {
	files := []pbtAttachFile{{domain: kv.AccountsDomain, from: 3, to: 4, path: "accounts.3-4.kv", data: true}}
	first, found := pbtAttachFirstStepPastPoint(files, 8, 17)
	require.True(t, found)
	require.Equal(t, uint64(3), first)
	err := pbtAttachFilesAheadError("/node", "hoodi", first, 8, 10, 2, 17)
	require.ErrorContains(t, err, "snapshots rm-state --datadir=/node --chain=hoodi --step=3+")
	require.ErrorContains(t, err, "stage_exec --datadir=/node --reset")
	spanning := []pbtAttachFile{{domain: kv.AccountsDomain, from: 1, to: 2, path: "accounts.1-2.kv", data: true}}
	first, found = pbtAttachFirstStepPastPoint(spanning, 8, 9)
	require.True(t, found)
	require.Equal(t, uint64(1), first)
	err = pbtAttachFilesAheadError("/node", "hoodi", first, 8, 10, 2, 9)
	require.ErrorContains(t, err, "stage_exec cannot stop at a mid-block point")
	require.NotContains(t, err.Error(), "rm-state")
}

func runPBTOfflineCommand(t *testing.T, binary string, args ...string) {
	runPBTOfflineCommandWithEnv(t, os.Environ(), binary, args...)
}

func runPBTOfflineCommandWithEnv(t *testing.T, environment []string, binary string, args ...string) {
	t.Helper()
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	if binary == "integration" && len(args) > 0 && args[0] == "stage_exec" {
		command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPBTAttachRemedyHelperProcess$")
		command.Env = append(
			os.Environ(),
			"GO_WANT_HELPER_PROCESS=1",
			"PBT_REMEDY_ARGS="+strings.Join(args[1:], "\x1f"),
		)
		output, err := command.CombinedOutput()
		require.NoError(t, err, "helper for %s: %s", strings.Join(args, " "), output)
		return
	}
	commandPath := binary
	if !filepath.IsAbs(commandPath) {
		commandPath = filepath.Join(workingDir, "..", "..", "..", "build", "bin", binary)
	}
	command := exec.CommandContext(t.Context(), commandPath, args...)
	command.Env = environment
	if filepath.Base(commandPath) == "erigon" {
		command.Stdin = strings.NewReader("1\n")
	}
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s %s: %s", commandPath, strings.Join(args, " "), output)
	if filepath.Base(commandPath) == "erigon" {
		t.Logf("rm-state output: %s", output)
	}
}

func printedPBTCommands(message, prefix string) [][]string {
	var commands [][]string
	for offset := 0; offset < len(message); {
		index := strings.Index(message[offset:], prefix)
		if index < 0 {
			break
		}
		index += offset
		fields := strings.Fields(message[index:])
		command := make([]string, 0, len(fields))
		for _, field := range fields {
			field = strings.Trim(field, ",.;")
			if len(command) > 0 && (field == "when" || field == "followed" || field == "then" || field == "or") {
				break
			}
			command = append(command, field)
		}
		commands = append(commands, command)
		offset = index + len(prefix)
	}
	return commands
}

func runPrintedPBTCommand(t *testing.T, erigonBinary string, command []string) {
	t.Helper()
	require.NotEmpty(t, command)
	environment := os.Environ()
	for len(command) > 0 {
		if !strings.Contains(command[0], "=") || strings.HasPrefix(command[0], "--") {
			break
		}
		environment = append(environment, command[0])
		command = command[1:]
	}
	require.NotEmpty(t, command)
	binary := command[0]
	if binary == "erigon" {
		binary = erigonBinary
	}
	runPBTOfflineCommandWithEnv(t, environment, binary, command[1:]...)
}

func TestPBTAttachRemedyHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	commitmentflags.Restore(t)
	fixture, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	fixture.Genesis.Config.ChainName = "test"
	genesisHash := fixture.Tester.Genesis.Hash()
	chainspec.RegisterChainSpec("pbt-remedy", chainspec.Spec{
		Name:        "pbt-remedy",
		GenesisHash: genesisHash,
		Config:      fixture.Genesis.Config,
		Genesis:     fixture.Genesis,
	})
	fixture.Tester.Close()
	previousDatadir, previousChaindata, previousChain := datadirCli, chaindata, chain
	previousReset, previousUnwind, previousBlock := reset, unwind, block
	t.Cleanup(func() {
		datadirCli, chaindata, chain = previousDatadir, previousChaindata, previousChain
		reset, unwind, block = previousReset, previousUnwind, previousBlock
	})
	args := strings.Split(os.Getenv("PBT_REMEDY_ARGS"), "\x1f")
	require.NoError(t, cmdStageExec.ParseFlags(args))
	chaindata = filepath.Join(datadirCli, "chaindata")
	chain = "pbt-remedy"
	cmdStageExec.PreRun(cmdStageExec, nil)
	db, err := openDB(t.Context(), dbCfg(dbcfg.ChainDB, chaindata), true, chain, log.New())
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, stageExec(db, t.Context(), log.New()))
}

func newPBTAttachRemedyFixture(t *testing.T, stepSize, sourceTx, nodeFileTx uint64) (datadir.Dirs, string) {
	t.Helper()
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChainWithStepSize(t, false, false, stepSize)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	node.Tester.ChainConfig.ChainName = "test"
	node.Genesis.Config.ChainName = "test"
	rawDB := node.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		return rawdb.WriteChainConfig(tx, genesisHash, node.Tester.ChainConfig)
	}))
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		config, err := rawdb.ReadChainConfig(tx, genesisHash)
		require.NoError(t, err)
		require.Equal(t, "test", config.ChainName)
		return nil
	}))
	buildPBTAcceptanceFilesAt(t, node, nodeFileTx)
	nodeDirs := node.Tester.Dirs
	checkDB := dbCfg(dbcfg.ChainDB, nodeDirs.Chaindata).MustOpen()
	require.NoError(t, checkDB.View(t.Context(), func(tx kv.Tx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		config, err := rawdb.ReadChainConfig(tx, genesisHash)
		require.NoError(t, err)
		require.Equal(t, "test", config.ChainName)
		return nil
	}))
	checkDB.Close()

	source, err := execmoduletester.NewPBTAcceptanceChainWithStepSize(t, false, false, stepSize)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	buildPBTAcceptanceFilesAt(t, source, sourceTx)
	sourceSettings, settingsErr := state.ReadErigonDBSettings(datadir.Open(source.Tester.Dirs.DataDir))
	require.NoError(t, settingsErr)
	sourcePoint, pointErr := readPBinSourcePoint(t.Context(), source.Tester.Dirs, sourceSettings, true, log.New())
	require.NoError(t, pointErr)
	_, sourceFound, sourceRootErr := pbtAttachHexRoot(t.Context(), source.Tester.Dirs, sourceSettings, sourcePoint.BlockNum, sourcePoint.TxNum, log.New())
	require.NoError(t, sourceRootErr)
	require.True(t, sourceFound)
	published := filepath.Join(t.TempDir(), "published")
	selectPBTCommandSuite(t)
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	return nodeDirs, published
}

func TestPBTAttachPrintedRemediesRunInFreshProcesses(t *testing.T) {
	commitmentflags.Restore(t)
	erigonBinary := buildPBTTestErigon(t)
	t.Run("block end", func(t *testing.T) {
		nodeDirs, published := newPBTAttachRemedyFixture(t, 1, 7, 7)
		err := attachPBT(t.Context(), nodeDirs.DataDir, published, "test", log.New())
		require.Error(t, err)
		commands := printedPBTCommands(err.Error(), "integration stage_exec")
		require.GreaterOrEqual(t, len(commands), 2)
		for _, command := range commands[len(commands)-2:] {
			runPrintedPBTCommand(t, erigonBinary, command)
		}
		require.NoError(t, attachPBT(t.Context(), nodeDirs.DataDir, published, "test", log.New()))
	})
	t.Run("mid block", func(t *testing.T) {
		nodeDirs, published := newPBTAttachRemedyFixture(t, 1, 9, 9)
		err := attachPBT(t.Context(), nodeDirs.DataDir, published, "test", log.New())
		require.Error(t, err)
		commands := printedPBTCommands(err.Error(), "integration stage_exec")
		require.Len(t, commands, 1)
		runPrintedPBTCommand(t, erigonBinary, commands[0])
		require.NoError(t, attachPBT(t.Context(), nodeDirs.DataDir, published, "test", log.New()))
	})
	for _, test := range []struct {
		name    string
		pointTx uint64
		nodeTx  uint64
	}{
		{name: "past block end", pointTx: 7, nodeTx: 15},
		{name: "past mid block", pointTx: 13, nodeTx: 15},
	} {
		t.Run(test.name, func(t *testing.T) {
			nodeDirs, published := newPBTAttachRemedyFixture(t, 1, test.pointTx, test.nodeTx)
			err := attachPBT(t.Context(), nodeDirs.DataDir, published, "test", log.New())
			require.Error(t, err)
			commands := printedPBTCommands(err.Error(), "erigon snapshots rm-state")
			require.Len(t, commands, 1)
			runPrintedPBTCommand(t, erigonBinary, commands[0])
			commands = printedPBTCommands(err.Error(), "integration stage_exec")
			require.Len(t, commands, 1)
			runPrintedPBTCommand(t, erigonBinary, commands[0])
			require.NoError(t, attachPBT(t.Context(), nodeDirs.DataDir, published, "test", log.New()))
		})
	}
}

func TestPBTAttachRejectsNodeBehindConversionTxNum(t *testing.T) {
	selectPBTCommandSuite(t)
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	rawDB := dbCfg(dbcfg.ChainDB, source.Chaindata).MustOpen()
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		if err := rawdbv3.TxNums.Truncate(tx, 1); err != nil {
			return err
		}
		return rawdbv3.TxNums.Append(tx, 1, 6)
	}))
	rawDB.Close()
	setExecutionProgress(t, source.Chaindata, 1)
	err := attachPBT(t.Context(), source.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "node is behind conversion txNum")
}

func buildPBTTestErigon(t *testing.T) string {
	t.Helper()
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	root := filepath.Join(workingDir, "..", "..", "..")
	binary := filepath.Join(t.TempDir(), "erigon")
	command := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/erigon")
	command.Dir = root
	output, err := command.CombinedOutput()
	require.NoError(t, err, "go build ./cmd/erigon: %s", output)
	return binary
}

func TestAdoptPBTFilesKeepsTorrentSidecarsWithBytes(t *testing.T) {
	node := datadir.New(filepath.Join(t.TempDir(), "node"))
	published := datadir.New(filepath.Join(t.TempDir(), "published"))
	nodeFile := filepath.Join(node.SnapDomain, "v1.0-accounts.0-1.kv")
	publishedFile := filepath.Join(published.SnapDomain, "v1.0-accounts.0-1.kv")
	require.NoError(t, os.MkdirAll(filepath.Dir(nodeFile), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(publishedFile), 0o755))
	require.NoError(t, os.WriteFile(nodeFile, []byte("old"), 0o644))
	require.NoError(t, os.WriteFile(nodeFile+".torrent", []byte("old torrent"), 0o644))
	orphan := filepath.Join(node.SnapDomain, "v1.0-storage.0-1.kv")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o644))
	require.NoError(t, os.WriteFile(orphan+".torrent", []byte("orphan torrent"), 0o644))
	require.NoError(t, os.WriteFile(publishedFile, []byte("new"), 0o644))
	require.NoError(t, os.WriteFile(publishedFile+".torrent", []byte("new torrent"), 0o644))
	require.NoError(t, adoptPBTFiles(node, published, 8, 7))
	got, err := os.ReadFile(nodeFile)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), got)
	got, err = os.ReadFile(nodeFile + ".torrent")
	require.NoError(t, err)
	require.Equal(t, []byte("new torrent"), got)
	var unpaired []string
	require.NoError(t, filepath.WalkDir(node.DataDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".torrent") {
			if _, statErr := os.Stat(strings.TrimSuffix(path, ".torrent")); errors.Is(statErr, fs.ErrNotExist) {
				unpaired = append(unpaired, path)
			}
		}
		return nil
	}))
	require.Empty(t, unpaired)
}

func TestAttachPBTRetryAfterEachInterruptedStep(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	for _, step := range []string{"marker", "swap", "reset", "settings"} {
		t.Run(step, func(t *testing.T) {
			source, _ := newPBTConversionSource(t)
			published := filepath.Join(t.TempDir(), "published")
			require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
			setExecutionProgress(t, source.Chaindata, 1)
			hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, step: func(current string) error {
				if current == step {
					return errors.New("interrupted attach")
				}
				return nil
			}}
			require.ErrorContains(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks), "interrupted attach")
			marker, err := state.ReadPBTAttachMarker(datadir.Open(source.DataDir))
			require.NoError(t, err)
			require.NotNil(t, marker)
			require.ErrorContains(t, state.RefusePBTAttachMarker(datadir.Open(source.DataDir)), "rerun attach-pbt")
			require.NoError(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}))
			marker, err = state.ReadPBTAttachMarker(datadir.Open(source.DataDir))
			require.NoError(t, err)
			require.Nil(t, marker)
		})
	}
}

func TestAttachPBTRetryAfterPartialSwap(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, step: func(step string) error {
		if step == "marker" {
			return errors.New("interrupted attach")
		}
		return nil
	}}
	require.ErrorContains(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks), "interrupted attach")
	files, err := pbtAttachFiles(datadir.Open(source.DataDir))
	require.NoError(t, err)
	for _, file := range files {
		if file.domain == kv.AccountsDomain && file.data {
			require.NoError(t, dir.RemoveFile(file.path))
			break
		}
	}
	require.NoError(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}))
}

func TestAttachPBTRetryAfterPartialSettingsWrite(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, step: func(step string) error {
		if step == "reset" {
			return errors.New("interrupted attach")
		}
		return nil
	}}
	require.ErrorContains(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks), "interrupted attach")
	require.NoError(t, os.WriteFile(filepath.Join(source.Snap, state.ERIGONDB_SETTINGS_FILE), []byte("trie_variant = \""), 0o644))
	require.NoError(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}))
}

func TestAttachPBTRejectsConversionPointBeyondPublishedCheckpoint(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	settings, err := state.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	conversionTx := uint64(16)
	settings.ConversionTxNum = &conversionTx
	require.NoError(t, state.WriteErigonDBSettings(datadir.Open(published), settings))
	setExecutionProgress(t, source.Chaindata, 2)
	err = attachPBT(t.Context(), source.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "ends at txNum 8 before conversion txNum 16")
}

func TestAttachPBTRejectsTruncatedPublishedAccountsFile(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	files, err := pbtAttachFiles(datadir.Open(published))
	require.NoError(t, err)
	for _, file := range files {
		if file.domain == kv.AccountsDomain && file.data {
			require.NoError(t, os.Truncate(file.path, 1))
			break
		}
	}
	setExecutionProgress(t, source.Chaindata, 1)
	err = attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis})
	require.ErrorContains(t, err, "accounts")

	recoverySource, _ := newPBTConversionSource(t)
	recoveryPublished := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), recoverySource.DataDir, recoveryPublished, true, "", log.New()))
	setExecutionProgress(t, recoverySource.Chaindata, 1)
	hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, step: func(step string) error {
		if step == "marker" {
			return errors.New("interrupted attach")
		}
		return nil
	}}
	require.ErrorContains(t, attachPBTWithHooks(t.Context(), recoverySource.DataDir, recoveryPublished, "", log.New(), hooks), "interrupted attach")
	recoveryFiles, err := pbtAttachFiles(datadir.Open(recoveryPublished))
	require.NoError(t, err)
	for _, file := range recoveryFiles {
		if file.domain == kv.AccountsDomain && file.data {
			require.NoError(t, os.Truncate(file.path, 1))
			break
		}
	}
	require.ErrorContains(t, attachPBTWithHooks(t.Context(), recoverySource.DataDir, recoveryPublished, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}), "accounts")
}

func TestAttachPBTRejectsPublishedLeafAfterConversion(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	rewritePBTAttachManifest(t, published, func(manifest *state.PBTManifest) {
		manifest.PBTRoot = common.HexToHash("0x09").Hex()
	})
	before := snapshotTree(t, source.DataDir)
	err := attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis})
	require.ErrorContains(t, err, "published binary root record")
	require.Equal(t, before, snapshotTree(t, source.DataDir))
}

func pbtAttachNoGenesis(context.Context, datadir.Dirs, datadir.Dirs, log.Logger) error {
	return nil
}

func renamePBTFilesRange(t *testing.T, root, from, to string) {
	renamePBTFilesRangeWithFilter(t, root, from, to, true)
}

func renamePBTAllFilesRange(t *testing.T, root, from, to string) {
	renamePBTFilesRangeWithFilter(t, root, from, to, false)
}

func renamePBTFilesRangeWithFilter(t *testing.T, root, from, to string, adoptedOnly bool) {
	t.Helper()
	var paths []string
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.Contains(entry.Name(), "."+from+".") {
			return err
		}
		ext := filepath.Ext(entry.Name())
		if adoptedOnly && ext != ".kv" && ext != ".bt" && ext != ".kvi" && ext != ".kvei" {
			return err
		}
		paths = append(paths, path)
		return nil
	}))
	for _, path := range paths {
		name := strings.Replace(filepath.Base(path), "."+from+".", "."+to+".", 1)
		require.NoError(t, os.Rename(path, filepath.Join(filepath.Dir(path), name)))
	}
}

func writePBTAttachSettings(t *testing.T, dirs datadir.Dirs, hash string, blockNum, txNum uint64) {
	t.Helper()
	refs := false
	variant := state.TrieVariantHexBin
	require.NoError(t, state.WriteErigonDBSettings(dirs, &state.ErigonDBSettings{
		StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs,
		TrieVariant: &variant, TrieHash: &hash, ConversionBlockNum: uint64Ptr(blockNum), ConversionTxNum: uint64Ptr(txNum),
	}))
}

func uint64Ptr(value uint64) *uint64 { return &value }

func corruptPBTBinTable(t *testing.T, rawPath string) {
	t.Helper()
	db := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	defer db.Close()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Put(kv.TblCommitmentBinVals, []byte{0}, []byte{1})
	}))
}

func newPBTAttachFileTrees(t *testing.T, both bool) (datadir.Dirs, datadir.Dirs) {
	t.Helper()
	node := datadir.New(t.TempDir())
	published := datadir.New(t.TempDir())
	for _, dirs := range []*datadir.Dirs{&node, &published} {
		require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))
		for _, name := range []string{
			"v1.0-accounts.0-1.kv",
			"v1.0-storage.0-1.kv",
			"v1.0-code.0-1.kv",
			"v3.0-commitment.0-1.kv",
		} {
			require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, name), []byte("node"), 0o644))
		}
		if both {
			require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v1.0-commitment-bin.0-1.kv"), []byte("node"), 0o644))
		}
	}
	for _, name := range []string{
		"v1.0-accounts.0-1.kv",
		"v1.0-storage.0-1.kv",
		"v1.0-code.0-1.kv",
		"v3.0-commitment.0-1.kv",
		"v1.0-commitment-bin.0-1.kv",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(published.SnapDomain, name), []byte("published"), 0o644))
	}
	return node, published
}

func TestAttachPBTResumeRejectsReplacedManifest(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip=%t", skip), func(t *testing.T) {
			selectPBTCommandSuite(t)
			source, _ := newPBTConversionSourceAtWithRows(t, 7, 0, 0, []byte{0x60, 0x01})
			published := filepath.Join(t.TempDir(), "published")
			require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
			setExecutionProgress(t, source.Chaindata, 1)
			hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, skipVerify: skip, step: func(step string) error {
				if step == "marker" {
					return errors.New("interrupted attach")
				}
				return nil
			}}
			require.ErrorContains(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks), "interrupted attach")
			replacement, _ := newPBTConversionSourceAtWithRows(t, 7, 0, 1, []byte{0x60, 0x01})
			replacementPublished := filepath.Join(t.TempDir(), "published")
			require.NoError(t, convertPBT(t.Context(), replacement.DataDir, replacementPublished, true, "", log.New()))
			require.NoError(t, os.Rename(published, published+".old"))
			require.NoError(t, os.Rename(replacementPublished, published))
			_, root, end, fork, _, err := pbtAttachBlockEnd(t.Context(), source.Dirs, 1, 7)
			require.NoError(t, err)
			require.True(t, end)
			require.True(t, fork)
			manifest, err := state.ReadPBTManifest(filepath.Join(datadir.Open(published).Snap, "pbt-manifest.toml"))
			require.NoError(t, err)
			require.NotEqual(t, common.HexToHash(manifest.PBTRoot), root)
			var steps []string
			hooks.step = func(step string) error { steps = append(steps, step); return nil }
			err = attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks)
			require.Error(t, err, "resume accepted replacement manifest with mismatched post-fork root: err=%v steps=%v", err, steps)
			require.Empty(t, steps)
		})
	}
}

func TestAttachPBTResumeAfterCommitmentRemoval(t *testing.T) {
	selectPBTCommandSuite(t)
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	db := dbCfg(dbcfg.ChainDB, source.Chaindata).MustOpen()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error { return tx.ClearTable(kv.TblCommitmentVals) }))
	db.Close()
	hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, step: func(step string) error {
		if step == "marker" {
			return errors.New("interrupted attach")
		}
		return nil
	}}
	require.ErrorContains(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks), "interrupted attach")
	marker, err := state.ReadPBTAttachMarker(source.Dirs)
	require.NoError(t, err)
	require.NotNil(t, marker)
	digest, err := state.PBTManifestDigest(filepath.Join(datadir.Open(published).Snap, "pbt-manifest.toml"))
	require.NoError(t, err)
	require.Equal(t, digest, marker.ManifestBlake3)
	require.NoError(t, removePBTFiles(source.Dirs, pbtAttachAdoptsFile))
	err = attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis})
	require.NoError(t, err, "same-manifest resume after interrupted file removal must finish: got %v", err)
}
