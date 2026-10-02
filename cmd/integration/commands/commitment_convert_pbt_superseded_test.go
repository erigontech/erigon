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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snaptype"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
)

func supersededSnapshotFiles(t *testing.T, dirs datadir.Dirs) []string {
	var out []string
	for _, root := range []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
		_ = filepath.WalkDir(root, func(p string, e fs.DirEntry, err error) error {
			if err == nil && !e.IsDir() {
				rel, _ := filepath.Rel(dirs.Snap, p)
				out = append(out, rel)
			}
			return nil
		})
	}
	sort.Strings(out)
	return out
}

type supersededRange struct {
	kind, ext string
	from, to  uint64
	rel       string
}

func supersededFiles(t *testing.T, dirs datadir.Dirs) []string {
	var files []supersededRange
	for _, rel := range supersededSnapshotFiles(t, dirs) {
		root := filepath.Join(dirs.Snap, filepath.Dir(rel))
		name := filepath.Base(rel)
		if strings.HasSuffix(name, ".torrent") {
			continue
		}
		parsed, _, ok := snaptype.ParseFileName(root, name)
		if !ok {
			continue
		}
		files = append(files, supersededRange{kind: parsed.TypeString, ext: filepath.Ext(name), from: parsed.From, to: parsed.To, rel: rel})
	}
	var hidden []string
	for i, f := range files {
		for j, o := range files {
			if i != j && f.kind == o.kind && f.ext == o.ext && o.from <= f.from && o.to >= f.to && (o.from < f.from || o.to > f.to) {
				hidden = append(hidden, f.rel)
				break
			}
		}
	}
	return hidden
}

func buildMixedSnapshotParts(t *testing.T, fx *execmoduletester.PBTAcceptanceChain, last uint64, mergedSteps uint64) {
	mergeLast := last
	if mergedSteps != 0 {
		mergeLast = mergedSteps - 1
	}
	buildPBTAcceptanceFilesAtWithMerge(t, fx, mergeLast, false)
	dirs := fx.Tester.Dirs
	hold := t.TempDir()
	for _, rel := range supersededSnapshotFiles(t, dirs) {
		dst := filepath.Join(hold, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
		require.NoError(t, os.Link(filepath.Join(dirs.Snap, rel), dst))
	}
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	settings.StepsInFrozenFile = 8
	if mergedSteps != 0 {
		settings.StepsInFrozenFile = mergedSteps
	}
	rawDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	require.NoError(t, agg.MergeLoop(t.Context()))
	agg.WaitForFiles()
	db.Close()
	agg.Close()
	rawDB.Close()
	restored := 0
	_ = filepath.WalkDir(hold, func(p string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(hold, p)
		dst := filepath.Join(dirs.Snap, rel)
		if _, statErr := os.Stat(dst); os.IsNotExist(statErr) {
			require.NoError(t, os.Link(p, dst))
			restored++
		}
		return nil
	})
	require.NotZero(t, restored)
	if mergedSteps != 0 {
		buildPBTAcceptanceFilesAtWithMerge(t, fx, last, false)
	}
}

func TestConvertPBTHandlesSupersededFiles(t *testing.T) { runSupersededConvertAttach(t, 0) }

func TestConvertPBTHandlesMixedSupersededFiles(t *testing.T) { runSupersededConvertAttach(t, 4) }

func TestConvertPBTPublishesVisibleFilesWithUnindexedHistoryMerge(t *testing.T) {
	selectPBTHexCommandSuite(t)
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })
	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	source.Tester.Close()
	lastTx := pbtAcceptanceLastTxNumRaw(t, source.Tester.Dirs.Chaindata)
	buildMixedSnapshotParts(t, source, lastTx, 4)
	var merged supersededRange
	for _, rel := range supersededSnapshotFiles(t, source.Tester.Dirs) {
		parsed, _, ok := snaptype.ParseFileName(filepath.Join(source.Tester.Dirs.Snap, filepath.Dir(rel)), filepath.Base(rel))
		if ok && strings.HasPrefix(rel, "history/") && parsed.TypeString == kv.ReceiptDomain.String() && filepath.Ext(rel) == ".v" && parsed.To-parsed.From > merged.to-merged.from {
			merged = supersededRange{kind: parsed.TypeString, from: parsed.From, to: parsed.To, rel: rel}
		}
	}
	require.NotEmpty(t, merged.rel)
	accessors, err := filepath.Glob(filepath.Join(source.Tester.Dirs.SnapAccessors, fmt.Sprintf("*-%s.%d-%d.vi", merged.kind, merged.from, merged.to)))
	require.NoError(t, err)
	require.NotEmpty(t, accessors)
	for _, accessor := range accessors {
		require.NoError(t, dir.RemoveFile(accessor))
	}
	source.Tester.Close()
	selectPBTCommandSuite(t)
	output := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New()))
	pubDirs := datadir.Open(output)
	pubHidden := supersededFiles(t, pubDirs)
	require.Empty(t, pubHidden)
	var hasHistory, hasAccessor bool
	for _, rel := range supersededSnapshotFiles(t, pubDirs) {
		if strings.HasPrefix(rel, "history/") && strings.Contains(filepath.Base(rel), "-receipt.") && filepath.Ext(rel) == ".v" {
			hasHistory = true
		}
		if strings.HasPrefix(rel, "accessor/") && strings.Contains(filepath.Base(rel), "-receipt.") && filepath.Ext(rel) == ".vi" {
			hasAccessor = true
		}
	}
	require.True(t, hasHistory, "visible history data must be published")
	require.True(t, hasAccessor, "visible history accessors must be published")
	for _, rel := range supersededSnapshotFiles(t, pubDirs) {
		if !strings.HasPrefix(rel, "history/") && !strings.HasPrefix(rel, "idx/") {
			continue
		}
		ext := filepath.Ext(rel)
		var wantExt string
		switch ext {
		case ".ef":
			wantExt = ".efi"
		case ".v":
			wantExt = ".vi"
		default:
			continue
		}
		parsed, _, ok := snaptype.ParseFileName(filepath.Join(pubDirs.Snap, filepath.Dir(rel)), filepath.Base(rel))
		require.True(t, ok)
		accessors, err := filepath.Glob(filepath.Join(pubDirs.SnapAccessors, fmt.Sprintf("*-%s.%d-%d%s", parsed.TypeString, parsed.From, parsed.To, wantExt)))
		require.NoError(t, err)
		require.NotEmpty(t, accessors, "data file %s must have its accessor", rel)
	}
}

func runSupersededConvertAttach(t *testing.T, mergedSteps uint64) {
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildMixedSnapshotParts(t, node, 7, mergedSteps)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	source.Tester.Close()
	lastTx := pbtAcceptanceLastTxNumRaw(t, source.Tester.Dirs.Chaindata)
	buildMixedSnapshotParts(t, source, lastTx, mergedSteps)

	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	pubDirs := datadir.Open(published)
	pubHidden := supersededFiles(t, pubDirs)
	require.Empty(t, pubHidden, "published set holds superseded files")

	publishedSettings, err := dbstate.ReadErigonDBSettings(pubDirs)
	require.NoError(t, err)
	conversionBlock, conversionTx, ok, err := publishedSettings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))
	require.Equal(t, readPBTFilesRoot(t, published), readPBTFilesRoot(t, node.Tester.Dirs.DataDir))

	dual, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, dual.Tester.InsertChain(dual.Chain))
	selectPBTCommandSuite(t)
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	agg := reopened.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	at := agg.BeginFilesRo()
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain, kv.CommitmentBinDomain} {
		for _, f := range at.Files(domain) {
			if strings.HasSuffix(f.Fullpath(), ".kv") {
				rel, _ := filepath.Rel(node.Tester.Dirs.Snap, f.Fullpath())
				pubInfo, statErr := os.Stat(filepath.Join(pubDirs.Snap, rel))
				nodeInfo, _ := os.Stat(f.Fullpath())
				if statErr != nil || !os.SameFile(pubInfo, nodeInfo) {
					parsed, _, _ := snaptype.ParseFileName(filepath.Dir(f.Fullpath()), filepath.Base(f.Fullpath()))
					if parsed.From*1 <= conversionTx {
						t.Errorf("reopened node uses its own %s file %s for a range at/below the conversion point", domain, rel)
					}
				}
			}
		}
	}
	at.Close()
	attachedRaw := reopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	dualRaw := dual.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	var attachedAtConversion, dualAtConversion []byte
	require.NoError(t, attachedRaw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		attachedAtConversion, err = rawdb.ReadShadowStateRoot(tx, node.Chain.Blocks[conversionBlock-1].Hash(), conversionBlock)
		return err
	}))
	require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		dualAtConversion, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[conversionBlock-1].Hash(), conversionBlock)
		return err
	}))
	require.Equal(t, dualAtConversion, attachedAtConversion)
	assertPBTAttachHistory(t, reopened, dual.Tester, conversionTx)
	require.NoError(t, agg.RemoveOverlapsAfterMerge(t.Context()))
	require.NoError(t, agg.RemoveOverlapsAfterMerge(t.Context()))
	reopened.Close()
}

func pbtAcceptanceLastTxNumRaw(t *testing.T, chaindataPath string) uint64 {
	rawDB := dbCfg(dbcfg.ChainDB, chaindataPath).MustOpen()
	defer rawDB.Close()
	var last uint64
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		var err error
		_, last, err = rawdbv3.TxNums.Last(tx)
		return err
	}))
	return last
}
