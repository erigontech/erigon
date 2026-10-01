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
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snaptype"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
)

func t35bSnapFiles(t *testing.T, dirs datadir.Dirs) []string {
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

type t35bRange struct {
	kind, ext string
	from, to  uint64
	rel       string
}

func t35bSuperseded(t *testing.T, dirs datadir.Dirs) []string {
	var files []t35bRange
	for _, rel := range t35bSnapFiles(t, dirs) {
		root := filepath.Join(dirs.Snap, filepath.Dir(rel))
		name := filepath.Base(rel)
		if strings.HasSuffix(name, ".torrent") {
			continue
		}
		parsed, _, ok := snaptype.ParseFileName(root, name)
		if !ok {
			continue
		}
		files = append(files, t35bRange{kind: parsed.TypeString, ext: filepath.Ext(name), from: parsed.From, to: parsed.To, rel: rel})
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

func t35bBuildWithParts(t *testing.T, fx *execmoduletester.PBTAcceptanceChain, last uint64) {
	t35bBuildMixed(t, fx, last, 0)
}

func t35bBuildMixed(t *testing.T, fx *execmoduletester.PBTAcceptanceChain, last uint64, mergedSteps uint64) {
	mergeLast := last
	if mergedSteps != 0 {
		mergeLast = mergedSteps - 1
	}
	buildPBTAcceptanceFilesAtWithMerge(t, fx, mergeLast, false)
	dirs := fx.Tester.Dirs
	hold := t.TempDir()
	for _, rel := range t35bSnapFiles(t, dirs) {
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
	t.Logf("stepsInFrozen=%d step=%d", agg.StepsInFrozenFile(), agg.StepSize())
	require.NoError(t, agg.MergeLoop(t.Context()))
	agg.WaitForFiles()
	t.Logf("after merge domain: %v", t35bFilter(t35bSnapFiles(t, dirs), "domain/"))
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
	t.Logf("%s: restored %d superseded parts; superseded now: %d", dirs.DataDir, restored, len(t35bSuperseded(t, dirs)))
	require.NotZero(t, restored)
	if mergedSteps != 0 {
		buildPBTAcceptanceFilesAtWithMerge(t, fx, last, false)
		t.Logf("mixed domain files: %v", t35bFilter(t35bSnapFiles(t, dirs), "domain/"))
	}
}

func TestT35bSupersededConvertAttach(t *testing.T) { t35bRunSuperseded(t, 0) }

func TestT35bSupersededMixedConvertAttach(t *testing.T) { t35bRunSuperseded(t, 4) }

func t35bRunSuperseded(t *testing.T, mergedSteps uint64) {
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	t35bBuildMixed(t, node, 7, mergedSteps)
	node.Tester.Close()
	nodeHidden := t35bSuperseded(t, node.Tester.Dirs)
	t.Logf("node superseded before attach: %v", nodeHidden)

	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	source.Tester.Close()
	lastTx := pbtAcceptanceLastTxNumRaw(t, source.Tester.Dirs.Chaindata)
	t.Logf("source lastTx=%d", lastTx)
	t35bBuildMixed(t, source, lastTx, mergedSteps)
	t.Logf("source files: %v", t35bSnapFiles(t, source.Tester.Dirs))
	t.Logf("source superseded: %v", t35bSuperseded(t, source.Tester.Dirs))

	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	pubDirs := datadir.Open(published)
	t.Logf("published files: %v", t35bSnapFiles(t, pubDirs))
	pubHidden := t35bSuperseded(t, pubDirs)
	t.Logf("published superseded: %v", pubHidden)
	require.Empty(t, pubHidden, "published set holds superseded files")

	publishedSettings, err := dbstate.ReadErigonDBSettings(pubDirs)
	require.NoError(t, err)
	conversionBlock, conversionTx, ok, err := publishedSettings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))
	t.Logf("node files after attach: %v", t35bSnapFiles(t, node.Tester.Dirs))
	t.Logf("node superseded after attach: %v", t35bSuperseded(t, node.Tester.Dirs))
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
		var names []string
		for _, f := range at.Files(domain) {
			names = append(names, filepath.Base(f.Fullpath()))
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
		t.Logf("reopened visible %s: %v", domain, names)
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
	t.Logf("node superseded after erigon cleanup x2: %v", t35bSuperseded(t, node.Tester.Dirs))
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

func t35bFilter(in []string, prefix string) []string {
	var out []string
	for _, s := range in {
		if strings.HasPrefix(s, prefix) {
			out = append(out, s)
		}
	}
	return out
}
