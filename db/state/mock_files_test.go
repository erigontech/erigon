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

package state

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/datastruct/posidx"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

type testFileRange struct {
	fromStep, toStep uint64
}

func populateFiles2(t *testing.T, dirs datadir.Dirs, schema SnapNameSchema, stepSize uint64, ranges []testFileRange) {
	t.Helper()
	var allFiles []string //nolint:prealloc
	v := version.V1_0
	acc := schema.AccessorList()

	for _, r := range ranges {
		from, to := RootNum(r.fromStep*stepSize), RootNum(r.toStep*stepSize)
		file, _ := schema.DataFile(v, from, to)
		allFiles = append(allFiles, file)
		if acc.Has(statecfg.AccessorBTree) {
			file, _ := schema.BtIdxFile(v, from, to)
			allFiles = append(allFiles, file)
		}
		if acc.Has(statecfg.AccessorExistence) {
			file, _ := schema.ExistenceFile(v, from, to)
			allFiles = append(allFiles, file)
		}
		if acc.Has(statecfg.AccessorHashMap) {
			// zero version: let the schema name it with its own accessor version
			file, _ := schema.AccessorIdxFile(statecfg.Version{}, from, to, 0)
			allFiles = append(allFiles, file)
		}
	}

	populateFiles(t, dirs, allFiles)
}

func populateFiles(t *testing.T, dirs datadir.Dirs, allFiles []string) {
	t.Helper()

	writeSegFile := func(filename string, words ...[]byte) {
		comp, err := seg.NewCompressor(t.Context(), t.Name(), filename, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
		require.NoError(t, err)
		defer comp.Close()
		comp.DisableFsync()
		for _, w := range words {
			require.NoError(t, comp.AddUncompressedWord(w))
		}
		require.NoError(t, comp.Compress())
	}

	fileGen := func(filename string) {
		switch {
		case strings.HasSuffix(filename, ".ef"):
			// Inverted index .ef stores 2 words per entry: key and multiencseq txNum list.
			var seq multiencseq.SequenceBuilder
			seq.Reset(0, 1, 0)
			seq.AddOffset(0)
			seq.Build()
			writeSegFile(filename, []byte("key"), seq.AppendBytes(nil))
			return
		case strings.HasSuffix(filename, ".kv"):
			// Domain .kv stores 2 words per entry: key and value.
			writeSegFile(filename, []byte("key"), []byte("value"))
			return
		case strings.HasSuffix(filename, ".v"):
			// Unpaged history .v (seg.DefaultCfg) stores 1 value word per txNum in .ef.
			writeSegFile(filename, []byte("value"))
			return
		}

		if strings.HasSuffix(filename, ".bt") {
			sampleFile := filename + ".sample"
			seg2, err := seg.NewCompressor(t.Context(), t.Name(), sampleFile, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
			require.NoError(t, err)
			defer seg2.Close()
			seg2.DisableFsync()
			if err = seg2.AddWord([]byte("key")); err != nil {
				t.Fatal(err)
			}
			if err = seg2.AddWord([]byte("value")); err != nil {
				t.Fatal(err)
			}
			require.NoError(t, seg2.Compress())
			seg3, err := seg.NewDecompressor(sampleFile)
			require.NoError(t, err)
			defer seg3.Close()
			defer dir.RemoveFile(sampleFile) //nolint:errcheck

			r := seg.NewReader(seg3.MakeGetter(), seg.CompressNone)
			kveiFile := strings.TrimSuffix(filename, ".bt") + ".kvei"
			if err := btindex.BuildBtreeIndexWithDecompressor(filename, kveiFile, r, background.NewProgressSet(), dirs.Tmp, uint32(1), log.New(), true, statecfg.AccessorBTree|statecfg.AccessorExistence); err != nil {
				t.Fatal(err)
			}
			seg3.Close()

			return
		}

		if strings.HasSuffix(filename, ".kvei") {
			filter, err := existence.NewFilter(0, filename)
			require.NoError(t, err)
			defer filter.Close()
			filter.DisableFsync()
			require.NoError(t, filter.Build())

			return
		}

		if strings.HasSuffix(filename, ".vi") {
			w, err := posidx.NewWriter(filename, t.TempDir(), 1, 1, 1, 1)
			require.NoError(t, err)
			defer w.Close()
			w.NoFsync()
			w.AddRun(1)
			w.AddPage(0)
			require.NoError(t, w.Build())

			return
		}

		if strings.HasSuffix(filename, ".kvi") || strings.HasSuffix(filename, ".efi") {
			salt := uint32(1)
			rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
				KeyCount:   1,
				BucketSize: 10,
				Salt:       &salt,
				TmpDir:     dirs.Tmp,
				IndexFile:  filename,
				LeafSize:   8,
			}, log.New())
			if err != nil {
				t.Fatal(err)
			}
			defer rs.Close()
			rs.DisableFsync()
			if err = rs.AddKey([]byte("first_key"), 0); err != nil {
				t.Error(err)
			}
			if err = rs.Build(t.Context()); err != nil {
				t.Errorf("test is expected to fail, too few keys added")
			}
			rs.Close()

			return
		}
	}

	for _, f := range allFiles {
		fileGen(f)
	}
}
