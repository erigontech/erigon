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
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
	"github.com/stretchr/testify/require"
)

var updatePBTConvertGolden = flag.Bool("update", false, "update PBT convert golden data")

const pbtConvertGoldenPath = "testdata/pbt-convert-golden/golden.json"

type pbtGoldenCorpus struct {
	name      string
	newSource func(*testing.T, string) pbtConversionSource
	binOnly   bool
}

type pbtGolden struct {
	Version int             `json:"version"`
	Cases   []pbtGoldenCase `json:"cases"`
}

type pbtGoldenCase struct {
	Name  string          `json:"name"`
	Root  string          `json:"root"`
	Files []pbtGoldenFile `json:"files"`
}

type pbtGoldenFile struct {
	Name string         `json:"name"`
	Rows []pbtGoldenRow `json:"rows"`
}

type pbtGoldenRow struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func TestConvertPBTOutputGolden(t *testing.T) {
	var got []pbtGoldenCase
	for _, corpus := range pbtGoldenCorpora() {
		t.Run(corpus.name, func(t *testing.T) {
			for _, hash := range []string{commitment.PBinHashKeccak, commitment.PBinHashBlake3} {
				source := corpus.newSource(t, hash)
				for _, keepHex := range []bool{true, false} {
					if !keepHex && !corpus.binOnly {
						continue
					}
					mode := "bin-only"
					if keepHex {
						mode = "keep-hex"
					}
					caseName := fmt.Sprintf("%s/%s/%s", corpus.name, mode, hash)
					t.Run(mode+"/"+hash, func(t *testing.T) {
						configurePBTGoldenSuite(t, hash, keepHex)
						output := filepath.Join(t.TempDir(), "output")
						require.NoError(t, convertPBTWithOptions(t.Context(), source.DataDir, output, keepHex, "", log.New(), pbtConvertHooks{}))
						got = append(got, readPBTGoldenCase(t, caseName, output, hash))
					})
				}
			}
		})
	}
	if *updatePBTConvertGolden {
		require.NoError(t, writePBTGolden(t, got))
		return
	}
	want := readPBTGolden(t)
	wantByName := make(map[string]pbtGoldenCase, len(want.Cases))
	for _, goldenCase := range want.Cases {
		wantByName[goldenCase.Name] = goldenCase
	}
	for _, goldenCase := range got {
		wantCase, ok := wantByName[goldenCase.Name]
		require.True(t, ok, "missing golden case %s", goldenCase.Name)
		comparePBTGoldenCase(t, wantCase, goldenCase)
	}
	require.Len(t, want.Cases, len(got), "golden case count")
}

func pbtGoldenCorpora() []pbtGoldenCorpus {
	return []pbtGoldenCorpus{
		{name: "single-account", newSource: func(t *testing.T, _ string) pbtConversionSource {
			configurePBTGoldenSuite(t, commitment.PBinHashBlake3, true)
			source, _ := newPBTConversionSource(t)
			return source
		}},
		{name: "multiple-ranges", newSource: func(t *testing.T, _ string) pbtConversionSource {
			configurePBTGoldenSuite(t, commitment.PBinHashBlake3, true)
			source, _ := newPBTConversionSourceAt(t, 15)
			return source
		}},
		{name: "past-frontier", newSource: func(t *testing.T, _ string) pbtConversionSource {
			configurePBTGoldenSuite(t, commitment.PBinHashBlake3, true)
			source, _ := newPBTConversionSourceAtWithFutureLeaf(t, 7, 8)
			return source
		}},
		{name: "large-storage-bucket", newSource: func(t *testing.T, _ string) pbtConversionSource {
			configurePBTGoldenSuite(t, commitment.PBinHashBlake3, true)
			source, _ := newPBTConversionSourceAtWithRows(t, 7, 0, 256, nil)
			return source
		}},
		{name: "empty-zones", newSource: func(t *testing.T, _ string) pbtConversionSource {
			configurePBTGoldenSuite(t, commitment.PBinHashBlake3, true)
			return newPBTEmptyConversionSource(t)
		}},
		{name: "shared-code-and-storage", newSource: newPBTGoldenAcceptanceSource, binOnly: true},
	}
}

func newPBTGoldenAcceptanceSource(t *testing.T, hash string) pbtConversionSource {
	configurePBTGoldenSuite(t, hash, true)
	sharedCode := make([]byte, eip8297.StemSubtreeWidth*eip8297.ChunkDataLen+1)
	for i := range sharedCode {
		sharedCode[i] = byte(i + 1)
	}
	source, _ := newPBTConversionSourceAtWithRows(t, 15, 0, 0, sharedCode)
	return source
}

func configurePBTGoldenSuite(t *testing.T, hash string, keepHex bool) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = keepHex
	statecfg.ExperimentalCommitmentV3 = keepHex
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = hash
	if keepHex {
		statecfg.InitSchemas()
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	}
	require.NoError(t, commitment.SetPBinHashSuite(hash))
}

func readPBTGoldenCase(t *testing.T, name, output, hash string) pbtGoldenCase {
	t.Helper()
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	domain := kv.CommitmentBinDomain
	if settings.TrieVariantName() == dbstate.TrieVariantBin {
		domain = kv.CommitmentDomain
	}
	dirs := datadir.Open(output)
	var files []pbtGoldenFile
	for _, fileName := range domainFileNames(t, dirs.SnapDomain) {
		parsed, _, ok := snaptype.ParseFileName(dirs.SnapDomain, fileName)
		if !ok || parsed.TypeString != domain.String() || filepath.Ext(fileName) != ".kv" {
			continue
		}
		files = append(files, readPBTGoldenFile(t, filepath.Join(dirs.SnapDomain, fileName), fileName))
	}
	require.NotEmpty(t, files, "%s: no binary data files", name)
	return pbtGoldenCase{Name: name, Root: readPBTGoldenRoot(t, files, hash).Hex(), Files: files}
}

func readPBTGoldenRoot(t *testing.T, files []pbtGoldenFile, hash string) common.Hash {
	t.Helper()
	require.Equal(t, hash, eip8297.HashSuiteName())
	entriesByKey := make(map[string]eip8297.Entry)
	for _, file := range files {
		for _, row := range file.Rows {
			key, err := hex.DecodeString(row.Key)
			require.NoError(t, err)
			value, err := hex.DecodeString(row.Value)
			require.NoError(t, err)
			if commitment.IsCommitmentStateKey(key) {
				continue
			}
			record, err := pbt.DecodeRecord(key, value)
			require.NoError(t, err, "%s: decode %s", file.Name, row.Key)
			for i := range record.Cells {
				cell := &record.Cells[i]
				if cell.Kind == pbt.LeafCell {
					entriesByKey[string(cell.Key)] = eip8297.Entry{Key: bytes.Clone(cell.Key), Value: bytes.Clone(cell.Value[:])}
				}
			}
		}
	}
	entries := make([]eip8297.Entry, 0, len(entriesByKey))
	for _, entry := range entriesByKey {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	builder, err := eip8297.NewStreamRootBuilder(eip8297.HashBytes)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NoError(t, builder.Add(entry.Key, entry.Value))
	}
	root, err := builder.RootHash()
	require.NoError(t, err)
	return root
}

func readPBTGoldenFile(t *testing.T, path, name string) pbtGoldenFile {
	t.Helper()
	decompressor, err := seg.NewDecompressor(path)
	require.NoError(t, err, name)
	reader := seg.NewReader(decompressor.MakeGetter(), seg.CompressNone)
	var rows []pbtGoldenRow
	for reader.HasNext() {
		key, _ := reader.Next(nil)
		require.True(t, reader.HasNext(), "%s: key without value", name)
		value, _ := reader.Next(nil)
		rows = append(rows, pbtGoldenRow{Key: hex.EncodeToString(key), Value: hex.EncodeToString(value)})
	}
	decompressor.Close()
	return pbtGoldenFile{Name: name, Rows: rows}
}

func comparePBTGoldenCase(t *testing.T, want, got pbtGoldenCase) {
	t.Helper()
	require.Equal(t, want.Root, got.Root, "%s: root", got.Name)
	require.Equal(t, goldenFileNames(want.Files), goldenFileNames(got.Files), "%s: bin file placement", got.Name)
	for i := range want.Files {
		wantFile, gotFile := want.Files[i], got.Files[i]
		require.Equal(t, wantFile.Name, gotFile.Name, "%s: bin file placement", got.Name)
		require.Equal(t, len(wantFile.Rows), len(gotFile.Rows), "%s: row count in %s", got.Name, gotFile.Name)
		for row := range wantFile.Rows {
			if wantFile.Rows[row].Key != gotFile.Rows[row].Key {
				require.Equal(t, wantFile.Rows[row].Key, gotFile.Rows[row].Key, "%s: first differing key in %s at row %d", got.Name, gotFile.Name, row)
			}
			require.Equal(t, wantFile.Rows[row].Value, gotFile.Rows[row].Value, "%s: differing value for key %s in %s", got.Name, gotFile.Rows[row].Key, gotFile.Name)
		}
	}
}

func goldenFileNames(files []pbtGoldenFile) []string {
	names := make([]string, 0, len(files))
	for _, file := range files {
		names = append(names, file.Name)
	}
	sort.Strings(names)
	return names
}

func readPBTGolden(t *testing.T) pbtGolden {
	t.Helper()
	data, err := os.ReadFile(pbtConvertGoldenPath)
	require.NoError(t, err, "read %s with -update to create it", pbtConvertGoldenPath)
	var golden pbtGolden
	require.NoError(t, json.Unmarshal(data, &golden))
	require.Equal(t, 1, golden.Version)
	return golden
}

func writePBTGolden(t *testing.T, cases []pbtGoldenCase) error {
	t.Helper()
	sort.Slice(cases, func(i, j int) bool { return cases[i].Name < cases[j].Name })
	data, err := json.MarshalIndent(pbtGolden{Version: 1, Cases: cases}, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(pbtConvertGoldenPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(pbtConvertGoldenPath, data, 0o644)
}

func TestPBTGoldenLargeStorageIsVisibleToConverter(t *testing.T) {
	configurePBTGoldenSuite(t, commitment.PBinHashBlake3, true)
	source, _ := newPBTConversionSourceAtWithRows(t, 7, 0, 256, nil)
	settings, err := dbstate.ReadErigonDBSettings(source.Dirs)
	require.NoError(t, err)
	agg, err := dbstate.NewPBTStateAggregator(source.Dirs, settings, log.New()).Open(t.Context())
	require.NoError(t, err)
	defer agg.Close()
	require.NoError(t, agg.OpenFolder(nil))
	at := agg.BeginFilesRo()
	defer at.Close()
	storageLeaves := 0
	require.NoError(t, dbstate.ForEachPBinLeaf(at, nil, true, func(leaf dbstate.PBinLeaf) error {
		if leaf.Key[0] == eip8297.StorageZone {
			storageLeaves++
		}
		return nil
	}))
	require.Equal(t, 256, storageLeaves, "large-storage golden corpus: converter must see the added nonzero slots in files")
}

func TestPBTGoldenSharedCodeCoversDifferentStampRanges(t *testing.T) {
	source := newPBTGoldenAcceptanceSource(t, commitment.PBinHashBlake3)
	settings, err := dbstate.ReadErigonDBSettings(source.Dirs)
	require.NoError(t, err)
	agg, err := dbstate.NewPBTStateAggregator(source.Dirs, settings, log.New()).Open(t.Context())
	require.NoError(t, err)
	defer agg.Close()
	require.NoError(t, agg.OpenFolder(nil))
	at := agg.BeginFilesRo()
	defer at.Close()
	iter, err := at.DbgDomain(kv.CodeDomain).DebugRangeLatestFromFiles(nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer iter.Close()
	stampsByCode := make(map[string][]uint64)
	for iter.HasNext() {
		_, code, nextErr := iter.Next()
		require.NoError(t, nextErr)
		if len(code) != 0 {
			stampsByCode[string(code)] = append(stampsByCode[string(code)], iter.Stamp())
		}
	}
	var duplicateStamps []uint64
	maxRanges := 0
	for _, stamps := range stampsByCode {
		if len(stamps) < 2 {
			continue
		}
		duplicateStamps = append(duplicateStamps, stamps...)
		ranges := make(map[uint64]bool)
		for _, stamp := range stamps {
			ranges[stamp/settings.StepSize] = true
		}
		maxRanges = max(maxRanges, len(ranges))
	}
	require.Greater(t, maxRanges, 1, "shared-code golden corpus must exercise max-stamp dedup across ranges; duplicate stamps=%v", duplicateStamps)
}
