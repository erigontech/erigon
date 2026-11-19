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

package snapshotsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/node/ethconfig"
)

func ptcWindowEpochTestTypes(cfg *clparams.BeaconChainConfig, rows map[uint64][]byte) SnapshotTypes {
	getter := func(slot uint64) ([]byte, []byte, error) {
		return nil, rows[slot], nil
	}
	return SnapshotTypes{
		KeyValueGetters: map[string]KeyValueGetter{kv.PtcWindowEpochTable: requirePtcWindowEpochRows(cfg, getter)},
		Compression:     map[string]bool{},
	}
}

func dumpPtcWindowEpochTestRange(t *testing.T, cfg *clparams.BeaconChainConfig, rows map[uint64][]byte, to uint64) []string {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.SnapCaplin, 0o755))
	require.NoError(t, os.MkdirAll(dirs.Tmp, 0o755))
	snapshots := NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: "test"}, dirs, ptcWindowEpochTestTypes(cfg, rows), log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.DumpCaplinState(context.Background(), to, to, 0, dirs, 1, log.LvlDebug, log.New()))
	files, err := filepath.Glob(filepath.Join(dirs.SnapCaplin, "*.seg"))
	require.NoError(t, err)
	return files
}

func readSegmentWords(t *testing.T, path string) [][]byte {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	t.Cleanup(d.Close)
	getter := d.MakeGetter()
	var words [][]byte
	for getter.HasNext() {
		word, _ := getter.Next(nil)
		words = append(words, append([]byte(nil), word...))
	}
	return words
}

func TestDumpPtcWindowEpochSkipsMissingPostForkRow(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.SlotsPerEpoch = 4
	cfg.GloasForkEpoch = 1
	files := dumpPtcWindowEpochTestRange(t, &cfg, nil, 8)
	require.Empty(t, files)
}

func TestDumpPtcWindowEpochWritesExactSparseWords(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.SlotsPerEpoch = 4
	cfg.GloasForkEpoch = 1
	want := []byte("compressed-window")
	files := dumpPtcWindowEpochTestRange(t, &cfg, map[uint64][]byte{4: want}, 8)
	require.Len(t, files, 1)
	words := readSegmentWords(t, files[0])
	require.Len(t, words, 8)
	for slot, word := range words {
		if slot == 4 {
			require.Equal(t, want, word)
		} else {
			require.Empty(t, word)
		}
	}
}

func TestDumpPtcWindowEpochAllowsEmptyPreForkRange(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.SlotsPerEpoch = 4
	cfg.GloasForkEpoch = 1
	files := dumpPtcWindowEpochTestRange(t, &cfg, map[uint64][]byte{0: []byte("ignored"), 1: []byte("also-ignored")}, 4)
	require.Len(t, files, 1)
	require.Equal(t, [][]byte{[]byte("ignored"), []byte("also-ignored"), nil, nil}, readSegmentWords(t, files[0]))
}
