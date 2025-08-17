// Copyright 2024 The Erigon Authors
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
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	p "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/snaptype"
)

func TestChangeInfoHashOfSameFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	cfg, err := downloadercfg.New(context.Background(), dirs, "", log.LvlInfo, 0, 0, nil, "testnet", false, downloadercfg.NewCfgOpts{})
	require.NoError(err)
	d, err := New(context.Background(), cfg, log.New(), log.LvlInfo)
	require.NoError(err)
	defer d.Close()
	err = d.RequestSnapshot(snaptype.Hex2InfoHash("aa"), "a.seg")
	require.NoError(err)
	tt, ok := d.torrentClient.Torrent(snaptype.Hex2InfoHash("aa"))
	require.True(ok)
	require.Equal("a.seg", tt.Name())

	// adding same file twice is ok
	err = d.RequestSnapshot(snaptype.Hex2InfoHash("aa"), "a.seg")
	require.NoError(err)

	// adding same file with another infoHash - is ok, must be skipped
	// use-cases:
	//	- release of re-compressed version of same file,
	//	- ErigonV1.24 produced file X, then ErigonV1.25 released with new compression algorithm and produced X with anouther infoHash.
	//		ErigonV1.24 node must keep using existing file instead of downloading new one.
	err = d.RequestSnapshot(snaptype.Hex2InfoHash("bb"), "a.seg")
	// I'm not sure if this is a good idea.
	//require.Error(err)
	_ = err
	tt, ok = d.torrentClient.Torrent(snaptype.Hex2InfoHash("aa"))
	require.True(ok)
	require.Equal("a.seg", tt.Name())
}

func TestNoEscape(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	ctx := context.Background()

	tf := NewAtomicTorrentFS(dirs.Snap)
	// allow adding files only if they are inside snapshots dir
	_, err := BuildTorrentIfNeed(ctx, "a.seg", dirs.Snap, tf)
	require.NoError(err)
	_, err = BuildTorrentIfNeed(ctx, "b/a.seg", dirs.Snap, tf)
	require.NoError(err)
	_, err = BuildTorrentIfNeed(ctx, filepath.Join(dirs.Snap, "a.seg"), dirs.Snap, tf)
	require.NoError(err)
	_, err = BuildTorrentIfNeed(ctx, filepath.Join(dirs.Snap, "b", "a.seg"), dirs.Snap, tf)
	require.NoError(err)

	// reject escaping snapshots dir
	_, err = BuildTorrentIfNeed(ctx, filepath.Join(dirs.Chaindata, "b", "a.seg"), dirs.Snap, tf)
	require.Error(err)
	_, err = BuildTorrentIfNeed(ctx, "./../a.seg", dirs.Snap, tf)
	require.Error(err)
}

func TestVerifyData(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	cfg, err := downloadercfg.New(context.Background(), dirs, "", log.LvlInfo, 0, 0, nil, "testnet", false, downloadercfg.NewCfgOpts{})
	require.NoError(err)
	d, err := New(context.Background(), cfg, log.New(), log.LvlInfo)
	require.NoError(err)
	defer d.Close()

	err = d.VerifyData(d.ctx, nil)
	require.NoError(err)
}

func TestAddDel(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	ctx := context.Background()

	cfg, err := downloadercfg.New(context.Background(), dirs, "", log.LvlInfo, 0, 0, nil, "testnet", false, downloadercfg.NewCfgOpts{})
	require.NoError(err)
	d, err := New(context.Background(), cfg, log.New(), log.LvlInfo)
	require.NoError(err)
	defer d.Close()

	f1Abs := filepath.Join(dirs.Snap, "a.seg")      // block file
	f2Abs := filepath.Join(dirs.SnapDomain, "a.kv") // state file
	_, _ = os.Create(f1Abs)
	_, _ = os.Create(f2Abs)

	srever, _ := NewGrpcServer(d)
	// Add: epxect relative paths
	_, err = srever.Add(ctx, &p.AddRequest{Items: []*p.AddItem{{Path: f1Abs}}})
	require.Error(err)
	_, err = srever.Add(ctx, &p.AddRequest{Items: []*p.AddItem{{Path: f2Abs}}})
	require.Error(err)
	require.Equal(0, len(d.torrentClient.Torrents()))

	f1, _ := filepath.Rel(dirs.Snap, f1Abs)
	f2, _ := filepath.Rel(dirs.Snap, f2Abs)
	_, err = srever.Add(ctx, &p.AddRequest{Items: []*p.AddItem{{Path: f1}}})
	require.NoError(err)
	_, err = srever.Add(ctx, &p.AddRequest{Items: []*p.AddItem{{Path: f2}}})
	require.NoError(err)
	require.Equal(2, len(d.torrentClient.Torrents()))

	// add idempotency
	_, err = srever.Add(ctx, &p.AddRequest{Items: []*p.AddItem{{Path: f1}}})
	require.NoError(err)
	_, err = srever.Add(ctx, &p.AddRequest{Items: []*p.AddItem{{Path: f2}}})
	require.NoError(err)
	require.Equal(2, len(d.torrentClient.Torrents()))

	// Del: epxect relative paths
	_, err = srever.Delete(ctx, &p.DeleteRequest{Paths: []string{f1Abs}})
	require.Error(err)
	_, err = srever.Delete(ctx, &p.DeleteRequest{Paths: []string{f2Abs}})
	require.Error(err)
	require.Equal(2, len(d.torrentClient.Torrents()))

	// Del: idempotency
	_, err = srever.Delete(ctx, &p.DeleteRequest{Paths: []string{f1}})
	require.NoError(err)
	require.Equal(1, len(d.torrentClient.Torrents()))
	_, err = srever.Delete(ctx, &p.DeleteRequest{Paths: []string{f1}})
	require.NoError(err)
	require.Equal(1, len(d.torrentClient.Torrents()))

	_, err = srever.Delete(ctx, &p.DeleteRequest{Paths: []string{f2}})
	require.NoError(err)
	require.Equal(0, len(d.torrentClient.Torrents()))
	_, err = srever.Delete(ctx, &p.DeleteRequest{Paths: []string{f2}})
	require.NoError(err)
	require.Equal(0, len(d.torrentClient.Torrents()))

	// Batch
	_, err = srever.Add(ctx, &p.AddRequest{Items: []*p.AddItem{{Path: f1}, {Path: f2}}})
	require.NoError(err)
	require.Equal(2, len(d.torrentClient.Torrents()))
	_, err = srever.Delete(ctx, &p.DeleteRequest{Paths: []string{f1, f2}})
	require.NoError(err)
	require.Equal(0, len(d.torrentClient.Torrents()))

}
