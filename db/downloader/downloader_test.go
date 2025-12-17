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
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

func TestConcurrentDownload(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("copied from TestChangeInfoHashOfSameFile")
	}

	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	cfg, err := downloadercfg.New(context.Background(), dirs, "", log.LvlInfo, 0, 0, nil, "testnet", false, downloadercfg.NewCfgOpts{})
	require.NoError(err)
	d, err := New(context.Background(), cfg, log.New())
	require.NoError(err)
	defer d.Close()
	const conc = 2
	waits := make(chan func(ctx context.Context) error, conc)
	g, ctx := errgroup.WithContext(t.Context())
	for range conc {
		g.Go(func() error {
			wait, err := d.testStartSingleDownload(ctx, snaptype.Hex2InfoHash("aa"), "a.seg")
			if err != nil {
				return err
			}
			waits <- wait
			return nil
		})
	}
	require.NoError(g.Wait())
	close(waits)
	d.Close()
	for w := range waits {
		// Make sure we don't get stuck. The torrents shouldn't exist, and the Downloader is closed.
		w(t.Context())
	}
}

func TestChangeInfoHashOfSameFile(t *testing.T) {
	ctx := t.Context()
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	cfg, err := downloadercfg.New(ctx, dirs, "", log.LvlInfo, 0, 0, nil, "testnet", false, downloadercfg.NewCfgOpts{})
	require.NoError(err)
	d, err := New(context.Background(), cfg, log.New())
	require.NoError(err)
	defer d.Close()
	err = d.testStartSingleDownloadNoWait(ctx, snaptype.Hex2InfoHash("aa"), "a.seg")
	require.NoError(err)
	tt, ok := d.torrentClient.Torrent(snaptype.Hex2InfoHash("aa"))
	require.True(ok)
	require.Equal("a.seg", tt.Name())

	// adding same file twice is ok
	err = d.testStartSingleDownloadNoWait(ctx, snaptype.Hex2InfoHash("aa"), "a.seg")
	require.NoError(err)

	// adding same file with another infoHash - is ok, must be skipped
	// use-cases:
	//	- release of re-compressed version of same file,
	//	- ErigonV1.24 produced file X, then ErigonV1.25 released with new compression algorithm and produced X with anouther infoHash.
	//		ErigonV1.24 node must keep using existing file instead of downloading new one.
	err = d.testStartSingleDownloadNoWait(ctx, snaptype.Hex2InfoHash("bb"), "a.seg")
	// I'm not sure if this is a good idea.
	//require.Error(err)
	_ = err
	tt, ok = d.torrentClient.Torrent(snaptype.Hex2InfoHash("aa"))
	require.True(ok)
	require.Equal("a.seg", tt.Name())
}

func TestNoEscape(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	ctx := context.Background()

	tf := NewAtomicTorrentFS(dirs.Snap)
	// allow adding files only if they are inside snapshots dir
	_, err := BuildTorrentIfNeed(ctx, "a.seg", dirs.Snap, tf)
	assert.ErrorIs(t, err, fs.ErrNotExist)
	_, err = BuildTorrentIfNeed(ctx, "b/a.seg", dirs.Snap, tf)
	assert.ErrorIs(t, err, fs.ErrNotExist)
	_, err = BuildTorrentIfNeed(ctx, filepath.Join(dirs.Snap, "a.seg"), dirs.Snap, tf)
	assert.ErrorIs(t, err, fs.ErrNotExist)
	_, err = BuildTorrentIfNeed(ctx, filepath.Join(dirs.Snap, "b", "a.seg"), dirs.Snap, tf)
	assert.ErrorIs(t, err, fs.ErrNotExist)

	// reject escaping snapshots dir
	_, err = BuildTorrentIfNeed(ctx, filepath.Join(dirs.Chaindata, "b", "a.seg"), dirs.Snap, tf)
	assert.NotErrorIs(t, err, fs.ErrNotExist)
	_, err = BuildTorrentIfNeed(ctx, "./../a.seg", dirs.Snap, tf)
	assert.NotErrorIs(t, err, fs.ErrNotExist)
}

func TestVerifyData(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	cfg, err := downloadercfg.New(context.Background(), dirs, "", log.LvlInfo, 0, 0, nil, "testnet", false, downloadercfg.NewCfgOpts{})
	require.NoError(err)
	d, err := New(context.Background(), cfg, log.New())
	require.NoError(err)
	defer d.Close()

	err = d.VerifyData(d.ctx, nil, false)
	require.NoError(err)
}

func TestAddDel(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	ctx := t.Context()

	cfg, err := downloadercfg.New(ctx, dirs, "", log.LvlInfo, 0, 0, nil, "testnet", false, downloadercfg.NewCfgOpts{})
	require.NoError(err)
	d, err := New(ctx, cfg, log.New())
	require.NoError(err)
	defer d.Close()

	f1Abs := filepath.Join(dirs.Snap, "a.seg")      // block file
	f2Abs := filepath.Join(dirs.SnapDomain, "a.kv") // state file
	_, _ = os.Create(f1Abs)
	require.NoError(os.WriteFile(f2Abs, []byte("a.kv"), 0o666))

	server, _ := NewGrpcServer(d)
	// Add: expect relative paths
	_, err = server.Seed(ctx, &downloaderproto.SeedRequest{Paths: []string{f1Abs}})
	require.Error(err)
	_, err = server.Seed(ctx, &downloaderproto.SeedRequest{Paths: []string{f2Abs}})
	require.Error(err)
	require.Equal(0, len(d.torrentClient.Torrents()))

	f1, _ := filepath.Rel(dirs.Snap, f1Abs)
	f2, _ := filepath.Rel(dirs.Snap, f2Abs)
	_, err = server.Seed(ctx, &downloaderproto.SeedRequest{Paths: []string{f1}})
	require.NoError(err)
	_, err = server.Seed(ctx, &downloaderproto.SeedRequest{Paths: []string{f2}})
	require.NoError(err)
	require.Equal(2, len(d.torrentClient.Torrents()))

	// add idempotency
	_, err = server.Seed(ctx, &downloaderproto.SeedRequest{Paths: []string{f1}})
	require.NoError(err)
	_, err = server.Seed(ctx, &downloaderproto.SeedRequest{Paths: []string{f2}})
	require.NoError(err)
	require.Equal(2, len(d.torrentClient.Torrents()))

	// Del: expect relative paths
	_, err = server.Delete(ctx, &downloaderproto.DeleteRequest{Paths: []string{f1Abs}})
	require.Error(err)
	_, err = server.Delete(ctx, &downloaderproto.DeleteRequest{Paths: []string{f2Abs}})
	require.Error(err)
	require.Equal(2, len(d.torrentClient.Torrents()))

	// Del: idempotency
	_, err = server.Delete(ctx, &downloaderproto.DeleteRequest{Paths: []string{f1}})
	require.NoError(err)
	require.Equal(1, len(d.torrentClient.Torrents()))
	_, err = server.Delete(ctx, &downloaderproto.DeleteRequest{Paths: []string{f1}})
	require.NoError(err)
	require.Equal(1, len(d.torrentClient.Torrents()))

	_, err = server.Delete(ctx, &downloaderproto.DeleteRequest{Paths: []string{f2}})
	require.NoError(err)
	require.Equal(0, len(d.torrentClient.Torrents()))
	_, err = server.Delete(ctx, &downloaderproto.DeleteRequest{Paths: []string{f2}})
	require.NoError(err)
	require.Equal(0, len(d.torrentClient.Torrents()))

	// Batch
	_, err = server.Seed(ctx, &downloaderproto.SeedRequest{Paths: []string{f1, f2}})
	require.NoError(err)
	require.Equal(2, len(d.torrentClient.Torrents()))
	_, err = server.Delete(ctx, &downloaderproto.DeleteRequest{Paths: []string{f1, f2}})
	require.NoError(err)
	require.Equal(0, len(d.torrentClient.Torrents()))

}
