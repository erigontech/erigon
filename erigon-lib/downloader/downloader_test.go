package downloader

import (
	"context"
	"path/filepath"
	"testing"

	lg "github.com/anacrolix/log"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	downloadercfg2 "github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestChangeInfoHashOfSameFile(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	cfg, err := downloadercfg2.New(dirs, "", lg.Info, 0, 0, 0, 0, 0, nil, nil, "testnet", false)
	require.NoError(err)
	d, err := New(context.Background(), cfg, dirs, log.New(), log.LvlInfo, true)
	require.NoError(err)
	defer d.Close()
	err = d.AddMagnetLink(d.ctx, snaptype.Hex2InfoHash("aa"), "a.seg")
	require.NoError(err)
	tt, ok := d.torrentClient.Torrent(snaptype.Hex2InfoHash("aa"))
	require.True(ok)
	require.Equal("a.seg", tt.Name())

	// adding same file twice is ok
	err = d.AddMagnetLink(d.ctx, snaptype.Hex2InfoHash("aa"), "a.seg")
	require.NoError(err)

	// adding same file with another infoHash - is ok, must be skipped
	// use-cases:
	//	- release of re-compressed version of same file,
	//	- ErigonV1.24 produced file X, then ErigonV1.25 released with new compression algorithm and produced X with anouther infoHash.
	//		ErigonV1.24 node must keep using existing file instead of downloading new one.
	err = d.AddMagnetLink(d.ctx, snaptype.Hex2InfoHash("bb"), "a.seg")
	require.NoError(err)
	tt, ok = d.torrentClient.Torrent(snaptype.Hex2InfoHash("aa"))
	require.True(ok)
	require.Equal("a.seg", tt.Name())
}

func TestNoEscape(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	ctx := context.Background()

	tf := NewAtomicTorrentFiles(dirs.Snap)
	// allow adding files only if they are inside snapshots dir
	err := BuildTorrentIfNeed(ctx, "a.seg", dirs.Snap, tf)
	require.NoError(err)
	err = BuildTorrentIfNeed(ctx, "b/a.seg", dirs.Snap, tf)
	require.NoError(err)
	err = BuildTorrentIfNeed(ctx, filepath.Join(dirs.Snap, "a.seg"), dirs.Snap, tf)
	require.NoError(err)
	err = BuildTorrentIfNeed(ctx, filepath.Join(dirs.Snap, "b", "a.seg"), dirs.Snap, tf)
	require.NoError(err)

	// reject escaping snapshots dir
	err = BuildTorrentIfNeed(ctx, filepath.Join(dirs.Chaindata, "b", "a.seg"), dirs.Snap, tf)
	require.Error(err)
	err = BuildTorrentIfNeed(ctx, "./../a.seg", dirs.Snap, tf)
	require.Error(err)
}
