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
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/snaptype"
)

func TestConcurrentDownload(t *testing.T) {
	require := require.New(t)
	test := newDownloaderTest(t)
	const conc = 2
	waits := make(chan func(ctx context.Context) error, conc)
	g, ctx := errgroup.WithContext(t.Context())
	for range conc {
		g.Go(func() error {
			wait, err := test.downloader.testStartSingleDownload(ctx, snaptype.Hex2InfoHash("aa"), "a.seg")
			if err != nil {
				return err
			}
			waits <- wait
			return nil
		})
	}
	require.NoError(g.Wait())
	close(waits)
	test.downloader.Close()
	for w := range waits {
		// Make sure we don't get stuck. The torrents shouldn't exist, and the Downloader is closed.
		w(t.Context())
	}
}

func TestChangeInfoHashOfSameFile(t *testing.T) {
	ctx := t.Context()
	require := require.New(t)
	test := newDownloaderTest(t)
	err := test.downloader.testStartSingleDownloadNoWait(ctx, snaptype.Hex2InfoHash("aa"), "a.seg")
	require.NoError(err)
	tt, ok := test.downloader.torrentClient.Torrent(snaptype.Hex2InfoHash("aa"))
	require.True(ok)
	require.Equal("a.seg", tt.Name())

	// adding same file twice is ok
	err = test.downloader.testStartSingleDownloadNoWait(ctx, snaptype.Hex2InfoHash("aa"), "a.seg")
	require.NoError(err)

	// adding same file with another infoHash - is ok, must be skipped
	// use-cases:
	//	- release of re-compressed version of same file,
	//	- ErigonV1.24 produced file X, then ErigonV1.25 released with new compression algorithm and produced X with anouther infoHash.
	//		ErigonV1.24 node must keep using existing file instead of downloading new one.
	err = test.downloader.testStartSingleDownloadNoWait(ctx, snaptype.Hex2InfoHash("bb"), "a.seg")
	// I'm not sure if this is a good idea.
	//require.Error(err)
	_ = err
	tt, ok = test.downloader.torrentClient.Torrent(snaptype.Hex2InfoHash("aa"))
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

func TestVerifyDataNoTorrents(t *testing.T) {
	require := require.New(t)
	test := newDownloaderTest(t)
	err := test.downloader.VerifyData(test.downloader.ctx, nil, false)
	require.NoError(err)
}

func TestVerifyData(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skip on Windows due to file locking issues")
	}
	require := require.New(t)
	test := newDownloaderTest(t)
	os.WriteFile(filepath.Join(test.dirs.Snap, "a"), nil, 0o644)
	err := test.downloader.AddNewSeedableFile(t.Context(), "a")
	require.NoError(err)
	err = test.downloader.VerifyData(test.downloader.ctx, nil, false)
	require.NoError(err)
}

func TestVerifyDataDownloaderClosed(t *testing.T) {
	require := require.New(t)
	test := newDownloaderTest(t)
	test.downloader.Close()
	err := test.downloader.VerifyData(test.downloader.ctx, nil, false)
	require.NoError(err)
}

func TestAddDel(t *testing.T) {
	require := require.New(t)
	test := newDownloaderTest(t)
	ctx := context.Background()

	// In the following tests we use combinations of f1Abs, f1, f2, and f1BadAbs. Absolute file
	// paths are allowed to calls to RpcClient if they're local to the SnapDir, it does the required
	// conversion. This is the behaviour consumers will see.

	f1Abs := filepath.Join(test.dirs.Snap, "a.seg")      // block file
	f2Abs := filepath.Join(test.dirs.SnapDomain, "a.kv") // state file
	f, err := os.Create(f1Abs)
	require.NoError(err)
	assert.NoError(t, f.Close())
	require.NoError(os.WriteFile(f2Abs, []byte("a.kv"), 0o666))

	// Create a second datadir, not relative to the one the Downloader expects.
	invalidDirs := datadir.New(t.TempDir())
	// Mixed and matched with f1Abs, which is now allowed but heavily warned against.
	f1BadAbs := filepath.Join(invalidDirs.Snap, "a.seg")

	grpcServer, _ := NewGrpcServer(test.downloader)

	server := NewRpcClient(DirectGrpcServerClient(grpcServer), test.dirs.Snap)

	// So... errors.AsType is coming.
	var errRpcSnapName errRpcSnapName

	// Add: expect relative paths
	err = server.Seed(ctx, []string{f1BadAbs})
	require.ErrorAs(err, &errRpcSnapName)
	require.Equal(0, len(test.downloader.torrentClient.Torrents()))

	f1, _ := filepath.Rel(test.dirs.Snap, f1Abs)
	f2, _ := filepath.Rel(test.dirs.Snap, f2Abs)
	err = server.Seed(ctx, []string{f1Abs})
	require.NoError(err)
	err = server.Seed(ctx, []string{f2})
	require.NoError(err)
	require.Equal(2, len(test.downloader.torrentClient.Torrents()))

	// add idempotency
	err = server.Seed(ctx, []string{f1})
	require.NoError(err)
	err = server.Seed(ctx, []string{f2})
	require.NoError(err)
	require.Equal(2, len(test.downloader.torrentClient.Torrents()))

	// Del: expect relative paths
	err = server.Delete(ctx, []string{f1BadAbs})
	require.ErrorAs(err, &errRpcSnapName)
	require.Equal(2, len(test.downloader.torrentClient.Torrents()))

	// Del: idempotency
	err = server.Delete(ctx, []string{f1Abs})
	require.NoError(err)
	require.Equal(1, len(test.downloader.torrentClient.Torrents()))
	err = server.Delete(ctx, []string{f1})
	require.NoError(err)
	require.Equal(1, len(test.downloader.torrentClient.Torrents()))

	err = server.Delete(ctx, []string{f2})
	require.NoError(err)
	require.Equal(0, len(test.downloader.torrentClient.Torrents()))
	err = server.Delete(ctx, []string{f2})
	require.NoError(err)
	require.Equal(0, len(test.downloader.torrentClient.Torrents()))

	// Batch
	err = server.Seed(ctx, []string{f1, f2})
	require.NoError(err)
	require.Equal(2, len(test.downloader.torrentClient.Torrents()))
	err = server.Delete(ctx, []string{f1Abs, f2})
	require.NoError(err)
	require.Equal(0, len(test.downloader.torrentClient.Torrents()))

}

// downloaderTest holds test fixtures for Downloader tests.
type downloaderTest struct {
	dirs       datadir.Dirs
	cfg        *downloadercfg.Cfg
	downloader *Downloader
}

// newDownloaderTest creates a Downloader with proper cleanup handling.
// All resources (cfg.TorrentLogFile and downloader) are automatically cleaned up via t.Cleanup.
func newDownloaderTest(t *testing.T) *downloaderTest {
	require := require.New(t)

	dirs := datadir.New(t.TempDir())
	cfg, err := downloadercfg.New(
		context.Background(),
		dirs,
		"",
		log.LvlInfo,
		0, 0,
		nil,
		"testnet",
		false,
		downloadercfg.NewCfgOpts{},
	)
	require.NoError(err)

	if runtime.GOOS == "windows" {
		// Disable UTP (UDP-based transport) to avoid Windows Server 2025 Hyper-V/WinNAT port
		// reservation conflicts where TCP and UDP port availability is asymmetric (WSAEACCES
		// on UDP bind).
		cfg.ClientConfig.DisableUTP = true
	}

	d, err := New(context.Background(), cfg, log.New())
	require.NoError(err)

	// Register cleanup in reverse order (downloader closes before config file)
	t.Cleanup(func() {
		d.Close()
		// This must be closed to cleanup test temp dir on Windows.
		if err := cfg.CloseTorrentLogFile(); err != nil {
			t.Logf("warning: failed to close torrent log file: %v", err)
		}
	})

	return &downloaderTest{
		dirs:       dirs,
		cfg:        cfg,
		downloader: d,
	}
}
