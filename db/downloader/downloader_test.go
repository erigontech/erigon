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
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/dir"
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

// TestAllActiveSnapshotsConcurrentWithWrites pins that allActiveSnapshots may run concurrently
// with torrentsByName mutations; it data-races under -race unless the map read holds d.lock.
func TestAllActiveSnapshotsConcurrentWithWrites(t *testing.T) {
	test := newDownloaderTest(t)
	d := test.downloader
	ctx := t.Context()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				d.allActiveSnapshots()
			}
		}
	})
	// Stop the reader on every exit path, including a require failure (Goexit).
	defer func() {
		close(stop)
		wg.Wait()
	}()

	for i := range 64 {
		name := fmt.Sprintf("v1-%06d-%06d-headers.seg", i, i+1)
		ih := snaptype.Hex2InfoHash(fmt.Sprintf("%040x", i+1))
		require.NoError(t, d.testStartSingleDownloadNoWait(ctx, ih, name))
	}
}

// TestAddNewSeedableFileConcurrentWithAllActiveSnapshots pins that AddNewSeedableFile's
// torrentsByName mutation holds d.lock, so it does not race allActiveSnapshots' iteration.
// Without the lock the map write and the RLock'd read report a data race under -race — an
// RLock cannot exclude a writer that holds no lock.
func TestAddNewSeedableFileConcurrentWithAllActiveSnapshots(t *testing.T) {
	test := newDownloaderTest(t)
	d := test.downloader
	ctx := t.Context()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				d.allActiveSnapshots()
			}
		}
	})
	defer func() {
		close(stop)
		wg.Wait()
	}()

	for i := range 64 {
		name := fmt.Sprintf("v1-%06d-%06d-headers.seg", i, i+1)
		require.NoError(t, os.WriteFile(filepath.Join(test.dirs.Snap, name), nil, 0o644))
		require.NoError(t, d.AddNewSeedableFile(ctx, name))
	}
}

// Caplin beacon-state snapshots (e.g. NextSyncCommittee) have no registered global
// snaptype, so ParseFileName returns a nil Type for them. They are still seedable by
// name, so AddNewSeedableFile must not reject them as malformed.
func TestAddNewSeedableFileCaplinStateType(t *testing.T) {
	test := newDownloaderTest(t)
	name := filepath.Join("caplin", "v1.1-000000-007150-NextSyncCommittee.seg")
	require.NoError(t, os.MkdirAll(filepath.Join(test.dirs.Snap, "caplin"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(test.dirs.Snap, name), nil, 0o644))
	require.NoError(t, test.downloader.AddNewSeedableFile(t.Context(), name))
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
	ctx := t.Context()

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
	ctx := t.Context()

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
		t.Context(),
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

	d, err := New(t.Context(), cfg, log.New())
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

// logBuffer is a log sink readable while the Downloader's goroutines write to it.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newLocalSnapshotTest(t *testing.T) (d *Downloader, logs *logBuffer, name, path string) {
	d = newDownloaderTest(t).downloader
	logs = &logBuffer{}
	d.logger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StreamHandler(logs, log.LogfmtFormat())))

	name = "domain/v2.0-accounts.0-1024.kv"
	path = d.filePathForName(name)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("locally rebuilt"), 0o644))
	return
}

// Renaming data to .part leaves a hole in the snapshot tier, so it must be logged at warn or louder.
func TestInvalidateDataRenamesLocalFile(t *testing.T) {
	require := require.New(t)
	d, logs, name, path := newLocalSnapshotTest(t)
	differentPreverifiedInfoHash := snaptype.Hex2InfoHash("aa")

	_, download := prepareLocalDataForDownload(t, d, differentPreverifiedInfoHash, name)
	require.True(download)

	require.NoFileExists(path)
	require.FileExists(path + ".part")
	require.Contains(logs.String(), "invalidated local snapshot data", "rename must be logged at warn or louder")
	require.Contains(logs.String(), name)
}

// A stale metainfo doesn't rescue the data while the initial download is incomplete.
func TestInvalidateDataWithStaleMetainfo(t *testing.T) {
	require := require.New(t)
	d, _, name, path := newLocalSnapshotTest(t)
	_, err := BuildTorrentIfNeed(t.Context(), name, d.snapDir(), d.torrentFS)
	require.NoError(err)

	_, download := prepareLocalDataForDownload(t, d, snaptype.Hex2InfoHash("aa"), name)
	require.True(download)

	require.NoFileExists(path)
	require.FileExists(path + ".part")
}

// Once preverified.toml exists, local data is kept whatever the manifest says.
func TestKeepsLocalSnapshotAfterInitialDownload(t *testing.T) {
	for _, withMetainfo := range []bool{false, true} {
		t.Run(fmt.Sprint("withMetainfo=", withMetainfo), func(t *testing.T) {
			require := require.New(t)
			d, logs, name, path := newLocalSnapshotTest(t)
			if withMetainfo {
				_, err := BuildTorrentIfNeed(t.Context(), name, d.snapDir(), d.torrentFS)
				require.NoError(err)
			}
			markInitialDownloadComplete(t, d)

			_, download := prepareLocalDataForDownload(t, d, snaptype.Hex2InfoHash("aa"), name)
			require.False(download, "preverified download must be skipped")

			require.FileExists(path)
			require.NoFileExists(path + ".part")
			require.Contains(logs.String(), "keeping local snapshot")
			require.Contains(logs.String(), name)
		})
	}
}

// Data that no longer matches its own metainfo backs neither manifest. Keeping it unverified
// leaves a hole in the snapshot tier, so it goes to the client, which hash-checks and repairs it.
func TestDownloadsLocalSnapshotNotMatchingItsMetainfo(t *testing.T) {
	require := require.New(t)
	d, logs, name, path := newLocalSnapshotTest(t)
	_, err := BuildTorrentIfNeed(t.Context(), name, d.snapDir(), d.torrentFS)
	require.NoError(err)
	require.NoError(os.WriteFile(path, []byte("truncated"), 0o644))
	markInitialDownloadComplete(t, d)

	_, download := prepareLocalDataForDownload(t, d, snaptype.Hex2InfoHash("aa"), name)
	require.True(download, "a file that backs no metainfo must be re-fetched, not kept")

	require.FileExists(path, "the client repairs in place, so the data must stay where it is")
	require.NoFileExists(path+".part", "invalidation stays forbidden after the initial download")
	require.Contains(logs.String(), name)
}

// Nothing local to protect: the preverified file is still downloaded.
func TestDownloadsMissingSnapshotAfterInitialDownload(t *testing.T) {
	require := require.New(t)
	d, _, name, path := newLocalSnapshotTest(t)
	require.NoError(dir.RemoveFile(path))
	markInitialDownloadComplete(t, d)

	preverifiedInfoHash := snaptype.Hex2InfoHash("aa")
	_, download := prepareLocalDataForDownload(t, d, preverifiedInfoHash, name)
	require.True(download)
}

// Under d.lock, as production callers hold it.
func prepareLocalDataForDownload(t *testing.T, d *Downloader, preverifiedInfoHash metainfo.Hash, name string) (
	localMetainfo g.Option[*metainfo.MetaInfo],
	download bool,
) {
	t.Helper()
	d.lock.Lock()
	defer d.lock.Unlock()
	localMetainfo, download, err := d.prepareLocalDataForDownload(preverifiedInfoHash, name)
	require.NoError(t, err)
	return localMetainfo, download
}

func markInitialDownloadComplete(t *testing.T, d *Downloader) {
	require.NoError(t, os.WriteFile(d.cfg.Dirs.PreverifiedPath(), nil, 0o644))
}

// The metainfo on disk is the preverified one: the file is used as-is.
func TestKeepsSnapshotMatchingPreverifiedHash(t *testing.T) {
	for _, initialDownloadComplete := range []bool{false, true} {
		t.Run(fmt.Sprint("initialDownloadComplete=", initialDownloadComplete), func(t *testing.T) {
			require := require.New(t)
			d, _, name, path := newLocalSnapshotTest(t)
			_, err := BuildTorrentIfNeed(t.Context(), name, d.snapDir(), d.torrentFS)
			require.NoError(err)
			if initialDownloadComplete {
				markInitialDownloadComplete(t, d)
			}
			localInfoHash := loadLocalInfoHash(t, d, name)

			localMetainfo, download := prepareLocalDataForDownload(t, d, localInfoHash, name)
			require.True(localMetainfo.Ok)
			require.True(download)

			require.FileExists(path)
			require.NoFileExists(path + ".part")
		})
	}
}

// A from-scratch sync has no data to invalidate, so it must stay quiet.
func TestInvalidateDataQuietWithoutLocalData(t *testing.T) {
	require := require.New(t)
	d, logs, name, path := newLocalSnapshotTest(t)
	require.NoError(dir.RemoveFile(path))

	_, download := prepareLocalDataForDownload(t, d, snaptype.Hex2InfoHash("aa"), name)
	require.True(download)

	require.NoFileExists(path + ".part")
	require.Empty(logs.String())
}

// An unreadable metainfo costs the file only while the manifest is still authoritative.
func TestUnreadableMetainfoEvictsOnlyDuringInitialDownload(t *testing.T) {
	for _, initialDownloadComplete := range []bool{false, true} {
		t.Run(fmt.Sprint("initialDownloadComplete=", initialDownloadComplete), func(t *testing.T) {
			require := require.New(t)
			d, _, name, path := newLocalSnapshotTest(t)
			require.NoError(os.WriteFile(d.metainfoFilePathForName(name), []byte("not bencode"), 0o644))
			if initialDownloadComplete {
				markInitialDownloadComplete(t, d)
			}

			_, download := prepareLocalDataForDownload(t, d, snaptype.Hex2InfoHash("aa"), name)
			if initialDownloadComplete {
				require.False(download)
				require.FileExists(path)
			} else {
				require.True(download)
				require.NoFileExists(path)
			}
		})
	}
}

// The skip reaches the caller as None, so the batch has nothing to wait for.
func TestAddPreverifiedSnapshotSkipsAfterInitialDownload(t *testing.T) {
	require := require.New(t)
	d, _, name, path := newLocalSnapshotTest(t)
	markInitialDownloadComplete(t, d)

	snapshotTorrent, firstDownloader, _, err := d.addPreverifiedSnapshotForDownload(snaptype.Hex2InfoHash("aa"), name)
	require.NoError(err)
	require.False(snapshotTorrent.Ok)
	require.False(firstDownloader)
	require.FileExists(path)
}

func loadLocalInfoHash(t *testing.T, d *Downloader, name string) metainfo.Hash {
	t.Helper()
	mi, err := metainfo.LoadFromFile(d.metainfoFilePathForName(name))
	require.NoError(t, err)
	return mi.HashInfoBytes()
}

type localSnapshotState struct {
	data     bool
	metainfo string // "none", "matching", "stale", "corrupt"
}

func (st localSnapshotState) name() string {
	return fmt.Sprintf("domain/v2.0-accounts.%d-%d.kv", boolToInt(st.data), len(st.metainfo))
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Writes one snapshot in the requested state and returns its name and the hash a caller should pass
// as the preverified one.
func writeLocalSnapshot(t *testing.T, d *Downloader, name string, st localSnapshotState) metainfo.Hash {
	t.Helper()
	path := d.filePathForName(name)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("locally rebuilt "+name), 0o644))
	preverified := snaptype.Hex2InfoHash("aa")
	switch st.metainfo {
	case "matching":
		_, err := BuildTorrentIfNeed(t.Context(), name, d.snapDir(), d.torrentFS)
		require.NoError(t, err)
		preverified = loadLocalInfoHash(t, d, name)
	case "stale":
		_, err := BuildTorrentIfNeed(t.Context(), name, d.snapDir(), d.torrentFS)
		require.NoError(t, err)
	case "corrupt":
		require.NoError(t, os.WriteFile(d.metainfoFilePathForName(name), []byte("not bencode"), 0o644))
	}
	if !st.data {
		require.NoError(t, dir.RemoveFile(path))
	}
	return preverified
}

func allLocalSnapshotStates() (all []localSnapshotState) {
	for _, data := range []bool{false, true} {
		for _, mi := range []string{"none", "matching", "stale", "corrupt"} {
			all = append(all, localSnapshotState{data: data, metainfo: mi})
		}
	}
	return
}

// Invariant: once preverified.toml exists, no state of the datadir may cost a data file, and a file
// we have is never re-downloaded.
func TestNeverEvictsAfterInitialDownload(t *testing.T) {
	require := require.New(t)
	d := newDownloaderTest(t).downloader
	markInitialDownloadComplete(t, d)

	for _, st := range allLocalSnapshotStates() {
		name := st.name()
		preverified := writeLocalSnapshot(t, d, name, st)

		_, download := prepareLocalDataForDownload(t, d, preverified, name)

		require.NoFileExists(d.filePathForName(name)+".part", "%+v", st)
		require.Equal(st.data, fileExists(d.filePathForName(name)), "%+v", st)
		// The preverified file is the local file: adding it is free, the client sees it complete.
		wantDownload := !st.data || st.metainfo == "matching"
		require.Equal(wantDownload, download, "%+v", st)
	}
}

// The snapshot stage writes preverified.toml mid-run, so the rule must be re-read, not cached from
// an earlier call.
func TestInitialDownloadCompletingMidRunKeepsLocalData(t *testing.T) {
	require := require.New(t)
	d := newDownloaderTest(t).downloader

	beforeName := "domain/v2.0-accounts.0-1024.kv"
	writeLocalSnapshot(t, d, beforeName, localSnapshotState{data: true, metainfo: "none"})
	_, download := prepareLocalDataForDownload(t, d, snaptype.Hex2InfoHash("aa"), beforeName)
	require.True(download)
	require.FileExists(d.filePathForName(beforeName) + ".part")

	markInitialDownloadComplete(t, d)

	afterName := "domain/v2.0-accounts.1024-2048.kv"
	writeLocalSnapshot(t, d, afterName, localSnapshotState{data: true, metainfo: "none"})
	_, download = prepareLocalDataForDownload(t, d, snaptype.Hex2InfoHash("aa"), afterName)
	require.False(download)
	require.FileExists(d.filePathForName(afterName))
	require.NoFileExists(d.filePathForName(afterName) + ".part")
}

// The rename itself refuses to run after the initial download, whatever the caller decided.
func TestInvalidateDataRefusesAfterInitialDownload(t *testing.T) {
	require := require.New(t)
	d, _, name, path := newLocalSnapshotTest(t)
	markInitialDownloadComplete(t, d)

	d.lock.Lock()
	err := d.invalidateData(name, snaptype.Hex2InfoHash("aa"))
	d.lock.Unlock()

	require.Error(err)
	require.FileExists(path)
	require.NoFileExists(path + ".part")
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
