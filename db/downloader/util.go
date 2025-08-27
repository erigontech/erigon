// Copyright 2021 The Erigon Authors
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
	//nolint:gosec
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/missinggo/v2/panicif"
	"golang.org/x/sync/errgroup"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	dir2 "github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
)

// TODO: Update this list, or pull from common location (central manifest or canonical multi-file torrent).
// udpOrHttpTrackers - torrent library spawning several goroutines and producing many requests for each tracker. So we limit amout of trackers by 8
var udpOrHttpTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://tracker.torrent.eu.org:451/announce",
	"udp://open.stealth.si:80/announce",
}

// nolint
var websocketTrackers = []string{
	"wss://tracker.btorrent.xyz",
}

// Trackers - break down by priority tier
var Trackers = [][]string{
	udpOrHttpTrackers,
	//websocketTrackers // TODO: Ws protocol producing too many errors and flooding logs. But it's also very fast and reactive.
}

func seedableSegmentFiles(dir string, chainName string, skipSeedableCheck bool) ([]string, error) {
	extensions := snaptype.SeedableV2Extensions()
	if skipSeedableCheck {
		extensions = snaptype.AllV2Extensions()
	}
	files, err := dir2.ListFiles(dir, extensions...)
	if err != nil {
		return nil, err
	}

	segConfig, _ := snapcfg.KnownCfg(chainName)

	res := make([]string, 0, len(files))
	for _, fPath := range files {
		_, name := filepath.Split(fPath)
		// A bit hacky but whatever... basically caplin is incompatible with enums.
		if strings.HasSuffix(fPath, path.Join("caplin", name)) {
			res = append(res, path.Join("caplin", name))
			continue
		}
		if strings.HasPrefix(name, "salt") && strings.HasSuffix(name, "txt") {
			res = append(res, name)
			continue
		}
		if !skipSeedableCheck && !snaptype.IsCorrectFileName(name) {
			continue
		}
		ff, isStateFile, ok := snaptype.ParseFileName(dir, name)
		if !skipSeedableCheck && (!ok || isStateFile) {
			continue
		}
		if !skipSeedableCheck && !segConfig.Seedable(ff) {
			continue
		}
		res = append(res, name)
	}
	return res, nil
}

func seedableStateFilesBySubDir(dir, subDir string, skipSeedableCheck bool) ([]string, error) {
	historyDir := filepath.Join(dir, subDir)
	dir2.MustExist(historyDir)
	extensions := snaptype.SeedableV3Extensions()
	if skipSeedableCheck {
		extensions = snaptype.AllV3Extensions()
	}
	files, err := dir2.ListFiles(historyDir, extensions...)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files))
	for _, fPath := range files {
		_, name := filepath.Split(fPath)
		if !skipSeedableCheck && !snaptype.IsStateFileSeedable(name) {
			continue
		}
		res = append(res, filepath.Join(subDir, name))
	}
	return res, nil
}

func ensureCantLeaveDir(fName, root string) (string, error) {
	if filepath.IsAbs(fName) {
		newFName, err := filepath.Rel(root, fName)
		if err != nil {
			return fName, err
		}
		if !filepath.IsLocal(newFName) {
			return fName, fmt.Errorf("file=%s, is outside of snapshots dir", fName)
		}
		fName = newFName
	}
	if !filepath.IsLocal(fName) {
		return fName, fmt.Errorf("relative paths are not allowed: %s", fName)
	}
	return fName, nil
}

func BuildTorrentIfNeed(ctx context.Context, fName, root string, torrentFiles *AtomicTorrentFS) (ok bool, err error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	fName, err = ensureCantLeaveDir(fName, root)
	if err != nil {
		return false, err
	}

	exists, err := torrentFiles.Exists(fName)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	fPath := filepath.Join(root, fName)
	exists, err = dir2.FileExist(fPath)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	info := &metainfo.Info{PieceLength: downloadercfg.DefaultPieceSize, Name: fName}
	if err := info.BuildFromFilePath(fPath); err != nil {
		return false, fmt.Errorf("createTorrentFileFromSegment: %w", err)
	}
	info.Name = fName

	return torrentFiles.CreateWithMetaInfo(info, nil)
}

// BuildTorrentFilesIfNeed - create .torrent files from .seg files (big IO) - if .seg files were added manually
func BuildTorrentFilesIfNeed(ctx context.Context, dirs datadir.Dirs, torrentFiles *AtomicTorrentFS, chain string, ignore snapcfg.PreverifiedItems, all bool) (int, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	files, err := SeedableFiles(dirs, chain, all)
	if err != nil {
		return 0, err
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(-1) * 16)
	var i atomic.Int32
	var createdAmount atomic.Int32

	for _, file := range files {
		file := file

		if ignore.Contains(file) {
			i.Add(1)
			continue
		}

		g.Go(func() error {
			defer i.Add(1)
			ok, err := BuildTorrentIfNeed(ctx, file, dirs.Snap, torrentFiles)
			if err != nil {
				return err
			}
			if ok {
				createdAmount.Add(1)
			}
			return nil
		})
	}

Loop:
	for int(i.Load()) < len(files) {
		select {
		case <-ctx.Done():
			break Loop // g.Wait() will return right error
		case <-logEvery.C:
			if int(i.Load()) == len(files) {
				break Loop
			}
			log.Info("[snapshots] Creating .torrent files", "progress", fmt.Sprintf("%d/%d", i.Load(), len(files)))
		}
	}
	if err := g.Wait(); err != nil {
		return int(createdAmount.Load()), err
	}
	return int(createdAmount.Load()), nil
}

func CreateMetaInfo(info *metainfo.Info, mi *metainfo.MetaInfo) (*metainfo.MetaInfo, error) {
	if mi == nil {
		infoBytes, err := bencode.Marshal(info)
		if err != nil {
			return nil, err
		}
		mi = &metainfo.MetaInfo{
			CreationDate: time.Now().Unix(),
			// TODO: Differentiate direct web source vs generated locally.
			CreatedBy:    "erigon",
			InfoBytes:    infoBytes,
			AnnounceList: Trackers,
		}
	} else {
		mi.AnnounceList = Trackers
	}
	return mi, nil
}

func AllTorrentPaths(dirs datadir.Dirs) ([]string, error) {
	files, err := dir2.ListFiles(dirs.Snap, ".torrent")
	if err != nil {
		return nil, err
	}
	if dbg.DownloaderOnlyBlocks {
		return files, nil
	}
	l1, err := dir2.ListFiles(dirs.SnapIdx, ".torrent")
	if err != nil {
		return nil, err
	}
	l2, err := dir2.ListFiles(dirs.SnapHistory, ".torrent")
	if err != nil {
		return nil, err
	}
	l3, err := dir2.ListFiles(dirs.SnapDomain, ".torrent")
	if err != nil {
		return nil, err
	}
	l4, err := dir2.ListFiles(dirs.SnapAccessors, ".torrent")
	if err != nil {
		return nil, err
	}
	l5, err := dir2.ListFiles(dirs.SnapCaplin, ".torrent")
	if err != nil {
		return nil, err
	}
	files = append(append(append(append(append(files, l1...), l2...), l3...), l4...), l5...)
	return files, nil
}

func AllTorrentSpecs(dirs datadir.Dirs, torrentFiles *AtomicTorrentFS) (res []*torrent.TorrentSpec, err error) {
	files, err := AllTorrentPaths(dirs)
	if err != nil {
		return nil, err
	}
	for _, fPath := range files {
		if len(fPath) == 0 {
			continue
		}
		a, err := torrentFiles.LoadByPath(fPath)
		if err != nil {
			return nil, fmt.Errorf("AllTorrentSpecs: %w", err)
		}
		res = append(res, a)
	}
	return res, nil
}

// if $DOWNLOADER_ONLY_BLOCKS!="" filters out all non-v1 snapshots. TODO: This does not belong here.
// Downloader should be stupid.
func IsSnapNameAllowed(name string) bool {
	if dbg.DownloaderOnlyBlocks {
		for _, p := range []string{"domain", "history", "idx", "accessor"} {
			if strings.HasPrefix(name, p) {
				return false
			}
		}
	}
	return true
}

// addTorrentSpec - adding .torrent file to torrentClient (and checking their hashes), if .torrent file
// added first time - pieces verification process will start (disk IO heavy) - Progress
// kept in `piece completion storage` (surviving reboot). Once it's done - no disk IO needed again.
// Don't need call torrent.VerifyData manually
func (d *Downloader) addTorrentSpec(
	ts *torrent.TorrentSpec,
	name string,
) (t *torrent.Torrent, first bool, err error) {
	ts.ChunkSize = downloadercfg.NetworkChunkSize
	ts.Trackers = nil // to reduce mutex contention - see `afterAdd`
	ts.Webseeds = nil
	ts.DisallowDataDownload = true
	ts.DisallowDataUpload = true
	// I wonder how this should be handled for AddNewSeedableFile. What if there's bad piece
	// completion data? We might want to clobber any piece completion and force the client to accept
	// what we provide, assuming we trust our own metainfo generation more.
	ts.IgnoreUnverifiedPieceCompletion = d.cfg.VerifyTorrentData
	ts.DisableInitialPieceCheck = d.cfg.ManualDataVerification
	// Non-zero chunk size is not allowed for existing torrents. If this breaks I will fix
	// anacrolix/torrent instead of working around it. See torrent.Client.AddTorrentOpt.
	t, first, err = d.torrentClient.AddTorrentSpec(ts)
	if err != nil {
		return
	}
	// This is rough, but we intend to download everything added to completion, so this is a good
	// time to start the clock. We shouldn't just do it on Torrent.DownloadAll because we might also
	// need to fetch the metainfo (source).
	if !t.Complete().Bool() {
		d.setStartTime()
	}
	g.MakeMapIfNil(&d.torrentsByName)
	hadOld := g.MapInsert(d.torrentsByName, name, t).Ok
	panicif.Eq(first, hadOld)
	return
}

func (d *Downloader) afterAdd() {
	for _, t := range d.torrentClient.Torrents() {
		// add webseed first - otherwise opts will be ignored
		t.AddWebSeeds(d.cfg.WebSeedUrls, d.addWebSeedOpts...)
		// Should be disabled by no download rate or the disable trackers flag.
		t.AddTrackers(Trackers)
		t.AllowDataDownload()
		t.AllowDataUpload()
	}
}

func savePeerID(db kv.RwDB, peerID torrent.PeerID) error {
	return db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.BittorrentInfo, []byte(kv.BittorrentPeerID), peerID[:])
	})
}

func readPeerID(db kv.RoDB) (peerID []byte, err error) {
	if err = db.View(context.Background(), func(tx kv.Tx) error {
		peerIDFromDB, err := tx.GetOne(kv.BittorrentInfo, []byte(kv.BittorrentPeerID))
		if err != nil {
			return fmt.Errorf("get peer id: %w", err)
		}
		peerID = common.Copy(peerIDFromDB)
		return nil
	}); err != nil {
		return nil, err
	}
	return peerID, nil
}

// Trigger all pieces to be verified with the given concurrency primitives. It's an error for a
// piece to not be complete or have an unknown state after verification.
func verifyTorrentComplete(
	ctx context.Context,
	eg *errgroup.Group,
	t *torrent.Torrent,
	verifiedBytes *atomic.Int64,
) {
	eg.Go(func() (err error) {
		// Wrap error for errgroup.Group return.
		defer func() {
			if err != nil {
				err = fmt.Errorf("verifying %v: %w", t.Name(), err)
			}
		}()
		err = t.VerifyDataContext(ctx)
		if err != nil {
			return
		}
		// This is a relatively new API feature, so please report if it does not work as expected.
		if t.Complete().Bool() {
			verifiedBytes.Add(t.Length())
		} else {
			err = errors.New("torrent not complete")
		}
		return
	})
}
