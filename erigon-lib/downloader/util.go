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
	"bytes"
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

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	dir2 "github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/downloader/downloaderrawdb"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
)

// udpOrHttpTrackers - torrent library spawning several goroutines and producing many requests for each tracker. So we limit amout of trackers by 8
var udpOrHttpTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://tracker.openbittorrent.com:6969/announce",
	"udp://opentracker.i2p.rocks:6969/announce",
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

	segConfig := snapcfg.KnownCfg(chainName)

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
		if !skipSeedableCheck && !snaptype.E3Seedable(name) {
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
func BuildTorrentFilesIfNeed(ctx context.Context, dirs datadir.Dirs, torrentFiles *AtomicTorrentFS, chain string, ignore snapcfg.Preverified, all bool) (int, error) {
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

// if $DOWNLOADER_ONLY_BLOCKS!="" filters out all non-v1 snapshots
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

// addTorrentFile - adding .torrent file to torrentClient (and checking their hashes), if .torrent file
// added first time - pieces verification process will start (disk IO heavy) - Progress
// kept in `piece completion storage` (surviving reboot). Once it done - no disk IO needed again.
// Don't need call torrent.VerifyData manually
func addTorrentFile(ctx context.Context, ts *torrent.TorrentSpec, torrentClient *torrent.Client, db kv.RwDB, webseeds *WebSeeds) (t *torrent.Torrent, ok bool, err error) {
	ts.ChunkSize = downloadercfg.DefaultNetworkChunkSize
	ts.DisallowDataDownload = true
	//ts.DisableInitialPieceCheck = true
	//re-try on panic, with 0 ChunkSize (lib doesn't allow change this field for existing torrents)
	defer func() {
		rec := recover()
		if rec != nil {
			ts.ChunkSize = 0
			t, ok, err = _addTorrentFile(ctx, ts, torrentClient, db, webseeds)
		}
	}()

	t, ok, err = _addTorrentFile(ctx, ts, torrentClient, db, webseeds)

	if err != nil {
		ts.ChunkSize = 0
		return _addTorrentFile(ctx, ts, torrentClient, db, webseeds)
	}
	return t, ok, err
}

func _addTorrentFile(ctx context.Context, ts *torrent.TorrentSpec, torrentClient *torrent.Client, db kv.RwDB, webseeds *WebSeeds) (t *torrent.Torrent, ok bool, err error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	default:
	}
	if !IsSnapNameAllowed(ts.DisplayName) {
		return nil, false, nil
	}
	ts.Webseeds, _ = webseeds.ByFileName(ts.DisplayName)
	var have bool
	t, have = torrentClient.Torrent(ts.InfoHash)

	if !have {
		t, _, err := torrentClient.AddTorrentSpec(ts)
		if err != nil {
			return nil, false, fmt.Errorf("addTorrentFile %s: %w", ts.DisplayName, err)
		}

		if t.Complete.Bool() {
			if err := db.Update(ctx, torrentInfoUpdater(ts.DisplayName, ts.InfoHash.Bytes(), 0, nil)); err != nil {
				return nil, false, fmt.Errorf("addTorrentFile %s: update failed: %w", ts.DisplayName, err)
			}
		} else {
			t.AddWebSeeds(ts.Webseeds)
			if err := db.Update(ctx, torrentInfoReset(ts.DisplayName, ts.InfoHash.Bytes(), 0)); err != nil {
				return nil, false, fmt.Errorf("addTorrentFile %s: reset failed: %w", ts.DisplayName, err)
			}
		}

		return t, true, nil
	}

	if t.Info() != nil {
		t.AddWebSeeds(ts.Webseeds)
		if err := db.Update(ctx, torrentInfoUpdater(ts.DisplayName, ts.InfoHash.Bytes(), t.Info().Length, nil)); err != nil {
			return nil, false, fmt.Errorf("update torrent info %s: %w", ts.DisplayName, err)
		}
	} else {
		t, _, err = torrentClient.AddTorrentSpec(ts)
		if err != nil {
			return t, true, fmt.Errorf("add torrent file %s: %w", ts.DisplayName, err)
		}

		db.Update(ctx, torrentInfoUpdater(ts.DisplayName, ts.InfoHash.Bytes(), 0, nil))
	}

	return t, true, nil
}

func torrentInfoUpdater(fileName string, infoHash []byte, length int64, completionTime *time.Time) func(tx kv.RwTx) error {
	return func(tx kv.RwTx) error {
		info, err := downloaderrawdb.ReadTorrentInfo(tx, fileName)
		if err != nil {
			return err
		}

		changed := false

		if err != nil || (len(infoHash) > 0 && !bytes.Equal(info.Hash, infoHash)) {
			now := time.Now()
			info.Name = fileName
			info.Hash = infoHash
			info.Created = &now
			info.Completed = nil
			changed = true
		}

		if length > 0 && (info.Length == nil || *info.Length != length) {
			info.Length = &length
			changed = true
		}

		if completionTime != nil {
			info.Completed = completionTime
			changed = true
		}

		if !changed {
			return nil
		}

		return downloaderrawdb.WriteTorrentInfo(tx, info)
	}
}

func torrentInfoReset(fileName string, infoHash []byte, length int64) func(tx kv.RwTx) error {
	return func(tx kv.RwTx) error {
		now := time.Now()

		info := downloaderrawdb.TorrentInfo{
			Name:    fileName,
			Hash:    infoHash,
			Created: &now,
		}

		if length > 0 {
			info.Length = &length
		}
		return downloaderrawdb.WriteTorrentInfo(tx, &info)
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

func ScheduleVerifyFile(ctx context.Context, t *torrent.Torrent, completePieces *atomic.Uint64) error {
	ctx, cancel := context.WithCancel(ctx)
	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(16)

	// piece changes happen asynchronously - we need to wait from them to complete
	pieceChanges := t.SubscribePieceStateChanges()
	inprogress := map[int]struct{}{}

	for i := 0; i < t.NumPieces(); i++ {
		inprogress[i] = struct{}{}

		i := i
		wg.Go(func() error {
			t.Piece(i).VerifyData()
			return nil
		})
	}

	for {
		select {
		case <-wgctx.Done():
			cancel()
			return wg.Wait()
		case change := <-pieceChanges.Values:
			if !change.Ok {
				var err error

				if change.Err != nil {
					err = change.Err
				} else {
					err = errors.New("unexpected piece change error")
				}

				cancel()
				return fmt.Errorf("piece %s:%d verify failed: %w", t.Name(), change.Index, err)
			}

			if !(change.Checking || change.Hashing || change.QueuedForHash || change.Marking) {
				if change.Complete {
					completePieces.Add(1)
				}
				delete(inprogress, change.Index)
			}

			if len(inprogress) == 0 {
				cancel()
				return wg.Wait()
			}
		}
	}
}
