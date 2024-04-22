/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package downloader

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
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

type torrentInfo struct {
	Name      string     `json:"name"`
	Hash      []byte     `json:"hash"`
	Length    *int64     `json:"length,omitempty"`
	Created   *time.Time `json:"created,omitempty"`
	Completed *time.Time `json:"completed,omitempty"`
}

func seedableSegmentFiles(dir string, chainName string) ([]string, error) {
	files, err := dir2.ListFiles(dir, snaptype.SeedableV2Extensions()...)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files))
	for _, fPath := range files {

		_, name := filepath.Split(fPath)
		if !snaptype.IsCorrectFileName(name) {
			continue
		}
		ff, _, ok := snaptype.ParseFileName(dir, name)
		if !ok {
			continue
		}
		if !snapcfg.Seedable(chainName, ff) {
			continue
		}
		res = append(res, name)
	}
	return res, nil
}

func seedableStateFilesBySubDir(dir, subDir string) ([]string, error) {
	historyDir := filepath.Join(dir, subDir)
	dir2.MustExist(historyDir)
	files, err := dir2.ListFiles(historyDir, snaptype.SeedableV3Extensions()...)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files))
	for _, fPath := range files {
		_, name := filepath.Split(fPath)
		if !snaptype.E3Seedable(name) {
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
		if !IsLocal(newFName) {
			return fName, fmt.Errorf("file=%s, is outside of snapshots dir", fName)
		}
		fName = newFName
	}
	if !IsLocal(fName) {
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

	if torrentFiles.Exists(fName) {
		return false, nil
	}

	fPath := filepath.Join(root, fName)
	if !dir2.FileExist(fPath) {
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
func BuildTorrentFilesIfNeed(ctx context.Context, dirs datadir.Dirs, torrentFiles *AtomicTorrentFS, chain string, ignore snapcfg.Preverified) (int, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	files, err := SeedableFiles(dirs, chain)
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
	files2, err := dir2.ListFiles(dirs.SnapHistory, ".torrent")
	if err != nil {
		return nil, err
	}
	files = append(files, files2...)
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
		for _, p := range []string{"domain", "history", "idx"} {
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
	ts.DisableInitialPieceCheck = true
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
			return nil, false, fmt.Errorf("add torrent file %s: %w", ts.DisplayName, err)
		}

		db.Update(ctx, torrentInfoUpdater(ts.DisplayName, ts.InfoHash.Bytes(), 0, nil))
	}

	return t, true, nil
}

func torrentInfoUpdater(fileName string, infoHash []byte, length int64, completionTime *time.Time) func(tx kv.RwTx) error {
	return func(tx kv.RwTx) error {
		infoBytes, err := tx.GetOne(kv.BittorrentInfo, []byte(fileName))

		if err != nil {
			return err
		}

		var info torrentInfo

		err = json.Unmarshal(infoBytes, &info)

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

		infoBytes, err = json.Marshal(info)

		if err != nil {
			return err
		}

		return tx.Put(kv.BittorrentInfo, []byte(fileName), infoBytes)
	}
}

func torrentInfoReset(fileName string, infoHash []byte, length int64) func(tx kv.RwTx) error {
	return func(tx kv.RwTx) error {
		now := time.Now()

		info := torrentInfo{
			Name:    fileName,
			Hash:    infoHash,
			Created: &now,
		}

		if length > 0 {
			info.Length = &length
		}

		infoBytes, err := json.Marshal(info)

		if err != nil {
			return err
		}

		return tx.Put(kv.BittorrentInfo, []byte(fileName), infoBytes)
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
		peerID = common2.Copy(peerIDFromDB)
		return nil
	}); err != nil {
		return nil, err
	}
	return peerID, nil
}

// Deprecated: use `filepath.IsLocal` after drop go1.19 support
func IsLocal(path string) bool {
	return isLocal(path)
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
					err = fmt.Errorf("unexpected piece change error")
				}

				cancel()
				return fmt.Errorf("piece %s:%d verify failed: %w", t.Name(), change.Index, err)
			}

			if change.Complete && !(change.Checking || change.Hashing || change.QueuedForHash || change.Marking) {
				completePieces.Add(1)
				delete(inprogress, change.Index)
			}

			if len(inprogress) == 0 {
				cancel()
				return wg.Wait()
			}
		}
	}
}

func VerifyFileFailFast(ctx context.Context, t *torrent.Torrent, root string, completePieces *atomic.Uint64) error {
	info := t.Info()
	file := info.UpvertedFiles()[0]
	fPath := filepath.Join(append([]string{root, info.Name}, file.Path...)...)
	f, err := os.Open(fPath)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	hasher := sha1.New()
	for i := 0; i < info.NumPieces(); i++ {
		p := info.Piece(i)
		hasher.Reset()
		_, err := io.Copy(hasher, io.NewSectionReader(f, p.Offset(), p.Length()))
		if err != nil {
			return err
		}
		good := bytes.Equal(hasher.Sum(nil), p.Hash().Bytes())
		if !good {
			return fmt.Errorf("hash mismatch at piece %d, file: %s", i, t.Name())
		}

		completePieces.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}
