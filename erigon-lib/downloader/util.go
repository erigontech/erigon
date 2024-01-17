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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/mmap_span"
	"github.com/anacrolix/torrent/storage"
	"github.com/edsrzf/mmap-go"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
)

// udpOrHttpTrackers - torrent library spawning several goroutines and producing many requests for each tracker. So we limit amout of trackers by 8
var udpOrHttpTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://9.rarbg.com:2810/announce",
	"udp://tracker.openbittorrent.com:6969/announce",
	"http://tracker.openbittorrent.com:80/announce",
	"udp://opentracker.i2p.rocks:6969/announce",
	"https://opentracker.i2p.rocks:443/announce",
	"udp://tracker.torrent.eu.org:451/announce",
	"udp://tracker.moeking.me:6969/announce",
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

func seedableSegmentFiles(dir string) ([]string, error) {
	files, err := dir2.ListFiles(dir, ".seg")
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files))
	for _, fPath := range files {
		_, name := filepath.Split(fPath)
		if !snaptype.IsCorrectFileName(name) {
			continue
		}
		ff, ok := snaptype.ParseFileName(dir, name)
		if !ok {
			continue
		}
		if !ff.Seedable() {
			continue
		}
		res = append(res, name)
	}
	return res, nil
}

var historyFileRegex = regexp.MustCompile("^([[:lower:]]+).([0-9]+)-([0-9]+).(.*)$")

func seedableSnapshotsBySubDir(dir, subDir string) ([]string, error) {
	historyDir := filepath.Join(dir, subDir)
	dir2.MustExist(historyDir)
	files, err := dir2.ListFiles(historyDir, ".kv", ".v", ".ef")
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files))
	for _, fPath := range files {
		_, name := filepath.Split(fPath)
		if !e3seedable(name) {
			continue
		}
		res = append(res, filepath.Join(subDir, name))
	}
	return res, nil
}

func e3seedable(name string) bool {
	subs := historyFileRegex.FindStringSubmatch(name)
	if len(subs) != 5 {
		return false
	}
	// Check that it's seedable
	from, err := strconv.ParseUint(subs[2], 10, 64)
	if err != nil {
		return false
	}
	to, err := strconv.ParseUint(subs[3], 10, 64)
	if err != nil {
		return false
	}
	if (to-from)%snaptype.Erigon3SeedableSteps != 0 {
		return false
	}
	return true
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

func BuildTorrentIfNeed(ctx context.Context, fName, root string, torrentFiles *TorrentFiles) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	fName, err = ensureCantLeaveDir(fName, root)
	if err != nil {
		return err
	}

	if torrentFiles.Exists(fName) {
		return nil
	}

	fPath := filepath.Join(root, fName)
	if !dir2.FileExist(fPath) {
		return nil
	}

	info := &metainfo.Info{PieceLength: downloadercfg.DefaultPieceSize, Name: fName}
	if err := info.BuildFromFilePath(fPath); err != nil {
		return fmt.Errorf("createTorrentFileFromSegment: %w", err)
	}
	info.Name = fName

	return CreateTorrentFileFromInfo(root, info, nil, torrentFiles)
}

// BuildTorrentFilesIfNeed - create .torrent files from .seg files (big IO) - if .seg files were added manually
func BuildTorrentFilesIfNeed(ctx context.Context, dirs datadir.Dirs, torrentFiles *TorrentFiles) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	files, err := seedableFiles(dirs)
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(-1) * 4)
	var i atomic.Int32

	for _, file := range files {
		file := file
		g.Go(func() error {
			defer i.Add(1)
			if err := BuildTorrentIfNeed(ctx, file, dirs.Snap, torrentFiles); err != nil {
				return err
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
		return err
	}
	return nil
}

func CreateTorrentFileIfNotExists(root string, info *metainfo.Info, mi *metainfo.MetaInfo, torrentFiles *TorrentFiles) error {
	if torrentFiles.Exists(info.Name) {
		return nil
	}
	if err := CreateTorrentFileFromInfo(root, info, mi, torrentFiles); err != nil {
		return err
	}
	return nil
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
func CreateTorrentFileFromInfo(root string, info *metainfo.Info, mi *metainfo.MetaInfo, torrentFiles *TorrentFiles) (err error) {
	mi, err = CreateMetaInfo(info, mi)
	if err != nil {
		return err
	}
	fPath := filepath.Join(root, info.Name+".torrent")
	return torrentFiles.CreateTorrentFromMetaInfo(fPath, mi)
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

func AllTorrentSpecs(dirs datadir.Dirs, torrentFiles *TorrentFiles) (res []*torrent.TorrentSpec, err error) {
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

// addTorrentFile - adding .torrent file to torrentClient (and checking their hashes), if .torrent file
// added first time - pieces verification process will start (disk IO heavy) - Progress
// kept in `piece completion storage` (surviving reboot). Once it done - no disk IO needed again.
// Don't need call torrent.VerifyData manually
func addTorrentFile(ctx context.Context, ts *torrent.TorrentSpec, torrentClient *torrent.Client, webseeds *WebSeeds) (t *torrent.Torrent, ok bool, err error) {
	ts.ChunkSize = downloadercfg.DefaultNetworkChunkSize
	ts.DisallowDataDownload = true
	ts.DisableInitialPieceCheck = true
	//re-try on panic, with 0 ChunkSize (lib doesn't allow change this field for existing torrents)
	defer func() {
		rec := recover()
		if rec != nil {
			ts.ChunkSize = 0
			t, ok, err = _addTorrentFile(ctx, ts, torrentClient, webseeds)
		}
	}()

	t, ok, err = _addTorrentFile(ctx, ts, torrentClient, webseeds)
	if err != nil {
		ts.ChunkSize = 0
		return _addTorrentFile(ctx, ts, torrentClient, webseeds)
	}
	return t, ok, err
}

func _addTorrentFile(ctx context.Context, ts *torrent.TorrentSpec, torrentClient *torrent.Client, webseeds *WebSeeds) (t *torrent.Torrent, ok bool, err error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	default:
	}

	ts.Webseeds, _ = webseeds.ByFileName(ts.DisplayName)
	var have bool
	t, have = torrentClient.Torrent(ts.InfoHash)
	if !have {
		t, _, err := torrentClient.AddTorrentSpec(ts)
		if err != nil {
			return nil, false, fmt.Errorf("addTorrentFile %s: %w", ts.DisplayName, err)
		}
		return t, true, nil
	}

	select {
	case <-t.GotInfo():
		t.AddWebSeeds(ts.Webseeds)
	default:
		t, _, err = torrentClient.AddTorrentSpec(ts)
		if err != nil {
			return nil, false, fmt.Errorf("addTorrentFile %s: %w", ts.DisplayName, err)
		}
	}

	return t, true, nil
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
	for i := 0; i < t.NumPieces(); i++ {
		t.Piece(i).VerifyData()

		completePieces.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func VerifyFileFailFast(ctx context.Context, t *torrent.Torrent, root string, completePieces *atomic.Uint64) error {
	span := new(mmap_span.MMapSpan)
	defer span.Close()
	info := t.Info()
	for _, file := range info.UpvertedFiles() {
		filename := filepath.Join(append([]string{root, info.Name}, file.Path...)...)
		mm, err := mmapFile(filename)
		if err != nil {
			return err
		}
		if int64(len(mm.Bytes())) != file.Length {
			return fmt.Errorf("file %q has wrong length", filename)
		}
		span.Append(mm)
	}
	span.InitIndex()

	hasher := sha1.New()
	for i := 0; i < info.NumPieces(); i++ {
		p := info.Piece(i)
		hasher.Reset()
		_, err := io.Copy(hasher, io.NewSectionReader(span, p.Offset(), p.Length()))
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

func mmapFile(name string) (mm storage.FileMapping, err error) {
	f, err := os.Open(name)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return
	}
	if fi.Size() == 0 {
		return
	}
	reg, err := mmap.MapRegion(f, -1, mmap.RDONLY, mmap.COPY, 0)
	if err != nil {
		return
	}
	return storage.WrapFileMapping(reg, f), nil
}
