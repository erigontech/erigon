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
	"context"
	"fmt"
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
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
)

// udpOrHttpTrackers - torrent library spawning several goroutines and producing many requests for each tracker. So we limit amout of trackers by 7
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

func AllTorrentPaths(dir string) ([]string, error) {
	files, err := AllTorrentFiles(dir)
	if err != nil {
		return nil, err
	}
	histDir := filepath.Join(dir, "history")
	files2, err := AllTorrentFiles(histDir)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files)+len(files2))
	for _, f := range files {
		torrentFilePath := filepath.Join(dir, f)
		res = append(res, torrentFilePath)
	}
	for _, f := range files2 {
		torrentFilePath := filepath.Join(histDir, f)
		res = append(res, torrentFilePath)
	}
	return res, nil
}

func AllTorrentFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files))
	for _, f := range files {
		if filepath.Ext(f.Name()) != ".torrent" { // filter out only compressed files
			continue
		}
		fileInfo, err := f.Info()
		if err != nil {
			return nil, err
		}
		if fileInfo.Size() == 0 {
			continue
		}
		res = append(res, f.Name())
	}
	return res, nil
}

func seedableSegmentFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if !f.Type().IsRegular() {
			continue
		}
		if !snaptype.IsCorrectFileName(f.Name()) {
			continue
		}
		if filepath.Ext(f.Name()) != ".seg" { // filter out only compressed files
			continue
		}
		ff, ok := snaptype.ParseFileName(dir, f.Name())
		if !ok {
			continue
		}
		if !ff.Seedable() {
			continue
		}
		res = append(res, f.Name())
	}
	return res, nil
}

var historyFileRegex = regexp.MustCompile("^([[:lower:]]+).([0-9]+)-([0-9]+).(.*)$")

func seedableHistorySnapshots(dir, subDir string) ([]string, error) {
	l, err := seedableSnapshotsBySubDir(dir, "history")
	if err != nil {
		return nil, err
	}
	l2, err := seedableSnapshotsBySubDir(dir, "warm")
	if err != nil {
		return nil, err
	}
	return append(l, l2...), nil
}

func seedableSnapshotsBySubDir(dir, subDir string) ([]string, error) {
	historyDir := filepath.Join(dir, subDir)
	dir2.MustExist(historyDir)
	files, err := os.ReadDir(historyDir)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if !f.Type().IsRegular() {
			continue
		}
		ext := filepath.Ext(f.Name())
		if ext != ".v" && ext != ".ef" { // filter out only compressed files
			continue
		}

		subs := historyFileRegex.FindStringSubmatch(f.Name())
		if len(subs) != 5 {
			continue
		}
		// Check that it's seedable
		from, err := strconv.ParseUint(subs[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ParseFileName: %w", err)
		}
		to, err := strconv.ParseUint(subs[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ParseFileName: %w", err)
		}
		if (to-from)%snaptype.Erigon3SeedableSteps != 0 {
			continue
		}
		res = append(res, filepath.Join(subDir, f.Name()))
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

func BuildTorrentIfNeed(ctx context.Context, fName, root string) (torrentFilePath string, err error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	fName, err = ensureCantLeaveDir(fName, root)
	if err != nil {
		return "", err
	}

	fPath := filepath.Join(root, fName)
	if dir2.FileExist(fPath + ".torrent") {
		return
	}
	if !dir2.FileExist(fPath) {
		return
	}

	info := &metainfo.Info{PieceLength: downloadercfg.DefaultPieceSize, Name: fName}
	if err := info.BuildFromFilePath(fPath); err != nil {
		return "", fmt.Errorf("createTorrentFileFromSegment: %w", err)
	}
	info.Name = fName

	return fPath + ".torrent", CreateTorrentFileFromInfo(root, info, nil)
}

// BuildTorrentFilesIfNeed - create .torrent files from .seg files (big IO) - if .seg files were added manually
func BuildTorrentFilesIfNeed(ctx context.Context, snapDir string) ([]string, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	files, err := seedableFiles(snapDir)
	if err != nil {
		return nil, err
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(cmp.Max(1, runtime.GOMAXPROCS(-1)-1) * 4)
	var i atomic.Int32

	for _, file := range files {
		file := file
		g.Go(func() error {
			defer i.Add(1)
			if _, err := BuildTorrentIfNeed(ctx, file, snapDir); err != nil {
				return err
			}
			return nil
		})
	}

	var m runtime.MemStats
Loop:
	for int(i.Load()) < len(files) {
		select {
		case <-ctx.Done():
			break Loop // g.Wait() will return right error
		case <-logEvery.C:
			dbg.ReadMemStats(&m)
			log.Info("[snapshots] Creating .torrent files", "progress", fmt.Sprintf("%d/%d", i.Load(), len(files)), "alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return files, nil
}

func CreateTorrentFileIfNotExists(root string, info *metainfo.Info, mi *metainfo.MetaInfo) error {
	fPath := filepath.Join(root, info.Name)
	if dir2.FileExist(fPath + ".torrent") {
		return nil
	}
	if err := CreateTorrentFileFromInfo(root, info, mi); err != nil {
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
func CreateTorrentFromMetaInfo(root string, info *metainfo.Info, mi *metainfo.MetaInfo) error {
	torrentFileName := filepath.Join(root, info.Name+".torrent")
	file, err := os.Create(torrentFileName)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := mi.Write(file); err != nil {
		return err
	}
	file.Sync()
	return nil
}
func CreateTorrentFileFromInfo(root string, info *metainfo.Info, mi *metainfo.MetaInfo) (err error) {
	mi, err = CreateMetaInfo(info, mi)
	if err != nil {
		return err
	}
	return CreateTorrentFromMetaInfo(root, info, mi)
}

func AddTorrentFiles(snapDir string, torrentClient *torrent.Client) error {
	files, err := allTorrentFiles(snapDir)
	if err != nil {
		return err
	}
	for _, ts := range files {
		_, err := addTorrentFile(ts, torrentClient)
		if err != nil {
			return err
		}
	}

	return nil
}

func allTorrentFiles(snapDir string) (res []*torrent.TorrentSpec, err error) {
	res, err = torrentInDir(snapDir)
	if err != nil {
		return nil, err
	}
	res2, err := torrentInDir(filepath.Join(snapDir, "history"))
	if err != nil {
		return nil, err
	}
	res = append(res, res2...)
	res2, err = torrentInDir(filepath.Join(snapDir, "warm"))
	if err != nil {
		return nil, err
	}
	res = append(res, res2...)
	return res, nil
}
func torrentInDir(snapDir string) (res []*torrent.TorrentSpec, err error) {
	files, err := os.ReadDir(snapDir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.IsDir() || !f.Type().IsRegular() {
			continue
		}
		if filepath.Ext(f.Name()) != ".torrent" { // filter out only compressed files
			continue
		}

		a, err := loadTorrent(filepath.Join(snapDir, f.Name()))
		if err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func loadTorrent(torrentFilePath string) (*torrent.TorrentSpec, error) {
	mi, err := metainfo.LoadFromFile(torrentFilePath)
	if err != nil {
		return nil, fmt.Errorf("LoadFromFile: %w, file=%s", err, torrentFilePath)
	}
	mi.AnnounceList = Trackers
	return torrent.TorrentSpecFromMetaInfoErr(mi)
}

// addTorrentFile - adding .torrent file to torrentClient (and checking their hashes), if .torrent file
// added first time - pieces verification process will start (disk IO heavy) - Progress
// kept in `piece completion storage` (surviving reboot). Once it done - no disk IO needed again.
// Don't need call torrent.VerifyData manually
func addTorrentFile(ts *torrent.TorrentSpec, torrentClient *torrent.Client) (*torrent.Torrent, error) {
	if _, ok := torrentClient.Torrent(ts.InfoHash); !ok { // can set ChunkSize only for new torrents
		ts.ChunkSize = downloadercfg.DefaultNetworkChunkSize
	} else {
		ts.ChunkSize = 0
	}

	ts.DisallowDataDownload = true
	t, _, err := torrentClient.AddTorrentSpec(ts)
	if err != nil {
		return nil, fmt.Errorf("addTorrentFile %s: %w", ts.DisplayName, err)
	}

	t.DisallowDataDownload()
	t.AllowDataUpload()
	return t, nil
}

var ErrSkip = fmt.Errorf("skip")

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
