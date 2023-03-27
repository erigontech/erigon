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
	"crypto/sha1" //nolint:gosec
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/mmap_span"
	"github.com/edsrzf/mmap-go"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/downloader/trackers"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"golang.org/x/sync/semaphore"
)

// Trackers - break down by priority tier
var Trackers = [][]string{
	trackers.First(7, trackers.Best),
	//trackers.First(3, trackers.Udp),
	//trackers.First(3, trackers.Https),
	//trackers.First(10, trackers.Ws),
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
		fileInfo, err := f.Info()
		if err != nil {
			return nil, err
		}
		if fileInfo.Size() == 0 {
			continue
		}
		if filepath.Ext(f.Name()) != ".seg" { // filter out only compressed files
			continue
		}
		ff, err := snaptype.ParseFileName(dir, f.Name())
		if err != nil {
			return nil, fmt.Errorf("ParseFileName: %w", err)
		}
		if !ff.Seedable() {
			continue
		}
		res = append(res, f.Name())
	}
	return res, nil
}

var historyFileRegex = regexp.MustCompile("^([[:lower:]]+).([0-9]+)-([0-9]+).(v|ef)$")

func seedableHistorySnapshots(dir string) ([]string, error) {
	historyDir := filepath.Join(dir, "history")
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
		fileInfo, err := f.Info()
		if err != nil {
			return nil, err
		}
		if fileInfo.Size() == 0 {
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

		from, err := strconv.ParseUint(subs[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ParseFileName: %w", err)
		}
		to, err := strconv.ParseUint(subs[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ParseFileName: %w", err)
		}
		if to-from != snaptype.Erigon3SeedableSteps {
			continue
		}
		res = append(res, filepath.Join("history", f.Name()))
	}
	return res, nil
}

func buildTorrentIfNeed(fName, root string) (err error) {
	fPath := filepath.Join(root, fName)
	if dir2.FileExist(fPath + ".torrent") {
		return
	}
	info := &metainfo.Info{PieceLength: downloadercfg.DefaultPieceSize, Name: fName}
	if err := info.BuildFromFilePath(fPath); err != nil {
		return fmt.Errorf("createTorrentFileFromSegment: %w", err)
	}
	info.Name = fName

	return createTorrentFileFromInfo(root, info, nil)
}

// AddSegment - add existing .seg file, create corresponding .torrent if need
func AddSegment(originalFileName, snapDir string, client *torrent.Client) (bool, error) {
	fPath := filepath.Join(snapDir, originalFileName)
	if !dir2.FileExist(fPath + ".torrent") {
		return false, nil
	}
	_, err := AddTorrentFile(fPath+".torrent", client)
	if err != nil {
		return false, fmt.Errorf("AddTorrentFile: %w", err)
	}
	return true, nil
}

// BuildTorrentFilesIfNeed - create .torrent files from .seg files (big IO) - if .seg files were added manually
func BuildTorrentFilesIfNeed(ctx context.Context, snapDir string) ([]string, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	files, err := seedableSegmentFiles(snapDir)
	if err != nil {
		return nil, err
	}
	files2, err := seedableHistorySnapshots(snapDir)
	if err != nil {
		return nil, err
	}
	files = append(files, files2...)

	errs := make(chan error, len(files)*2)
	wg := &sync.WaitGroup{}
	workers := cmp.Max(1, runtime.GOMAXPROCS(-1)-1) * 2
	var sem = semaphore.NewWeighted(int64(workers))
	i := atomic.Int32{}
	for _, f := range files {
		wg.Add(1)
		if err := sem.Acquire(ctx, 1); err != nil {
			return nil, err
		}
		go func(f string) {
			defer i.Add(1)
			defer sem.Release(1)
			defer wg.Done()
			if err := buildTorrentIfNeed(f, snapDir); err != nil {
				errs <- err
			}

			select {
			default:
			case <-ctx.Done():
				errs <- ctx.Err()
			case <-logEvery.C:
				log.Info("[snapshots] Creating .torrent files", "Progress", fmt.Sprintf("%d/%d", i.Load(), len(files)))
			}
		}(f)
	}
	go func() {
		wg.Wait()
		close(errs)
	}()
	for err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return files, nil
}

func CreateTorrentFileIfNotExists(root string, info *metainfo.Info, mi *metainfo.MetaInfo) error {
	fPath := filepath.Join(root, info.Name)
	if dir2.FileExist(fPath + ".torrent") {
		return nil
	}
	if err := createTorrentFileFromInfo(root, info, mi); err != nil {
		return err
	}
	return nil
}

func createTorrentFileFromInfo(root string, info *metainfo.Info, mi *metainfo.MetaInfo) error {
	if mi == nil {
		infoBytes, err := bencode.Marshal(info)
		if err != nil {
			return err
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
	torrentFileName := filepath.Join(root, info.Name+".torrent")

	file, err := os.Create(torrentFileName)
	if err != nil {
		return err
	}
	defer file.Sync()
	defer file.Close()
	if err := mi.Write(file); err != nil {
		return err
	}
	return nil
}

// nolint
func segmentFileNameFromTorrentFileName(in string) string {
	ext := filepath.Ext(in)
	return in[0 : len(in)-len(ext)]
}

func mmapFile(name string) (mm mmap.MMap, err error) {
	f, err := os.Open(name)
	if err != nil {
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return
	}
	if fi.Size() == 0 {
		return
	}
	return mmap.MapRegion(f, -1, mmap.RDONLY, mmap.COPY, 0)
}

func verifyTorrent(info *metainfo.Info, root string, consumer func(i int, good bool) error) error {
	span := new(mmap_span.MMapSpan)
	for _, file := range info.UpvertedFiles() {
		filename := filepath.Join(append([]string{root, info.Name}, file.Path...)...)
		mm, err := mmapFile(filename)
		if err != nil {
			return err
		}
		if int64(len(mm)) != file.Length {
			return fmt.Errorf("file %q has wrong length", filename)
		}
		span.Append(mm)
	}
	span.InitIndex()
	for i, numPieces := 0, info.NumPieces(); i < numPieces; i += 1 {
		p := info.Piece(i)
		hash := sha1.New() //nolint:gosec
		_, err := io.Copy(hash, io.NewSectionReader(span, p.Offset(), p.Length()))
		if err != nil {
			return err
		}
		good := bytes.Equal(hash.Sum(nil), p.Hash().Bytes())
		if err := consumer(i, good); err != nil {
			return err
		}
	}
	return nil
}

// AddTorrentFile - adding .torrent file to torrentClient (and checking their hashes), if .torrent file
// added first time - pieces verification process will start (disk IO heavy) - Progress
// kept in `piece completion storage` (surviving reboot). Once it done - no disk IO needed again.
// Don't need call torrent.VerifyData manually
func AddTorrentFile(torrentFilePath string, torrentClient *torrent.Client) (*torrent.Torrent, error) {
	mi, err := metainfo.LoadFromFile(torrentFilePath)
	if err != nil {
		return nil, err
	}
	mi.AnnounceList = Trackers
	ts, err := torrent.TorrentSpecFromMetaInfoErr(mi)
	if err != nil {
		return nil, err
	}

	if _, ok := torrentClient.Torrent(ts.InfoHash); !ok { // can set ChunkSize only for new torrents
		ts.ChunkSize = downloadercfg.DefaultNetworkChunkSize
	} else {
		ts.ChunkSize = 0
	}

	ts.DisallowDataDownload = true
	t, _, err := torrentClient.AddTorrentSpec(ts)
	if err != nil {
		return nil, err
	}

	t.DisallowDataDownload()
	t.AllowDataUpload()
	return t, nil
}

var ErrSkip = fmt.Errorf("skip")

func VerifyDtaFiles(ctx context.Context, snapDir string) error {
	logEvery := time.NewTicker(5 * time.Second)
	defer logEvery.Stop()

	files, err := AllTorrentPaths(snapDir)
	if err != nil {
		return err
	}
	totalPieces := 0
	for _, f := range files {
		metaInfo, err := metainfo.LoadFromFile(f)
		if err != nil {
			return err
		}
		info, err := metaInfo.UnmarshalInfo()
		if err != nil {
			return err
		}
		totalPieces += info.NumPieces()
	}

	j := 0
	failsAmount := 0
	for _, f := range files {
		metaInfo, err := metainfo.LoadFromFile(f)
		if err != nil {
			return err
		}
		info, err := metaInfo.UnmarshalInfo()
		if err != nil {
			return err
		}

		if err = verifyTorrent(&info, snapDir, func(i int, good bool) error {
			j++
			if !good {
				failsAmount++
				log.Error("[snapshots] Verify hash mismatch", "at piece", i, "file", info.Name)
				return ErrSkip
			}
			select {
			case <-logEvery.C:
				log.Info("[snapshots] Verify", "Progress", fmt.Sprintf("%.2f%%", 100*float64(j)/float64(totalPieces)))
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			return nil
		}); err != nil {
			if errors.Is(ErrSkip, err) {
				continue
			}
			return err
		}
	}
	if failsAmount > 0 {
		return fmt.Errorf("not all files are valid")
	}
	log.Info("[snapshots] Verify done")
	return nil
}

func portMustBeTCPAndUDPOpen(port int) error {
	tcpAddr := &net.TCPAddr{
		Port: port,
		IP:   net.ParseIP("127.0.0.1"),
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return fmt.Errorf("please open port %d for TCP and UDP. %w", port, err)
	}
	_ = ln.Close()
	udpAddr := &net.UDPAddr{
		Port: port,
		IP:   net.ParseIP("127.0.0.1"),
	}
	ser, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("please open port %d for UDP. %w", port, err)
	}
	_ = ser.Close()
	return nil
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
