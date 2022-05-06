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
	"runtime"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/mmap_span"
	"github.com/edsrzf/mmap-go"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader/torrentcfg"
	"github.com/ledgerwatch/erigon/cmd/downloader/trackers"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"
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
	var res []string
	for _, f := range files {
		torrentFilePath := filepath.Join(dir, f)
		res = append(res, torrentFilePath)
	}
	return res, nil
}

func AllTorrentFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, f := range files {
		if !snap.IsCorrectFileName(f.Name()) {
			continue
		}
		fileInfo, err := f.Info()
		if err != nil {
			return nil, err
		}
		if fileInfo.Size() == 0 {
			continue
		}
		if filepath.Ext(f.Name()) != ".torrent" { // filter out only compressed files
			continue
		}
		res = append(res, f.Name())
	}
	return res, nil
}
func allSegmentFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, f := range files {
		if !snap.IsCorrectFileName(f.Name()) {
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
		res = append(res, f.Name())
	}
	return res, nil
}

// BuildTorrentFileIfNeed - create .torrent files from .seg files (big IO) - if .seg files were added manually
func BuildTorrentFileIfNeed(ctx context.Context, originalFileName string, root *dir.Rw) (ok bool, err error) {
	f, err := snap.ParseFileName(root.Path, originalFileName)
	if err != nil {
		return false, err
	}
	if f.To-f.From != snap.DEFAULT_SEGMENT_SIZE {
		return false, nil
	}
	torrentFilePath := filepath.Join(root.Path, originalFileName+".torrent")
	if _, err := os.Stat(torrentFilePath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		info := &metainfo.Info{PieceLength: torrentcfg.DefaultPieceSize}
		if err := info.BuildFromFilePath(filepath.Join(root.Path, originalFileName)); err != nil {
			return false, err
		}
		if err != nil {
			return false, err
		}
		if err := CreateTorrentFile(root, info, nil); err != nil {
			return false, err
		}
	}
	return true, nil
}

func BuildTorrentAndAdd(ctx context.Context, originalFileName string, snapshotDir *dir.Rw, client *torrent.Client) error {
	ok, err := BuildTorrentFileIfNeed(ctx, originalFileName, snapshotDir)
	if err != nil {
		return fmt.Errorf("BuildTorrentFileIfNeed: %w", err)
	}
	if !ok {
		return nil
	}
	torrentFilePath := filepath.Join(snapshotDir.Path, originalFileName+".torrent")
	_, err = AddTorrentFile(ctx, torrentFilePath, client)
	if err != nil {
		return fmt.Errorf("AddTorrentFile: %w", err)
	}
	return nil
}

// BuildTorrentFilesIfNeed - create .torrent files from .seg files (big IO) - if .seg files were added manually
func BuildTorrentFilesIfNeed(ctx context.Context, snapshotDir *dir.Rw) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	files, err := allSegmentFiles(snapshotDir.Path)
	if err != nil {
		return err
	}
	errs := make(chan error, len(files)*2)
	wg := &sync.WaitGroup{}
	for i, f := range files {
		wg.Add(1)
		go func(f string, i int) {
			defer wg.Done()
			_, err = BuildTorrentFileIfNeed(ctx, f, snapshotDir)
			if err != nil {
				errs <- err
			}

			select {
			default:
			case <-ctx.Done():
				errs <- ctx.Err()
			case <-logEvery.C:
				log.Info("[Snapshots] Creating .torrent files", "Progress", fmt.Sprintf("%d/%d", i, len(files)))
			}
		}(f, i)
	}
	go func() {
		wg.Wait()
		close(errs)
	}()
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// BuildTorrentsAndAdd - create .torrent files from .seg files (big IO) - if .seg files were placed manually to snapshotDir
func BuildTorrentsAndAdd(ctx context.Context, snapshotDir *dir.Rw, client *torrent.Client) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	files, err := allSegmentFiles(snapshotDir.Path)
	if err != nil {
		return fmt.Errorf("allSegmentFiles: %w", err)
	}
	errs := make(chan error, len(files)*2)
	wg := &sync.WaitGroup{}
	workers := runtime.GOMAXPROCS(-1) - 1
	if workers < 1 {
		workers = 1
	}
	var sem = semaphore.NewWeighted(int64(workers))
	for i, f := range files {
		wg.Add(1)
		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}
		go func(f string, i int) {
			defer sem.Release(1)
			defer wg.Done()

			select {
			case <-ctx.Done():
				errs <- ctx.Err()
			case <-logEvery.C:
				log.Info("[Snapshots] Verify snapshots", "Progress", fmt.Sprintf("%d/%d", i, len(files)))
			default:
			}
			errs <- BuildTorrentAndAdd(ctx, f, snapshotDir, client)
		}(f, i)
	}
	go func() {
		wg.Wait()
		close(errs)
	}()
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func CreateTorrentFileIfNotExists(root *dir.Rw, info *metainfo.Info, mi *metainfo.MetaInfo) error {
	torrentFileName := filepath.Join(root.Path, info.Name+".torrent")
	if _, err := os.Stat(torrentFileName); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return CreateTorrentFile(root, info, mi)
		}
		return err
	}
	return nil
}

func CreateTorrentFile(root *dir.Rw, info *metainfo.Info, mi *metainfo.MetaInfo) error {
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
	torrentFileName := filepath.Join(root.Path, info.Name+".torrent")

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
func AddTorrentFile(ctx context.Context, torrentFilePath string, torrentClient *torrent.Client) (*torrent.Torrent, error) {
	mi, err := metainfo.LoadFromFile(torrentFilePath)
	if err != nil {
		return nil, err
	}
	mi.AnnounceList = Trackers
	ts, err := torrent.TorrentSpecFromMetaInfoErr(mi)
	if err != nil {
		return nil, err
	}
	ts.ChunkSize = torrentcfg.DefaultNetworkChunkSize
	t, _, err := torrentClient.AddTorrentSpec(ts)
	if err != nil {
		return nil, err
	}
	t.DisallowDataDownload()
	t.AllowDataUpload()
	return t, nil
}

func VerifyDtaFiles(ctx context.Context, snapshotDir string) error {
	logEvery := time.NewTicker(5 * time.Second)
	defer logEvery.Stop()
	files, err := AllTorrentPaths(snapshotDir)
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
	for _, f := range files {
		metaInfo, err := metainfo.LoadFromFile(f)
		if err != nil {
			return err
		}
		info, err := metaInfo.UnmarshalInfo()
		if err != nil {
			return err
		}

		err = verifyTorrent(&info, snapshotDir, func(i int, good bool) error {
			j++
			if !good {
				log.Error("[Snapshots] Verify hash mismatch", "at piece", i, "file", f)
				return fmt.Errorf("invalid file")
			}
			select {
			case <-logEvery.C:
				log.Info("[Snapshots] Verify", "Progress", fmt.Sprintf("%.2f%%", 100*float64(j)/float64(totalPieces)))
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	log.Info("[Snapshots] Verify succeed")
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
