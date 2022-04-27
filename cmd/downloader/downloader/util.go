package downloader

import (
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/mmap_span"
	"github.com/edsrzf/mmap-go"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader/torrentcfg"
	"github.com/ledgerwatch/erigon/cmd/downloader/trackers"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
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
		if !snapshotsync.IsCorrectFileName(f.Name()) {
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
		if !snapshotsync.IsCorrectFileName(f.Name()) {
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
func BuildTorrentFileIfNeed(ctx context.Context, originalFileName string, root *dir.Rw) (err error) {
	f, err := snapshotsync.ParseFileName(root.Path, originalFileName)
	if err != nil {
		return err
	}
	if f.To-f.From != snapshotsync.DEFAULT_SEGMENT_SIZE {
		return nil
	}
	torrentFilePath := filepath.Join(root.Path, originalFileName+".torrent")
	if _, err := os.Stat(torrentFilePath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		info := &metainfo.Info{PieceLength: torrentcfg.DefaultPieceSize}
		if err := info.BuildFromFilePath(filepath.Join(root.Path, originalFileName)); err != nil {
			return err
		}
		if err != nil {
			return err
		}
		if err := CreateTorrentFile(root, info, nil); err != nil {
			return err
		}
	}
	return nil
}

func BuildTorrentAndAdd(ctx context.Context, originalFileName string, snapshotDir *dir.Rw, client *torrent.Client) (*torrent.Torrent, error) {
	if err := BuildTorrentFileIfNeed(ctx, originalFileName, snapshotDir); err != nil {
		return nil, err
	}
	t, err := AddTorrentFile(ctx, filepath.Join(snapshotDir.Path, originalFileName+".torrent"), client)
	if err != nil {
		return nil, err
	}
	return t, nil
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
			errs <- BuildTorrentFileIfNeed(ctx, f, snapshotDir)

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
		return err
	}
	for i, f := range files {
		if _, err := BuildTorrentAndAdd(ctx, f, snapshotDir, client); err != nil {
			return err
		}

		select {
		default:
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			_, fname := filepath.Split(f)
			log.Info("[Snapshots] Verify existing snapshots", "Progress", fmt.Sprintf("%d/%d", i, len(files)), "file", fname)
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
