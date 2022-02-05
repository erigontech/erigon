package downloader

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon/cmd/downloader/trackers"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
)

// DefaultPieceSize - Erigon serves many big files, bigger pieces will reduce
// amount of network announcements, but can't go over 2Mb
// see https://wiki.theory.org/BitTorrentSpecification#Metainfo_File_Structure
const DefaultPieceSize = 2 * 1024 * 1024

// Trackers - break down by priority tier
var Trackers = [][]string{
	//trackers.First(5, trackers.Best),
	trackers.First(3, trackers.Udp),
	trackers.First(3, trackers.Https),
	//trackers.First(3, trackers.Ws),
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
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, f := range files {
		if !snapshotsync.IsCorrectFileName(f.Name()) {
			continue
		}
		if f.Size() == 0 {
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
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, f := range files {
		if !snapshotsync.IsCorrectFileName(f.Name()) {
			continue
		}
		if f.Size() == 0 {
			continue
		}
		if filepath.Ext(f.Name()) != ".seg" { // filter out only compressed files
			continue
		}
		res = append(res, f.Name())
	}
	return res, nil
}

// BuildTorrentFilesIfNeed - create .torrent files from .seg files (big IO) - if .seg files were added manually
func BuildTorrentFilesIfNeed(ctx context.Context, root string) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	files, err := allSegmentFiles(root)
	if err != nil {
		return err
	}
	for i, f := range files {
		torrentFileName := path.Join(root, f+".torrent")
		if _, err := os.Stat(torrentFileName); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			info, err := BuildInfoBytesForFile(root, f)
			if err != nil {
				return err
			}
			if err := CreateTorrentFile(root, info, nil); err != nil {
				return err
			}
		}

		select {
		default:
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			log.Info("[torrent] Creating .torrent files", "progress", fmt.Sprintf("%d/%d", i, len(files)))
		}
	}
	return nil
}

func BuildInfoBytesForFile(root string, fileName string) (*metainfo.Info, error) {
	info := &metainfo.Info{PieceLength: DefaultPieceSize}
	if err := info.BuildFromFilePath(filepath.Join(root, fileName)); err != nil {
		return nil, err
	}
	return info, nil
}

func CreateTorrentFileIfNotExists(root string, info *metainfo.Info, mi *metainfo.MetaInfo) error {
	torrentFileName := filepath.Join(root, info.Name+".torrent")
	if _, err := os.Stat(torrentFileName); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return CreateTorrentFile(root, info, mi)
		}
		return err
	}
	return nil
}

func CreateTorrentFile(root string, info *metainfo.Info, mi *metainfo.MetaInfo) error {
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
