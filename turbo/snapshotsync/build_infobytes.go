package snapshotsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/log/v3"
)

func CreateTorrentFile(root string, info *metainfo.Info) error {
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		return err
	}
	mi := &metainfo.MetaInfo{
		CreationDate: time.Now().Unix(),
		CreatedBy:    "erigon",
		InfoBytes:    infoBytes,
		//AnnounceList: Trackers,
	}
	torrentFileName := filepath.Join(root, info.Name+".torrent")
	fmt.Printf("create: %s\n", torrentFileName)

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

func BuildTorrentFilesIfNeed(ctx context.Context, root string) error {
	logEvery := time.NewTicker(120 * time.Second)
	defer logEvery.Stop()

	var files []string
	for _, snType := range AllSnapshotTypes {
		filesOfType, err := segments(root, snType)
		if err != nil {
			return err
		}
		files = append(files, filesOfType...)
	}
	for i, f := range files {
		torrentFileName := path.Join(root, f+".torrent")
		if _, err := os.Stat(torrentFileName); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			info, err := BuildInfoBytesForSnapshot(root, f)
			if err != nil {
				return err
			}
			if err := CreateTorrentFile(root, info); err != nil {
				return err
			}
		}

		select {
		default:
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			log.Info("[torrent] Create .torrent files", "progress", fmt.Sprintf("%d/%d", i, len(files)))
		}
	}
	return nil
}

func ForEachTorrentFile(root string, walker func(torrentFileName string) error) error {
	for _, snType := range AllSnapshotTypes {
		files, err := segments(root, snType)
		if err != nil {
			return err
		}
		for _, f := range files {
			torrentFileName := filepath.Join(root, f+".torrent")
			if _, err := os.Stat(torrentFileName); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return err
			}
			if err := walker(torrentFileName); err != nil {
				return err
			}
		}
	}
	return nil
}

func BuildInfoBytesForSnapshot(root string, fileName string) (*metainfo.Info, error) {
	info := &metainfo.Info{PieceLength: DefaultPieceSize}
	if err := info.BuildFromFilePath(filepath.Join(root, fileName)); err != nil {
		return nil, err
	}
	return info, nil
}
