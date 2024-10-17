package downloaderrawdb

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/kv"
	kv2 "github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"golang.org/x/sync/semaphore"
)

type TorrentInfo struct {
	Name      string     `json:"name"`
	Hash      []byte     `json:"hash"`
	Length    *int64     `json:"length,omitempty"`
	Created   *time.Time `json:"created,omitempty"`
	Completed *time.Time `json:"completed,omitempty"`
}

func ReadTorrentInfo(downloaderDBTx kv.Tx, name string) (*TorrentInfo, error) {
	var info TorrentInfo
	infoBytes, err := downloaderDBTx.GetOne(kv.BittorrentInfo, []byte(name))
	if err != nil {
		return nil, err
	}
	if len(infoBytes) == 0 {
		return &info, nil
	}
	if err = json.Unmarshal(infoBytes, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func ReadTorrentInfoHash(downloaderDBTx kv.Tx, name string) (hashBytes []byte, err error) {
	infoBytes, err := downloaderDBTx.GetOne(kv.BittorrentInfo, []byte(name))
	if err != nil {
		return nil, err
	}

	if len(infoBytes) == 20 {
		return infoBytes, nil
	}

	var info TorrentInfo
	if err = json.Unmarshal(infoBytes, &info); err == nil {
		return info.Hash, nil
	}
	return nil, nil
}

func WriteTorrentInfo(tx kv.RwTx, info *TorrentInfo) error {
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return tx.Put(kv.BittorrentInfo, []byte(info.Name), infoBytes)
}

func CheckFileComplete(tx kv.Tx, name string, snapDir string) (bool, int64, *time.Time) {
	info, err := ReadTorrentInfo(tx, name)
	if err != nil {
		return false, 0, nil
	}
	if info.Completed != nil && info.Completed.Before(time.Now()) {
		if info.Length != nil {
			if fi, err := os.Stat(filepath.Join(snapDir, name)); err == nil {
				return fi.Size() == *info.Length && fi.ModTime().Equal(*info.Completed), *info.Length, info.Completed
			}
		}
	}
	return false, 0, nil
}

var AllCompleteFlagKey = []byte("all_complete")

func WriteSegmentsDownloadComplete(tx kv.RwTx) error {
	return tx.Put(kv.BittorrentInfo, AllCompleteFlagKey, []byte{1})
}
func ReadSegmentsDownloadComplete(tx kv.Tx) (bool, error) {
	v, err := tx.GetOne(kv.BittorrentInfo, AllCompleteFlagKey)
	if err != nil {
		return false, err
	}
	if len(v) == 0 {
		return false, nil
	}
	return v[0] == 1, nil
}

func openDB(dirs datadir.Dirs) (db kv.RwDB, exists bool, err error) {
	if exists, err := dir.FileExist(filepath.Join(dirs.Downloader, "mdbx.dat")); err != nil || !exists {
		return nil, false, err
	}

	limiter := semaphore.NewWeighted(9_000)
	downloaderDB, err := kv2.NewMDBX(log.Root()).Label(kv.DownloaderDB).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TablesCfgByLabel(kv.DownloaderDB)
	}).RoTxsLimiter(limiter).Path(dirs.Downloader).Accede().Open(context.Background())
	if err != nil {
		return nil, false, err
	}
	return downloaderDB, true, nil
}

func ReadSegmentsDownloadCompleteWithoutDB(dirs datadir.Dirs) (allFilesDownloadComplete bool, err error) {
	downloaderDB, exists, err := openDB(dirs)
	if err != nil || !exists {
		return false, err
	}
	defer downloaderDB.Close()

	if err := downloaderDB.View(context.Background(), func(tx kv.Tx) error {
		allFilesDownloadComplete, err = ReadSegmentsDownloadComplete(tx)
		return err
	}); err != nil {
		return false, err
	}
	return allFilesDownloadComplete, nil
}
func WriteSegmentsDownloadCompleteWithoutDB(dirs datadir.Dirs) (err error) {
	downloaderDB, exists, err := openDB(dirs)
	if err != nil || !exists {
		return err
	}
	defer downloaderDB.Close()

	if err := downloaderDB.Update(context.Background(), WriteSegmentsDownloadComplete); err != nil {
		return err
	}
	return nil
}