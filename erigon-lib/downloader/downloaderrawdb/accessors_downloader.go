package downloaderrawdb

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
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

func allFilesComplete(tx kv.Tx, preverifiedCfg *snapcfg.Cfg, dirs datadir.Dirs) (allFilesDownloadComplete bool, lastUncomplete string) {
	for _, p := range preverifiedCfg.Preverified {
		complete, _, _ := CheckFileComplete(tx, p.Name, dirs.Snap)
		if !complete {
			return false, p.Name
		}
	}
	return true, ""
}

func AllFilesComplete(preverifiedCfg *snapcfg.Cfg, dirs datadir.Dirs) (allFilesDownloadComplete bool, lastUncomplete string, err error) {
	limiter := semaphore.NewWeighted(9_000)
	downloaderDB, err := kv2.NewMDBX(log.Root()).Label(kv.DownloaderDB).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TablesCfgByLabel(kv.DownloaderDB)
	}).RoTxsLimiter(limiter).Path(dirs.Downloader).Accede().Open(context.Background())
	if err != nil {
		return false, "", err
	}
	defer downloaderDB.Close()

	if err := downloaderDB.View(context.Background(), func(tx kv.Tx) error {
		allFilesDownloadComplete, lastUncomplete = allFilesComplete(tx, preverifiedCfg, dirs)
		return nil
	}); err != nil {
		return false, "", err
	}
	return allFilesDownloadComplete, lastUncomplete, nil
}
