package downloaderrawdb

import (
	"os"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon-lib/fastjson"
	"github.com/erigontech/erigon-lib/kv"
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
	if err = fastjson.Unmarshal(infoBytes, &info); err != nil {
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
	if err = fastjson.Unmarshal(infoBytes, &info); err == nil {
		return info.Hash, nil
	}
	return nil, nil
}

func WriteTorrentInfo(tx kv.RwTx, info *TorrentInfo) error {
	infoBytes, err := fastjson.Marshal(info)
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
