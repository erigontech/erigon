package snapshotsync

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"strconv"

	"github.com/ledgerwatch/erigon-lib/kv"
)

func SnapshotName(baseDir, name string, blockNum uint64) string {
	return filepath.Join(baseDir, name) + strconv.FormatUint(blockNum, 10)
}

func GetSnapshotInfo(db kv.RwDB) (uint64, []byte, error) {
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()
	v, err := tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotBlock)
	if err != nil {
		return 0, nil, err
	}
	if v == nil {
		return 0, nil, err
	}
	var snapshotBlock uint64
	if len(v) == 8 {
		snapshotBlock = binary.BigEndian.Uint64(v)
	}

	infohash, err := tx.GetOne(kv.BittorrentInfo, kv.CurrentHeadersSnapshotHash)
	if err != nil {
		return 0, nil, err
	}
	if infohash == nil {
		return 0, nil, err
	}
	return snapshotBlock, infohash, nil
}
