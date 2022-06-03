package snap

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
)

var (
	blockSnapshotEnabledKey = []byte("blocksSnapshotEnabled")
)

func Enabled(tx kv.Getter) (bool, error) {
	return kv.GetBool(tx, kv.DatabaseInfo, blockSnapshotEnabledKey)
}

func EnsureNotChanged(tx kv.GetPut, cfg ethconfig.Snapshot) (bool, ethconfig.SyncMode, error) {
	ok, v, err := kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, blockSnapshotEnabledKey, cfg.Enabled)
	if err != nil {
		return false, "", err
	}
	if !ok {
		if v {
			return false, ethconfig.SnapSync, nil
		} else {
			return false, ethconfig.FullSync, nil
		}
	}
	return true, "", nil
}

// ForceSetFlags - if you know what you are doing
func ForceSetFlags(tx kv.GetPut, cfg ethconfig.Snapshot) error {
	if cfg.Enabled {
		if err := tx.Put(kv.DatabaseInfo, blockSnapshotEnabledKey, []byte{1}); err != nil {
			return err
		}
	} else {
		if err := tx.Put(kv.DatabaseInfo, blockSnapshotEnabledKey, []byte{0}); err != nil {
			return err
		}
	}
	return nil
}
