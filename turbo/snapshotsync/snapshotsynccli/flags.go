package snapshotsynccli

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
)

var (
	blockSnapshotEnabledKey = []byte("blocksSnapshotEnabled")
)

func EnsureNotChanged(tx kv.GetPut, cfg ethconfig.Snapshot) error {
	ok, v, err := kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, blockSnapshotEnabledKey, cfg.Enabled)
	if err != nil {
		return err
	}
	if !ok {
		if v {
			return fmt.Errorf("node was started with --syncmode=snap, can't change it")
		} else {
			return fmt.Errorf("node was started with --syncmode=fast, can't change it")
		}
	}
	return nil
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
