package snapshotsynccli

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
)

var (
	blockSnapshotEnabledKey       = []byte("blocksSnapshotEnabled")
	blockSnapshotRetireEnabledKey = []byte("blocksSnapshotRetireEnabled")
)

func EnsureNotChanged(tx kv.GetPut, cfg ethconfig.Snapshot) error {
	ok, v, err := kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, blockSnapshotEnabledKey, cfg.Enabled)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("node was started with --%s=%v, can't change it", ethconfig.FlagSnapshot, v)
	}

	bytesTrue := []byte{1}
	tx.Put(kv.DatabaseInfo, blockSnapshotRetireEnabledKey, bytesTrue)

	ok, v, err = kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, blockSnapshotRetireEnabledKey, cfg.RetireEnabled)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("node was started with --%s=%v, can't change it", ethconfig.FlagSnapshotRetire, v)
	}
	return nil
}
