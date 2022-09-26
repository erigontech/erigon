package rawdb

import (
	"github.com/ledgerwatch/erigon-lib/kv"
)

type ConfigKey []byte

var (
	HistoryV3 = ConfigKey("history.v3")
)

func (k ConfigKey) Enabled(tx kv.Tx) (bool, error) { return kv.GetBool(tx, kv.DatabaseInfo, k) }
func (k ConfigKey) WriteOnce(tx kv.RwTx, v bool) (bool, error) {
	_, enabled, err := kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, k, v)
	return enabled, err
}
func (k ConfigKey) EnsureNotChanged(tx kv.RwTx, value bool) (ok, enabled bool, err error) {
	return kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, k, value)
}
func (k ConfigKey) ForceWrite(tx kv.RwTx, enabled bool) error {
	if enabled {
		if err := tx.Put(kv.DatabaseInfo, k, []byte{1}); err != nil {
			return err
		}
	} else {
		if err := tx.Put(kv.DatabaseInfo, k, []byte{0}); err != nil {
			return err
		}
	}
	return nil
}
