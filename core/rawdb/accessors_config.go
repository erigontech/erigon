package rawdb

import (
	"github.com/ledgerwatch/erigon-lib/kv"
)

var historyV2EnabledKey = []byte("history.v2")

func HisoryV2Enabled(tx kv.Tx) (bool, error) {
	return kv.GetBool(tx, kv.DatabaseInfo, historyV2EnabledKey)
}
func WriteHisoryV2(tx kv.RwTx, v bool) (bool, error) {
	_, enabled, err := kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, historyV2EnabledKey, v)
	return enabled, err
}
