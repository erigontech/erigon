package mock

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

func withHermezDb(tx kv.RwTx) error {
	return hermez_db.CreateHermezBuckets(tx)
}
