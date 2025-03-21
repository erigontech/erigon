package stages

import (
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/zk/hermez_db"
)

func withHermezDb(tx kv.RwTx) error {
	return hermez_db.CreateHermezBuckets(tx)
}
