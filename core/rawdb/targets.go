package rawdb

import (
	"os"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
)

var CodePrefix = []byte("c") // CodePrefix + code hash -> account code

func NewMemoryDatabase() kv.RwDB {
	tmp := os.TempDir()

	return memdb.New(tmp, kv.ChainDB)
}
