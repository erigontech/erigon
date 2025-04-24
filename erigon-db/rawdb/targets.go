package rawdb

import (
	"bytes"
	"os"

	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
)

var CodePrefix = []byte("c") // CodePrefix + code hash -> account code

func NewMemoryDatabase() kv.RwDB {
	tmp := os.TempDir()

	return memdb.New(tmp, kv.ChainDB)
}

// IsCodeKey reports whether the given byte slice is the key of contract code,
// if so return the raw code hash as well.
func IsCodeKey(key []byte) (bool, []byte) {
	if bytes.HasPrefix(key, CodePrefix) && len(key) == length.Hash+len(CodePrefix) {
		return true, key[len(CodePrefix):]
	}
	return false, nil
}
