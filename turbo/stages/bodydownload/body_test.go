package bodydownload

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"

	"github.com/ledgerwatch/erigon/consensus/ethash"
)

func TestCreateBodyDownload(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	bd := NewBodyDownload(100, ethash.NewFaker())
	if _, _, _, _, err := bd.UpdateFromDb(tx); err != nil {
		t.Fatalf("update from db: %v", err)
	}
}
