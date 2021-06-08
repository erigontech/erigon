package bodydownload

import (
	"testing"

	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/ethdb"
)

func TestCreateBodyDownload(t *testing.T) {
	_, tx := ethdb.NewTestTx(t)
	bd := NewBodyDownload(100, ethash.NewFaker())
	if _, _, _, err := bd.UpdateFromDb(tx); err != nil {
		t.Fatalf("update from db: %v", err)
	}
}
