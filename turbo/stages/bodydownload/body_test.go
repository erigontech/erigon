package bodydownload

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestCreateBodyDownload(t *testing.T) {
	_, tx := ethdb.NewTestTx(t)
	bd := NewBodyDownload(100, ethash.NewFaker())
	if _, _, _, err := bd.UpdateFromDb(tx); err != nil {
		t.Fatalf("update from db: %v", err)
	}
}
