package bodydownload

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestCreateBodyDownload(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	bd := NewBodyDownload(100)
	if err := bd.UpdateFromDb(db); err != nil {
		t.Fatalf("update from db: %v", err)
	}
}
