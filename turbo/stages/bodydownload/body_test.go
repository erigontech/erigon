package bodydownload

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"testing"
)

func TestCreateBodyDownload(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	bd := NewBodyDownload()
	if err := bd.RestoreFromDb(db); err != nil {
		t.Fatalf("restore from db: %v", err)
	}
}
