package bodydownload

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
)

func TestCreateBodyDownload(t *testing.T) {
	db := ethdb.NewMemKV()
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	bd := NewBodyDownload(100, ethash.NewFaker())
	if _, _, _, err := bd.UpdateFromDb(tx); err != nil {
		t.Fatalf("update from db: %v", err)
	}
}
