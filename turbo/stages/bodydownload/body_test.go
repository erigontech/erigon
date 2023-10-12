package bodydownload_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/consensus/ethash"
)

func TestCreateBodyDownload(t *testing.T) {
	m := mock.Mock(t)
	tx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	bd := bodydownload.NewBodyDownload(ethash.NewFaker(), 128, 100, m.BlockReader, m.Log)
	if _, _, _, _, err := bd.UpdateFromDb(tx); err != nil {
		t.Fatalf("update from db: %v", err)
	}
}
