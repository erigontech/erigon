package bodydownload_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/consensus/ethash"
)

func TestCreateBodyDownload(t *testing.T) {
	m := stages.Mock(t)
	tx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	blockReader, _ := m.NewBlocksIO()
	bd := bodydownload.NewBodyDownload(ethash.NewFaker(), 100, blockReader)
	if _, _, _, _, err := bd.UpdateFromDb(tx); err != nil {
		t.Fatalf("update from db: %v", err)
	}
}
