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
	bd := bodydownload.NewBodyDownload(ethash.NewFaker(), 100, m.BlockReader)
	if _, _, _, _, err := bd.UpdateFromDb(tx); err != nil {
		t.Fatalf("update from db: %v", err)
	}
}
