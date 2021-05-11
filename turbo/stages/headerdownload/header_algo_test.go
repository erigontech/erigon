package headerdownload

import (
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
)

func TestInserter1(t *testing.T) {
	db := ethdb.NewMemKV()
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	// Set up parent difficulty
	if err := rawdb.WriteTd(tx, common.Hash{}, 4, big.NewInt(0)); err != nil {
		t.Fatalf("write parent diff: %v", err)
	}
	hi := NewHeaderInserter("headers", big.NewInt(0), 0)
	if err := hi.FeedHeader(tx, &types.Header{Number: big.NewInt(5), Difficulty: big.NewInt(1)}, 5); err != nil {
		t.Errorf("feed empty header: %v", err)
	}
}
