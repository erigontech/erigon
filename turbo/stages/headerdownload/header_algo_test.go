package headerdownload

import (
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
)

func TestInserter1(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	// Set up parent difficulty
	if err := rawdb.WriteTd(tx, common.Hash{}, 4, big.NewInt(0)); err != nil {
		t.Fatalf("write parent diff: %v", err)
	}
	hi := NewHeaderInserter("headers", big.NewInt(0), 0)
	if err := hi.FeedHeader(tx, &types.Header{Number: big.NewInt(5), Difficulty: big.NewInt(1)}, 5); err != nil {
		t.Errorf("feed empty header: %v", err)
	}
}
