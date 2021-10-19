package headerdownload

import (
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

func TestInserter1(t *testing.T) {
	funds := big.NewInt(1000000000)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := params.AllEthashProtocolChanges
	gspec := &core.Genesis{
		Config: chainConfig,
		Alloc: core.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	db := memdb.NewTestDB(t)
	defer db.Close()
	_, genesis, err := core.CommitGenesisBlock(db, gspec)
	if err != nil {
		t.Fatal(err)
	}
	var tx kv.RwTx
	if tx, err = db.BeginRw(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	hi := NewHeaderInserter("headers", big.NewInt(0), 0)
	var h1, h2 types.Header
	h1.Number = big.NewInt(1)
	h1.Difficulty = big.NewInt(10)
	h1.ParentHash = genesis.Hash()
	h2.Number = big.NewInt(2)
	h2.Difficulty = big.NewInt(1010)
	h2.ParentHash = h1.Hash()
	if err = hi.FeedHeader(tx, &h1, 1); err != nil {
		t.Errorf("feed empty header 1: %v", err)
	}
	if err = hi.FeedHeader(tx, &h2, 2); err != nil {
		t.Errorf("feed empty header 2: %v", err)
	}
}
