package headerdownload_test

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
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

func TestInserter1(t *testing.T) {
	m := stages.Mock(t)
	funds := big.NewInt(1000000000)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := params.AllProtocolChanges
	gspec := &core.Genesis{
		Config: chainConfig,
		Alloc: core.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	db := memdb.NewTestDB(t)
	defer db.Close()
	_, genesis, err := core.CommitGenesisBlock(db, gspec, "")
	if err != nil {
		t.Fatal(err)
	}
	var tx kv.RwTx
	if tx, err = db.BeginRw(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	hi := headerdownload.NewHeaderInserter("headers", big.NewInt(0), 0, snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3))
	h1 := types.Header{
		Number:     big.NewInt(1),
		Difficulty: big.NewInt(10),
		ParentHash: genesis.Hash(),
	}
	h1Hash := h1.Hash()
	h2 := types.Header{
		Number:     big.NewInt(2),
		Difficulty: big.NewInt(1010),
		ParentHash: h1Hash,
	}
	h2Hash := h2.Hash()
	data1, _ := rlp.EncodeToBytes(&h1)
	if _, err = hi.FeedHeaderPoW(tx, snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3), &h1, data1, h1Hash, 1); err != nil {
		t.Errorf("feed empty header 1: %v", err)
	}
	data2, _ := rlp.EncodeToBytes(&h2)
	if _, err = hi.FeedHeaderPoW(tx, snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3), &h2, data2, h2Hash, 2); err != nil {
		t.Errorf("feed empty header 2: %v", err)
	}
}
