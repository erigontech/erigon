package stages_test


import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/stages"
)


// Tests that chain reorganisations handle transaction removals and reinsertions.
func TestChainTxReorgs2(t *testing.T) {
	var (
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key2, _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		key3, _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		addr2   = crypto.PubkeyToAddress(key2.PublicKey)
		addr3   = crypto.PubkeyToAddress(key3.PublicKey)
		gspec   = &core.Genesis{
			Config:   params.TestChainConfig,
			GasLimit: 3141592,
			Alloc: core.GenesisAlloc{
				addr1: {Balance: big.NewInt(1000000)},
				addr2: {Balance: big.NewInt(1000000)},
				addr3: {Balance: big.NewInt(1000000)},
			},
		}
		signer = types.LatestSigner(gspec.Config)
	)

	sm:=ethdb.DefaultStorageMode
	sm.SnapshotLayout=true
	m := stages.MockWithGenesisStorageMode(t, gspec, key1, sm)
	m2 := stages.MockWithGenesisStorageMode(t, gspec, key1, sm)
	defer m2.DB.Close()

	tx1,err := types.SignTx(types.NewTransaction(0, addr1, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key2)
	if err!=nil {
		t.Fatal(err)
	}
	tx2replaced,err := types.SignTx(types.NewTransaction(1, addr1, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key2)
	if err!=nil {
		t.Fatal(err)
	}
	tx2added,err := types.SignTx(types.NewTransaction(0, addr1, uint256.NewInt(10000), params.TxGas, nil, nil), *signer, key3)
	if err!=nil {
		t.Fatal(err)
	}




	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 9, func(i int, gen *core.BlockGen) {
		switch i {
		case 1:
			gen.AddTx(tx1)

		case 7:
			gen.AddTx(tx2replaced)
			gen.OffsetTime(9) // Lower the block difficulty to simulate a weaker chain
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	// Import the chain. This runs all block validation rules.
	if err1 := m.InsertChain(chain); err1 != nil {
		t.Fatalf("failed to insert original chain: %v", err1)
	}

	// overwrite the old chain
	chain, err = core.GenerateChain(m2.ChainConfig, m2.Genesis, m2.Engine, m2.DB, 10, func(i int, gen *core.BlockGen) {
		switch i {
		case 1:
			gen.AddTx(tx1)

		case 7:
			gen.AddTx(tx2added)
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	if err := m.InsertChain(chain); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}
	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// added tx
	txs := types.Transactions{tx1, tx2replaced, tx2added}
	for i, txn := range txs {
		if txn, _, _, _, _ := rawdb.ReadTransaction(tx, txn.Hash()); txn == nil {
			t.Logf("add %d: expected tx to be found", i)
		}
		if rcpt, _, _, _, _ := rawdb.ReadReceipt(tx, txn.Hash()); rcpt == nil {
			t.Logf("add %d: expected receipt to be found", i)
		}
	}

	err = tx.ForEach(dbutils.PlainStateBucket, []byte{}, func(k, v []byte) error {
		acc:=&accounts.Account{}
		err = acc.DecodeForStorage(v)
		if err!=nil {
		    return err
		}
		fmt.Println(common.Bytes2Hex(k), acc)
		return nil
	})
	if err!=nil {
	    t.Fatal(err)
	}
}
