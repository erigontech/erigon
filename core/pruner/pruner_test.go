package pruner

import (
	"fmt"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"math/big"
	"testing"
)

func TestOne(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		//key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address  = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		//address2 = crypto.PubkeyToAddress(key2.PublicKey)
		//theAddr  = common.Address{1}
		funds = big.NewInt(1000000000)
		gspec = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				EIP2027Block:        big.NewInt(4),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
			},
		}
		genesis   = gspec.MustCommit(db)
		genesisDb = db.MemCopy()
		// this code generates a log
		signer = types.HomesteadSigner{}
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blocks, _ := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, 3, func(i int, block *core.BlockGen) {
		var (
			tx     *types.Transaction
			genErr error
		)

		switch i {
		case 0:
			tx, genErr = types.SignTx(types.NewTransaction(block.TxNonce(address), address1, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 1:
			tx, genErr = types.SignTx(types.NewTransaction(block.TxNonce(address), address1, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 2:
			tx, genErr = types.SignTx(types.NewTransaction(block.TxNonce(address), address1, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		}
		if genErr != nil {
			t.Fatal(genErr)
		}
		block.AddTx(tx)
	})

	for i := range blocks {
		_, err = blockchain.InsertChain(types.Blocks{blocks[i]})
		if err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("=============================================================================================")

	err = db.DB().View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			var nameCopy = make([]byte, len(name))
			copy(nameCopy, name)
			return b.ForEach(func(k, _ []byte) error {
				var keyCopy = make([]byte, len(k)+len(name))
				copy(keyCopy, nameCopy)
				copy(keyCopy[len(name):], k)
				fmt.Println(" - ", string(keyCopy))
				fmt.Println(" -- ", keyCopy)

				return nil
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("=============================================================================================")

	//db.Get(dbutils.SuffixBucket, )

	//st, _, _ := blockchain.State()
	//if !st.Exist(address) {
	//	t.Error("expected account to exist")
	//}

}
