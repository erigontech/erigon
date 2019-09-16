package pruner

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
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
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address  = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr  = common.Address{1}
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
				address1: {Balance: funds},
				address2: {Balance: funds},
			},
		}
		genesis   = gspec.MustCommit(db)
		genesisDb = db.MemCopy()
		// this code generates a log
		signer = types.HomesteadSigner{}
	)

	nubBlocks:=10
	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blocks, _ := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, nubBlocks, func(i int, block *core.BlockGen) {
		var (
			tx     *types.Transaction
			genErr error
		)
		var addr common.Address
		var k *ecdsa.PrivateKey
		switch i%3 {
		case 0:
			addr = address
			k = key
		case 1:
			addr = address1
			k = key1
		case 2:
			addr = address2
			k = key2
		}
		tx, genErr = types.SignTx(types.NewTransaction(block.TxNonce(addr), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, k)
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

	err=db.Walk(dbutils.SuffixBucket,[]byte{}, 0, func(k, v []byte) (b bool, e error) {
		fmt.Println("---------------------------------------------")
		ts, _:=dbutils.DecodeTimestamp(k)
		fmt.Println("k", k, "ts",  ts)

		fmt.Println("AccountsHistoryBucket", bytes.HasSuffix(k, dbutils.AccountsHistoryBucket))
		fmt.Println("StorageHistoryBucket", bytes.HasSuffix(k, dbutils.StorageHistoryBucket))

		fmt.Println("v", v)
		d:=dbutils.Suffix(v)
		d.Walk(func(k []byte) error {
			fmt.Println("-- ", common.Bytes2Hex(k))
			compKey,_:=dbutils.CompositeKeySuffix(k, ts)
			b, err:=db.Get(dbutils.AccountsHistoryBucket, compKey)
			if err!=nil {
				fmt.Println(err)
			} else {
				acc:=&accounts.Account{}
				fmt.Println(b)
				errInn:=acc.DecodeForStorage(b)
				if errInn!=nil {
					fmt.Println(errInn)
				} else {
					spew.Dump(acc)
				}
			}

			return nil
		})
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
}


func decodeTimestamp(suffix []byte) (uint64, []byte) {
	bytecount := int(suffix[0] >> 5)
	timestamp := uint64(suffix[0] & 0x1f)
	for i := 1; i < bytecount; i++ {
		timestamp = (timestamp << 8) | uint64(suffix[i])
	}
	return timestamp, suffix[bytecount:]
}


// If highZero is true, the most significant bits of every byte is left zero
func encodeTimestamp(timestamp uint64) []byte {
	var suffix []byte
	var limit uint64
	limit = 32
	for bytecount := 1; bytecount <= 8; bytecount++ {
		if timestamp < limit {
			suffix = make([]byte, bytecount)
			b := timestamp
			for i := bytecount - 1; i > 0; i-- {
				suffix[i] = byte(b & 0xff)
				b >>= 8
			}
			suffix[0] = byte(b) | (byte(bytecount) << 5) // 3 most significant bits of the first byte are bytecount
			break
		}
		limit <<= 8
	}
	return suffix
}