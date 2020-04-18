package main

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/trie"
)

// NOTE: This file is not the part of the Turbo-Geth binary. It i s part of the experimental utility, state
// to perform data analysis related to the state growth, state rent, and statelesss clients

func constructSnapshot(ethDb ethdb.Database, blockNum uint64) {
	diskDb, err := bolt.Open(fmt.Sprintf("/Volumes/tb4/turbo-geth-copy/state_%d", blockNum), 0600, &bolt.Options{})
	check(err)
	defer diskDb.Close()
	var startKey [32]byte
	txDisk, err := diskDb.Begin(true)
	check(err)
	bDisk, err := txDisk.CreateBucket(dbutils.AccountsBucket, true)
	check(err)
	count := 0
	err = ethDb.WalkAsOf(dbutils.AccountsBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, blockNum+1,
		func(key []byte, value []byte) (bool, error) {
			if len(value) == 0 {
				return true, nil
			}
			if err = bDisk.Put(common.CopyBytes(key), common.CopyBytes(value)); err != nil {
				return false, err
			}
			count++
			if count%1000 == 0 {
				if err := txDisk.Commit(); err != nil {
					return false, err
				}
				fmt.Printf("Committed %d records\n", count)
				var err error
				txDisk, err = diskDb.Begin(true)
				if err != nil {
					return false, err
				}
				bDisk = txDisk.Bucket(dbutils.AccountsBucket)
			}
			return true, nil
		},
	)
	check(err)
	err = txDisk.Commit()
	check(err)
	txDisk, err = diskDb.Begin(true)
	check(err)
	b := txDisk.Bucket(dbutils.AccountsBucket)
	sbDisk, err := txDisk.CreateBucket(dbutils.StorageBucket, true)
	check(err)
	count = 0
	var address common.Address
	//var hash common.Hash
	exist := make(map[common.Address]bool)
	var sk [52]byte
	err = ethDb.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, sk[:], 0, blockNum,
		func(key []byte, value []byte) (bool, error) {
			if len(value) == 0 {
				return true, nil
			}
			copy(address[:], key[:20])
			if e, ok := exist[address]; ok {
				if !e {
					return true, nil
				}
			} else {
				v, _ := b.Get(crypto.Keccak256(address[:]))
				exist[address] = v != nil
			}
			if err = sbDisk.Put(common.CopyBytes(key), common.CopyBytes(value)); err != nil {
				return false, err
			}
			count++
			if count%1000 == 0 {
				if err := txDisk.Commit(); err != nil {
					return false, err
				}
				fmt.Printf("Committed %d records\n", count)
				var err error
				txDisk, err = diskDb.Begin(true)
				if err != nil {
					return false, err
				}
				b = txDisk.Bucket(dbutils.AccountsBucket)
				sbDisk = txDisk.Bucket(dbutils.StorageBucket)
			}
			return true, nil
		},
	)
	check(err)
	err = txDisk.Commit()
	check(err)
}

func save_snapshot(db *bolt.DB, filename string) {
	fmt.Printf("Saving snapshot to %s\n", filename)
	diskDb, err := bolt.Open(filename, 0600, &bolt.Options{})
	check(err)
	defer diskDb.Close()
	diskTx, err := diskDb.Begin(true)
	check(err)
	bDisk, err := diskTx.CreateBucket(dbutils.AccountsBucket, true)
	check(err)
	sbDisk, err := diskTx.CreateBucket(dbutils.StorageBucket, true)
	check(err)
	count := 0
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.AccountsBucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := bDisk.Put(common.CopyBytes(k), common.CopyBytes(v)); err != nil {
				return err
			}
			count++
			if count%100000 == 0 {
				if err := diskTx.Commit(); err != nil {
					return err
				}
				fmt.Printf("Commited %d records\n", count)
				diskTx, err = diskDb.Begin(true)
				bDisk = diskTx.Bucket(dbutils.AccountsBucket)
				sbDisk = diskTx.Bucket(dbutils.StorageBucket)
			}
		}
		b = tx.Bucket(dbutils.StorageBucket)
		c = b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := sbDisk.Put(common.CopyBytes(k), common.CopyBytes(v)); err != nil {
				return err
			}
			count++
			if count%100000 == 0 {
				if err := diskTx.Commit(); err != nil {
					return err
				}
				fmt.Printf("Commited %d records\n", count)
				diskTx, err = diskDb.Begin(true)
				bDisk = diskTx.Bucket(dbutils.AccountsBucket)
				sbDisk = diskTx.Bucket(dbutils.StorageBucket)
			}
		}
		return nil
	})
	check(err)
	err = diskTx.Commit()
	check(err)
}

func load_snapshot(db *bolt.DB, filename string) {
	fmt.Printf("Loading snapshot from %s\n", filename)
	diskDb, err := bolt.Open(filename, 0600, &bolt.Options{})
	check(err)
	tx, err := db.Begin(true)
	check(err)
	b, err := tx.CreateBucket(dbutils.AccountsBucket, true)
	check(err)
	sb, err := tx.CreateBucket(dbutils.StorageBucket, true)
	check(err)
	count := 0
	err = diskDb.View(func(txDisk *bolt.Tx) error {
		bDisk := txDisk.Bucket(dbutils.AccountsBucket)
		cDisk := bDisk.Cursor()
		for k, v := cDisk.First(); k != nil; k, v = cDisk.Next() {
			if err := b.Put(common.CopyBytes(k), common.CopyBytes(v)); err != nil {
				return err
			}
			count++
			if count%100000 == 0 {
				if err := tx.Commit(); err != nil {
					return err
				}
				fmt.Printf("Committed %d records\n", count)
				var err error
				tx, err = db.Begin(true)
				if err != nil {
					return err
				}
				b = tx.Bucket(dbutils.AccountsBucket)
				sb = tx.Bucket(dbutils.StorageBucket)
			}
		}
		sbDisk := txDisk.Bucket(dbutils.StorageBucket)
		count = 0
		cDisk = sbDisk.Cursor()
		for k, v := cDisk.First(); k != nil; k, v = cDisk.Next() {
			if err := sb.Put(common.CopyBytes(k), common.CopyBytes(v)); err != nil {
				return err
			}
			count++
			if count%100000 == 0 {
				if err := tx.Commit(); err != nil {
					return err
				}
				fmt.Printf("Committed %d records\n", count)
				var err error
				tx, err = db.Begin(true)
				if err != nil {
					return err
				}
				b = tx.Bucket(dbutils.AccountsBucket)
				sb = tx.Bucket(dbutils.StorageBucket)
			}
		}
		return nil
	})
	check(err)
	err = tx.Commit()
	check(err)
	diskDb.Close()
}

func loadCodes(db *bolt.DB, codeDb ethdb.Database) error {
	var account accounts.Account
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.AccountsBucket)
		cb, err := tx.CreateBucket(dbutils.CodeBucket, true)
		if err != nil {
			return err
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := account.DecodeForStorage(v); err != nil {
				return err
			}
			if !account.IsEmptyCodeHash() {
				code, _ := codeDb.Get(dbutils.CodeBucket, account.CodeHash[:])
				if code != nil {
					if err := cb.Put(account.CodeHash[:], code); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	return err
}

func compare_snapshot(stateDb ethdb.Database, db *bolt.DB, filename string) {
	fmt.Printf("Loading snapshot from %s\n", filename)
	diskDb, err := bolt.Open(filename, 0600, &bolt.Options{})
	check(err)
	defer diskDb.Close()
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.AccountsBucket)
		sb := tx.Bucket(dbutils.StorageBucket)
		preimage := tx.Bucket(dbutils.PreimagePrefix)
		count := 0
		err = diskDb.View(func(txDisk *bolt.Tx) error {
			bDisk := txDisk.Bucket(dbutils.AccountsBucket)
			cDisk := bDisk.Cursor()
			for k, v := cDisk.First(); k != nil; k, v = cDisk.Next() {
				vv, _ := b.Get(k)
				p, _ := preimage.Get(k)
				if !bytes.Equal(v, vv) {
					fmt.Printf("Diff for %x (%x): disk: %x, mem: %x\n", k, p, v, vv)
				}
				count++
				if count%100000 == 0 {
					fmt.Printf("Compared %d records\n", count)
				}
			}
			sbDisk := txDisk.Bucket(dbutils.StorageBucket)
			count = 0
			cDisk = sbDisk.Cursor()
			for k, v := cDisk.First(); k != nil; k, v = cDisk.Next() {
				vv, _ := sb.Get(k)
				if !bytes.Equal(v, vv) {
					fmt.Printf("Diff for %x: disk: %x, mem: %x\n", k, v, vv)
				}
				count++
				if count%100000 == 0 {
					fmt.Printf("Committed %d records\n", count)
				}
			}
			count = 0
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				vv, _ := bDisk.Get(k)
				p, _ := preimage.Get(k)
				if len(vv) == 0 {
					fmt.Printf("Diff for %x (%x): disk: %x, mem: %x\n", k, p, vv, v)
				}
				count++
				if count%100000 == 0 {
					fmt.Printf("Compared %d records\n", count)
				}
			}
			c = sb.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				vv, _ := sbDisk.Get(k)
				p, _ := preimage.Get(k)
				if len(vv) == 0 {
					fmt.Printf("Diff for %x (%x): disk: %x, mem: %x\n", k, p, vv, v)
				}
				count++
				if count%100000 == 0 {
					fmt.Printf("Compared %d records\n", count)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	check(err)
}

func check_roots(stateDb ethdb.Database, db *bolt.DB, rootHash common.Hash, blockNum uint64) {
	startTime := time.Now()
	t := trie.New(rootHash)
	r := trie.NewResolver(0, true, blockNum)
	key := []byte{}
	req := t.NewResolveRequest(nil, key, 0, rootHash[:])
	r.AddRequest(req)
	err := r.ResolveWithDb(stateDb, blockNum)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	fmt.Printf("Trie computation took %v\n", time.Since(startTime))
	startTime = time.Now()
	var address common.Address
	roots := make(map[common.Address]common.Hash)
	err = db.View(func(tx *bolt.Tx) error {
		sb := tx.Bucket(dbutils.StorageBucket)
		b := tx.Bucket(dbutils.AccountsBucket)
		c := sb.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			copy(address[:], k[:20])
			if _, ok := roots[address]; !ok {
				if enc, _ := b.Get(crypto.Keccak256(address[:])); enc == nil {
					roots[address] = common.Hash{}
				} else {
					var account accounts.Account
					if err = account.DecodeForStorage(enc); err != nil {
						return err
					}
					roots[address] = account.Root
				}
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	for address, root := range roots {
		if root != (common.Hash{}) && root != trie.EmptyRoot {
			st := trie.New(root)
			sr := trie.NewResolver(32, false, blockNum)
			key := []byte{}
			contractPrefix := make([]byte, common.HashLength+state.IncarnationLength)
			addrHash := crypto.Keccak256(address.Bytes())
			copy(contractPrefix, addrHash)
			// TODO Issue 99 [Boris] support incarnations
			streq := st.NewResolveRequest(contractPrefix, key, 0, root[:])
			sr.AddRequest(streq)
			err = sr.ResolveWithDb(stateDb, blockNum)
			if err != nil {
				fmt.Printf("%x: %v\n", address, err)
				filename := fmt.Sprintf("tries/root_%x.txt", address)
				f, err := os.Create(filename)
				if err == nil {
					defer f.Close()
					st.Print(f)
				}
			}
		}
	}
	fmt.Printf("Storage trie computation took %v\n", time.Since(startTime))
}

func stateSnapshot() error {
	startTime := time.Now()
	var blockNum uint64 = uint64(*block)
	//ethDb, err := ethdb.NewBoltDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb4/turbo-geth-copy/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata1")
	check(err)
	defer ethDb.Close()
	stateDb, db := ethdb.NewMemDatabase2()
	defer stateDb.Close()
	if _, err := os.Stat("statedb0"); err == nil {
		load_snapshot(db, "statedb0")
		if err := loadCodes(db, ethDb); err != nil {
			return err
		}
	} else {
		constructSnapshot(ethDb, blockNum)
	}
	fmt.Printf("Snapshot took %v\n", time.Since(startTime))
	startTime = time.Now()
	bc, err := core.NewBlockChain(ethDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil)
	check(err)
	block := bc.GetBlockByNumber(blockNum)
	fmt.Printf("Block number: %d\n", blockNum)
	fmt.Printf("Block root hash: %x\n", block.Root())
	check_roots(ethDb, ethDb.DB(), block.Root(), blockNum)
	return nil
}

func verify_snapshot() {
	blockNum := uint64(*block)
	ethDb, err := ethdb.NewBoltDatabase(fmt.Sprintf("/Volumes/tb4/turbo-geth-copy/state_%d", blockNum))
	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	engine := ethash.NewFullFaker()
	chainConfig := params.MainnetChainConfig
	bcb, err := core.NewBlockChain(ethDb, nil, chainConfig, engine, vm.Config{}, nil)
	check(err)
	block := bcb.GetBlockByNumber(blockNum)
	fmt.Printf("Block number: %d\n", blockNum)
	fmt.Printf("Block root hash: %x\n", block.Root())
	preRoot := block.Root()
	stateDb, db := ethdb.NewMemDatabase2()
	defer stateDb.Close()
	load_snapshot(db, fmt.Sprintf("state_%d", blockNum))
	check_roots(ethDb, db, preRoot, blockNum)
}
