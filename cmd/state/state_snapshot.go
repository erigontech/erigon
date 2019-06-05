package main

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/bolt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/trie"
)

func construct_snapshot(ethDb ethdb.Database, stateDb ethdb.Database, db *bolt.DB, blockNum uint64) {
	diskDb, err := bolt.Open("statedb", 0600, &bolt.Options{})
	check(err)
	defer diskDb.Close()
	var startKey [32]byte
	tx, err := db.Begin(true)
	check(err)
	txDisk, err := diskDb.Begin(true)
	check(err)
	b, err := tx.CreateBucket(state.AccountsBucket, true)
	check(err)
	bDisk, err := txDisk.CreateBucket(state.AccountsBucket, true)
	check(err)
	count := 0
	err = ethDb.WalkAsOf(state.AccountsBucket, state.AccountsHistoryBucket, startKey[:], 0, blockNum+1,
		func(key []byte, value []byte) (bool, error) {
			if len(value) == 0 {
				return true, nil
			}
			if err := b.Put(key, value); err != nil {
				return false, err
			}
			if err := bDisk.Put(key, value); err != nil {
				return false, err
			}
			count++
			if count%1000 == 0 {
				if err := tx.Commit(); err != nil {
					return false, err
				}
				if err := txDisk.Commit(); err != nil {
					return false, err
				}
				fmt.Printf("Committed %d records\n", count)
				var err error
				tx, err = db.Begin(true)
				if err != nil {
					return false, err
				}
				txDisk, err = diskDb.Begin(true)
				if err != nil {
					return false, err
				}
				b = tx.Bucket(state.AccountsBucket)
				bDisk = txDisk.Bucket(state.AccountsBucket)
			}
			return true, nil
		},
	)
	check(err)
	err = tx.Commit()
	check(err)
	err = txDisk.Commit()
	check(err)
	tx, err = db.Begin(true)
	check(err)
	txDisk, err = diskDb.Begin(true)
	check(err)
	b = tx.Bucket(state.AccountsBucket)
	sb, err := tx.CreateBucket(state.StorageBucket, true)
	check(err)
	sbDisk, err := txDisk.CreateBucket(state.StorageBucket, true)
	check(err)
	count = 0
	var address common.Address
	//var hash common.Hash
	exist := make(map[common.Address]bool)
	var sk [52]byte
	err = ethDb.WalkAsOf(state.StorageBucket, state.StorageHistoryBucket, sk[:], 0, blockNum,
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
			if err := sb.Put(key, value); err != nil {
				return false, err
			}
			if err := sbDisk.Put(key, value); err != nil {
				return false, err
			}
			count++
			if count%1000 == 0 {
				if err := tx.Commit(); err != nil {
					return false, err
				}
				if err := txDisk.Commit(); err != nil {
					return false, err
				}
				fmt.Printf("Committed %d records\n", count)
				var err error
				tx, err = db.Begin(true)
				if err != nil {
					return false, err
				}
				txDisk, err = diskDb.Begin(true)
				if err != nil {
					return false, err
				}
				b = tx.Bucket(state.AccountsBucket)
				sb = tx.Bucket(state.StorageBucket)
				sbDisk = txDisk.Bucket(state.StorageBucket)
			}
			return true, nil
		},
	)
	check(err)
	err = tx.Commit()
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
	bDisk, err := diskTx.CreateBucket(state.AccountsBucket, true)
	check(err)
	sbDisk, err := diskTx.CreateBucket(state.StorageBucket, true)
	check(err)
	count := 0
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.AccountsBucket)
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
				bDisk = diskTx.Bucket(state.AccountsBucket)
				sbDisk = diskTx.Bucket(state.StorageBucket)
			}
		}
		b = tx.Bucket(state.StorageBucket)
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
				bDisk = diskTx.Bucket(state.AccountsBucket)
				sbDisk = diskTx.Bucket(state.StorageBucket)
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
	defer diskDb.Close()
	tx, err := db.Begin(true)
	check(err)
	b, err := tx.CreateBucket(state.AccountsBucket, true)
	sb, err := tx.CreateBucket(state.StorageBucket, true)
	check(err)
	count := 0
	err = diskDb.View(func(txDisk *bolt.Tx) error {
		bDisk := txDisk.Bucket(state.AccountsBucket)
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
				b = tx.Bucket(state.AccountsBucket)
				sb = tx.Bucket(state.StorageBucket)
			}
		}
		sbDisk := txDisk.Bucket(state.StorageBucket)
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
				b = tx.Bucket(state.AccountsBucket)
				sb = tx.Bucket(state.StorageBucket)
			}
		}
		return nil
	})
	check(err)
	err = tx.Commit()
	check(err)
}

func load_codes(db *bolt.DB, codeDb ethdb.Database) {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.AccountsBucket)
		cb, err := tx.CreateBucket(state.CodeBucket, true)
		if err != nil {
			return err
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			account, err := encodingToAccount(v)
			if err != nil {
				return err
			}
			if !bytes.Equal(account.CodeHash, emptyCodeHash) {
				code, _ := codeDb.Get(state.CodeBucket, account.CodeHash)
				if code != nil {
					cb.Put(account.CodeHash, code)
				}
			}
		}
		return nil
	})
	check(err)
}

func compare_snapshot(stateDb ethdb.Database, db *bolt.DB, filename string) {
	fmt.Printf("Loading snapshot from %s\n", filename)
	diskDb, err := bolt.Open(filename, 0600, &bolt.Options{})
	check(err)
	defer diskDb.Close()
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.AccountsBucket)
		sb := tx.Bucket(state.StorageBucket)
		preimage := tx.Bucket([]byte("secure-key-"))
		count := 0
		err = diskDb.View(func(txDisk *bolt.Tx) error {
			bDisk := txDisk.Bucket(state.AccountsBucket)
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
			sbDisk := txDisk.Bucket(state.StorageBucket)
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
	t := trie.New(rootHash, state.AccountsBucket, nil, false)
	r := trie.NewResolver(false, true, blockNum)
	key := []byte{}
	tc := t.NewContinuation(key, 0, rootHash[:])
	r.AddContinuation(tc)
	err := r.ResolveWithDb(stateDb, blockNum)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	fmt.Printf("Trie computation took %v\n", time.Since(startTime))
	startTime = time.Now()
	var address common.Address
	roots := make(map[common.Address]common.Hash)
	err = db.View(func(tx *bolt.Tx) error {
		sb := tx.Bucket(state.StorageBucket)
		b := tx.Bucket(state.AccountsBucket)
		c := sb.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			copy(address[:], k[:20])
			if _, ok := roots[address]; !ok {
				if enc, _ := b.Get(crypto.Keccak256(address[:])); enc == nil {
					roots[address] = common.Hash{}
				} else {
					account, err := encodingToAccount(enc)
					if err != nil {
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
		if root != (common.Hash{}) && root != emptyRoot {
			st := trie.New(root, state.StorageBucket, address[:], true)
			sr := trie.NewResolver(false, false, blockNum)
			key := []byte{}
			stc := st.NewContinuation(key, 0, root[:])
			sr.AddContinuation(stc)
			err = sr.ResolveWithDb(stateDb, blockNum)
			if err != nil {
				fmt.Printf("%x: %v\n", address, err)
			}
		}
	}
	fmt.Printf("Storage trie computation took %v\n", time.Since(startTime))
}

func state_snapshot() {
	startTime := time.Now()
	var blockNum uint64 = uint64(*block)
	//ethDb, err := ethdb.NewBoltDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb4/turbo-geth-10/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata1")
	check(err)
	defer ethDb.Close()
	stateDb, db := ethdb.NewMemDatabase2()
	defer stateDb.Close()
	if _, err := os.Stat("statedb0"); err == nil {
		load_snapshot(db, "statedb0")
		load_codes(db, ethDb)
	} else {
		construct_snapshot(ethDb, stateDb, db, blockNum)
	}
	fmt.Printf("Snapshot took %v\n", time.Since(startTime))
	startTime = time.Now()
	bc, err := core.NewBlockChain(ethDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil)
	check(err)
	block := bc.GetBlockByNumber(blockNum)
	fmt.Printf("Block number: %d\n", blockNum)
	fmt.Printf("Block root hash: %x\n", block.Root())
	check_roots(stateDb, db, block.Root(), blockNum)
	nextBlock := bc.GetBlockByNumber(blockNum + 1)
	fmt.Printf("Next Block number: %d\n", blockNum+1)
	fmt.Printf("Next Block root hash: %x\n", nextBlock.Root())

	chainConfig := params.MainnetChainConfig
	vmConfig := vm.Config{}
	engine := ethash.NewFullFaker()
	batch := stateDb.NewBatch()
	tds, err := state.NewTrieDbState(block.Root(), batch, blockNum)
	tds.SetNoHistory(true)
	statedb := state.New(tds)
	fmt.Printf("Gas limit: %d\n", nextBlock.GasLimit())
	gp := new(core.GasPool).AddGas(nextBlock.GasLimit())
	usedGas := new(uint64)
	nextHeader := nextBlock.Header()
	var receipts types.Receipts
	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(nextBlock.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	for i, tx := range nextBlock.Transactions() {
		statedb.Prepare(tx.Hash(), nextBlock.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, bc, nil, gp, statedb, tds.TrieStateWriter(), nextHeader, tx, usedGas, vmConfig)
		if err != nil {
			panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
		}
		if !chainConfig.IsByzantium(nextHeader.Number) {
			rootHash, err := tds.TrieRoot()
			if err != nil {
				panic(fmt.Errorf("tx %d, %x failed: %v", i, tx.Hash(), err))
			}
			receipt.PostState = rootHash.Bytes()
		}
		receipts = append(receipts, receipt)
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	_, err = engine.Finalize(chainConfig, nextHeader, statedb, nextBlock.Transactions(), nextBlock.Uncles(), receipts)
	if err != nil {
		panic(fmt.Errorf("Finalize of block %d failed: %v", blockNum+1, err))
	}
	nextRoot, err := tds.IntermediateRoot(statedb, chainConfig.IsEIP158(nextHeader.Number))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Next root %x\n", nextRoot)
	err = statedb.Commit(chainConfig.IsEIP158(nextHeader.Number), tds.DbStateWriter())
	if err != nil {
		panic(fmt.Errorf("Commiting block %d failed: %v", blockNum+1, err))
	}
	if _, err := batch.Commit(); err != nil {
		panic(err)
	}
	check_roots(stateDb, db, nextBlock.Root(), blockNum+1)
	//compare_snapshot(stateDb, db, "statedb1")
}

func verify_snapshot() {
	//ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb4/turbo-geth-10/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	engine := ethash.NewFullFaker()
	chainConfig := params.MainnetChainConfig
	bcb, err := core.NewBlockChain(ethDb, nil, chainConfig, engine, vm.Config{}, nil)
	check(err)
	blockNum := uint64(*block)
	block := bcb.GetBlockByNumber(blockNum)
	fmt.Printf("Block number: %d\n", blockNum)
	fmt.Printf("Block root hash: %x\n", block.Root())
	preRoot := block.Root()
	stateDb, db := ethdb.NewMemDatabase2()
	defer stateDb.Close()
	load_snapshot(db, fmt.Sprintf("state_%d", blockNum))
	check_roots(stateDb, db, preRoot, blockNum)
}
