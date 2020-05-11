package stateless

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/migrations"
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
	bDisk, err := txDisk.CreateBucket(dbutils.CurrentStateBucket, true)
	check(err)
	count := 0
	err = ethDb.WalkAsOf(dbutils.CurrentStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, blockNum+1,
		func(key []byte, value []byte) (bool, error) {
			if len(key) != 32 {
				return false, nil
			}
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
				bDisk = txDisk.Bucket(dbutils.CurrentStateBucket)
			}
			return true, nil
		},
	)
	check(err)
	err = txDisk.Commit()
	check(err)
	txDisk, err = diskDb.Begin(true)
	check(err)
	b := txDisk.Bucket(dbutils.CurrentStateBucket)
	count = 0
	var address common.Address
	//var hash common.Hash
	exist := make(map[common.Address]bool)
	var sk [52]byte
	err = ethDb.WalkAsOf(dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, sk[:], 0, blockNum,
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
			if err = b.Put(common.CopyBytes(key), common.CopyBytes(value)); err != nil {
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
				b = txDisk.Bucket(dbutils.CurrentStateBucket)
			}
			return true, nil
		},
	)
	check(err)
	err = txDisk.Commit()
	check(err)
}

type bucketWriter struct {
	db      ethdb.Database
	bucket  []byte
	pending ethdb.DbWithPendingMutations
	written uint64
}

func (bw *bucketWriter) printStats() {
	if bw.written == 0 {
		fmt.Printf(" -- nothing to copy for bucket: '%s'...", string(bw.bucket))
	} else {
		fmt.Printf("\r -- commited %d records for bucket: '%s'...", bw.written, string(bw.bucket))
	}
}

func (bw *bucketWriter) walker(k, v []byte) (bool, error) {
	if bw.pending == nil {
		bw.pending = bw.db.NewBatch()
	}

	if err := bw.pending.Put(bw.bucket, common.CopyBytes(k), common.CopyBytes(v)); err != nil {
		return false, err
	}
	bw.written++

	if bw.pending.BatchSize() >= 100000 {
		if _, err := bw.pending.Commit(); err != nil {
			return false, err
		}

		bw.printStats()

		bw.pending = bw.db.NewBatch()
	}

	return true, nil
}

func (bw *bucketWriter) commit() error {
	defer bw.printStats()

	if bw.pending != nil {
		_, err := bw.pending.Commit()
		return err
	}

	return nil
}

func newBucketWriter(db ethdb.Database, bucket []byte) *bucketWriter {
	return &bucketWriter{
		db:      db,
		bucket:  bucket,
		pending: nil,
		written: 0,
	}
}

func copyDatabase(fromDB ethdb.Database, toDB ethdb.Database) error {
	for _, bucket := range [][]byte{dbutils.CurrentStateBucket, dbutils.CodeBucket, dbutils.DatabaseInfoBucket} {
		fmt.Printf(" - copying bucket '%s'...\n", string(bucket))
		writer := newBucketWriter(toDB, bucket)

		if err := fromDB.Walk(bucket, nil, 0, writer.walker); err != nil {
			fmt.Println("FAIL")
			return err
		}

		if err := writer.commit(); err != nil {
			fmt.Println("FAIL")
			return err
		}
		fmt.Println("OK")
	}
	return nil
}

func saveSnapshot(db ethdb.Database, filename string, createDb CreateDbFunc) {
	fmt.Printf("Saving snapshot to %s\n", filename)

	diskDb, err := createDb(filename)
	check(err)
	defer diskDb.Close()

	err = copyDatabase(db, diskDb)
	check(err)
}

func loadSnapshot(db ethdb.Database, filename string, createDb CreateDbFunc) {
	fmt.Printf("Loading snapshot from %s\n", filename)

	diskDb, err := createDb(filename)
	check(err)
	defer diskDb.Close()

	err = copyDatabase(diskDb, db)
	check(err)
	err = migrations.NewMigrator().Apply(diskDb, false, false, false, false, false)
	check(err)
}

func loadCodes(db *bolt.DB, codeDb ethdb.Database) error {
	var account accounts.Account
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.CurrentStateBucket)
		cb, err := tx.CreateBucket(dbutils.CodeBucket, true)
		if err != nil {
			return err
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(k) != 32 {
				continue
			}
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
		b := tx.Bucket(dbutils.CurrentStateBucket)
		preimage := tx.Bucket(dbutils.PreimagePrefix)
		count := 0
		err = diskDb.View(func(txDisk *bolt.Tx) error {
			bDisk := txDisk.Bucket(dbutils.CurrentStateBucket)
			cDisk := bDisk.Cursor()
			for k, v := cDisk.First(); k != nil; k, v = cDisk.Next() {
				if len(k) != 32 {
					continue
				}
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
			count = 0
			cDisk = bDisk.Cursor()
			for k, v := cDisk.First(); k != nil; k, v = cDisk.Next() {
				if len(k) == 32 {
					continue
				}
				vv, _ := b.Get(k)
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
				if len(k) != 32 {
					continue
				}
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
			c = b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if len(k) == 32 {
					continue
				}
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
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	check(err)
}

func checkRoots(stateDb ethdb.Database, rootHash common.Hash, blockNum uint64) {
	startTime := time.Now()
	if blockNum > 0 {
		r := trie.NewResolver(blockNum)
		fmt.Printf("new resolve request for root block with hash %x\n", rootHash)
		rs := trie.NewResolveSet(0)
		subTries, err := r.ResolveWithDb(stateDb, blockNum, rs, [][]byte{nil}, []int{0}, false)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
		if subTries.Hashes[0] != rootHash {
			fmt.Printf("State root hash mismatch, got %x, expected %x\n", subTries.Hashes[0], rootHash)
		}
		fmt.Printf("Trie computation took %v\n", time.Since(startTime))
	} else {
		fmt.Printf("block number is unknown, account trie verification skipped\n")
	}
	startTime = time.Now()
	roots := make(map[common.Hash]*accounts.Account)
	incarnationMap := make(map[uint64]int)
	if err := stateDb.Walk(dbutils.CurrentStateBucket, nil, 0, func(k, v []byte) (bool, error) {
		var addrHash common.Hash
		copy(addrHash[:], k[:32])
		if _, ok := roots[addrHash]; !ok {
			account := &accounts.Account{}
			if ok, err := rawdb.ReadAccount(stateDb, addrHash, account); err != nil {
				return false, err
			} else if !ok {
				roots[addrHash] = nil
			} else {
				roots[addrHash] = account
				incarnationMap[account.Incarnation]++
			}
		}
		return true, nil
	}); err != nil {
		panic(err)
	}
	fmt.Printf("Incarnation map: %v\n", incarnationMap)

	for addrHash, account := range roots {
		if account != nil {
			sr := trie.NewResolver(blockNum)
			contractPrefix := make([]byte, common.HashLength+common.IncarnationLength)
			copy(contractPrefix, addrHash[:])
			binary.BigEndian.PutUint64(contractPrefix[common.HashLength:], ^account.Incarnation)
			var nibbles = make([]byte, 2*len(contractPrefix))
			for i, b := range contractPrefix {
				nibbles[i*2] = b / 16
				nibbles[i*2+1] = b % 16
			}
			rs := trie.NewResolveSet(0)
			subTries, err := sr.ResolveWithDb(stateDb, blockNum, rs, [][]byte{contractPrefix}, []int{8*len(contractPrefix)}, false)
			if err != nil {
				fmt.Printf("%x: %v\n", addrHash, err)
				fmt.Printf("incarnation: %d, account.Root: %x\n", account.Incarnation, account.Root)
			}
			if subTries.Hashes[0] != account.Root {
				fmt.Printf("Storage root hash mismatch for %x, got %x, expected %x\n", addrHash, subTries.Hashes[0], account.Root)
			}
		}
	}
	fmt.Printf("Storage trie computation took %v\n", time.Since(startTime))
}

func StateSnapshot(blockNum uint64) error {
	startTime := time.Now()
	//ethDb, err := ethdb.NewBoltDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb4/turbo-geth-copy/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata1")
	check(err)
	defer ethDb.Close()
	stateDb, db := ethdb.NewMemDatabase2()
	defer stateDb.Close()
	if _, err := os.Stat("statedb0"); err == nil {
		loadSnapshot(stateDb, "statedb0", func(path string) (ethdb.Database, error) {
			return ethdb.NewBoltDatabase(path)
		})
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
	checkRoots(ethDb, block.Root(), blockNum)
	return nil
}

func VerifySnapshot(path string) {
	ethDb, err := ethdb.NewBoltDatabase(path)
	check(err)
	defer ethDb.Close()
	hash := rawdb.ReadHeadBlockHash(ethDb)
	number := rawdb.ReadHeaderNumber(ethDb, hash)
	var currentBlockNr uint64
	var preRoot common.Hash
	if number != nil {
		header := rawdb.ReadHeader(ethDb, hash, *number)
		currentBlockNr = *number
		preRoot = header.Root
	}
	fmt.Printf("Block number: %d\n", currentBlockNr)
	fmt.Printf("Block root hash: %x\n", preRoot)
	checkRoots(ethDb, preRoot, currentBlockNr)
}
