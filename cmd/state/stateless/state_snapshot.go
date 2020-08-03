package stateless

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/migrations"
	"github.com/ledgerwatch/turbo-geth/trie"
)

// NOTE: This file is not the part of the Turbo-Geth binary. It i s part of the experimental utility, state
// to perform data analysis related to the state growth, state rent, and statelesss clients

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
	err = migrations.NewMigrator().Apply(diskDb, "")
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

//nolint
func compare_snapshot(stateDb ethdb.Database, db ethdb.KV, filename string) {
	fmt.Printf("Loading snapshot from %s\n", filename)

	diskDb := ethdb.MustOpen(filename)
	defer diskDb.Close()
	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
		b := tx.Bucket(dbutils.CurrentStateBucket)
		preimage := tx.Bucket(dbutils.PreimagePrefix)
		count := 0
		if err := diskDb.KV().View(context.Background(), func(txDisk ethdb.Tx) error {
			bDisk := txDisk.Bucket(dbutils.CurrentStateBucket)
			cDisk := bDisk.Cursor()
			for k, v, err := cDisk.First(); k != nil; k, v, err = cDisk.Next() {
				if err != nil {
					return err
				}
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
			for k, v, err := cDisk.First(); k != nil; k, v, err = cDisk.Next() {
				if err != nil {
					return err
				}
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
			for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
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
			for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
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
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func checkRoots(stateDb ethdb.Database, rootHash common.Hash, blockNum uint64) {
	startTime := time.Now()
	if blockNum > 0 {
		l := trie.NewSubTrieLoader(blockNum)
		fmt.Printf("new resolve request for root block with hash %x\n", rootHash)
		rl := trie.NewRetainList(0)
		subTries, err := l.LoadSubTries(stateDb, blockNum, rl, nil /* HashCollector */, [][]byte{nil}, []int{0}, false)
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
			sl := trie.NewSubTrieLoader(blockNum)
			contractPrefix := make([]byte, common.HashLength+common.IncarnationLength)
			copy(contractPrefix, addrHash[:])
			binary.BigEndian.PutUint64(contractPrefix[common.HashLength:], account.Incarnation)
			rl := trie.NewRetainList(0)
			subTries, err := sl.LoadSubTries(stateDb, blockNum, rl, nil /* HashCollector */, [][]byte{contractPrefix}, []int{8 * len(contractPrefix)}, false)
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

func VerifySnapshot(path string) {
	ethDb := ethdb.MustOpen(path)
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
