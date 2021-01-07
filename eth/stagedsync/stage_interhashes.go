package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

func SpawnIntermediateHashesStage(s *StageState, db ethdb.Database, checkRoot bool, cache *shards.StateCache, tmpdir string, quit <-chan struct{}) error {
	to, err := s.ExecutionAt(db)
	if err != nil {
		return err
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		s.Done()
		return nil
	}
	//fmt.Printf("%d->%d\n", s.BlockNumber, to)
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	hash, err := rawdb.ReadCanonicalHash(tx, to)
	if err != nil {
		return err
	}
	syncHeadHeader := rawdb.ReadHeader(tx, hash, to)
	expectedRootHash := syncHeadHeader.Root

	logPrefix := s.state.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Generating intermediate hashes", logPrefix), "from", s.BlockNumber, "to", to)
	if s.BlockNumber == 0 {
		if err := RegenerateIntermediateHashes(logPrefix, tx, checkRoot, cache, tmpdir, expectedRootHash, quit); err != nil {
			return err
		}
	} else {
		if err := incrementIntermediateHashes(logPrefix, s, tx, to, checkRoot, cache, tmpdir, expectedRootHash, quit); err != nil {
			return err
		}
	}

	if err := s.DoneAndUpdate(tx, to); err != nil {
		return err
	}

	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func RegenerateIntermediateHashes(logPrefix string, db ethdb.Database, checkRoot bool, cache *shards.StateCache, tmpdir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	log.Info(fmt.Sprintf("[%s] Regeneration intermediate hashes started", logPrefix))
	// Clear IH bucket
	c := db.(ethdb.HasTx).Tx().Cursor(dbutils.IntermediateHashOfAccountBucket)
	for k, _, err := c.First(); k != nil; k, _, err = c.First() {
		if err != nil {
			return err
		}
		if err = c.DeleteCurrent(); err != nil {
			return err
		}
	}
	c.Close()
	c = db.(ethdb.HasTx).Tx().Cursor(dbutils.IntermediateHashOfStorageBucket)
	for k, _, err := c.First(); k != nil; k, _, err = c.First() {
		if err != nil {
			return err
		}
		if err = c.DeleteCurrent(); err != nil {
			return err
		}
	}
	c.Close()

	if cache != nil {
		for i := 0; i < 16; i++ {
			unfurl := trie.NewRetainList(0)
			accountIHCollector := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
			buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
			comparator := db.(ethdb.HasTx).Tx().Comparator(dbutils.IntermediateHashOfStorageBucket)
			buf.SetComparator(comparator)
			storageIHCollector := etl.NewCollector(tmpdir, buf)
			hashCollector := func(keyHex []byte, hash []byte) error {
				if len(keyHex) == 0 {
					return nil
				}
				return accountIHCollector.Collect(keyHex, hash)
			}
			storageHashCollector := func(accWithInc []byte, keyHex []byte, hash []byte) error {
				if len(keyHex) == 0 {
					return nil
				}
				if len(hash) == 0 {
					return nil
				}
				return storageIHCollector.Collect(accWithInc, append(keyHex, hash...))
			}
			loader := trie.NewFlatDBTrieLoader(logPrefix)
			// hashCollector in the line below will collect deletes
			if err := loader.Reset(unfurl, hashCollector, storageHashCollector, false); err != nil {
				return err
			}
			hash, err := loader.CalcTrieRoot(db, []byte{uint8(i)}, quit)
			if err != nil {
				return err
			}
			cache.SetAccountHashWrite([]byte{uint8(i)}, hash.Bytes())
			if err := accountIHCollector.Load(logPrefix, db,
				dbutils.IntermediateHashOfAccountBucket,
				etl.IdentityLoadFunc,
				etl.TransformArgs{
					Quit: quit,
				},
			); err != nil {
				return err
			}
			if err := storageIHCollector.Load(logPrefix, db,
				dbutils.IntermediateHashOfStorageBucket,
				etl.IdentityLoadFunc,
				etl.TransformArgs{
					Comparator: comparator,
					Quit:       quit,
				},
			); err != nil {
				return err
			}
		}

		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(trie.NewRetainList(0), func(keyHex []byte, hash []byte) error {
			return nil
		}, func(accWithInc []byte, keyHex []byte, hash []byte) error {
			return nil
		}, false); err != nil {
			return err
		}
		t := time.Now()
		hash, err := loader.CalcTrieRootOnCache2(cache)
		if err != nil {
			return err
		}
		generationIHTook := time.Since(t)
		if checkRoot && hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Collection finished", logPrefix),
			"root hash", hash.Hex(),
			"gen IH", generationIHTook,
		)

		writes := cache.PrepareWrites()
		shards.WalkAccountHashesWrites(writes, func(prefix []byte, hash common.Hash) {
			if err := db.Put(dbutils.IntermediateHashOfAccountBucket, prefix, hash.Bytes()); err != nil {
				panic(err)
			}
		}, func(prefix []byte, hash common.Hash) {
			if err := db.Delete(dbutils.IntermediateHashOfAccountBucket, prefix, nil); err != nil {
				panic(err)
			}
		})
		shards.WalkStorageHashesWrites(writes, func(addrHash common.Hash, incarnation uint64, prefix []byte, hash common.Hash) {
			newKey := make([]byte, 40)
			copy(newKey, addrHash.Bytes())
			binary.BigEndian.PutUint64(newKey[32:], incarnation)
			if err := db.Put(dbutils.IntermediateHashOfStorageBucket, newKey, append(prefix, hash.Bytes()...)); err != nil {
				panic(err)
			}
		}, func(addrHash common.Hash, incarnation uint64, prefix []byte, hash common.Hash) {
			newKey := make([]byte, 40)
			copy(newKey, addrHash.Bytes())
			binary.BigEndian.PutUint64(newKey[32:], incarnation)
			if err := db.Delete(dbutils.IntermediateHashOfStorageBucket, newKey, append(prefix, hash.Bytes()...)); err != nil {
				panic(err)
			}
		})
		cache.TurnWritesToReads(writes)
	} else {
		accountIHCollector := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
		comparator := db.(ethdb.HasTx).Tx().Comparator(dbutils.IntermediateHashOfStorageBucket)
		buf.SetComparator(comparator)
		storageIHCollector := etl.NewCollector(tmpdir, buf)
		hashCollector := func(keyHex []byte, hash []byte) error {
			if len(keyHex) == 0 {
				return nil
			}
			return accountIHCollector.Collect(keyHex, hash)
		}
		storageHashCollector := func(accWithInc []byte, keyHex []byte, hash []byte) error {
			if len(keyHex) == 0 {
				return nil
			}
			if len(hash) == 0 {
				return nil
			}
			return storageIHCollector.Collect(accWithInc, append(keyHex, hash...))
		}
		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(trie.NewRetainList(0), hashCollector /* HashCollector */, storageHashCollector, false); err != nil {
			return err
		}
		t := time.Now()
		hash, err := loader.CalcTrieRoot(db, []byte{}, quit)
		if err != nil {
			return err
		}
		generationIHTook := time.Since(t)
		if checkRoot && hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Debug("Collection finished",
			"root hash", hash.Hex(),
			"gen IH", generationIHTook,
		)
		if err := accountIHCollector.Load(logPrefix, db,
			dbutils.IntermediateHashOfAccountBucket,
			etl.IdentityLoadFunc,
			etl.TransformArgs{
				Quit: quit,
			},
		); err != nil {
			return err
		}
		if err := storageIHCollector.Load(logPrefix, db,
			dbutils.IntermediateHashOfStorageBucket,
			etl.IdentityLoadFunc,
			etl.TransformArgs{
				Comparator: comparator,
				Quit:       quit,
			},
		); err != nil {
			return err
		}
	}
	log.Info(fmt.Sprintf("[%s] Regeneration ended", logPrefix))

	return nil
}

type HashPromoter struct {
	db               ethdb.Database
	ChangeSetBufSize uint64
	TempDir          string
	quitCh           <-chan struct{}
}

func NewHashPromoter(db ethdb.Database, quitCh <-chan struct{}) *HashPromoter {
	return &HashPromoter{
		db:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          os.TempDir(),
		quitCh:           quitCh,
	}
}

func (p *HashPromoter) Promote(logPrefix string, s *StageState, from, to uint64, storage bool, load etl.LoadFunc) error {
	var changeSetBucket string
	if storage {
		changeSetBucket = dbutils.PlainStorageChangeSetBucket
	} else {
		changeSetBucket = dbutils.PlainAccountChangeSetBucket
	}
	log.Debug(fmt.Sprintf("[%s] Incremental state promotion of intermediate hashes", logPrefix), "from", from, "to", to, "csbucket", changeSetBucket)

	startkey := dbutils.EncodeBlockNumber(from + 1)

	decode := changeset.Mapper[changeSetBucket].Decode
	extract := func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, _ := decode(dbKey, dbValue)
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		return next(dbKey, newK, nil)
	}

	var l OldestAppearedLoad
	l.innerLoadFunc = load

	if err := etl.Transform(
		logPrefix,
		p.db,
		changeSetBucket,
		"",
		p.TempDir,
		extract,
		l.LoadFunc,
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			Quit:            p.quitCh,
		},
	); err != nil {
		return err
	}
	return nil
}

func (p *HashPromoter) Unwind(logPrefix string, s *StageState, u *UnwindState, storage bool, load etl.LoadFunc) error {
	to := u.UnwindPoint
	var changeSetBucket string
	if storage {
		changeSetBucket = dbutils.PlainStorageChangeSetBucket
	} else {
		changeSetBucket = dbutils.PlainAccountChangeSetBucket
	}
	log.Info(fmt.Sprintf("[%s] Unwinding of intermediate hashes", logPrefix), "from", s.BlockNumber, "to", to, "csbucket", changeSetBucket)

	startkey := dbutils.EncodeBlockNumber(to + 1)

	decode := changeset.Mapper[changeSetBucket].Decode
	extract := func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, _ := decode(dbKey, dbValue)
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		return next(k, newK, nil)
	}

	var l OldestAppearedLoad
	l.innerLoadFunc = load

	if err := etl.Transform(
		logPrefix,
		p.db,
		changeSetBucket,
		"",
		p.TempDir,
		extract,
		l.LoadFunc,
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			Quit:            p.quitCh,
		},
	); err != nil {
		return err
	}
	return nil
}

func incrementIntermediateHashes(logPrefix string, s *StageState, db ethdb.Database, to uint64, checkRoot bool, cache *shards.StateCache, tmpdir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	p := NewHashPromoter(db, quit)
	p.TempDir = tmpdir
	var exclude [][]byte
	collect := func(k []byte, _ []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		exclude = append(exclude, k)
		return nil
	}
	if err := p.Promote(logPrefix, s, s.BlockNumber, to, false /* storage */, collect); err != nil {
		return err
	}
	if err := p.Promote(logPrefix, s, s.BlockNumber, to, true /* storage */, collect); err != nil {
		return err
	}

	defer func(t time.Time) { fmt.Printf("stage_interhashes.go:390: %s\n", time.Since(t)) }(time.Now())
	if cache != nil {
		var prefixes [16][][]byte
		for i := range exclude {
			id := exclude[i][0] / 16
			prefixes[id] = append(prefixes[id], exclude[i])
		}
		for i, prefix := range prefixes {
			sort.Slice(prefix, func(i, j int) bool { return bytes.Compare(prefix[i], prefix[j]) < 0 })
			unfurl := trie.NewRetainList(0)
			for i := range prefix {
				unfurl.AddKey(prefix[i])
			}

			accountIHCollector := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
			buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
			comparator := db.(ethdb.HasTx).Tx().Comparator(dbutils.IntermediateHashOfStorageBucket)
			buf.SetComparator(comparator)
			storageIHCollector := etl.NewCollector(tmpdir, buf)
			hashCollector := func(keyHex []byte, hash []byte) error {
				if len(keyHex) == 0 {
					return nil
				}
				return accountIHCollector.Collect(keyHex, hash)
			}
			storageHashCollector := func(accWithInc []byte, keyHex []byte, hash []byte) error {
				if len(keyHex) == 0 {
					return nil
				}
				if len(hash) == 0 {
					return nil
				}
				return storageIHCollector.Collect(accWithInc, append(keyHex, hash...))
			}
			// hashCollector in the line below will collect deletes
			loader := trie.NewFlatDBTrieLoader(logPrefix)
			if err := loader.Reset(unfurl, hashCollector, storageHashCollector, false); err != nil {
				return err
			}
			hash, err := loader.CalcTrieRoot(db, []byte{uint8(i)}, quit)
			if err != nil {
				return err
			}
			cache.SetAccountHashWrite([]byte{uint8(i)}, hash.Bytes())
			//if err := accountIHCollector.Load(logPrefix, db,
			//	dbutils.IntermediateHashOfAccountBucket,
			//	etl.IdentityLoadFunc,
			//	etl.TransformArgs{
			//		Quit: quit,
			//	},
			//); err != nil {
			//	return err
			//}
			//if err := storageIHCollector.Load(logPrefix, db,
			//	dbutils.IntermediateHashOfStorageBucket,
			//	etl.IdentityLoadFunc,
			//	etl.TransformArgs{
			//		Comparator: comparator,
			//		Quit:       quit,
			//	},
			//); err != nil {
			//	return err
			//}
		}

		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(trie.NewRetainList(0), func(keyHex []byte, hash []byte) error {
			return nil
		}, func(accWithInc []byte, keyHex []byte, hash []byte) error {
			return nil
		}, false); err != nil {
			return err
		}
		t := time.Now()
		hash, err := loader.CalcTrieRootOnCache2(cache)
		if err != nil {
			return err
		}
		generationIHTook := time.Since(t)
		if checkRoot && hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Collection finished", logPrefix),
			"root hash", hash.Hex(),
			"gen IH", generationIHTook,
		)

		writes := cache.PrepareWrites()
		shards.WalkAccountHashesWrites(writes, func(prefix []byte, hash common.Hash) {
			if err := db.Put(dbutils.IntermediateHashOfAccountBucket, prefix, hash.Bytes()); err != nil {
				panic(err)
			}
		}, func(prefix []byte, hash common.Hash) {
			if err := db.Delete(dbutils.IntermediateHashOfAccountBucket, prefix, nil); err != nil {
				panic(err)
			}
		})
		shards.WalkStorageHashesWrites(writes, func(addrHash common.Hash, incarnation uint64, prefix []byte, hash common.Hash) {
			newKey := make([]byte, 40)
			copy(newKey, addrHash.Bytes())
			binary.BigEndian.PutUint64(newKey[32:], incarnation)
			if err := db.Put(dbutils.IntermediateHashOfStorageBucket, newKey, append(prefix, hash.Bytes()...)); err != nil {
				panic(err)
			}
		}, func(addrHash common.Hash, incarnation uint64, prefix []byte, hash common.Hash) {
			newKey := make([]byte, 40)
			copy(newKey, addrHash.Bytes())
			binary.BigEndian.PutUint64(newKey[32:], incarnation)
			if err := db.Delete(dbutils.IntermediateHashOfStorageBucket, newKey, append(prefix, hash.Bytes()...)); err != nil {
				panic(err)
			}
		})
		cache.TurnWritesToReads(writes)

		/*
			hashCollector := func(keyHex []byte, hash []byte) error {
				if len(keyHex) == 0 {
					return nil
				}
				if len(hash) == 0 {
					return nil
				}
				cache.SetAccountHashWrite(keyHex, hash)
				return nil
			}
			storageHashCollector := func(accWithInc []byte, keyHex []byte, hash []byte) error {
				if len(keyHex) == 0 {
					return nil
				}
				if hash == nil {
					return nil
				}
				cache.SetStorageHashWrite(common.BytesToHash(accWithInc[:32]), binary.BigEndian.Uint64(accWithInc[32:]), keyHex, common.BytesToHash(hash))
				return nil
			}
			// hashCollector in the line below will collect deletes
			if err := loader.Reset(unfurl, hashCollector, storageHashCollector, false); err != nil {
				return err
			}
			t := time.Now()
			hash, err := loader.CalcTrieRootOnCache(db, []byte{}, nil, cache, quit)
			if err != nil {
				return err
			}
			generationIHTook := time.Since(t)
			if checkRoot && hash != expectedRootHash {
				return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
			}
			log.Info(fmt.Sprintf("[%s] Collection finished", logPrefix),
				"root hash", hash.Hex(),
				"gen IH", generationIHTook,
			)
			writes := cache.PrepareWrites()
			prefixes.WalkAccountHashesWrites(writes, func(prefix []byte, hash common.Hash) {
				if err := db.Put(dbutils.IntermediateHashOfAccountBucket, prefix, hash.Bytes()); err != nil {
					panic(err)
				}
			}, func(prefix []byte, hash common.Hash) {
				if err := db.Delete(dbutils.IntermediateHashOfAccountBucket, prefix, nil); err != nil {
					panic(err)
				}
			})
			prefixes.WalkStorageHashesWrites(writes, func(addrHash common.Hash, incarnation uint64, prefix []byte, hash common.Hash) {
				newKey := make([]byte, 40)
				copy(newKey, addrHash.Bytes())
				binary.BigEndian.PutUint64(newKey[32:], incarnation)
				if err := db.Put(dbutils.IntermediateHashOfStorageBucket, newKey, append(prefix, hash.Bytes()...)); err != nil {
					panic(err)
				}
			}, func(addrHash common.Hash, incarnation uint64, prefix []byte, hash common.Hash) {
				newKey := make([]byte, 40)
				copy(newKey, addrHash.Bytes())
				binary.BigEndian.PutUint64(newKey[32:], incarnation)
				if err := db.Delete(dbutils.IntermediateHashOfStorageBucket, newKey, append(prefix, hash.Bytes()...)); err != nil {
					panic(err)
				}
			})
			cache.TurnWritesToReads(writes)
		*/
	} else {
		sort.Slice(exclude, func(i, j int) bool { return bytes.Compare(exclude[i], exclude[j]) < 0 })
		unfurl := trie.NewRetainList(2)
		for i := range exclude {
			unfurl.AddKey(exclude[i])
		}

		accountIHCollector := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
		comparator := db.(ethdb.HasTx).Tx().Comparator(dbutils.IntermediateHashOfStorageBucket)
		buf.SetComparator(comparator)
		storageIHCollector := etl.NewCollector(tmpdir, buf)
		hashCollector := func(keyHex []byte, hash []byte) error {
			if len(keyHex) == 0 {
				return nil
			}
			return accountIHCollector.Collect(keyHex, hash)
		}
		storageHashCollector := func(accWithInc []byte, keyHex []byte, hash []byte) error {
			if len(keyHex) == 0 {
				return nil
			}
			if len(hash) == 0 {
				return nil
			}
			return storageIHCollector.Collect(accWithInc, append(keyHex, hash...))
		}
		// hashCollector in the line below will collect deletes
		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(unfurl, hashCollector, storageHashCollector, false); err != nil {
			return err
		}
		t := time.Now()
		hash, err := loader.CalcTrieRoot(db, []byte{}, quit)
		if err != nil {
			return err
		}
		generationIHTook := time.Since(t)
		if checkRoot && hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Collection finished", logPrefix),
			"root hash", hash.Hex(),
			"gen IH", generationIHTook,
		)
		if err := accountIHCollector.Load(logPrefix, db,
			dbutils.IntermediateHashOfAccountBucket,
			etl.IdentityLoadFunc,
			etl.TransformArgs{
				Quit: quit,
			},
		); err != nil {
			return err
		}
		if err := storageIHCollector.Load(logPrefix, db,
			dbutils.IntermediateHashOfStorageBucket,
			etl.IdentityLoadFunc,
			etl.TransformArgs{
				Comparator: comparator,
				Quit:       quit,
			},
		); err != nil {
			return err
		}
	}
	return nil
}

func UnwindIntermediateHashesStage(u *UnwindState, s *StageState, db ethdb.Database, cache *shards.StateCache, tmpdir string, quit <-chan struct{}) error {
	hash, err := rawdb.ReadCanonicalHash(db, u.UnwindPoint)
	if err != nil {
		return fmt.Errorf("read canonical hash: %w", err)
	}
	syncHeadHeader := rawdb.ReadHeader(db, hash, u.UnwindPoint)
	expectedRootHash := syncHeadHeader.Root
	//fmt.Printf("u: %d->%d\n", s.BlockNumber, u.UnwindPoint)

	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return fmt.Errorf("open transcation: %w", err)
		}
		defer tx.Rollback()
	}

	logPrefix := s.state.LogPrefix()
	if err := unwindIntermediateHashesStageImpl(logPrefix, u, s, tx, cache, tmpdir, expectedRootHash, quit); err != nil {
		return err
	}
	if err := u.Done(tx); err != nil {
		return fmt.Errorf("%s: reset: %w", logPrefix, err)
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindIntermediateHashesStageImpl(logPrefix string, u *UnwindState, s *StageState, db ethdb.Database, cache *shards.StateCache, tmpdir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	p := NewHashPromoter(db, quit)
	p.TempDir = tmpdir
	var exclude [][]byte
	collect := func(k []byte, _ []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		exclude = append(exclude, k)
		return nil
	}
	if err := p.Unwind(logPrefix, s, u, false /* storage */, collect); err != nil {
		return err
	}
	if err := p.Unwind(logPrefix, s, u, true /* storage */, collect); err != nil {
		return err
	}

	if cache != nil {
		var prefixes [16][][]byte
		for i := range exclude {
			id := exclude[i][0] / 16
			prefixes[id] = append(prefixes[id], exclude[i])
		}
		for i, prefix := range prefixes {
			sort.Slice(prefix, func(i, j int) bool { return bytes.Compare(prefix[i], prefix[j]) < 0 })
			unfurl := trie.NewRetainList(0)
			for i := range prefix {
				unfurl.AddKey(prefix[i])
			}

			accountIHCollector := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
			buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
			comparator := db.(ethdb.HasTx).Tx().Comparator(dbutils.IntermediateHashOfStorageBucket)
			buf.SetComparator(comparator)
			storageIHCollector := etl.NewCollector(tmpdir, buf)
			hashCollector := func(keyHex []byte, hash []byte) error {
				if len(keyHex) == 0 {
					return nil
				}
				return accountIHCollector.Collect(keyHex, hash)
			}
			storageHashCollector := func(accWithInc []byte, keyHex []byte, hash []byte) error {
				if len(keyHex) == 0 {
					return nil
				}
				if len(hash) == 0 {
					return nil
				}
				return storageIHCollector.Collect(accWithInc, append(keyHex, hash...))
			}
			loader := trie.NewFlatDBTrieLoader(logPrefix)
			// hashCollector in the line below will collect deletes
			if err := loader.Reset(unfurl, hashCollector, storageHashCollector, false); err != nil {
				return err
			}
			hash, err := loader.CalcTrieRoot(db, []byte{uint8(i)}, quit)
			if err != nil {
				return err
			}
			cache.SetAccountHashWrite([]byte{uint8(i)}, hash.Bytes())
			if err := accountIHCollector.Load(logPrefix, db,
				dbutils.IntermediateHashOfAccountBucket,
				etl.IdentityLoadFunc,
				etl.TransformArgs{
					Quit: quit,
				},
			); err != nil {
				return err
			}
			if err := storageIHCollector.Load(logPrefix, db,
				dbutils.IntermediateHashOfStorageBucket,
				etl.IdentityLoadFunc,
				etl.TransformArgs{
					Comparator: comparator,
					Quit:       quit,
				},
			); err != nil {
				return err
			}
		}

		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(trie.NewRetainList(0), func(keyHex []byte, hash []byte) error {
			return nil
		}, func(accWithInc []byte, keyHex []byte, hash []byte) error {
			return nil
		}, false); err != nil {
			return err
		}
		t := time.Now()
		hash, err := loader.CalcTrieRootOnCache2(cache)
		if err != nil {
			return err
		}
		generationIHTook := time.Since(t)
		if hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Collection finished", logPrefix),
			"root hash", hash.Hex(),
			"gen IH", generationIHTook,
		)

		writes := cache.PrepareWrites()
		shards.WalkAccountHashesWrites(writes, func(prefix []byte, hash common.Hash) {
			if err := db.Put(dbutils.IntermediateHashOfAccountBucket, prefix, hash.Bytes()); err != nil {
				panic(err)
			}
		}, func(prefix []byte, hash common.Hash) {
			if err := db.Delete(dbutils.IntermediateHashOfAccountBucket, prefix, nil); err != nil {
				panic(err)
			}
		})
		shards.WalkStorageHashesWrites(writes, func(addrHash common.Hash, incarnation uint64, prefix []byte, hash common.Hash) {
			newKey := make([]byte, 40)
			copy(newKey, addrHash.Bytes())
			binary.BigEndian.PutUint64(newKey[32:], incarnation)
			if err := db.Put(dbutils.IntermediateHashOfStorageBucket, newKey, append(prefix, hash.Bytes()...)); err != nil {
				panic(err)
			}
		}, func(addrHash common.Hash, incarnation uint64, prefix []byte, hash common.Hash) {
			newKey := make([]byte, 40)
			copy(newKey, addrHash.Bytes())
			binary.BigEndian.PutUint64(newKey[32:], incarnation)
			if err := db.Delete(dbutils.IntermediateHashOfStorageBucket, newKey, append(prefix, hash.Bytes()...)); err != nil {
				panic(err)
			}
		})
		cache.TurnWritesToReads(writes)

	} else {
		sort.Slice(exclude, func(i, j int) bool { return bytes.Compare(exclude[i], exclude[j]) < 0 })
		unfurl := trie.NewRetainList(0)
		for i := range exclude {
			unfurl.AddKey(exclude[i])
		}

		accountIHCollector := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
		comparator := db.(ethdb.HasTx).Tx().Comparator(dbutils.IntermediateHashOfStorageBucket)
		buf.SetComparator(comparator)
		storageIHCollector := etl.NewCollector(tmpdir, buf)
		hashCollector := func(keyHex []byte, hash []byte) error {
			if len(keyHex) == 0 {
				return nil
			}
			return accountIHCollector.Collect(keyHex, hash)
		}
		storageHashCollector := func(accWithInc []byte, keyHex []byte, hash []byte) error {
			if len(keyHex) == 0 {
				return nil
			}
			if hash == nil {
				return nil
			}
			return storageIHCollector.Collect(accWithInc, append(keyHex, hash...))
		}
		// hashCollector in the line below will collect deletes
		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(unfurl, hashCollector, storageHashCollector, false); err != nil {
			return err
		}
		t := time.Now()
		hash, err := loader.CalcTrieRoot(db, []byte{}, quit)
		if err != nil {
			return err
		}
		generationIHTook := time.Since(t)
		if hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Collection finished", logPrefix),
			"root hash", hash.Hex(),
			"gen IH", generationIHTook,
		)
		if err := accountIHCollector.Load(logPrefix, db,
			dbutils.IntermediateHashOfAccountBucket,
			etl.IdentityLoadFunc,
			etl.TransformArgs{
				Quit: quit,
			},
		); err != nil {
			return err
		}
		if err := storageIHCollector.Load(logPrefix, db,
			dbutils.IntermediateHashOfStorageBucket,
			etl.IdentityLoadFunc,
			etl.TransformArgs{
				Comparator: comparator,
				Quit:       quit,
			},
		); err != nil {
			return err
		}
	}
	return nil
}

func ResetHashState(db ethdb.Database) error {
	if err := db.(ethdb.BucketsMigrator).ClearBuckets(
		dbutils.HashedAccountsBucket,
		dbutils.HashedStorageBucket,
		dbutils.ContractCodeBucket,
	); err != nil {
		return err
	}
	batch := db.NewBatch()
	if err := stages.SaveStageProgress(batch, stages.HashState, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(batch, stages.HashState, 0); err != nil {
		return err
	}
	if _, err := batch.Commit(); err != nil {
		return err
	}

	return nil
}

func ResetIH(db ethdb.Database) error {
	if err := db.(ethdb.BucketsMigrator).ClearBuckets(
		dbutils.IntermediateHashOfAccountBucket,
		dbutils.IntermediateHashOfStorageBucket,
	); err != nil {
		return err
	}
	batch := db.NewBatch()
	if err := stages.SaveStageProgress(batch, stages.IntermediateHashes, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(batch, stages.IntermediateHashes, 0); err != nil {
		return err
	}
	if _, err := batch.Commit(); err != nil {
		return err
	}

	return nil
}
