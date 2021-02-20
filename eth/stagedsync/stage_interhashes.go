package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"sort"
	"time"

	"github.com/google/btree"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/eth/integrity"
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
	//fmt.Printf("\n\n%d->%d\n", s.BlockNumber, to)

	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if cache != nil {
		if err = cacheWarmUpIfNeed(tx, cache); err != nil {
			return err
		}
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
	_ = db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.TrieOfAccountsBucket, dbutils.TrieOfStorageBucket)

	if cache != nil {
		for i := 0; i < 16; i++ {
			rl := trie.NewRetainList(0)
			loader := trie.NewFlatDBTrieLoader(logPrefix)
			if err := loader.Reset(rl, accountTrieCollectorForCache(cache), storageTrieCollectorForCache(cache), false); err != nil {
				return err
			}
			_, err := loader.CalcSubTrieRootOnCache(db, []byte{uint8(i)}, cache, quit)
			if err != nil {
				return err
			}
		}
		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(trie.NewRetainList(0), func(keyHex []byte, hasState, hasBranch, hasHash uint16, hashes []byte, rootHash []byte) error {
			return nil
		}, func(accWithInc []byte, keyHex []byte, hasState, hasBranch, hasHash uint16, hashes []byte, rootHash []byte) error {
			return nil
		}, false); err != nil {
			return err
		}
		t := time.Now()
		hash, err := loader.CalcTrieRootOnCache(cache)
		if err != nil {
			return err
		}
		generationIHTook := time.Since(t)
		if checkRoot && hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Collection finished", logPrefix),
			"root hash", hash.Hex(),
			"gen AccTrie", generationIHTook,
		)
		writes := cache.PrepareWrites()
		flushTrieWritesToDB(writes, db)
		cache.TurnWritesToReads(writes)
	} else {
		accTrieCollector, accTrieCollectorFunc := accountTrieCollector(tmpdir)
		stTrieCollector, stTrieCollectorFunc := storageTrieCollector(tmpdir)
		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(trie.NewRetainList(0), accTrieCollectorFunc, stTrieCollectorFunc, false); err != nil {
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
			"gen AccTrie", generationIHTook,
		)
		load := func(k []byte, value []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return next(k, k, value)
		}

		if err := accTrieCollector.Load(logPrefix, db, dbutils.TrieOfAccountsBucket, load, etl.TransformArgs{Quit: quit}); err != nil {
			return err
		}
		if err := stTrieCollector.Load(logPrefix, db, dbutils.TrieOfStorageBucket, load, etl.TransformArgs{Quit: quit}); err != nil {
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
	var deletedAccounts [][]byte
	extract := func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, v := decode(dbKey, dbValue)
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		if !storage {
			value, err := p.db.Get(dbutils.PlainStateBucket, k)
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				return err
			}
			if len(value) == 0 && len(v) > 0 { // self-destructed
				deletedAccounts = append(deletedAccounts, newK)
			} else if len(value) > 0 && len(v) > 0 { // turns incarnation to zero
				var acc accounts.Account
				if err := acc.DecodeForStorage(v); err != nil {
					return err
				}
				var dbAcc accounts.Account
				if err := dbAcc.DecodeForStorage(value); err != nil {
					return err
				}
				if dbAcc.Incarnation == 0 && acc.Incarnation > 0 {
					deletedAccounts = append(deletedAccounts, newK)
				}
			}
		}

		return next(dbKey, newK, v)
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

	if !storage { // delete Intermediate hashes of deleted accounts
		sort.Slice(deletedAccounts, func(i, j int) bool { return bytes.Compare(deletedAccounts[i], deletedAccounts[j]) < 0 })
		for _, k := range deletedAccounts {
			if err := p.db.Walk(dbutils.TrieOfStorageBucket, k, 8*len(k), func(k, v []byte) (bool, error) {
				return true, p.db.Delete(dbutils.TrieOfStorageBucket, k, v)
			}); err != nil {
				return err
			}
		}
		return nil
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
	var deletedAccounts [][]byte
	extract := func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, v := decode(dbKey, dbValue)
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		if !storage {
			if len(v) == 0 { // self-destructed
				deletedAccounts = append(deletedAccounts, newK)
			} else {
				var acc accounts.Account
				if err = acc.DecodeForStorage(v); err != nil {
					return err
				}
				if acc.Incarnation == 0 {
					deletedAccounts = append(deletedAccounts, newK)
				}
			}
		}
		// Plain state not unwind yet, it means - if key not-exists in PlainState but has value from ChangeSets - then need mark it as "created" in RetainList
		value, err := p.db.Get(dbutils.PlainStateBucket, k)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return err
		}
		return next(k, newK, value)
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

	if !storage { // delete Intermediate hashes of deleted accounts
		sort.Slice(deletedAccounts, func(i, j int) bool { return bytes.Compare(deletedAccounts[i], deletedAccounts[j]) < 0 })
		for _, k := range deletedAccounts {
			if err := p.db.Walk(dbutils.TrieOfStorageBucket, k, 8*len(k), func(k, _ []byte) (bool, error) {
				return true, p.db.Delete(dbutils.TrieOfStorageBucket, k, nil)
			}); err != nil {
				return err
			}
		}
		return nil
	}

	return nil
}

func incrementIntermediateHashes(logPrefix string, s *StageState, db ethdb.Database, to uint64, checkRoot bool, cache *shards.StateCache, tmpdir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	p := NewHashPromoter(db, quit)
	p.TempDir = tmpdir
	rl := trie.NewRetainList(0)
	collect := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		rl.AddKeyWithMarker(k, len(v) == 0)
		return nil
	}
	if err := p.Promote(logPrefix, s, s.BlockNumber, to, false /* storage */, collect); err != nil {
		return err
	}
	if err := p.Promote(logPrefix, s, s.BlockNumber, to, true /* storage */, collect); err != nil {
		return err
	}

	if cache != nil {
		for i := 0; i < 16; i++ {
			loader := trie.NewFlatDBTrieLoader(logPrefix)
			if err := loader.Reset(rl, accountTrieCollectorForCache(cache), storageTrieCollectorForCache(cache), false); err != nil {
				return err
			}
			_, err := loader.CalcSubTrieRootOnCache(db, []byte{uint8(i)}, cache, quit)
			if err != nil {
				return err
			}
		}

		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(trie.NewRetainList(0), func(keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error {
			return nil
		}, func(accWithInc []byte, keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error {
			return nil
		}, false); err != nil {
			return err
		}
		t := time.Now()
		hash, err := loader.CalcTrieRootOnCache(cache)
		if err != nil {
			return err
		}
		generationIHTook := time.Since(t)
		if checkRoot && hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Collection finished", logPrefix),
			"root hash", hash.Hex(),
			"gen AccTrie", generationIHTook,
		)

		writes := cache.PrepareWrites()
		flushTrieWritesToDB(writes, db)
		cache.TurnWritesToReads(writes)
	} else {
		accTrieCollector, accTrieCollectorFunc := accountTrieCollector(tmpdir)
		stTrieCollector, stTrieCollectorFunc := storageTrieCollector(tmpdir)
		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(rl, accTrieCollectorFunc, stTrieCollectorFunc, false); err != nil {
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
			"gen AccTrie", generationIHTook,
		)
		load := func(k []byte, value []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error { // hack to prevent use of APPEND
			return next(k, k, value)
		}

		if err := accTrieCollector.Load(logPrefix, db, dbutils.TrieOfAccountsBucket, load, etl.TransformArgs{Quit: quit}); err != nil {
			return err
		}
		if err := stTrieCollector.Load(logPrefix, db, dbutils.TrieOfStorageBucket, load, etl.TransformArgs{Quit: quit}); err != nil {
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
	//fmt.Printf("\n\nu: %d->%d\n", s.BlockNumber, u.UnwindPoint)

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

	if cache != nil {
		if err = cacheWarmUpIfNeed(tx, cache); err != nil {
			return err
		}
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
	rl := trie.NewRetainList(0)
	collect := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		rl.AddKeyWithMarker(k, len(v) == 0)
		return nil
	}
	if err := p.Unwind(logPrefix, s, u, false /* storage */, collect); err != nil {
		return err
	}
	if err := p.Unwind(logPrefix, s, u, true /* storage */, collect); err != nil {
		return err
	}

	if cache != nil {
		for i := 0; i < 16; i++ {
			loader := trie.NewFlatDBTrieLoader(logPrefix)
			if err := loader.Reset(rl, accountTrieCollectorForCache(cache), storageTrieCollectorForCache(cache), false); err != nil {
				return err
			}
			_, err := loader.CalcSubTrieRootOnCache(db, []byte{uint8(i)}, cache, quit)
			if err != nil {
				return err
			}
		}

		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(trie.NewRetainList(0), func(keyHex []byte, _, _, _ uint16, hashes []byte, rootHash []byte) error {
			return nil
		}, func(accWithInc []byte, keyHex []byte, _, _, _ uint16, hashes []byte, rootHash []byte) error {
			return nil
		}, false); err != nil {
			return err
		}
		t := time.Now()
		hash, err := loader.CalcTrieRootOnCache(cache)
		if err != nil {
			return err
		}
		generationIHTook := time.Since(t)
		if hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Collection finished", logPrefix),
			"root hash", hash.Hex(),
			"gen AccTrie", generationIHTook,
		)

		writes := cache.PrepareWrites()
		flushTrieWritesToDB(writes, db)
		cache.TurnWritesToReads(writes)

	} else {
		accTrieCollector, accTrieCollectorFunc := accountTrieCollector(tmpdir)
		stTrieCollector, stTrieCollectorFunc := storageTrieCollector(tmpdir)
		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(rl, accTrieCollectorFunc, stTrieCollectorFunc, false); err != nil {
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
			"gen AccTrie", generationIHTook,
		)
		load := func(k []byte, value []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return next(k, k, value)
		}
		if err := accTrieCollector.Load(logPrefix, db, dbutils.TrieOfAccountsBucket, load, etl.TransformArgs{Quit: quit}); err != nil {
			return err
		}
		if err := stTrieCollector.Load(logPrefix, db, dbutils.TrieOfStorageBucket, load, etl.TransformArgs{Quit: quit}); err != nil {
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
		dbutils.TrieOfAccountsBucket,
		dbutils.TrieOfStorageBucket,
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

func assertSubset(a, b uint16) {
	if (a & b) != a { // a & b == a - checks whether a is subset of b
		panic(fmt.Errorf("invariant 'is subset' failed: %b, %b", a, b))
	}
}

func cacheWarmUpIfNeed(db ethdb.Database, cache *shards.StateCache) error {
	if cache.HasAccountWithInPrefix([]byte{0}) {
		return nil
	}

	//TODO: re-implement by seeks - don't scan full table
	if err := db.Walk(dbutils.TrieOfAccountsBucket, nil, 0, func(k, v []byte) (bool, error) {
		if len(k) > 2 {
			return true, nil
		}
		hasState, hasBranch, hasHash, hashes := trie.UnmarshalTrieNodeTyped(v)
		cache.SetAccountHashesRead(k, hasState, hasBranch, hasHash, hashes)
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}

func accountTrieCollectorForCache(cache *shards.StateCache) trie.HashCollector2 {
	return func(keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error {
		if len(keyHex) == 0 {
			return nil
		}
		if hashes == nil && rootHash == nil {
			cache.SetAccountHashDelete(keyHex)
			return nil
		}
		newV := trie.CastTrieNodeValue(hashes, rootHash)
		cache.SetAccountHashWrite(keyHex, hasState, hasBranch, hasHash, newV)
		return nil
	}
}

func storageTrieCollectorForCache(cache *shards.StateCache) trie.StorageHashCollector2 {
	return func(accWithInc []byte, keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error {
		addr, inc := common.BytesToHash(accWithInc[:32]), binary.BigEndian.Uint64(accWithInc[32:])
		if hashes == nil && rootHash == nil {
			cache.SetStorageHashDelete(addr, inc, keyHex, hasState, hasBranch, hasHash, nil)
			return nil
		}
		newV := trie.CastTrieNodeValue(hashes, rootHash)
		cache.SetStorageHashWrite(addr, inc, keyHex, hasState, hasBranch, hasHash, newV)
		return nil
	}
}

func accountTrieCollector(tmpdir string) (*etl.Collector, trie.HashCollector2) {
	collector := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	newV := make([]byte, 0, 1024)
	return collector, func(keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error {
		if len(keyHex) == 0 {
			return nil
		}
		if hashes == nil {
			return collector.Collect(keyHex, nil)
		}
		if bits.OnesCount16(hasHash) != len(hashes)/common.HashLength {
			panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/common.HashLength))
		}
		assertSubset(hasBranch, hasState)
		assertSubset(hasHash, hasState)
		newV = trie.MarshalTrieNode(hasState, hasBranch, hasHash, hashes, rootHash, newV)
		return collector.Collect(keyHex, newV)
	}
}

func storageTrieCollector(tmpdir string) (*etl.Collector, trie.StorageHashCollector2) {
	storageIHCollector := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	newK := make([]byte, 0, 128)
	newV := make([]byte, 0, 1024)
	return storageIHCollector, func(accWithInc []byte, keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error {
		newK = append(append(newK[:0], accWithInc...), keyHex...)
		if hashes == nil {
			return storageIHCollector.Collect(newK, nil)
		}
		if bits.OnesCount16(hasHash) != len(hashes)/common.HashLength {
			panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/common.HashLength))
		}
		assertSubset(hasBranch, hasState)
		assertSubset(hasHash, hasState)
		newV = trie.MarshalTrieNode(hasState, hasBranch, hasHash, hashes, rootHash, newV)
		return storageIHCollector.Collect(newK, newV)
	}
}

func flushTrieWritesToDB(writes [5]*btree.BTree, db ethdb.Database) {
	newV := make([]byte, 0, 1024)
	shards.WalkAccountHashesWrites(writes, func(prefix []byte, hasState, hasBranch, hasHash uint16, h []common.Hash) {
		newV = trie.MarshalTrieNodeTyped(hasState, hasBranch, hasHash, h, newV)
		integrity.AssertSubset(prefix, hasBranch, hasState)
		integrity.AssertSubset(prefix, hasHash, hasState)
		if err := db.Put(dbutils.TrieOfAccountsBucket, prefix, newV); err != nil {
			panic(err)
		}
	}, func(prefix []byte, hasState, hasBranch, hasHash uint16, h []common.Hash) {
		if err := db.Delete(dbutils.TrieOfAccountsBucket, prefix, nil); err != nil {
			panic(err)
		}
	})
	shards.WalkStorageHashesWrites(writes, func(addrHash common.Hash, incarnation uint64, prefix []byte, hasState, hasBranch, hasHash uint16, h []common.Hash) {
		k := trie.StorageKey(addrHash.Bytes(), incarnation, prefix)
		newV = trie.MarshalTrieNodeTyped(hasState, hasBranch, hasHash, h, newV)
		if err := db.Put(dbutils.TrieOfStorageBucket, k, newV); err != nil {
			panic(err)
		}
	}, func(addrHash common.Hash, incarnation uint64, prefix []byte, hasState, hasBranch, hasHash uint16, h []common.Hash) {
		k := trie.StorageKey(addrHash.Bytes(), incarnation, prefix)
		if err := db.Delete(dbutils.TrieOfStorageBucket, k, nil); err != nil {
			panic(err)
		}
	})
}
