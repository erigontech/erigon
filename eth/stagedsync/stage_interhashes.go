package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
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

func SpawnIntermediateHashesStage(s *StageState, db ethdb.Database, checkRoot bool, cache *shards.StateCache, tmpdir string, quit <-chan struct{}) (common.Hash, error) {
	to, err := s.ExecutionAt(db)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		s.Done()
		return trie.EmptyRoot, nil
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
			return trie.EmptyRoot, err
		}
		defer tx.Rollback()
	}
	if cache != nil {
		if err = cacheWarmUpIfNeed(tx, cache); err != nil {
			return trie.EmptyRoot, err
		}
	}

	var expectedRootHash common.Hash
	if checkRoot {
		var hash common.Hash
		hash, err = rawdb.ReadCanonicalHash(tx, to)
		if err != nil {
			return trie.EmptyRoot, err
		}
		syncHeadHeader := rawdb.ReadHeader(tx, hash, to)
		expectedRootHash = syncHeadHeader.Root
	}

	logPrefix := s.state.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Generating intermediate hashes", logPrefix), "from", s.BlockNumber, "to", to)
	var root common.Hash
	if s.BlockNumber == 0 {
		if root, err = RegenerateIntermediateHashes(logPrefix, tx.(ethdb.HasTx).Tx().(ethdb.RwTx), checkRoot, cache, tmpdir, expectedRootHash, quit); err != nil {
			return trie.EmptyRoot, err
		}
	} else {
		if root, err = incrementIntermediateHashes(logPrefix, s, tx.(ethdb.HasTx).Tx().(ethdb.RwTx), to, checkRoot, cache, tmpdir, expectedRootHash, quit); err != nil {
			return trie.EmptyRoot, err
		}
	}

	if err = s.DoneAndUpdate(tx, to); err != nil {
		return trie.EmptyRoot, err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}

	return root, nil
}

func RegenerateIntermediateHashes(logPrefix string, db ethdb.RwTx, checkRoot bool, cache *shards.StateCache, tmpdir string, expectedRootHash common.Hash, quit <-chan struct{}) (common.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes started", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Regeneration ended", logPrefix))
	_ = db.(ethdb.BucketMigrator).ClearBucket(dbutils.TrieOfAccountsBucket)
	_ = db.(ethdb.BucketMigrator).ClearBucket(dbutils.TrieOfStorageBucket)
	if cache != nil {
		for i := 0; i < 16; i++ {
			rl := trie.NewRetainList(0)
			loader := trie.NewFlatDBTrieLoader(logPrefix)
			if err := loader.Reset(rl, accountTrieCollectorForCache(cache), storageTrieCollectorForCache(cache), false); err != nil {
				return trie.EmptyRoot, err
			}
			_, err := loader.CalcSubTrieRootOnCache(db, []byte{uint8(i)}, cache, quit)
			if err != nil {
				return trie.EmptyRoot, err
			}
		}
		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(trie.NewRetainList(0), func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes []byte, rootHash []byte) error {
			return nil
		}, func(accWithInc []byte, keyHex []byte, hasState, hasTree, hasHash uint16, hashes []byte, rootHash []byte) error {
			return nil
		}, false); err != nil {
			return trie.EmptyRoot, err
		}
		calcStart := time.Now()
		hash, err := loader.CalcTrieRootOnCache(cache)
		if err != nil {
			return trie.EmptyRoot, err
		}
		if checkRoot && hash != expectedRootHash {
			return trie.EmptyRoot, fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex(), "in", time.Since(calcStart))
		writes := cache.PrepareWrites()
		flushTrieWritesToDB(writes, db)
		cache.TurnWritesToReads(writes)
		return hash, nil
	}
	accTrieCollector, accTrieCollectorFunc := accountTrieCollector(tmpdir)
	stTrieCollector, stTrieCollectorFunc := storageTrieCollector(tmpdir)
	loader := trie.NewFlatDBTrieLoader(logPrefix)
	if err := loader.Reset(trie.NewRetainList(0), accTrieCollectorFunc, stTrieCollectorFunc, false); err != nil {
		return trie.EmptyRoot, err
	}
	calcStart := time.Now()
	hash, err := loader.CalcTrieRoot(db, []byte{}, quit)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if checkRoot && hash != expectedRootHash {
		return trie.EmptyRoot, fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
	}
	log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex(),
		"in", time.Since(calcStart))

	if err := accTrieCollector.Load(logPrefix, db, dbutils.TrieOfAccountsBucket, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return trie.EmptyRoot, err
	}
	if err := stTrieCollector.Load(logPrefix, db, dbutils.TrieOfStorageBucket, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return trie.EmptyRoot, err
	}
	return hash, nil
}

type HashPromoter struct {
	db               ethdb.RwTx
	ChangeSetBufSize uint64
	TempDir          string
	quitCh           <-chan struct{}
}

func NewHashPromoter(db ethdb.RwTx, quitCh <-chan struct{}) *HashPromoter {
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
			newValue, err := p.db.GetOne(dbutils.PlainStateBucket, k)
			if err != nil {
				return err
			}
			if len(v) > 0 {
				var oldAccount accounts.Account
				if err := oldAccount.DecodeForStorage(v); err != nil {
					return err
				}
				if oldAccount.Incarnation > 0 {
					if len(newValue) == 0 { // self-destructed
						deletedAccounts = append(deletedAccounts, newK)
					} else { // turns incarnation to zero
						var newAccount accounts.Account
						if err := newAccount.DecodeForStorage(newValue); err != nil {
							return err
						}
						if newAccount.Incarnation < oldAccount.Incarnation {
							deletedAccounts = append(deletedAccounts, newK)
						}
					}
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
		c, err := p.db.Cursor(dbutils.TrieOfStorageBucket)
		if err != nil {
			return err
		}
		defer c.Close()
		cDel, err := p.db.RwCursor(dbutils.TrieOfStorageBucket)
		if err != nil {
			return err
		}
		defer cDel.Close()
		for _, k := range deletedAccounts {
			if err := ethdb.Walk(c, k, 8*len(k), func(k, v []byte) (bool, error) {
				return true, cDel.Delete(k, v)
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
	log.Info(fmt.Sprintf("[%s] Unwinding of trie hashes", logPrefix), "from", s.BlockNumber, "to", to, "csbucket", changeSetBucket)

	startkey := dbutils.EncodeBlockNumber(to + 1)

	decode := changeset.Mapper[changeSetBucket].Decode
	var deletedAccounts [][]byte
	extract := func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, v := decode(dbKey, dbValue)
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		// Plain state not unwind yet, it means - if key not-exists in PlainState but has value from ChangeSets - then need mark it as "created" in RetainList
		value, err := p.db.GetOne(dbutils.PlainStateBucket, k)
		if err != nil {
			return err
		}

		if !storage && len(value) > 0 {
			var oldAccount accounts.Account
			if err = oldAccount.DecodeForStorage(value); err != nil {
				return err
			}
			if oldAccount.Incarnation > 0 {
				if len(v) == 0 { // self-destructed
					deletedAccounts = append(deletedAccounts, newK)
				} else {
					var newAccount accounts.Account
					if err = newAccount.DecodeForStorage(v); err != nil {
						return err
					}
					if newAccount.Incarnation > oldAccount.Incarnation {
						deletedAccounts = append(deletedAccounts, newK)
					}
				}
			}
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
		c, err := p.db.Cursor(dbutils.TrieOfStorageBucket)
		if err != nil {
			return err
		}
		defer c.Close()
		cDel, err := p.db.RwCursor(dbutils.TrieOfStorageBucket)
		if err != nil {
			return err
		}
		defer cDel.Close()
		for _, k := range deletedAccounts {
			if err := ethdb.Walk(c, k, 8*len(k), func(k, _ []byte) (bool, error) {
				return true, cDel.Delete(k, nil)
			}); err != nil {
				return err
			}
		}
		return nil
	}

	return nil
}

func incrementIntermediateHashes(logPrefix string, s *StageState, db ethdb.RwTx, to uint64, checkRoot bool, cache *shards.StateCache, tmpdir string, expectedRootHash common.Hash, quit <-chan struct{}) (common.Hash, error) {
	p := NewHashPromoter(db, quit)
	p.TempDir = tmpdir
	rl := trie.NewRetainList(0)
	collect := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		rl.AddKeyWithMarker(k, len(v) == 0)
		return nil
	}
	if err := p.Promote(logPrefix, s, s.BlockNumber, to, false /* storage */, collect); err != nil {
		return trie.EmptyRoot, err
	}
	if err := p.Promote(logPrefix, s, s.BlockNumber, to, true /* storage */, collect); err != nil {
		return trie.EmptyRoot, err
	}

	if cache != nil {
		for i := 0; i < 16; i++ {
			loader := trie.NewFlatDBTrieLoader(logPrefix)
			if err := loader.Reset(rl, accountTrieCollectorForCache(cache), storageTrieCollectorForCache(cache), false); err != nil {
				return trie.EmptyRoot, err
			}
			_, err := loader.CalcSubTrieRootOnCache(db, []byte{uint8(i)}, cache, quit)
			if err != nil {
				return trie.EmptyRoot, err
			}
		}

		loader := trie.NewFlatDBTrieLoader(logPrefix)
		if err := loader.Reset(trie.NewRetainList(0), func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
			return nil
		}, func(accWithInc []byte, keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
			return nil
		}, false); err != nil {
			return trie.EmptyRoot, err
		}
		calcStart := time.Now()
		hash, err := loader.CalcTrieRootOnCache(cache)
		if err != nil {
			return trie.EmptyRoot, err
		}
		if checkRoot && hash != expectedRootHash {
			return trie.EmptyRoot, fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex(), "in", time.Since(calcStart))

		writes := cache.PrepareWrites()
		flushTrieWritesToDB(writes, db)
		cache.TurnWritesToReads(writes)
		return hash, nil
	}
	accTrieCollector, accTrieCollectorFunc := accountTrieCollector(tmpdir)
	stTrieCollector, stTrieCollectorFunc := storageTrieCollector(tmpdir)
	loader := trie.NewFlatDBTrieLoader(logPrefix)
	if err := loader.Reset(rl, accTrieCollectorFunc, stTrieCollectorFunc, false); err != nil {
		return trie.EmptyRoot, err
	}
	calcStart := time.Now()
	hash, err := loader.CalcTrieRoot(db, []byte{}, quit)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if checkRoot && hash != expectedRootHash {
		return trie.EmptyRoot, fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
	}
	log.Info(fmt.Sprintf("[%s] Trie root", logPrefix),
		" hash", hash.Hex(),
		"in", time.Since(calcStart))

	if err := accTrieCollector.Load(logPrefix, db, dbutils.TrieOfAccountsBucket, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return trie.EmptyRoot, err
	}
	if err := stTrieCollector.Load(logPrefix, db, dbutils.TrieOfStorageBucket, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return trie.EmptyRoot, err
	}
	return hash, nil
}

func UnwindIntermediateHashesStage(u *UnwindState, s *StageState, db ethdb.Database, _ *shards.StateCache, tmpdir string, quit <-chan struct{}) error {
	var cache *shards.StateCache
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
		var txErr error
		tx, txErr = db.Begin(context.Background(), ethdb.RW)
		if txErr != nil {
			return fmt.Errorf("open transcation: %w", txErr)
		}
		defer tx.Rollback()
	}

	// if cache != nil {
	// 	if err = cacheWarmUpIfNeed(tx, cache); err != nil {
	// 		return err
	// 	}
	// }

	logPrefix := s.state.LogPrefix()
	if err := unwindIntermediateHashesStageImpl(logPrefix, u, s, tx.(ethdb.HasTx).Tx().(ethdb.RwTx), cache, tmpdir, expectedRootHash, quit); err != nil {
		return err
	}
	if err := u.Done(tx); err != nil {
		return fmt.Errorf("%s: reset: %w", logPrefix, err)
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindIntermediateHashesStageImpl(logPrefix string, u *UnwindState, s *StageState, db ethdb.RwTx, cache *shards.StateCache, tmpdir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
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
		calcStart := time.Now()
		hash, err := loader.CalcTrieRootOnCache(cache)
		if err != nil {
			return err
		}
		if hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex(), "in", time.Since(calcStart))

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
		calcStart := time.Now()
		hash, err := loader.CalcTrieRoot(db, []byte{}, quit)
		if err != nil {
			return err
		}
		if hash != expectedRootHash {
			return fmt.Errorf("%s: wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash)
		}
		log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex(), "in", time.Since(calcStart))
		if err := accTrieCollector.Load(logPrefix, db, dbutils.TrieOfAccountsBucket, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
			return err
		}
		if err := stTrieCollector.Load(logPrefix, db, dbutils.TrieOfStorageBucket, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
			return err
		}
	}
	return nil
}

func ResetHashState(tx ethdb.RwTx) error {
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.HashedAccountsBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.HashedStorageBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.ContractCodeBucket); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.HashState, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(tx, stages.HashState, 0); err != nil {
		return err
	}

	return nil
}

func ResetIH(tx ethdb.RwTx) error {
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.TrieOfAccountsBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.TrieOfStorageBucket); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.IntermediateHashes, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(tx, stages.IntermediateHashes, 0); err != nil {
		return err
	}
	return nil
}

func assertSubset(a, b uint16) {
	if (a & b) != a { // a & b == a - checks whether a is subset of b
		panic(fmt.Errorf("invariant 'is subset' failed: %b, %b", a, b))
	}
}

func cacheWarmUpIfNeed(db ethdb.Getter, cache *shards.StateCache) error {
	if cache.HasAccountWithInPrefix([]byte{0}) {
		return nil
	}

	//TODO: re-implement by seeks - don't scan full table
	if err := db.Walk(dbutils.TrieOfAccountsBucket, nil, 0, func(k, v []byte) (bool, error) {
		if len(k) > 2 {
			return true, nil
		}
		hasState, hasTree, hasHash, hashes, _ := trie.UnmarshalTrieNodeTyped(v)
		cache.SetAccountHashesRead(k, hasState, hasTree, hasHash, hashes)
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}

func accountTrieCollectorForCache(cache *shards.StateCache) trie.HashCollector2 {
	return func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, _ []byte) error {
		if len(keyHex) == 0 {
			return nil
		}
		if hasState == 0 {
			cache.SetAccountHashDelete(keyHex)
			return nil
		}
		newV := trie.CastTrieNodeValue(hashes, nil)
		cache.SetAccountHashWrite(keyHex, hasState, hasTree, hasHash, newV)
		return nil
	}
}

func storageTrieCollectorForCache(cache *shards.StateCache) trie.StorageHashCollector2 {
	return func(accWithInc []byte, keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		addr, inc := common.BytesToHash(accWithInc[:32]), binary.BigEndian.Uint64(accWithInc[32:])
		if hasState == 0 {
			cache.SetStorageHashDelete(addr, inc, keyHex, hasState, hasTree, hasHash, nil)
			return nil
		}
		newV := trie.CastTrieNodeValue(hashes, rootHash)
		cache.SetStorageHashWrite(addr, inc, keyHex, hasState, hasTree, hasHash, newV)
		return nil
	}
}

func accountTrieCollector(tmpdir string) (*etl.Collector, trie.HashCollector2) {
	collector := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	newV := make([]byte, 0, 1024)
	return collector, func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, _ []byte) error {
		if len(keyHex) == 0 {
			return nil
		}
		if hasState == 0 {
			return collector.Collect(keyHex, nil)
		}
		if bits.OnesCount16(hasHash) != len(hashes)/common.HashLength {
			panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/common.HashLength))
		}
		assertSubset(hasTree, hasState)
		assertSubset(hasHash, hasState)
		newV = trie.MarshalTrieNode(hasState, hasTree, hasHash, hashes, nil, newV)
		return collector.Collect(keyHex, newV)
	}
}

func storageTrieCollector(tmpdir string) (*etl.Collector, trie.StorageHashCollector2) {
	storageIHCollector := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	newK := make([]byte, 0, 128)
	newV := make([]byte, 0, 1024)
	return storageIHCollector, func(accWithInc []byte, keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		newK = append(append(newK[:0], accWithInc...), keyHex...)
		if hasState == 0 {
			return storageIHCollector.Collect(newK, nil)
		}
		if len(keyHex) > 0 && hasHash == 0 && hasTree == 0 {
			return nil
		}
		if bits.OnesCount16(hasHash) != len(hashes)/common.HashLength {
			panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/common.HashLength))
		}
		assertSubset(hasTree, hasState)
		assertSubset(hasHash, hasState)
		newV = trie.MarshalTrieNode(hasState, hasTree, hasHash, hashes, rootHash, newV)
		return storageIHCollector.Collect(newK, newV)
	}
}

func flushTrieWritesToDB(writes [5]*btree.BTree, db ethdb.RwTx) {
	newV := make([]byte, 0, 1024)
	acc, err := db.RwCursor(dbutils.TrieOfAccountsBucket)
	if err != nil {
		panic(err)
	}
	defer acc.Close()
	shards.WalkAccountHashesWrites(writes, func(prefix []byte, hasState, hasTree, hasHash uint16, h []common.Hash) {
		newV = trie.MarshalTrieNodeTyped(hasState, hasTree, hasHash, h, newV)
		integrity.AssertSubset(prefix, hasTree, hasState)
		integrity.AssertSubset(prefix, hasHash, hasState)
		if err := acc.Put(prefix, newV); err != nil {
			panic(err)
		}
	}, func(prefix []byte, hasState, hasTree, hasHash uint16, h []common.Hash) {
		if err := acc.Delete(prefix, nil); err != nil {
			panic(err)
		}
	})
	st, err := db.RwCursor(dbutils.TrieOfStorageBucket)
	if err != nil {
		panic(err)
	}
	defer acc.Close()

	shards.WalkStorageHashesWrites(writes, func(addrHash common.Hash, incarnation uint64, prefix []byte, hasState, hasTree, hasHash uint16, h []common.Hash) {
		k := trie.StorageKey(addrHash.Bytes(), incarnation, prefix)
		newV = trie.MarshalTrieNodeTyped(hasState, hasTree, hasHash, h, newV)
		if err := st.Put(k, newV); err != nil {
			panic(err)
		}
	}, func(addrHash common.Hash, incarnation uint64, prefix []byte, hasState, hasTree, hasHash uint16, h []common.Hash) {
		k := trie.StorageKey(addrHash.Bytes(), incarnation, prefix)
		if err := st.Delete(k, nil); err != nil {
			panic(err)
		}
	})
}
