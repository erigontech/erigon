package stagedsync

import (
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/trie"
)

func SpawnIntermediateHashesStage(s *StageState, db ethdb.Database, datadir string, quit <-chan struct{}) error {
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

	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	hash := rawdb.ReadCanonicalHash(tx, to)
	syncHeadHeader := rawdb.ReadHeader(tx, hash, to)
	expectedRootHash := syncHeadHeader.Root

	log.Info("Generating intermediate hashes", "from", s.BlockNumber, "to", to)
	if s.BlockNumber == 0 {
		if err := regenerateIntermediateHashes(tx, datadir, expectedRootHash, quit); err != nil {
			return err
		}
	} else {
		if err := incrementIntermediateHashes(s, tx, to, datadir, expectedRootHash, quit); err != nil {
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

func regenerateIntermediateHashes(db ethdb.Database, datadir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	log.Info("Regeneration intermediate hashes started")
	buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
	comparator := db.(ethdb.HasTx).Tx().Comparator(dbutils.IntermediateTrieHashBucket)
	buf.SetComparator(comparator)
	collector := etl.NewCollector(datadir, buf)
	hashCollector := func(keyHex []byte, hash []byte) error {
		if len(keyHex)%2 != 0 || len(keyHex) == 0 {
			return nil
		}
		k := make([]byte, len(keyHex)/2)
		trie.CompressNibbles(keyHex, &k)
		if hash == nil {
			return nil
		}
		if len(k) > 40 {
			hash = append(k[40:], hash...)
			k = k[:40]
		}
		return collector.Collect(k, common.CopyBytes(hash))
	}
	loader := trie.NewFlatDBTrieLoader(dbutils.CurrentStateBucket, dbutils.IntermediateTrieHashBucket)
	if err := loader.Reset(trie.NewPrefixFilter(), hashCollector /* HashCollector */, false); err != nil {
		return err
	}
	t := time.Now()
	if hash, err := loader.CalcTrieRoot(db, quit); err == nil {
		generationIHTook := time.Since(t)
		if hash != expectedRootHash {
			return fmt.Errorf("wrong trie root: %x, expected (from header): %x", hash, expectedRootHash)
		}
		log.Debug("Collection finished",
			"root hash", hash.Hex(),
			"gen IH", generationIHTook,
		)
	} else {
		return err
	}
	if err := collector.Load(db, dbutils.IntermediateTrieHashBucket, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:       quit,
		Comparator: comparator,
	}); err != nil {
		return fmt.Errorf("gen ih stage: fail load data to bucket: %w", err)
	}
	log.Info("Regeneration ended")
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

func (p *HashPromoter) Promote(s *StageState, from, to uint64, storage bool, load etl.LoadFunc) error {
	var changeSetBucket string
	if storage {
		changeSetBucket = dbutils.PlainStorageChangeSetBucket
	} else {
		changeSetBucket = dbutils.PlainAccountChangeSetBucket
	}
	log.Debug("Incremental state promotion of intermediate hashes", "from", from, "to", to, "csbucket", changeSetBucket)

	startkey := dbutils.EncodeTimestamp(from + 1)

	walkerAdapter := changeset.Mapper[changeSetBucket].WalkerAdapter
	extract := func(_, changesetBytes []byte, next etl.ExtractNextFunc) error {
		return walkerAdapter(changesetBytes).Walk(func(k, v []byte) error {
			newK, err := transformPlainStateKey(k)
			if err != nil {
				return err
			}
			return next(k, newK, nil)
		})
	}

	var l OldestAppearedLoad
	l.innerLoadFunc = load

	if err := etl.Transform(
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

func (p *HashPromoter) Unwind(s *StageState, u *UnwindState, storage bool, load etl.LoadFunc) error {
	to := u.UnwindPoint
	var changeSetBucket string
	if storage {
		changeSetBucket = dbutils.PlainStorageChangeSetBucket
	} else {
		changeSetBucket = dbutils.PlainAccountChangeSetBucket
	}
	log.Info("Unwinding of intermediate hashes", "from", s.BlockNumber, "to", to, "csbucket", changeSetBucket)

	startkey := dbutils.EncodeTimestamp(to + 1)

	walkerAdapter := changeset.Mapper[changeSetBucket].WalkerAdapter
	extract := func(_, changesetBytes []byte, next etl.ExtractNextFunc) error {
		return walkerAdapter(changesetBytes).Walk(func(k, v []byte) error {
			newK, err := transformPlainStateKey(k)
			if err != nil {
				return err
			}
			return next(k, newK, nil)
		})
	}

	var l OldestAppearedLoad
	l.innerLoadFunc = load

	if err := etl.Transform(
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

func incrementIntermediateHashes(s *StageState, db ethdb.Database, to uint64, datadir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	defer func(t time.Time) { fmt.Printf("stage_interhashes.go:226: %s\n", time.Since(t)) }(time.Now())
	p := NewHashPromoter(db, quit)
	p.TempDir = datadir
	ihFilter := trie.NewPrefixFilter()
	collect := func(k []byte, _ []byte, _ etl.State, _ etl.LoadNextFunc) error {
		ihFilter.Add(k)
		return nil
	}

	if err := p.Promote(s, s.BlockNumber, to, false /* storage */, collect); err != nil {
		return err
	}
	if err := p.Promote(s, s.BlockNumber, to, true /* storage */, collect); err != nil {
		return err
	}
	ihFilter.Sort()
	buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
	comparator := db.(ethdb.HasTx).Tx().Comparator(dbutils.IntermediateTrieHashBucket)
	buf.SetComparator(comparator)
	collector := etl.NewCollector(datadir, buf)
	hashCollector := func(keyHex []byte, hash []byte) error {
		if len(keyHex)%2 != 0 || len(keyHex) == 0 {
			return nil
		}
		k := make([]byte, len(keyHex)/2)
		trie.CompressNibbles(keyHex, &k)
		if hash == nil {
			return fmt.Errorf("IH cursor must call Delete by itself")
		}
		if len(k) > 40 {
			hash = append(k[40:], hash...)
			k = k[:40]
		}

		return collector.Collect(k, common.CopyBytes(hash))
	}
	loader := trie.NewFlatDBTrieLoader(dbutils.CurrentStateBucket, dbutils.IntermediateTrieHashBucket)
	// hashCollector in the line below will collect deletes
	if err := loader.Reset(ihFilter, hashCollector, false); err != nil {
		return err
	}
	t := time.Now()
	hash, err := loader.CalcTrieRoot(db, quit)
	if err != nil {
		return err
	}
	generationIHTook := time.Since(t)
	if hash != expectedRootHash {
		return fmt.Errorf("wrong trie root: %x, expected (from header): %x", hash, expectedRootHash)
	}
	log.Info("Collection finished",
		"root hash", hash.Hex(),
		"gen IH", generationIHTook,
	)
	if err := collector.Load(db,
		dbutils.IntermediateTrieHashBucket,
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			Quit:       quit,
			Comparator: comparator,
			LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"progress", etl.ProgressFromKey(k)}
			},
			LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"progress", etl.ProgressFromKey(k) + 50} // loading is the second stage, from 50..100
			},
		},
	); err != nil {
		return err
	}
	return nil
}

func UnwindIntermediateHashesStage(u *UnwindState, s *StageState, db ethdb.Database, datadir string, quit <-chan struct{}) error {
	hash := rawdb.ReadCanonicalHash(db, u.UnwindPoint)
	syncHeadHeader := rawdb.ReadHeader(db, hash, u.UnwindPoint)
	expectedRootHash := syncHeadHeader.Root

	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err := unwindIntermediateHashesStageImpl(u, s, tx, datadir, expectedRootHash, quit); err != nil {
		return err
	}
	if err := u.Done(tx); err != nil {
		return fmt.Errorf("unwind IntermediateHashes: reset: %w", err)
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindIntermediateHashesStageImpl(u *UnwindState, s *StageState, db ethdb.Database, datadir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	p := NewHashPromoter(db, quit)
	p.TempDir = datadir
	ihFilter := trie.NewPrefixFilter()
	collect := func(k []byte, _ []byte, _ etl.State, _ etl.LoadNextFunc) error {
		ihFilter.Add(k)
		return nil
	}
	if err := p.Unwind(s, u, false /* storage */, collect); err != nil {
		return err
	}
	if err := p.Unwind(s, u, true /* storage */, collect); err != nil {
		return err
	}
	ihFilter.Sort()

	buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
	comparator := db.(ethdb.HasTx).Tx().Comparator(dbutils.IntermediateTrieHashBucket)
	buf.SetComparator(comparator)
	collector := etl.NewCollector(datadir, buf)
	hashCollector := func(keyHex []byte, hash []byte) error {
		if len(keyHex)%2 != 0 || len(keyHex) == 0 {
			return nil
		}
		k := make([]byte, len(keyHex)/2)
		trie.CompressNibbles(keyHex, &k)
		if hash == nil {
			return fmt.Errorf("IH cursor must call Delete by itself")
		}
		if len(k) > 40 {
			hash = append(k[40:], hash...)
			k = k[:40]
		}

		return collector.Collect(k, common.CopyBytes(hash))
	}
	loader := trie.NewFlatDBTrieLoader(dbutils.CurrentStateBucket, dbutils.IntermediateTrieHashBucket)
	// hashCollector in the line below will collect deletes
	if err := loader.Reset(ihFilter, hashCollector, false); err != nil {
		return err
	}
	t := time.Now()
	hash, err := loader.CalcTrieRoot(db, quit)
	if err != nil {
		return err
	}
	generationIHTook := time.Since(t)
	if hash != expectedRootHash {
		return fmt.Errorf("wrong trie root: %x, expected (from header): %x", hash, expectedRootHash)
	}
	log.Info("Collection finished",
		"root hash", hash.Hex(),
		"gen IH", generationIHTook,
	)
	if err := collector.Load(db,
		dbutils.IntermediateTrieHashBucket,
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			Quit:       quit,
			Comparator: comparator,
			LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"progress", etl.ProgressFromKey(k)}
			},
			LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"progress", etl.ProgressFromKey(k) + 50} // loading is the second stage, from 50..100
			},
		},
	); err != nil {
		return err
	}
	return nil
}

func ResetHashState(db ethdb.Database) error {
	if err := db.(ethdb.BucketsMigrator).ClearBuckets(
		dbutils.CurrentStateBucket,
		dbutils.ContractCodeBucket,
		dbutils.IntermediateTrieHashBucket,
	); err != nil {
		return err
	}
	batch := db.NewBatch()
	if err := stages.SaveStageProgress(batch, stages.IntermediateHashes, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(batch, stages.IntermediateHashes, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(batch, stages.HashState, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(batch, stages.HashState, 0, nil); err != nil {
		return err
	}
	if _, err := batch.Commit(); err != nil {
		return err
	}

	return nil
}
