package stagedsync

import (
	"bytes"
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

	hash := rawdb.ReadCanonicalHash(db, to)
	syncHeadHeader := rawdb.ReadHeader(db, hash, to)
	expectedRootHash := syncHeadHeader.Root

	log.Info("Generating intermediate hashes", "from", s.BlockNumber, "to", to)
	if s.BlockNumber == 0 {
		if err := regenerateIntermediateHashes(db, datadir, expectedRootHash, quit); err != nil {
			return err
		}
	} else {
		if err := incrementIntermediateHashes(s, db, to, datadir, expectedRootHash, quit); err != nil {
			return err
		}
	}
	return s.DoneAndUpdate(db, to)
}

func regenerateIntermediateHashes(db ethdb.Database, datadir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	log.Info("Regeneration intermediate hashes started")
	collector := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	hashCollector := func(keyHex []byte, hash []byte) error {
		if len(keyHex)%2 != 0 || len(keyHex) == 0 {
			return nil
		}
		k := make([]byte, len(keyHex)/2)
		trie.CompressNibbles(keyHex, &k)
		return collector.Collect(k, common.CopyBytes(hash))
	}
	loader := trie.NewPreOrderTraverse(dbutils.CurrentStateBucket, dbutils.IntermediateTrieHashBucket)
	if err := loader.Reset(db, trie.NewRetainList(0), hashCollector /* HashCollector */, false); err != nil {
		return err
	}
	t := time.Now()
	if hash, err := loader.CalcTrieRoot(); err == nil {
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
	if err := collector.Load(db, dbutils.IntermediateTrieHashBucket, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return fmt.Errorf("gen ih stage: fail load data to bucket: %d", err)
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
	p := NewHashPromoter(db, quit)
	p.TempDir = datadir
	var exclude [][]byte
	collect := func(k []byte, _ []byte, _ etl.State, _ etl.LoadNextFunc) error {
		exclude = append(exclude, k)
		return nil
	}
	if err := p.Promote(s, s.BlockNumber, to, false /* storage */, collect); err != nil {
		return err
	}
	if err := p.Promote(s, s.BlockNumber, to, true /* storage */, collect); err != nil {
		return err
	}
	sort.Slice(exclude, func(i, j int) bool { return bytes.Compare(exclude[i], exclude[j]) < 0 })

	unfurl := trie.NewRetainList(0)
	for i := range exclude {
		unfurl.AddKey(exclude[i])
	}
	collector := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	hashCollector := func(keyHex []byte, hash []byte) error {
		if len(keyHex)%2 != 0 || len(keyHex) == 0 {
			return nil
		}
		k := make([]byte, len(keyHex)/2)
		trie.CompressNibbles(keyHex, &k)
		return collector.Collect(k, common.CopyBytes(hash))
	}
	loader := trie.NewPreOrderTraverse(dbutils.CurrentStateBucket, dbutils.IntermediateTrieHashBucket)
	// hashCollector in the line below will collect deletes
	if err := loader.Reset(db, unfurl, hashCollector, false); err != nil {
		return err
	}
	t := time.Now()
	hash, err := loader.CalcTrieRoot()
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
			Quit: quit,
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

	if err := unwindIntermediateHashesStageImpl(u, s, db, datadir, expectedRootHash, quit); err != nil {
		return err
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind IntermediateHashes: reset: %w", err)
	}
	return nil
}

func unwindIntermediateHashesStageImpl(u *UnwindState, s *StageState, db ethdb.Database, datadir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	p := NewHashPromoter(db, quit)
	p.TempDir = datadir
	var exclude [][]byte
	collect := func(k []byte, _ []byte, _ etl.State, _ etl.LoadNextFunc) error {
		exclude = append(exclude, k)
		return nil
	}
	if err := p.Unwind(s, u, false /* storage */, collect); err != nil {
		return err
	}
	if err := p.Unwind(s, u, true /* storage */, collect); err != nil {
		return err
	}
	sort.Slice(exclude, func(i, j int) bool { return bytes.Compare(exclude[i], exclude[j]) < 0 })

	unfurl := trie.NewRetainList(0)
	for i := range exclude {
		unfurl.AddKey(exclude[i])
	}
	collector := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	hashCollector := func(keyHex []byte, hash []byte) error {
		if len(keyHex)%2 != 0 || len(keyHex) == 0 {
			return nil
		}
		k := make([]byte, len(keyHex)/2)
		trie.CompressNibbles(keyHex, &k)
		return collector.Collect(k, common.CopyBytes(hash))
	}
	loader := trie.NewPreOrderTraverse(dbutils.CurrentStateBucket, dbutils.IntermediateTrieHashBucket)
	// hashCollector in the line below will collect deletes
	if err := loader.Reset(db, unfurl, hashCollector, false); err != nil {
		return err
	}
	t := time.Now()
	hash, err := loader.CalcTrieRoot()
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
			Quit: quit,
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
	if err := db.(ethdb.NonTransactional).ClearBuckets(
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
