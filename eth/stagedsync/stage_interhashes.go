package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"math/bits"
	"os"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

type TrieCfg struct {
	db                kv.RwDB
	checkRoot         bool
	badBlockHalt      bool
	tmpDir            string
	saveNewHashesToDB bool // no reason to save changes when calculating root for mining
	blockReader       services.FullBlockReader
	hd                *headerdownload.HeaderDownload
}

func StageTrieCfg(db kv.RwDB, checkRoot, saveNewHashesToDB, badBlockHalt bool, tmpDir string, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload) TrieCfg {
	return TrieCfg{
		db:                db,
		checkRoot:         checkRoot,
		tmpDir:            tmpDir,
		saveNewHashesToDB: saveNewHashesToDB,
		badBlockHalt:      badBlockHalt,
		blockReader:       blockReader,
		hd:                hd,
	}
}

func SpawnIntermediateHashesStage(s *StageState, u Unwinder, tx kv.RwTx, cfg TrieCfg, ctx context.Context) (common.Hash, error) {
	quit := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return trie.EmptyRoot, err
		}
		defer tx.Rollback()
	}

	to, err := s.ExecutionAt(tx)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		return trie.EmptyRoot, nil
	}

	var expectedRootHash common.Hash
	var headerHash common.Hash
	if cfg.checkRoot {
		syncHeadHeader, err := cfg.blockReader.HeaderByNumber(ctx, tx, to)
		if err != nil {
			return trie.EmptyRoot, err
		}
		expectedRootHash = syncHeadHeader.Root
		headerHash = syncHeadHeader.Hash()
	}
	logPrefix := s.LogPrefix()
	if to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Generating intermediate hashes", logPrefix), "from", s.BlockNumber, "to", to)
	}
	var root common.Hash
	tooBigJump := to > s.BlockNumber && to-s.BlockNumber > 100_000 // RetainList is in-memory structure and it will OOM if jump is too big, such big jump anyway invalidate most of existing Intermediate hashes
	if s.BlockNumber == 0 || tooBigJump {
		if root, err = RegenerateIntermediateHashes(logPrefix, tx, cfg, expectedRootHash, quit); err != nil {
			return trie.EmptyRoot, err
		}
	} else {
		if root, err = incrementIntermediateHashes(logPrefix, s, tx, to, cfg, expectedRootHash, quit); err != nil {
			return trie.EmptyRoot, err
		}
	}

	if err == nil {
		if cfg.checkRoot && root != expectedRootHash {
			log.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", logPrefix, to, root, expectedRootHash, headerHash))
			if cfg.badBlockHalt {
				return trie.EmptyRoot, fmt.Errorf("Wrong trie root")
			}
			if cfg.hd != nil {
				header := rawdb.ReadHeader(tx, headerHash, to)
				cfg.hd.ReportBadHeaderPoS(headerHash, header.ParentHash)
			}
			if to > s.BlockNumber {
				unwindTo := (to + s.BlockNumber) / 2 // Binary search for the correct block, biased to the lower numbers
				log.Warn("Unwinding due to incorrect root hash", "to", unwindTo)
				u.UnwindTo(unwindTo, headerHash)
			}
		} else if err = s.Update(tx, to); err != nil {
			return trie.EmptyRoot, err
		}
	} else {
		return trie.EmptyRoot, err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}

	return root, err
}

func RegenerateIntermediateHashes(logPrefix string, db kv.RwTx, cfg TrieCfg, expectedRootHash common.Hash, quit <-chan struct{}) (common.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes started", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Regeneration ended", logPrefix))
	_ = db.ClearBucket(kv.TrieOfAccounts)
	_ = db.ClearBucket(kv.TrieOfStorage)

	accTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer accTrieCollector.Close()
	accTrieCollectorFunc := accountTrieCollector(accTrieCollector)

	stTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer stTrieCollector.Close()
	stTrieCollectorFunc := storageTrieCollector(stTrieCollector)

	loader := trie.NewFlatDBTrieLoader(logPrefix)
	if err := loader.Reset(trie.NewRetainList(0), accTrieCollectorFunc, stTrieCollectorFunc, false); err != nil {
		return trie.EmptyRoot, err
	}
	hash, err := loader.CalcTrieRoot(db, []byte{}, quit)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if cfg.checkRoot && hash != expectedRootHash {
		return hash, nil
	}
	log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex())

	if err := accTrieCollector.Load(db, kv.TrieOfAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return trie.EmptyRoot, err
	}
	if err := stTrieCollector.Load(db, kv.TrieOfStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return trie.EmptyRoot, err
	}
	return hash, nil
}

type HashPromoter struct {
	db               kv.RwTx
	ChangeSetBufSize uint64
	TempDir          string
	quitCh           <-chan struct{}
}

func NewHashPromoter(db kv.RwTx, quitCh <-chan struct{}) *HashPromoter {
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
		changeSetBucket = kv.StorageChangeSet
	} else {
		changeSetBucket = kv.AccountChangeSet
	}
	log.Trace(fmt.Sprintf("[%s] Incremental state promotion of intermediate hashes", logPrefix), "from", from, "to", to, "csbucket", changeSetBucket)

	startkey := dbutils.EncodeBlockNumber(from + 1)

	decode := changeset.Mapper[changeSetBucket].Decode
	var deletedAccounts [][]byte
	extract := func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, v, err := decode(dbKey, dbValue)
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		if !storage && len(v) > 0 {

			var oldAccount accounts.Account
			if err := oldAccount.DecodeForStorage(v); err != nil {
				return err
			}

			if oldAccount.Incarnation > 0 {

				newValue, err := p.db.GetOne(kv.PlainState, k)
				if err != nil {
					return err
				}

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
		slices.SortFunc(deletedAccounts, func(a, b []byte) bool { return bytes.Compare(a, b) < 0 })
		for _, k := range deletedAccounts {
			if err := p.db.ForPrefix(kv.TrieOfStorage, k, func(k, v []byte) error {
				if err := p.db.Delete(kv.TrieOfStorage, k); err != nil {
					return err
				}
				return nil
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
		changeSetBucket = kv.StorageChangeSet
	} else {
		changeSetBucket = kv.AccountChangeSet
	}
	log.Info(fmt.Sprintf("[%s] Unwinding", logPrefix), "from", s.BlockNumber, "to", to, "csbucket", changeSetBucket)

	startkey := dbutils.EncodeBlockNumber(to + 1)

	decode := changeset.Mapper[changeSetBucket].Decode
	var deletedAccounts [][]byte
	extract := func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, v, err := decode(dbKey, dbValue)
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		// Plain state not unwind yet, it means - if key not-exists in PlainState but has value from ChangeSets - then need mark it as "created" in RetainList
		value, err := p.db.GetOne(kv.PlainState, k)
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
		slices.SortFunc(deletedAccounts, func(a, b []byte) bool { return bytes.Compare(a, b) < 0 })
		for _, k := range deletedAccounts {
			if err := p.db.ForPrefix(kv.TrieOfStorage, k, func(k, v []byte) error {
				if err := p.db.Delete(kv.TrieOfStorage, k); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}

	return nil
}

func incrementIntermediateHashes(logPrefix string, s *StageState, db kv.RwTx, to uint64, cfg TrieCfg, expectedRootHash common.Hash, quit <-chan struct{}) (common.Hash, error) {
	p := NewHashPromoter(db, quit)
	p.TempDir = cfg.tmpDir
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

	accTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer accTrieCollector.Close()
	accTrieCollectorFunc := accountTrieCollector(accTrieCollector)

	stTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer stTrieCollector.Close()
	stTrieCollectorFunc := storageTrieCollector(stTrieCollector)

	loader := trie.NewFlatDBTrieLoader(logPrefix)
	if err := loader.Reset(rl, accTrieCollectorFunc, stTrieCollectorFunc, false); err != nil {
		return trie.EmptyRoot, err
	}
	hash, err := loader.CalcTrieRoot(db, []byte{}, quit)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if cfg.checkRoot && hash != expectedRootHash {
		return hash, nil
	}

	if err := accTrieCollector.Load(db, kv.TrieOfAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return trie.EmptyRoot, err
	}
	if err := stTrieCollector.Load(db, kv.TrieOfStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return trie.EmptyRoot, err
	}
	return hash, nil
}

func UnwindIntermediateHashesStage(u *UnwindState, s *StageState, tx kv.RwTx, cfg TrieCfg, ctx context.Context) (err error) {
	quit := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	syncHeadHeader, err := cfg.blockReader.HeaderByNumber(ctx, tx, u.UnwindPoint)
	if err != nil {
		return err
	}
	if syncHeadHeader == nil {
		return fmt.Errorf("header not found for block number %d", u.UnwindPoint)
	}
	expectedRootHash := syncHeadHeader.Root

	logPrefix := s.LogPrefix()
	if err := unwindIntermediateHashesStageImpl(logPrefix, u, s, tx, cfg, expectedRootHash, quit); err != nil {
		return err
	}
	if err := u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindIntermediateHashesStageImpl(logPrefix string, u *UnwindState, s *StageState, db kv.RwTx, cfg TrieCfg, expectedRootHash common.Hash, quit <-chan struct{}) error {
	p := NewHashPromoter(db, quit)
	p.TempDir = cfg.tmpDir
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

	accTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer accTrieCollector.Close()
	accTrieCollectorFunc := accountTrieCollector(accTrieCollector)

	stTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer stTrieCollector.Close()
	stTrieCollectorFunc := storageTrieCollector(stTrieCollector)

	loader := trie.NewFlatDBTrieLoader(logPrefix)
	if err := loader.Reset(rl, accTrieCollectorFunc, stTrieCollectorFunc, false); err != nil {
		return err
	}
	hash, err := loader.CalcTrieRoot(db, []byte{}, quit)
	if err != nil {
		return err
	}
	if hash != expectedRootHash {
		return fmt.Errorf("wrong trie root: %x, expected (from header): %x", hash, expectedRootHash)
	}
	log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex())
	if err := accTrieCollector.Load(db, kv.TrieOfAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	if err := stTrieCollector.Load(db, kv.TrieOfStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	return nil
}

func ResetHashState(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.HashedAccounts); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.HashedStorage); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.ContractCode); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.HashState, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.HashState, 0); err != nil {
		return err
	}

	return nil
}

func ResetIH(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.TrieOfAccounts); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.TrieOfStorage); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.IntermediateHashes, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.IntermediateHashes, 0); err != nil {
		return err
	}
	return nil
}

func assertSubset(a, b uint16) {
	if (a & b) != a { // a & b == a - checks whether a is subset of b
		panic(fmt.Errorf("invariant 'is subset' failed: %b, %b", a, b))
	}
}

func accountTrieCollector(collector *etl.Collector) trie.HashCollector2 {
	newV := make([]byte, 0, 1024)
	return func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, _ []byte) error {
		if len(keyHex) == 0 {
			return nil
		}
		if hasState == 0 {
			return collector.Collect(keyHex, nil)
		}
		if bits.OnesCount16(hasHash) != len(hashes)/length.Hash {
			panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/length.Hash))
		}
		assertSubset(hasTree, hasState)
		assertSubset(hasHash, hasState)
		newV = trie.MarshalTrieNode(hasState, hasTree, hasHash, hashes, nil, newV)
		return collector.Collect(keyHex, newV)
	}
}

func storageTrieCollector(collector *etl.Collector) trie.StorageHashCollector2 {
	newK := make([]byte, 0, 128)
	newV := make([]byte, 0, 1024)
	return func(accWithInc []byte, keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		newK = append(append(newK[:0], accWithInc...), keyHex...)
		if hasState == 0 {
			return collector.Collect(newK, nil)
		}
		if len(keyHex) > 0 && hasHash == 0 && hasTree == 0 {
			return nil
		}
		if bits.OnesCount16(hasHash) != len(hashes)/length.Hash {
			panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/length.Hash))
		}
		assertSubset(hasTree, hasState)
		assertSubset(hasHash, hasState)
		newV = trie.MarshalTrieNode(hasState, hasTree, hasHash, hashes, rootHash, newV)
		return collector.Collect(newK, newV)
	}
}

func PruneIntermediateHashesStage(s *PruneState, tx kv.RwTx, cfg TrieCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	s.Done(tx)

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
