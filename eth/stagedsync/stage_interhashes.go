package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"sync/atomic"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

type TrieCfg struct {
	db                kv.RwDB
	checkRoot         bool
	badBlockHalt      bool
	tmpDir            string
	saveNewHashesToDB bool // no reason to save changes when calculating root for mining
	blockReader       services.FullBlockReader
	hd                *headerdownload.HeaderDownload

	historyV3 bool
	agg       *state.AggregatorV3
}

func StageTrieCfg(db kv.RwDB, checkRoot, saveNewHashesToDB, badBlockHalt bool, tmpDir string, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload, historyV3 bool, agg *state.AggregatorV3) TrieCfg {
	return TrieCfg{
		db:                db,
		checkRoot:         checkRoot,
		tmpDir:            tmpDir,
		saveNewHashesToDB: saveNewHashesToDB,
		badBlockHalt:      badBlockHalt,
		blockReader:       blockReader,
		hd:                hd,

		historyV3: historyV3,
		agg:       agg,
	}
}

func SpawnIntermediateHashesStage(s *StageState, u Unwinder, tx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (libcommon.Hash, error) {
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
	if s.BlockNumber > to { // Erigon will self-heal (download missed blocks) eventually
		return trie.EmptyRoot, nil
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		return trie.EmptyRoot, nil
	}

	var expectedRootHash libcommon.Hash
	var headerHash libcommon.Hash
	var syncHeadHeader *types.Header
	if cfg.checkRoot {
		syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, tx, to)
		if err != nil {
			return trie.EmptyRoot, err
		}
		if syncHeadHeader == nil {
			return trie.EmptyRoot, fmt.Errorf("no header found with number %d", to)
		}
		expectedRootHash = syncHeadHeader.Root
		headerHash = syncHeadHeader.Hash()
	}
	logPrefix := s.LogPrefix()
	if to > s.BlockNumber+16 {
		logger.Info(fmt.Sprintf("[%s] Generating intermediate hashes", logPrefix), "from", s.BlockNumber, "to", to)
	}

	var root libcommon.Hash
	tooBigJump := to > s.BlockNumber && to-s.BlockNumber > 100_000 // RetainList is in-memory structure and it will OOM if jump is too big, such big jump anyway invalidate most of existing Intermediate hashes
	if !tooBigJump && cfg.historyV3 && to-s.BlockNumber > 10 {
		//incremental can work only on DB data, not on snapshots
		_, n, err := rawdbv3.TxNums.FindBlockNum(tx, cfg.agg.EndTxNumMinimax())
		if err != nil {
			return trie.EmptyRoot, err
		}
		tooBigJump = s.BlockNumber < n
	}
	if s.BlockNumber == 0 || tooBigJump {
		if root, err = RegenerateIntermediateHashes(logPrefix, tx, cfg, expectedRootHash, ctx, logger); err != nil {
			return trie.EmptyRoot, err
		}
	} else {
		if root, err = IncrementIntermediateHashes(logPrefix, s, tx, to, cfg, expectedRootHash, quit, logger); err != nil {
			return trie.EmptyRoot, err
		}
	}

	if cfg.checkRoot && root != expectedRootHash {
		logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", logPrefix, to, root, expectedRootHash, headerHash))
		if cfg.badBlockHalt {
			return trie.EmptyRoot, fmt.Errorf("%w: wrong trie root", consensus.ErrInvalidBlock)
		}
		if cfg.hd != nil {
			cfg.hd.ReportBadHeaderPoS(headerHash, syncHeadHeader.ParentHash)
		}

		if to > s.BlockNumber {
			unwindTo := (to + s.BlockNumber) / 2 // Binary search for the correct block, biased to the lower numbers
			logger.Warn("Unwinding due to incorrect root hash", "to", unwindTo)
			u.UnwindTo(unwindTo, BadBlock(headerHash, fmt.Errorf("Incorrect root hash")))
		}
	} else if err = s.Update(tx, to); err != nil {
		return trie.EmptyRoot, err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}

	return root, err
}

func RegenerateIntermediateHashes(logPrefix string, db kv.RwTx, cfg TrieCfg, expectedRootHash libcommon.Hash, ctx context.Context, logger log.Logger) (libcommon.Hash, error) {
	logger.Info(fmt.Sprintf("[%s] Regeneration trie hashes started", logPrefix))
	defer logger.Info(fmt.Sprintf("[%s] Regeneration ended", logPrefix))
	_ = db.ClearBucket(kv.TrieOfAccounts)
	_ = db.ClearBucket(kv.TrieOfStorage)
	clean := kv.ReadAhead(ctx, cfg.db, &atomic.Bool{}, kv.HashedAccounts, nil, math.MaxUint32)
	defer clean()
	clean2 := kv.ReadAhead(ctx, cfg.db, &atomic.Bool{}, kv.HashedStorage, nil, math.MaxUint32)
	defer clean2()

	accTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer accTrieCollector.Close()
	accTrieCollectorFunc := accountTrieCollector(accTrieCollector)

	stTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer stTrieCollector.Close()
	stTrieCollectorFunc := storageTrieCollector(stTrieCollector)

	loader := trie.NewFlatDBTrieLoader(logPrefix, trie.NewRetainList(0), accTrieCollectorFunc, stTrieCollectorFunc, false)
	hash, err := loader.CalcTrieRoot(db, ctx.Done())
	if err != nil {
		return trie.EmptyRoot, err
	}

	if cfg.checkRoot && hash != expectedRootHash {
		return hash, nil
	}
	logger.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex())

	if err := accTrieCollector.Load(db, kv.TrieOfAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return trie.EmptyRoot, err
	}
	if err := stTrieCollector.Load(db, kv.TrieOfStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return trie.EmptyRoot, err
	}
	return hash, nil
}

type HashPromoter struct {
	tx               kv.RwTx
	ChangeSetBufSize uint64
	TempDir          string
	logPrefix        string
	quitCh           <-chan struct{}
	logger           log.Logger
}

func NewHashPromoter(db kv.RwTx, tempDir string, quitCh <-chan struct{}, logPrefix string, logger log.Logger) *HashPromoter {
	return &HashPromoter{
		tx:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          tempDir,
		quitCh:           quitCh,
		logPrefix:        logPrefix,
		logger:           logger,
	}
}

func (p *HashPromoter) PromoteOnHistoryV3(logPrefix string, from, to uint64, storage bool, load func(k []byte, v []byte) error) error {
	nonEmptyMarker := []byte{1}

	txnFrom, err := rawdbv3.TxNums.Min(p.tx, from+1)
	if err != nil {
		return err
	}
	txnTo := uint64(math.MaxUint64)

	if storage {
		compositeKey := make([]byte, length.Hash+length.Hash)
		it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.StorageHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
		if err != nil {
			return err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return err
			}
			addrHash, err := common.HashData(k[:length.Addr])
			if err != nil {
				return err
			}
			secKey, err := common.HashData(k[length.Addr:])
			if err != nil {
				return err
			}
			copy(compositeKey, addrHash[:])
			copy(compositeKey[length.Hash:], secKey[:])
			if len(v) != 0 {
				v = nonEmptyMarker
			}
			if err := load(compositeKey, v); err != nil {
				return err
			}
		}
		return nil
	}

	it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.AccountsHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		if len(v) != 0 {
			v = nonEmptyMarker
		}
		if err := load(newK, v); err != nil {
			return err
		}
	}
	return nil
}

func (p *HashPromoter) Promote(logPrefix string, from, to uint64, storage bool, load etl.LoadFunc) error {
	var changeSetBucket string
	if storage {
		changeSetBucket = kv.StorageChangeSet
	} else {
		changeSetBucket = kv.AccountChangeSet
	}
	p.logger.Trace(fmt.Sprintf("[%s] Incremental state promotion of intermediate hashes", logPrefix), "from", from, "to", to, "csbucket", changeSetBucket)

	startkey := hexutility.EncodeTs(from + 1)

	decode := historyv2.Mapper[changeSetBucket].Decode
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

				newValue, err := p.tx.GetOne(kv.PlainState, k)
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
		p.tx,
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
		p.logger,
	); err != nil {
		return err
	}

	if !storage { // delete Intermediate hashes of deleted accounts
		slices.SortFunc(deletedAccounts, bytes.Compare)
		for _, k := range deletedAccounts {
			if err := p.tx.ForPrefix(kv.TrieOfStorage, k, func(k, v []byte) error {
				if err := p.tx.Delete(kv.TrieOfStorage, k); err != nil {
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

func (p *HashPromoter) UnwindOnHistoryV3(logPrefix string, unwindFrom, unwindTo uint64, storage bool, load func(k []byte, v []byte)) error {
	txnFrom, err := rawdbv3.TxNums.Min(p.tx, unwindTo)
	if err != nil {
		return err
	}
	txnTo := uint64(math.MaxUint64)
	var deletedAccounts [][]byte

	if storage {
		it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.StorageHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
		if err != nil {
			return err
		}
		for it.HasNext() {
			k, _, err := it.Next()
			if err != nil {
				return err
			}
			// Plain state not unwind yet, it means - if key not-exists in PlainState but has value from ChangeSets - then need mark it as "created" in RetainList
			enc, err := p.tx.GetOne(kv.PlainState, k[:20])
			if err != nil {
				return err
			}
			incarnation := uint64(1)
			if len(enc) != 0 {
				oldInc, _ := accounts.DecodeIncarnationFromStorage(enc)
				incarnation = oldInc
			}
			plainKey := dbutils.PlainGenerateCompositeStorageKey(k[:20], incarnation, k[20:])
			value, err := p.tx.GetOne(kv.PlainState, plainKey)
			if err != nil {
				return err
			}
			newK, err := transformPlainStateKey(plainKey)
			if err != nil {
				return err
			}
			load(newK, value)
		}
		return nil
	}

	it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.AccountsHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		// Plain state not unwind yet, it means - if key not-exists in PlainState but has value from ChangeSets - then need mark it as "created" in RetainList
		value, err := p.tx.GetOne(kv.PlainState, k)
		if err != nil {
			return err
		}

		if len(value) > 0 {
			oldInc, _ := accounts.DecodeIncarnationFromStorage(value)
			if oldInc > 0 {
				if len(v) == 0 { // self-destructed
					deletedAccounts = append(deletedAccounts, newK)
				} else {
					var newAccount accounts.Account
					if err = accounts.DeserialiseV3(&newAccount, v); err != nil {
						return err
					}
					if newAccount.Incarnation > oldInc {
						deletedAccounts = append(deletedAccounts, newK)
					}
				}
			}
		}

		load(newK, value)
	}

	// delete Intermediate hashes of deleted accounts
	slices.SortFunc(deletedAccounts, bytes.Compare)
	for _, k := range deletedAccounts {
		if err := p.tx.ForPrefix(kv.TrieOfStorage, k, func(k, v []byte) error {
			if err := p.tx.Delete(kv.TrieOfStorage, k); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
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
	p.logger.Info(fmt.Sprintf("[%s] Unwinding", logPrefix), "from", s.BlockNumber, "to", to, "csbucket", changeSetBucket)

	startkey := hexutility.EncodeTs(to + 1)

	decode := historyv2.Mapper[changeSetBucket].Decode
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
		value, err := p.tx.GetOne(kv.PlainState, k)
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
		p.tx,
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
		p.logger,
	); err != nil {
		return err
	}

	if !storage { // delete Intermediate hashes of deleted accounts
		slices.SortFunc(deletedAccounts, bytes.Compare)
		for _, k := range deletedAccounts {
			if err := p.tx.ForPrefix(kv.TrieOfStorage, k, func(k, v []byte) error {
				if err := p.tx.Delete(kv.TrieOfStorage, k); err != nil {
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

func IncrementIntermediateHashes(logPrefix string, s *StageState, db kv.RwTx, to uint64, cfg TrieCfg, expectedRootHash libcommon.Hash, quit <-chan struct{}, logger log.Logger) (libcommon.Hash, error) {
	p := NewHashPromoter(db, cfg.tmpDir, quit, logPrefix, logger)
	rl := trie.NewRetainList(0)
	if cfg.historyV3 {
		collect := func(k, v []byte) error {
			if len(k) == 32 {
				rl.AddKeyWithMarker(k, len(v) == 0)
				return nil
			}
			accBytes, err := p.tx.GetOne(kv.HashedAccounts, k[:32])
			if err != nil {
				return err
			}
			incarnation := uint64(1)
			if len(accBytes) != 0 {
				incarnation, err = accounts.DecodeIncarnationFromStorage(accBytes)
				if err != nil {
					return err
				}
				if incarnation == 0 {
					return nil
				}
			}
			compositeKey := make([]byte, length.Hash+length.Incarnation+length.Hash)
			copy(compositeKey, k[:32])
			binary.BigEndian.PutUint64(compositeKey[32:], incarnation)
			copy(compositeKey[40:], k[32:])
			rl.AddKeyWithMarker(compositeKey, len(v) == 0)
			return nil
		}
		if err := p.PromoteOnHistoryV3(logPrefix, s.BlockNumber, to, false, collect); err != nil {
			return trie.EmptyRoot, err
		}
		if err := p.PromoteOnHistoryV3(logPrefix, s.BlockNumber, to, true, collect); err != nil {
			return trie.EmptyRoot, err
		}
	} else {
		collect := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			rl.AddKeyWithMarker(k, len(v) == 0)
			return nil
		}
		if err := p.Promote(logPrefix, s.BlockNumber, to, false, collect); err != nil {
			return trie.EmptyRoot, err
		}
		if err := p.Promote(logPrefix, s.BlockNumber, to, true, collect); err != nil {
			return trie.EmptyRoot, err
		}
	}
	accTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer accTrieCollector.Close()
	accTrieCollectorFunc := accountTrieCollector(accTrieCollector)

	stTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer stTrieCollector.Close()
	stTrieCollectorFunc := storageTrieCollector(stTrieCollector)

	loader := trie.NewFlatDBTrieLoader(logPrefix, rl, accTrieCollectorFunc, stTrieCollectorFunc, false)
	hash, err := loader.CalcTrieRoot(db, quit)
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

func UnwindIntermediateHashesStage(u *UnwindState, s *StageState, tx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (err error) {
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
	if err := unwindIntermediateHashesStageImpl(logPrefix, u, s, tx, cfg, expectedRootHash, quit, logger); err != nil {
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

func UnwindIntermediateHashesForTrieLoader(logPrefix string, rl *trie.RetainList, u *UnwindState, s *StageState, db kv.RwTx, cfg TrieCfg, accTrieCollectorFunc trie.HashCollector2, stTrieCollectorFunc trie.StorageHashCollector2, quit <-chan struct{}, logger log.Logger) (*trie.FlatDBTrieLoader, error) {
	p := NewHashPromoter(db, cfg.tmpDir, quit, logPrefix, logger)
	if cfg.historyV3 {
		collect := func(k, v []byte) {
			rl.AddKeyWithMarker(k, len(v) == 0)
		}
		if err := p.UnwindOnHistoryV3(logPrefix, s.BlockNumber, u.UnwindPoint, false, collect); err != nil {
			return nil, err
		}
		if err := p.UnwindOnHistoryV3(logPrefix, s.BlockNumber, u.UnwindPoint, true, collect); err != nil {
			return nil, err
		}
	} else {
		collect := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			rl.AddKeyWithMarker(k, len(v) == 0)
			return nil
		}
		if err := p.Unwind(logPrefix, s, u, false /* storage */, collect); err != nil {
			return nil, err
		}
		if err := p.Unwind(logPrefix, s, u, true /* storage */, collect); err != nil {
			return nil, err
		}
	}

	return trie.NewFlatDBTrieLoader(logPrefix, rl, accTrieCollectorFunc, stTrieCollectorFunc, false), nil
}

func unwindIntermediateHashesStageImpl(logPrefix string, u *UnwindState, s *StageState, db kv.RwTx, cfg TrieCfg, expectedRootHash libcommon.Hash, quit <-chan struct{}, logger log.Logger) error {
	accTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer accTrieCollector.Close()
	accTrieCollectorFunc := accountTrieCollector(accTrieCollector)

	stTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer stTrieCollector.Close()
	stTrieCollectorFunc := storageTrieCollector(stTrieCollector)

	rl := trie.NewRetainList(0)

	loader, err := UnwindIntermediateHashesForTrieLoader(logPrefix, rl, u, s, db, cfg, accTrieCollectorFunc, stTrieCollectorFunc, quit, logger)
	if err != nil {
		return err
	}

	hash, err := loader.CalcTrieRoot(db, quit)
	if err != nil {
		return err
	}
	if hash != expectedRootHash {
		return fmt.Errorf("wrong trie root: %x, expected (from header): %x", hash, expectedRootHash)
	}
	logger.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex())
	if err := accTrieCollector.Load(db, kv.TrieOfAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	if err := stTrieCollector.Load(db, kv.TrieOfStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
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
