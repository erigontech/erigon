package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"runtime"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
)

type HashStateCfg struct {
	db   kv.RwDB
	dirs datadir.Dirs

	historyV3 bool
}

func StageHashStateCfg(db kv.RwDB, dirs datadir.Dirs, historyV3 bool) HashStateCfg {
	return HashStateCfg{
		db:        db,
		dirs:      dirs,
		historyV3: historyV3,
	}
}

func SpawnHashStateStage(s *StageState, tx kv.RwTx, cfg HashStateCfg, ctx context.Context, logger log.Logger) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := s.LogPrefix()
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		return nil
	}
	if s.BlockNumber > to { // Erigon will self-heal (download missed blocks) eventually
		log.Warn(fmt.Sprintf("[%s] promotion backwards from %d to %d", s.LogPrefix(), s.BlockNumber, to))
		return nil
	}

	if to > s.BlockNumber+16 {
		logger.Info(fmt.Sprintf("[%s] Promoting plain state", logPrefix), "from", s.BlockNumber, "to", to)
	}
	if s.BlockNumber == 0 { // Initial hashing of the state is performed at the previous stage
		if err := PromoteHashedStateCleanly(logPrefix, tx, cfg, ctx, logger); err != nil {
			return err
		}
	} else {
		if err := promoteHashedStateIncrementally(logPrefix, s.BlockNumber, to, tx, cfg, ctx, logger); err != nil {
			return err
		}
	}

	if err = s.Update(tx, to); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindHashStateStage(u *UnwindState, s *StageState, tx kv.RwTx, cfg HashStateCfg, ctx context.Context, logger log.Logger) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := u.LogPrefix()
	if err = unwindHashStateStageImpl(logPrefix, u, s, tx, cfg, ctx, logger); err != nil {
		return err
	}
	if err = u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindHashStateStageImpl(logPrefix string, u *UnwindState, s *StageState, tx kv.RwTx, cfg HashStateCfg, ctx context.Context, logger log.Logger) error {
	// Currently it does not require unwinding because it does not create any Intermediate Hash records
	// and recomputes the state root from scratch
	prom := NewPromoter(tx, cfg.dirs, ctx, logger)
	if cfg.historyV3 {
		if err := prom.UnwindOnHistoryV3(logPrefix, s.BlockNumber, u.UnwindPoint, false, true); err != nil {
			return err
		}
		if err := prom.UnwindOnHistoryV3(logPrefix, s.BlockNumber, u.UnwindPoint, false, false); err != nil {
			return err
		}
		if err := prom.UnwindOnHistoryV3(logPrefix, s.BlockNumber, u.UnwindPoint, true, false); err != nil {
			return err
		}
		return nil
	}
	if err := prom.Unwind(logPrefix, s, u, false /* storage */, true /* codes */); err != nil {
		return err
	}
	if err := prom.Unwind(logPrefix, s, u, false /* storage */, false /* codes */); err != nil {
		return err
	}
	if err := prom.Unwind(logPrefix, s, u, true /* storage */, false /* codes */); err != nil {
		return err
	}
	return nil
}

func PromoteHashedStateCleanly(logPrefix string, tx kv.RwTx, cfg HashStateCfg, ctx context.Context, logger log.Logger) error {
	err := promotePlainState(
		logPrefix,
		cfg.db,
		tx,
		cfg.dirs.Tmp,
		ctx,
		logger,
	)
	if err != nil {
		return err
	}

	return etl.Transform(
		logPrefix,
		tx,
		kv.PlainContractCode,
		kv.ContractCode,
		cfg.dirs.Tmp,
		func(k, v []byte, next etl.ExtractNextFunc) error {
			newK, err := transformContractCodeKey(k)
			if err != nil {
				return err
			}
			return next(k, newK, v)
		},
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			Quit: ctx.Done(),
		},
		logger,
	)
}

func promotePlainState(
	logPrefix string,
	db kv.RoDB,
	tx kv.RwTx,
	tmpdir string,
	ctx context.Context,
	logger log.Logger,
) error {
	accCollector := etl.NewCollector(logPrefix, tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer accCollector.Close()
	accCollector.LogLvl(log.LvlTrace)
	storageCollector := etl.NewCollector(logPrefix, tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer storageCollector.Close()
	storageCollector.LogLvl(log.LvlTrace)

	transform := func(k, v []byte) ([]byte, []byte, error) {
		newK, err := transformPlainStateKey(k)
		return newK, v, err
	}
	collect := func(k, v []byte) error {
		if len(k) == 32 {
			return accCollector.Collect(k, v)
		}
		return storageCollector.Collect(k, v)
	}

	{ //errgroup cancelation scope
		// pipeline: extract -> transform -> collect
		in, out := make(chan pair, 10_000), make(chan pair, 10_000)
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			defer close(out)
			return parallelTransform(ctx, in, out, transform, estimate.AlmostAllCPUs()).Wait()
		})
		g.Go(func() error { return collectChan(ctx, out, collect) })
		g.Go(func() error { return parallelWarmup(ctx, db, kv.PlainState, 2) })

		if err := extractTableToChan(ctx, tx, kv.PlainState, in, logPrefix, logger); err != nil {
			// if ctx canceled, then maybe it's because of error in errgroup
			//
			// errgroup doesn't play with pattern where some 1 goroutine-producer is outside of errgroup
			// but RwTx doesn't allow move between goroutines
			if errors.Is(err, context.Canceled) {
				return g.Wait()
			}
			return err
		}

		if err := g.Wait(); err != nil {
			return fmt.Errorf("g.Wait: %w", err)
		}
	}

	args := etl.TransformArgs{Quit: ctx.Done()}
	if err := accCollector.Load(tx, kv.HashedAccounts, etl.IdentityLoadFunc, args); err != nil {
		return fmt.Errorf("accCollector.Load: %w", err)
	}
	if err := storageCollector.Load(tx, kv.HashedStorage, etl.IdentityLoadFunc, args); err != nil {
		return fmt.Errorf("storageCollector.Load: %w", err)
	}

	return nil
}

type pair struct{ k, v []byte }

func extractTableToChan(ctx context.Context, tx kv.Tx, table string, in chan pair, logPrefix string, logger log.Logger) error {
	defer close(in)
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	var m runtime.MemStats

	return tx.ForEach(table, nil, func(k, v []byte) error {
		select { // this select can't print logs, because of
		case in <- pair{k: k, v: v}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-logEvery.C:
			dbg.ReadMemStats(&m)
			logger.Info(fmt.Sprintf("[%s] ETL [1/2] Extracting", logPrefix), "current_prefix", hex.EncodeToString(k[:4]), "alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
		default:
		}
		return nil
	})
}

func collectChan(ctx context.Context, out chan pair, collect func(k, v []byte) error) error {
	for {
		select {
		case item, ok := <-out:
			if !ok {
				return nil
			}
			if err := collect(item.k, item.v); err != nil {
				return fmt.Errorf("collectChan: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func parallelTransform(ctx context.Context, in chan pair, out chan pair, transform func(k, v []byte) ([]byte, []byte, error), workers int) *errgroup.Group {
	hashG, ctx := errgroup.WithContext(ctx)
	for i := 0; i < workers; i++ {
		hashG.Go(func() error {
			for item := range in {
				k, v, err := transform(item.k, item.v)
				if err != nil {
					return err
				}

				select {
				case out <- pair{k: k, v: v}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}
	return hashG
}

func parallelWarmup(ctx context.Context, db kv.RoDB, bucket string, workers int) error {
	if db == nil || ctx == nil || workers == 0 {
		return nil
	}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for i := 0; i < 256; i++ {
		i := i
		g.Go(func() error {
			return db.View(ctx, func(tx kv.Tx) error {
				it, err := tx.Prefix(bucket, []byte{byte(i)})
				if err != nil {
					return err
				}
				for it.HasNext() {
					_, _, err = it.Next()
					if err != nil {
						return err
					}
				}
				return nil
			})
		})
	}
	return g.Wait()
}

func transformPlainStateKey(key []byte) ([]byte, error) {
	switch len(key) {
	case length.Addr:
		// account
		hash, err := libcommon.HashData(key)
		return hash[:], err
	case length.Addr + length.Incarnation + length.Hash:
		// storage
		addrHash, err := libcommon.HashData(key[:length.Addr])
		if err != nil {
			return nil, err
		}
		inc := binary.BigEndian.Uint64(key[length.Addr:])
		secKey, err := libcommon.HashData(key[length.Addr+length.Incarnation:])
		if err != nil {
			return nil, err
		}
		compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, inc, secKey)
		return compositeKey, nil
	default:
		// no other keys are supported
		return nil, fmt.Errorf("could not convert key from plain to hashed, unexpected len: %d", len(key))
	}
}

func transformContractCodeKey(key []byte) ([]byte, error) {
	if len(key) != length.Addr+length.Incarnation {
		return nil, fmt.Errorf("could not convert code key from plain to hashed, unexpected len: %d", len(key))
	}
	address, incarnation := dbutils.PlainParseStoragePrefix(key)

	addrHash, err := libcommon.HashData(address[:])
	if err != nil {
		return nil, err
	}

	compositeKey := dbutils.GenerateStoragePrefix(addrHash[:], incarnation)
	return compositeKey, nil
}

type OldestAppearedLoad struct {
	innerLoadFunc etl.LoadFunc
	lastKey       bytes.Buffer
}

func (l *OldestAppearedLoad) LoadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	if bytes.Equal(k, l.lastKey.Bytes()) {
		return nil
	}
	l.lastKey.Reset()
	//nolint:errcheck
	l.lastKey.Write(k)
	return l.innerLoadFunc(k, v, table, next)
}

func NewPromoter(db kv.RwTx, dirs datadir.Dirs, ctx context.Context, logger log.Logger) *Promoter {
	return &Promoter{
		tx:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		dirs:             dirs,
		ctx:              ctx,
		logger:           logger,
	}
}

type Promoter struct {
	tx               kv.RwTx
	ChangeSetBufSize uint64
	dirs             datadir.Dirs
	ctx              context.Context
	logger           log.Logger
}

func getExtractFunc(db kv.Tx, changeSetBucket string) etl.ExtractFunc {
	decode := historyv2.Mapper[changeSetBucket].Decode
	return func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, _, err := decode(dbKey, dbValue)
		if err != nil {
			return err
		}
		// ignoring value un purpose, we want the latest one and it is in PlainState
		value, err := db.GetOne(kv.PlainState, k)
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		return next(dbKey, newK, value)
	}
}

func getExtractCode(db kv.Tx, changeSetBucket string) etl.ExtractFunc {
	decode := historyv2.Mapper[changeSetBucket].Decode
	return func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, _, err := decode(dbKey, dbValue)
		if err != nil {
			return err
		}
		value, err := db.GetOne(kv.PlainState, k)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			return nil
		}

		incarnation, err := accounts.DecodeIncarnationFromStorage(value)
		if err != nil {
			return err
		}
		if incarnation == 0 {
			return nil
		}
		plainKey := dbutils.PlainGenerateStoragePrefix(k, incarnation)
		var codeHash []byte
		codeHash, err = db.GetOne(kv.PlainContractCode, plainKey)
		if err != nil {
			return fmt.Errorf("getFromPlainCodesAndLoad for %x, inc %d: %w", plainKey, incarnation, err)
		}
		if codeHash == nil {
			return nil
		}
		newK, err := transformContractCodeKey(plainKey)
		if err != nil {
			return err
		}
		return next(dbKey, newK, codeHash)
	}
}

func getUnwindExtractStorage(changeSetBucket string) etl.ExtractFunc {
	decode := historyv2.Mapper[changeSetBucket].Decode
	return func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, v, err := decode(dbKey, dbValue)
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		return next(dbKey, newK, v)
	}
}

func getUnwindExtractAccounts(db kv.Tx, changeSetBucket string) etl.ExtractFunc {
	decode := historyv2.Mapper[changeSetBucket].Decode
	return func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, v, err := decode(dbKey, dbValue)
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		if len(v) == 0 {
			return next(dbKey, newK, v)
		}
		var acc accounts.Account
		if err = acc.DecodeForStorage(v); err != nil {
			return err
		}
		if !(acc.Incarnation > 0 && acc.IsEmptyCodeHash()) {
			return next(dbKey, newK, v)
		}

		if codeHash, err := db.GetOne(kv.ContractCode, dbutils.GenerateStoragePrefix(newK, acc.Incarnation)); err == nil {
			copy(acc.CodeHash[:], codeHash)
		} else {
			return fmt.Errorf("adjusting codeHash for ks %x, inc %d: %w", newK, acc.Incarnation, err)
		}

		value := make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(value)
		return next(dbKey, newK, value)
	}
}

func getCodeUnwindExtractFunc(db kv.Tx, changeSetBucket string) etl.ExtractFunc {
	decode := historyv2.Mapper[changeSetBucket].Decode
	return func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, v, err := decode(dbKey, dbValue)
		if err != nil {
			return err
		}
		if len(v) == 0 {
			return nil
		}
		var (
			newK     []byte
			codeHash []byte
		)
		incarnation, err := accounts.DecodeIncarnationFromStorage(v)
		if err != nil {
			return err
		}
		if incarnation == 0 {
			return nil
		}
		plainKey := dbutils.PlainGenerateStoragePrefix(k, incarnation)
		codeHash, err = db.GetOne(kv.PlainContractCode, plainKey)
		if err != nil {
			return fmt.Errorf("getCodeUnwindExtractFunc: %w, key=%x", err, plainKey)
		}
		newK, err = transformContractCodeKey(plainKey)
		if err != nil {
			return err
		}
		return next(dbKey, newK, codeHash)
	}
}

func (p *Promoter) PromoteOnHistoryV3(logPrefix string, from, to uint64, storage bool) error {
	if to > from+16 {
		p.logger.Info(fmt.Sprintf("[%s] Incremental promotion", logPrefix), "from", from, "to", to, "storage", storage)
	}

	txnFrom, err := rawdbv3.TxNums.Min(p.tx, from+1)
	if err != nil {
		return err
	}
	txnTo := uint64(math.MaxUint64)
	collector := etl.NewCollector(logPrefix, p.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), p.logger)
	defer collector.Close()

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

			accBytes, err := p.tx.GetOne(kv.PlainState, k[:20])
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
					continue
				}
			}
			plainKey := dbutils.PlainGenerateCompositeStorageKey(k[:20], incarnation, k[20:])
			newV, err := p.tx.GetOne(kv.PlainState, plainKey)
			if err != nil {
				return err
			}
			newK, err := transformPlainStateKey(plainKey)
			if err != nil {
				return err
			}
			if err := collector.Collect(newK, newV); err != nil {
				return err
			}
		}
		if err := collector.Load(p.tx, kv.HashedStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()}); err != nil {
			return err
		}
		return nil
	}

	codeCollector := etl.NewCollector(logPrefix, p.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), p.logger)
	defer codeCollector.Close()

	it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.AccountsHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return err
		}
		newV, err := p.tx.GetOne(kv.PlainState, k)
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}

		if err := collector.Collect(newK, newV); err != nil {
			return err
		}

		//code
		if len(newV) == 0 {
			continue
		}
		incarnation, err := accounts.DecodeIncarnationFromStorage(newV)
		if err != nil {
			return err
		}
		if incarnation == 0 {
			continue
		}
		plainKey := dbutils.PlainGenerateStoragePrefix(k, incarnation)
		var codeHash []byte
		codeHash, err = p.tx.GetOne(kv.PlainContractCode, plainKey)
		if err != nil {
			return fmt.Errorf("getFromPlainCodesAndLoad for %x, inc %d: %w", plainKey, incarnation, err)
		}
		if codeHash == nil {
			continue
		}
		newCodeK, err := transformContractCodeKey(plainKey)
		if err != nil {
			return err
		}
		if err = codeCollector.Collect(newCodeK, newV); err != nil {
			return err
		}
	}
	if err := collector.Load(p.tx, kv.HashedAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()}); err != nil {
		return err
	}
	if err := codeCollector.Load(p.tx, kv.ContractCode, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()}); err != nil {
		return err
	}
	return nil
}
func (p *Promoter) Promote(logPrefix string, from, to uint64, storage, codes bool) error {
	var changeSetBucket string
	if storage {
		changeSetBucket = kv.StorageChangeSet
	} else {
		changeSetBucket = kv.AccountChangeSet
	}
	if to > from+16 {
		p.logger.Info(fmt.Sprintf("[%s] Incremental promotion", logPrefix), "from", from, "to", to, "codes", codes, "csbucket", changeSetBucket)
	}

	startkey := hexutility.EncodeTs(from + 1)

	var loadBucket string
	var extract etl.ExtractFunc
	if codes {
		loadBucket = kv.ContractCode
		extract = getExtractCode(p.tx, changeSetBucket)
	} else {
		if storage {
			loadBucket = kv.HashedStorage
		} else {
			loadBucket = kv.HashedAccounts
		}
		extract = getExtractFunc(p.tx, changeSetBucket)
	}

	if err := etl.Transform(
		logPrefix,
		p.tx,
		changeSetBucket,
		loadBucket,
		p.dirs.Tmp,
		extract,
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			Quit:            p.ctx.Done(),
		},
		p.logger,
	); err != nil {
		return err
	}

	return nil
}

func (p *Promoter) UnwindOnHistoryV3(logPrefix string, unwindFrom, unwindTo uint64, storage, codes bool) error {
	p.logger.Info(fmt.Sprintf("[%s] Unwinding started", logPrefix), "from", unwindFrom, "to", unwindTo, "storage", storage, "codes", codes)

	txnFrom, err := rawdbv3.TxNums.Min(p.tx, unwindTo+1)
	if err != nil {
		return err
	}
	txnTo := uint64(math.MaxUint64)
	collector := etl.NewCollector(logPrefix, p.dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize), p.logger)
	defer collector.Close()

	acc := accounts.NewAccount()
	if codes {
		it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.AccountsHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
		if err != nil {
			return err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return err
			}

			if len(v) == 0 {
				continue
			}
			if err := accounts.DeserialiseV3(&acc, v); err != nil {
				return err
			}

			incarnation := acc.Incarnation
			if incarnation == 0 {
				continue
			}
			plainKey := dbutils.PlainGenerateStoragePrefix(k, incarnation)
			codeHash, err := p.tx.GetOne(kv.PlainContractCode, plainKey)
			if err != nil {
				return fmt.Errorf("getCodeUnwindExtractFunc: %w, key=%x", err, plainKey)
			}
			newK, err := transformContractCodeKey(plainKey)
			if err != nil {
				return err
			}
			if err = collector.Collect(newK, codeHash); err != nil {
				return err
			}
		}

		return collector.Load(p.tx, kv.ContractCode, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()})
	}

	if storage {
		it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.StorageHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
		if err != nil {
			return err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return err
			}
			val, err := p.tx.GetOne(kv.PlainState, k[:20])
			if err != nil {
				return err
			}
			incarnation := uint64(1)
			if len(val) != 0 {
				oldInc, _ := accounts.DecodeIncarnationFromStorage(val)
				incarnation = oldInc
			}
			plainKey := dbutils.PlainGenerateCompositeStorageKey(k[:20], incarnation, k[20:])
			newK, err := transformPlainStateKey(plainKey)
			if err != nil {
				return err
			}
			if err := collector.Collect(newK, v); err != nil {
				return err
			}
		}
		return collector.Load(p.tx, kv.HashedStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()})
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

		if len(v) == 0 {
			if err = collector.Collect(newK, nil); err != nil {
				return err
			}
			continue
		}
		if err := accounts.DeserialiseV3(&acc, v); err != nil {
			return err
		}
		if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
			if codeHash, err := p.tx.GetOne(kv.ContractCode, dbutils.GenerateStoragePrefix(newK, acc.Incarnation)); err == nil {
				copy(acc.CodeHash[:], codeHash)
			} else {
				return fmt.Errorf("adjusting codeHash for ks %x, inc %d: %w", newK, acc.Incarnation, err)
			}
		}

		value := make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(value)
		if err := collector.Collect(newK, value); err != nil {
			return err
		}
	}
	return collector.Load(p.tx, kv.HashedAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()})
}

func (p *Promoter) Unwind(logPrefix string, s *StageState, u *UnwindState, storage bool, codes bool) error {
	var changeSetBucket string
	if storage {
		changeSetBucket = kv.StorageChangeSet
	} else {
		changeSetBucket = kv.AccountChangeSet
	}
	from := s.BlockNumber
	to := u.UnwindPoint

	p.logger.Info(fmt.Sprintf("[%s] Unwinding started", logPrefix), "from", from, "to", to, "storage", storage, "codes", codes)

	startkey := hexutility.EncodeTs(to + 1)

	var l OldestAppearedLoad
	l.innerLoadFunc = etl.IdentityLoadFunc
	var loadBucket string
	var extractFunc etl.ExtractFunc
	if codes {
		loadBucket = kv.ContractCode
		extractFunc = getCodeUnwindExtractFunc(p.tx, changeSetBucket)
	} else {
		if storage {
			loadBucket = kv.HashedStorage
			extractFunc = getUnwindExtractStorage(changeSetBucket)
		} else {
			loadBucket = kv.HashedAccounts
			extractFunc = getUnwindExtractAccounts(p.tx, changeSetBucket)
		}
	}

	return etl.Transform(
		logPrefix,
		p.tx,
		changeSetBucket,
		loadBucket,
		p.dirs.Tmp,
		extractFunc,
		l.LoadFunc,
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			Quit:            p.ctx.Done(),
			LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"progress", etl.ProgressFromKey(k)}
			},
			LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"progress", etl.ProgressFromKey(k) + 50} // loading is the second stage, from 50..100
			},
		},
		p.logger,
	)
}

func promoteHashedStateIncrementally(logPrefix string, from, to uint64, tx kv.RwTx, cfg HashStateCfg, ctx context.Context, logger log.Logger) error {
	prom := NewPromoter(tx, cfg.dirs, ctx, logger)
	if cfg.historyV3 {
		if err := prom.PromoteOnHistoryV3(logPrefix, from, to, false); err != nil {
			return err
		}
		if err := prom.PromoteOnHistoryV3(logPrefix, from, to, true); err != nil {
			return err
		}
		return nil
	}

	if err := prom.Promote(logPrefix, from, to, false, true); err != nil {
		return err
	}
	if err := prom.Promote(logPrefix, from, to, false, false); err != nil {
		return err
	}
	if err := prom.Promote(logPrefix, from, to, true, false); err != nil {
		return err
	}
	return nil
}

func PruneHashStateStage(s *PruneState, tx kv.RwTx, cfg HashStateCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
