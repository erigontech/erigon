package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/log/v3"
)

type HashStateCfg struct {
	db     kv.RwDB
	tmpDir string
}

func StageHashStateCfg(db kv.RwDB, tmpDir string) HashStateCfg {
	return HashStateCfg{
		db:     db,
		tmpDir: tmpDir,
	}
}

func SpawnHashStateStage(s *StageState, tx kv.RwTx, cfg HashStateCfg, ctx context.Context) error {
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
	if s.BlockNumber > to {
		return fmt.Errorf("hashstate: promotion backwards from %d to %d", s.BlockNumber, to)
	}

	if to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Promoting plain state", logPrefix), "from", s.BlockNumber, "to", to)
	}
	if s.BlockNumber == 0 { // Initial hashing of the state is performed at the previous stage
		if err := PromoteHashedStateCleanly(logPrefix, tx, cfg, ctx); err != nil {
			return err
		}
	} else {
		if err := promoteHashedStateIncrementally(logPrefix, s, s.BlockNumber, to, tx, cfg, ctx.Done()); err != nil {
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

func UnwindHashStateStage(u *UnwindState, s *StageState, tx kv.RwTx, cfg HashStateCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := u.LogPrefix()
	if err = unwindHashStateStageImpl(logPrefix, u, s, tx, cfg, ctx.Done()); err != nil {
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

func unwindHashStateStageImpl(logPrefix string, u *UnwindState, s *StageState, tx kv.RwTx, cfg HashStateCfg, quit <-chan struct{}) error {
	// Currently it does not require unwinding because it does not create any Intermediate Hash records
	// and recomputes the state root from scratch
	prom := NewPromoter(tx, quit)
	prom.TempDir = cfg.tmpDir
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

func PromoteHashedStateCleanly(logPrefix string, tx kv.RwTx, cfg HashStateCfg, ctx context.Context) error {
	if err := promotePlainState(
		logPrefix,
		tx,
		cfg.tmpDir,
		etl.IdentityLoadFunc,
		ctx.Done(),
	); err != nil {
		return err
	}

	return etl.Transform(
		logPrefix,
		tx,
		kv.PlainContractCode,
		kv.ContractCode,
		cfg.tmpDir,
		keyTransformExtractFunc(transformContractCodeKey),
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			Quit: ctx.Done(),
		},
	)
}

func promotePlainState(
	logPrefix string,
	tx kv.RwTx,
	tmpdir string,
	loadFunc etl.LoadFunc,
	quit <-chan struct{},
) error {
	bufferSize := etl.BufferOptimalSize

	accCollector := etl.NewCollector(logPrefix, tmpdir, etl.NewSortableBuffer(bufferSize))
	defer accCollector.Close()
	storageCollector := etl.NewCollector(logPrefix, tmpdir, etl.NewSortableBuffer(bufferSize))
	defer storageCollector.Close()

	t := time.Now()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	var m runtime.MemStats

	c, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	defer c.Close()

	convertAccFunc := func(key []byte) ([]byte, error) {
		hash, err := common.HashData(key)
		return hash[:], err
	}

	convertStorageFunc := func(key []byte) ([]byte, error) {
		addrHash, err := common.HashData(key[:length.Addr])
		if err != nil {
			return nil, err
		}
		inc := binary.BigEndian.Uint64(key[length.Addr:])
		secKey, err := common.HashData(key[length.Addr+length.Incarnation:])
		if err != nil {
			return nil, err
		}
		compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, inc, secKey)
		return compositeKey, nil
	}

	var startkey []byte

	// reading kv.PlainState
	for k, v, e := c.Seek(startkey); k != nil; k, v, e = c.Next() {
		if e != nil {
			return e
		}
		if err := libcommon.Stopped(quit); err != nil {
			return err
		}

		if len(k) == 20 {
			newK, err := convertAccFunc(k)
			if err != nil {
				return err
			}
			if err := accCollector.Collect(newK, v); err != nil {
				return err
			}
		} else {
			newK, err := convertStorageFunc(k)
			if err != nil {
				return err
			}
			if err := storageCollector.Collect(newK, v); err != nil {
				return err
			}
		}

		select {
		default:
		case <-logEvery.C:
			libcommon.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s] ETL [1/2] Extracting", logPrefix), "current key", fmt.Sprintf("%x...", k[:6]), "alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
		}
	}

	log.Trace(fmt.Sprintf("[%s] Extraction finished", logPrefix), "took", time.Since(t))
	defer func(t time.Time) {
		log.Trace(fmt.Sprintf("[%s] Load finished", logPrefix), "took", time.Since(t))
	}(time.Now())

	args := etl.TransformArgs{
		Quit: quit,
	}

	if err := accCollector.Load(tx, kv.HashedAccounts, loadFunc, args); err != nil {
		return err
	}

	if err := storageCollector.Load(tx, kv.HashedStorage, loadFunc, args); err != nil {
		return err
	}

	return nil
}

func keyTransformExtractFunc(transformKey func([]byte) ([]byte, error)) etl.ExtractFunc {
	return func(k, v []byte, next etl.ExtractNextFunc) error {
		newK, err := transformKey(k)
		if err != nil {
			return err
		}
		return next(k, newK, v)
	}
}

func transformPlainStateKey(key []byte) ([]byte, error) {
	switch len(key) {
	case length.Addr:
		// account
		hash, err := common.HashData(key)
		return hash[:], err
	case length.Addr + length.Incarnation + length.Hash:
		// storage
		addrHash, err := common.HashData(key[:length.Addr])
		if err != nil {
			return nil, err
		}
		inc := binary.BigEndian.Uint64(key[length.Addr:])
		secKey, err := common.HashData(key[length.Addr+length.Incarnation:])
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

	addrHash, err := common.HashData(address[:])
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

func NewPromoter(db kv.RwTx, quitCh <-chan struct{}) *Promoter {
	return &Promoter{
		db:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          os.TempDir(),
		quitCh:           quitCh,
	}
}

type Promoter struct {
	db               kv.RwTx
	ChangeSetBufSize uint64
	TempDir          string
	quitCh           <-chan struct{}
}

func getExtractFunc(db kv.Tx, changeSetBucket string) etl.ExtractFunc {
	decode := changeset.Mapper[changeSetBucket].Decode
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
	decode := changeset.Mapper[changeSetBucket].Decode
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
	decode := changeset.Mapper[changeSetBucket].Decode
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
	decode := changeset.Mapper[changeSetBucket].Decode
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
	decode := changeset.Mapper[changeSetBucket].Decode
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

func (p *Promoter) Promote(logPrefix string, s *StageState, from, to uint64, storage bool, codes bool) error {
	var changeSetBucket string
	if storage {
		changeSetBucket = kv.StorageChangeSet
	} else {
		changeSetBucket = kv.AccountChangeSet
	}
	if to > from+16 {
		log.Info(fmt.Sprintf("[%s] Incremental promotion started", logPrefix), "from", from, "to", to, "codes", codes, "csbucket", changeSetBucket)
	}

	startkey := dbutils.EncodeBlockNumber(from + 1)

	var loadBucket string
	var extract etl.ExtractFunc
	if codes {
		loadBucket = kv.ContractCode
		extract = getExtractCode(p.db, changeSetBucket)
	} else {
		if storage {
			loadBucket = kv.HashedStorage
		} else {
			loadBucket = kv.HashedAccounts
		}
		extract = getExtractFunc(p.db, changeSetBucket)
	}

	if err := etl.Transform(
		logPrefix,
		p.db,
		changeSetBucket,
		loadBucket,
		p.TempDir,
		extract,
		etl.IdentityLoadFunc,
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

func (p *Promoter) Unwind(logPrefix string, s *StageState, u *UnwindState, storage bool, codes bool) error {
	var changeSetBucket string
	if storage {
		changeSetBucket = kv.StorageChangeSet
	} else {
		changeSetBucket = kv.AccountChangeSet
	}
	from := s.BlockNumber
	to := u.UnwindPoint

	log.Info(fmt.Sprintf("[%s] Unwinding started", logPrefix), "from", from, "to", to, "storage", storage, "codes", codes)

	startkey := dbutils.EncodeBlockNumber(to + 1)

	var l OldestAppearedLoad
	var loadBucket string
	var extractFunc etl.ExtractFunc
	if codes {
		loadBucket = kv.ContractCode
		extractFunc = getCodeUnwindExtractFunc(p.db, changeSetBucket)
		l.innerLoadFunc = etl.IdentityLoadFunc
	} else {
		l.innerLoadFunc = etl.IdentityLoadFunc
		if storage {
			loadBucket = kv.HashedStorage
			extractFunc = getUnwindExtractStorage(changeSetBucket)
		} else {
			loadBucket = kv.HashedAccounts
			extractFunc = getUnwindExtractAccounts(p.db, changeSetBucket)
		}
	}

	return etl.Transform(
		logPrefix,
		p.db,
		changeSetBucket,
		loadBucket,
		p.TempDir,
		extractFunc,
		l.LoadFunc,
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			Quit:            p.quitCh,
			LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"progress", etl.ProgressFromKey(k)}
			},
			LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"progress", etl.ProgressFromKey(k) + 50} // loading is the second stage, from 50..100
			},
		},
	)
}

func promoteHashedStateIncrementally(logPrefix string, s *StageState, from, to uint64, db kv.RwTx, cfg HashStateCfg, quit <-chan struct{}) error {
	prom := NewPromoter(db, quit)
	prom.TempDir = cfg.tmpDir
	if err := prom.Promote(logPrefix, s, from, to, false /* storage */, true /* codes */); err != nil {
		return err
	}
	if err := prom.Promote(logPrefix, s, from, to, false /* storage */, false /* codes */); err != nil {
		return err
	}
	if err := prom.Promote(logPrefix, s, from, to, true /* storage */, false /* codes */); err != nil {
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
