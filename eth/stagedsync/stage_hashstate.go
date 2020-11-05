package stagedsync

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func SpawnHashStateStage(s *StageState, db ethdb.Database, tmpdir string, quit <-chan struct{}) error {
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
	if s.BlockNumber > to {
		return fmt.Errorf("hashstate: promotion backwards from %d to %d", s.BlockNumber, to)
	}

	logPrefix := s.state.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Promoting plain state", logPrefix), "from", s.BlockNumber, "to", to)
	if s.BlockNumber == 0 { // Initial hashing of the state is performed at the previous stage
		if err := promoteHashedStateCleanly(logPrefix, db, tmpdir, quit); err != nil {
			return err
		}
	} else {
		if err := promoteHashedStateIncrementally(logPrefix, s, s.BlockNumber, to, db, tmpdir, quit); err != nil {
			return err
		}
	}

	return s.DoneAndUpdate(db, to)
}

func UnwindHashStateStage(u *UnwindState, s *StageState, db ethdb.Database, tmpdir string, quit <-chan struct{}) error {
	logPrefix := s.state.LogPrefix()
	if err := unwindHashStateStageImpl(logPrefix, u, s, db, tmpdir, quit); err != nil {
		return err
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("%s: reset: %v", logPrefix, err)
	}
	return nil
}

func unwindHashStateStageImpl(logPrefix string, u *UnwindState, s *StageState, stateDB ethdb.Database, tmpdir string, quit <-chan struct{}) error {
	// Currently it does not require unwinding because it does not create any Intemediate Hash records
	// and recomputes the state root from scratch
	prom := NewPromoter(stateDB, quit)
	prom.TempDir = tmpdir
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

func promoteHashedStateCleanly(logPrefix string, db ethdb.Database, tmpdir string, quit <-chan struct{}) error {
	err := etl.Transform(
		logPrefix,
		db,
		dbutils.PlainStateBucket,
		dbutils.CurrentStateBucket,
		tmpdir,
		keyTransformExtractFunc(transformPlainStateKey),
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			Quit: quit,
		},
	)
	if err != nil {
		return err
	}

	return etl.Transform(
		logPrefix,
		db,
		dbutils.PlainContractCodeBucket,
		dbutils.ContractCodeBucket,
		tmpdir,
		keyTransformExtractFunc(transformContractCodeKey),
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			Quit: quit,
		},
	)
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
	case common.AddressLength:
		// account
		hash, err := common.HashData(key)
		return hash[:], err
	case common.AddressLength + common.IncarnationLength + common.HashLength:
		// storage
		address, incarnation, key := dbutils.PlainParseCompositeStorageKey(key)
		addrHash, err := common.HashData(address[:])
		if err != nil {
			return nil, err
		}
		secKey, err := common.HashData(key[:])
		if err != nil {
			return nil, err
		}
		compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, secKey)
		return compositeKey, nil
	default:
		// no other keys are supported
		return nil, fmt.Errorf("could not convert key from plain to hashed, unexpected len: %d", len(key))
	}
}

func transformContractCodeKey(key []byte) ([]byte, error) {
	if len(key) != common.AddressLength+common.IncarnationLength {
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

func (l *OldestAppearedLoad) LoadFunc(k []byte, value []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	if bytes.Equal(k, l.lastKey.Bytes()) {
		return nil
	}
	l.lastKey.Reset()
	//nolint:errcheck
	l.lastKey.Write(k)
	return l.innerLoadFunc(k, value, table, next)
}

func NewPromoter(db ethdb.Database, quitCh <-chan struct{}) *Promoter {
	return &Promoter{
		db:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          os.TempDir(),
	}
}

type Promoter struct {
	db               ethdb.Database
	ChangeSetBufSize uint64
	TempDir          string
	quitCh           chan struct{}
}

func getExtractFunc(db ethdb.Getter, changeSetBucket string) etl.ExtractFunc {
	keySize := changeset.Mapper[changeSetBucket].KeySize
	return func(_, changesetBytes []byte, next etl.ExtractNextFunc) error {
		k := changesetBytes[:keySize]
		// ignoring value un purpose, we want the latest one and it is in PlainStateBucket
		value, err := db.Get(dbutils.PlainStateBucket, k)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		return next(k, newK, value)
	}
}

func getExtractCode(db ethdb.Getter, changeSetBucket string) etl.ExtractFunc {
	keySize := changeset.Mapper[changeSetBucket].KeySize
	return func(_, changesetBytes []byte, next etl.ExtractNextFunc) error {
		k := changesetBytes[:keySize]
		value, err := db.Get(dbutils.PlainStateBucket, k)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return err
		}
		if len(value) == 0 {
			return nil
		}
		var a accounts.Account
		if err = a.DecodeForStorage(value); err != nil {
			return err
		}
		if a.Incarnation == 0 {
			return nil
		}
		plainKey := dbutils.PlainGenerateStoragePrefix(k, a.Incarnation)
		var codeHash []byte
		codeHash, err = db.Get(dbutils.PlainContractCodeBucket, plainKey)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return fmt.Errorf("getFromPlainCodesAndLoad for %x, inc %d: %w", plainKey, a.Incarnation, err)
		}
		if codeHash == nil {
			return nil
		}
		newK, err := transformContractCodeKey(plainKey)
		if err != nil {
			return err
		}
		return next(k, newK, codeHash)
	}
}

func getUnwindExtractStorage(changeSetBucket string) etl.ExtractFunc {
	mapper := changeset.Mapper[changeSetBucket]
	return func(_, changesetBytes []byte, next etl.ExtractNextFunc) error {
		k, v := changesetBytes[:mapper.KeySize], changesetBytes[mapper.KeySize:]
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		return next(k, newK, v)
	}
}

func getUnwindExtractAccounts(db ethdb.Getter, changeSetBucket string) etl.ExtractFunc {
	mapper := changeset.Mapper[changeSetBucket]
	return func(_, changesetBytes []byte, next etl.ExtractNextFunc) error {
		k, v := changesetBytes[:mapper.KeySize], changesetBytes[mapper.KeySize:]

		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}

		if len(v) == 0 {
			return next(k, newK, v)
		}

		var acc accounts.Account
		if err = acc.DecodeForStorage(v); err != nil {
			return err
		}
		if !(acc.Incarnation > 0 && acc.IsEmptyCodeHash()) {
			return next(k, newK, v)
		}

		if codeHash, err := db.Get(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(newK, acc.Incarnation)); err == nil {
			copy(acc.CodeHash[:], codeHash)
		} else if !errors.Is(err, ethdb.ErrKeyNotFound) {
			return fmt.Errorf("adjusting codeHash for ks %x, inc %d: %w", newK, acc.Incarnation, err)
		}

		value := make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(value)

		return next(k, newK, value)
	}
}

func getCodeUnwindExtractFunc(db ethdb.Getter) etl.ExtractFunc {
	return func(_, changesetBytes []byte, next etl.ExtractNextFunc) error {
		k, v := changesetBytes[:common.HashLength], changesetBytes[common.HashLength:]
		if len(v) == 0 {
			return nil
		}
		var err error
		var a accounts.Account
		if err = a.DecodeForStorage(v); err != nil {
			return err
		}
		if a.Incarnation == 0 {
			return nil
		}
		plainKey := dbutils.PlainGenerateStoragePrefix(k, a.Incarnation)
		var codeHash []byte
		codeHash, err = db.Get(dbutils.PlainContractCodeBucket, plainKey)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return fmt.Errorf("getCodeUnwindExtractFunc: %w, key=%x", err, plainKey)
		}
		newK, err := transformContractCodeKey(plainKey)
		if err != nil {
			return err
		}
		return next(k, newK, codeHash)
	}
}

func (p *Promoter) Promote(logPrefix string, s *StageState, from, to uint64, storage bool, codes bool) error {

	var changeSetBucket string
	if storage {
		changeSetBucket = dbutils.PlainStorageChangeSetBucket2
	} else {
		changeSetBucket = dbutils.PlainAccountChangeSetBucket2
	}
	log.Info(fmt.Sprintf("[%s] Incremental promotion started", logPrefix), "from", from, "to", to, "codes", codes, "csbucket", changeSetBucket)

	startkey := dbutils.EncodeBlockNumber(from + 1)

	var l OldestAppearedLoad
	l.innerLoadFunc = etl.IdentityLoadFunc

	var loadBucket string
	var extract etl.ExtractFunc
	if codes {
		loadBucket = dbutils.ContractCodeBucket
		extract = getExtractCode(p.db, changeSetBucket)
	} else {
		loadBucket = dbutils.CurrentStateBucket
		extract = getExtractFunc(p.db, changeSetBucket)
	}

	return etl.Transform(
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
	)
}

func (p *Promoter) Unwind(logPrefix string, s *StageState, u *UnwindState, storage bool, codes bool) error {
	var changeSetBucket string
	if storage {
		changeSetBucket = dbutils.PlainStorageChangeSetBucket2
	} else {
		changeSetBucket = dbutils.PlainAccountChangeSetBucket2
	}
	from := s.BlockNumber
	to := u.UnwindPoint

	log.Info(fmt.Sprintf("[%s] Unwinding started", logPrefix), "from", from, "to", to, "storage", storage, "codes", codes)

	startkey := dbutils.EncodeBlockNumber(to + 1)

	var l OldestAppearedLoad
	var loadBucket string
	var extractFunc etl.ExtractFunc
	if codes {
		loadBucket = dbutils.ContractCodeBucket
		extractFunc = getCodeUnwindExtractFunc(p.db)
		l.innerLoadFunc = etl.IdentityLoadFunc
	} else {
		l.innerLoadFunc = etl.IdentityLoadFunc
		loadBucket = dbutils.CurrentStateBucket
		if storage {
			extractFunc = getUnwindExtractStorage(changeSetBucket)
		} else {
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

func promoteHashedStateIncrementally(logPrefix string, s *StageState, from, to uint64, db ethdb.Database, tmpdir string, quit <-chan struct{}) error {
	prom := NewPromoter(db, quit)
	prom.TempDir = tmpdir
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
