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

func SpawnHashStateStage(s *StageState, db ethdb.Database, datadir string, quit <-chan struct{}) error {
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

	if s.WasInterrupted() {
		log.Info("Cleanly promoting plain state", "from", s.BlockNumber, "to", to)
		if err := ResetHashState(db); err != nil {
			return err
		}
		log.Debug("Clean bucket: done")
		if err := promoteHashedStateCleanly(s, db, datadir, quit); err != nil {
			return err
		}
		return s.DoneAndUpdate(db, to)
	}

	if s.BlockNumber > 0 { // Initial hashing of the state is performed at the previous stage
		log.Info("Promoting plain state", "from", s.BlockNumber, "to", to)
		if err := promoteHashedStateIncrementally(s, s.BlockNumber, to, db, datadir, quit); err != nil {
			return err
		}
	}

	return s.DoneAndUpdate(db, to)
}

func UnwindHashStateStage(u *UnwindState, s *StageState, db ethdb.Database, datadir string, quit <-chan struct{}) error {
	fromScratch := u.UnwindPoint == 0 || u.WasInterrupted()
	if fromScratch {
		if err := ResetHashState(db); err != nil {
			return err
		}

		if err := promoteHashedStateCleanly(s, db, datadir, quit); err != nil {
			return err
		}
		// here we are on same block as Exec step
	}

	if err := unwindHashStateStageImpl(u, s, db, datadir, quit); err != nil {
		return err
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind HashState: reset: %v", err)
	}
	return nil
}

func unwindHashStateStageImpl(u *UnwindState, s *StageState, stateDB ethdb.Database, datadir string, quit <-chan struct{}) error {
	// Currently it does not require unwinding because it does not create any Intemediate Hash records
	// and recomputes the state root from scratch
	prom := NewPromoter(stateDB, quit)
	prom.TempDir = datadir
	if err := prom.Unwind(s, u, false /* storage */, false /* codes */, 0x00); err != nil {
		return err
	}
	if err := prom.Unwind(s, u, false /* storage */, true /* codes */, 0x01); err != nil {
		return err
	}
	if err := prom.Unwind(s, u, true /* storage */, false /* codes */, 0x02); err != nil {
		return err
	}
	return nil
}

func promoteHashedStateCleanly(s *StageState, db ethdb.Database, datadir string, quit <-chan struct{}) error {
	toStateStageData := func(k []byte) []byte {
		return append([]byte{0xFF}, k...)
	}

	err := etl.Transform(
		db,
		dbutils.PlainStateBucket,
		dbutils.CurrentStateBucket,
		datadir,
		keyTransformExtractFunc(transformPlainStateKey),
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			Quit: quit,
			OnLoadCommit: func(batch ethdb.Putter, key []byte, isDone bool) error {
				if isDone {
					return s.UpdateWithStageData(batch, s.BlockNumber, toStateStageData(nil))
				}
				return s.UpdateWithStageData(batch, s.BlockNumber, toStateStageData(key))
			},
		},
	)
	if err != nil {
		return err
	}

	toCodeStageData := func(k []byte) []byte {
		return append([]byte{0xCD}, k...)
	}

	return etl.Transform(
		db,
		dbutils.PlainContractCodeBucket,
		dbutils.ContractCodeBucket,
		datadir,
		keyTransformExtractFunc(transformContractCodeKey),
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			Quit: quit,
			OnLoadCommit: func(batch ethdb.Putter, key []byte, isDone bool) error {
				if isDone {
					return s.UpdateWithStageData(batch, s.BlockNumber, nil)
				}
				return s.UpdateWithStageData(batch, s.BlockNumber, toCodeStageData(key))
			},
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

func keyTransformLoadFunc(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
	newK, err := transformPlainStateKey(k)
	if err != nil {
		return err
	}
	return next(k, newK, value)
}

func codeKeyTransformLoadFunc(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
	newK, err := transformContractCodeKey(k)
	if err != nil {
		return err
	}
	return next(k, newK, value)
}

type OldestAppearedLoad struct {
	innerLoadFunc etl.LoadFunc
	lastKey       bytes.Buffer
}

func (l *OldestAppearedLoad) LoadFunc(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
	if bytes.Equal(k, l.lastKey.Bytes()) {
		return nil
	}
	l.lastKey.Reset()
	//nolint:errcheck
	l.lastKey.Write(k)
	return l.innerLoadFunc(k, value, state, next)
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

func getExtractFunc(changeSetBucket []byte) etl.ExtractFunc {
	walkerAdapter := changeset.Mapper[string(changeSetBucket)].WalkerAdapter
	return func(_, changesetBytes []byte, next etl.ExtractNextFunc) error {
		return walkerAdapter(changesetBytes).Walk(func(k, _ []byte) error {
			return next(k, k, nil)
		})
	}
}

func getUnwindExtractFunc(changeSetBucket []byte) etl.ExtractFunc {
	walkerAdapter := changeset.Mapper[string(changeSetBucket)].WalkerAdapter
	return func(_, changesetBytes []byte, next etl.ExtractNextFunc) error {
		return walkerAdapter(changesetBytes).Walk(func(k, v []byte) error {
			return next(k, k, v)
		})
	}
}

func getCodeUnwindExtractFunc(db ethdb.Getter) etl.ExtractFunc {
	return func(_, changesetBytes []byte, next etl.ExtractNextFunc) error {
		return changeset.AccountChangeSetPlainBytes(changesetBytes).Walk(func(k, v []byte) error {
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
			newK := dbutils.PlainGenerateStoragePrefix(k, a.Incarnation)
			var codeHash []byte
			codeHash, err = db.Get(dbutils.PlainContractCodeBucket, newK)
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				return fmt.Errorf("getCodeUnwindExtractFunc: %w, key=%x", err, newK)
			}
			return next(k, newK, codeHash)
		})
	}
}

func getFromPlainStateAndLoad(db ethdb.Getter, loadFunc etl.LoadFunc) etl.LoadFunc {
	return func(k []byte, _ []byte, state etl.State, next etl.LoadNextFunc) error {
		// ignoring value un purpose, we want the latest one and it is in PlainStateBucket
		value, err := db.Get(dbutils.PlainStateBucket, k)
		if err == nil || errors.Is(err, ethdb.ErrKeyNotFound) {
			return loadFunc(k, value, state, next)
		}
		return err
	}
}

func getFromPlainCodesAndLoad(db ethdb.Getter, loadFunc etl.LoadFunc) etl.LoadFunc {
	return func(k []byte, _ []byte, state etl.State, next etl.LoadNextFunc) error {
		// ignoring value un purpose, we want the latest one and it is in PlainStateBucket
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
		newK := dbutils.PlainGenerateStoragePrefix(k, a.Incarnation)
		var codeHash []byte
		codeHash, err = db.Get(dbutils.PlainContractCodeBucket, newK)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return fmt.Errorf("getFromPlainCodesAndLoad for %x, inc %d: %w", newK, a.Incarnation, err)
		}
		if codeHash == nil {
			return nil
		}
		return loadFunc(newK, codeHash, state, next)
	}
}

func (p *Promoter) Promote(s *StageState, from, to uint64, storage bool, codes bool, index byte) error {
	var changeSetBucket []byte
	if storage {
		changeSetBucket = dbutils.PlainStorageChangeSetBucket
	} else {
		changeSetBucket = dbutils.PlainAccountChangeSetBucket
	}
	log.Debug("Incremental promotion started", "from", from, "to", to, "csbucket", string(changeSetBucket))

	startkey := dbutils.EncodeTimestamp(from + 1)

	var l OldestAppearedLoad
	var loadBucket []byte
	if codes {
		loadBucket = dbutils.ContractCodeBucket
		l.innerLoadFunc = getFromPlainCodesAndLoad(p.db, codeKeyTransformLoadFunc)
	} else {
		loadBucket = dbutils.CurrentStateBucket
		l.innerLoadFunc = getFromPlainStateAndLoad(p.db, keyTransformLoadFunc)
	}

	return etl.Transform(
		p.db,
		changeSetBucket,
		loadBucket,
		p.TempDir,
		getExtractFunc(changeSetBucket),
		// here we avoid getting the state from changesets,
		// we just care about the accounts that did change,
		// so we can directly read from the PlainTextBuffer
		l.LoadFunc,
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			OnLoadCommit: func(putter ethdb.Putter, key []byte, isDone bool) error {
				if isDone {
					return s.UpdateWithStageData(putter, from, []byte{index})
				}
				return s.UpdateWithStageData(putter, from, append([]byte{index}, key...))
			},
			Quit: p.quitCh,
		},
	)
}

func (p *Promoter) Unwind(s *StageState, u *UnwindState, storage bool, codes bool, index byte) error {
	var changeSetBucket []byte
	if storage {
		changeSetBucket = dbutils.PlainStorageChangeSetBucket
	} else {
		changeSetBucket = dbutils.PlainAccountChangeSetBucket
	}
	from := s.BlockNumber
	to := u.UnwindPoint

	log.Debug("Unwinding started", "from", from, "to", to, "storage", storage, "codes", codes)

	startkey := dbutils.EncodeTimestamp(to + 1)

	var l OldestAppearedLoad
	var loadBucket []byte
	var extractFunc etl.ExtractFunc
	if codes {
		loadBucket = dbutils.ContractCodeBucket
		extractFunc = getCodeUnwindExtractFunc(p.db)
		l.innerLoadFunc = codeKeyTransformLoadFunc
	} else {
		loadBucket = dbutils.CurrentStateBucket
		extractFunc = getUnwindExtractFunc(changeSetBucket)
		l.innerLoadFunc = keyTransformLoadFunc
	}

	return etl.Transform(
		p.db,
		changeSetBucket,
		loadBucket,
		p.TempDir,
		extractFunc,
		l.LoadFunc,
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			OnLoadCommit: func(putter ethdb.Putter, key []byte, isDone bool) error {
				if isDone {
					return u.UpdateWithStageData(putter, []byte{index})
				}
				return u.UpdateWithStageData(putter, append([]byte{index}, key...))
			},
			Quit: p.quitCh,
		},
	)
}

func promoteHashedStateIncrementally(s *StageState, from, to uint64, db ethdb.Database, datadir string, quit <-chan struct{}) error {
	prom := NewPromoter(db, quit)
	prom.TempDir = datadir
	if err := prom.Promote(s, from, to, false /* storage */, false /* codes */, 0x00); err != nil {
		return err
	}
	if err := prom.Promote(s, from, to, false /* storage */, true /* codes */, 0x01); err != nil {
		return err
	}
	if err := prom.Promote(s, from, to, true /* storage */, false /* codes */, 0x02); err != nil {
		return err
	}
	return nil
}
