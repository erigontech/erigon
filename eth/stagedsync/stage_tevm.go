package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/olddb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

type TranspileCfg struct {
	db          kv.RwDB
	batchSize   datasize.ByteSize
	chainConfig *params.ChainConfig
}

func StageTranspileCfg(
	kv kv.RwDB,
	batchSize datasize.ByteSize,
	chainConfig *params.ChainConfig,
) TranspileCfg {
	return TranspileCfg{
		db:          kv,
		batchSize:   batchSize,
		chainConfig: chainConfig,
	}
}

func SpawnTranspileStage(s *StageState, tx kv.RwTx, toBlock uint64, cfg TranspileCfg, ctx context.Context) error {
	var prevStageProgress uint64
	var errStart error

	if tx == nil {
		errStart = cfg.db.View(ctx, func(tx kv.Tx) error {
			prevStageProgress, errStart = stages.GetStageProgress(tx, stages.Execution)
			return errStart
		})
	} else {
		prevStageProgress, errStart = stages.GetStageProgress(tx, stages.Execution)
	}

	if errStart != nil {
		return errStart
	}

	var to = prevStageProgress
	if toBlock > 0 {
		to = cmp.Min(prevStageProgress, toBlock)
	}

	if to <= s.BlockNumber {
		return nil
	}

	stageProgress := uint64(0)
	logPrefix := s.LogPrefix()
	if to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Contract translation", logPrefix), "from", s.BlockNumber, "to", to)
	}

	empty := common.Address{}

	observedAddresses := map[common.Address]struct{}{
		empty: {},
	}
	observedCodeHashes := map[common.Hash]struct{}{}

	var err error
	for stageProgress <= toBlock {
		stageProgress, err = transpileBatch(logPrefix, stageProgress, to, cfg, tx, observedAddresses, observedCodeHashes, ctx.Done())
		if err != nil {
			return err
		}
	}

	if to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", toBlock)
	}

	return nil
}

func transpileBatch(logPrefix string, stageProgress, toBlock uint64, cfg TranspileCfg, tx kv.RwTx, observedAddresses map[common.Address]struct{}, observedCodeHashes map[common.Hash]struct{}, quitCh <-chan struct{}) (uint64, error) {
	useExternalTx := tx != nil
	var err error
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return 0, err
		}
		defer tx.Rollback()
	}

	batch := olddb.NewBatch(tx, quitCh)
	defer batch.Rollback()

	// read contracts pending for translation
	c, err := tx.CursorDupSort(kv.CallTraceSet)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	logTime := time.Now()
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	stateReader := state.NewPlainStateReader(batch)

	var (
		codeHash       common.Hash
		codeHashBytes  []byte
		addr           common.Address
		addrBytes      []byte
		acc            *accounts.Account
		evmContract    []byte
		transpiledCode []byte
		ok             bool
	)

	prevContract := stageProgress
	blockKey := dbutils.EncodeBlockNumber(stageProgress)

	var addressStatus []byte
	for blockKey, addressStatus, err = c.SeekExact(blockKey); blockKey != nil; blockKey, addressStatus, err = c.Next() {
		if err != nil {
			return 0, fmt.Errorf("can't read pending code translations: %w", err)
		}

		select {
		case <-quitCh:
			return 0, libcommon.ErrStopped
		case <-logEvery.C:
			prevContract, logTime = logTEVMProgress(logPrefix, prevContract, logTime, stageProgress)
			tx.CollectMetrics()
		default:
		}

		stageProgress, err = dbutils.DecodeBlockNumber(blockKey)
		if err != nil {
			return 0, fmt.Errorf("can't read pending code translations. incorrect block key: %w", err)
		}

		if stageProgress > toBlock {
			break
		}

		if addressStatus[len(addressStatus)-1]&4 == 0 {
			continue
		}

		addrBytes = addressStatus[:len(addressStatus)-1]
		addr = common.BytesToAddress(addrBytes)

		_, ok = observedAddresses[addr]
		if ok {
			continue
		}
		observedAddresses[addr] = struct{}{}

		acc, err = stateReader.ReadAccountData(addr)
		if err != nil {
			if errors.Is(err, ethdb.ErrKeyNotFound) {
				continue
			}
			return 0, fmt.Errorf("can't read account by address %q: %w", addr, err)
		}
		if acc == nil {
			continue
		}

		codeHash = acc.CodeHash
		if ok = accounts.IsEmptyCodeHash(codeHash); ok {
			continue
		}
		codeHashBytes = codeHash.Bytes()

		_, ok = observedCodeHashes[codeHash]
		if ok {
			continue
		}
		observedCodeHashes[codeHash] = struct{}{}

		// check if we already have TEVM code
		ok, err = batch.Has(kv.ContractTEVMCode, codeHashBytes)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return 0, fmt.Errorf("can't read code TEVM bucket by contract hash %q: %w", codeHash, err)
		}
		if ok && err == nil {
			// already has TEVM code
			continue
		}

		// load the contract code
		evmContract, err = batch.GetOne(kv.Code, codeHashBytes)
		if err != nil {
			if errors.Is(err, ethdb.ErrKeyNotFound) {
				continue
			}
			return 0, fmt.Errorf("can't read pending code translations. incorrect code hash in the bucket: %w", err)
		}
		if len(evmContract) == 0 {
			continue
		}

		// call a transpiler
		transpiledCode, err = transpileCode(evmContract)
		if err != nil {
			if errors.Is(err, ethdb.ErrKeyNotFound) {
				log.Warn("cannot find EVM contract", "address", addr, "hash", codeHash)
				continue
			}
			return 0, fmt.Errorf("contract %q cannot be translated: %w", codeHash, err)
		}

		// store TEVM contract code
		err = batch.Put(kv.ContractTEVMCode, codeHashBytes, transpiledCode)
		if err != nil {
			return 0, fmt.Errorf("cannot store TEVM code %q: %w", codeHash, err)
		}

		if batch.BatchSize() >= int(cfg.batchSize) {
			break // limit RAM usage. Break to commit batch
		}
	}

	if err = batch.Commit(); err != nil {
		return 0, fmt.Errorf("cannot commit the batch of translations on %q: %w", codeHash, err)
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return 0, fmt.Errorf("cannot commit the external transation on %q: %w", codeHash, err)
		}
	}

	return stageProgress, nil
}

func logTEVMProgress(logPrefix string, prevContract uint64, prevTime time.Time, currentContract uint64) (uint64, time.Time) {
	currentTime := time.Now()
	interval := currentTime.Sub(prevTime)
	speed := float64(currentContract-prevContract) / float64(interval/time.Second)
	var m runtime.MemStats
	libcommon.ReadMemStats(&m)
	var logpairs = []interface{}{
		"number", currentContract,
		"contracts/s", speed,
	}
	logpairs = append(logpairs, "alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
	log.Info(fmt.Sprintf("[%s] Translated contracts", logPrefix), logpairs...)

	return currentContract, currentTime
}

func UnwindTranspileStage(u *UnwindState, s *StageState, tx kv.RwTx, cfg TranspileCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	keyStart := dbutils.EncodeBlockNumber(u.UnwindPoint + 1)
	c, err := tx.CursorDupSort(kv.CallTraceSet)
	if err != nil {
		return err
	}
	defer c.Close()

	var (
		codeHash      common.Hash
		codeHashBytes []byte
		addr          common.Address
		addrBytes     []byte
		acc           *accounts.Account
		ok            bool
	)

	stateReader := state.NewPlainStateReader(tx)

	for k, addrStatus, err := c.Seek(keyStart); k != nil; k, addrStatus, err = c.Next() {
		if err != nil {
			return err
		}

		if addrStatus[len(addrStatus)-1]&4 == 0 {
			continue
		}

		addrBytes = addrStatus[:len(addrStatus)-1]
		addr = common.BytesToAddress(addrBytes)

		acc, err = stateReader.ReadAccountData(addr)
		if err != nil {
			if errors.Is(err, ethdb.ErrKeyNotFound) {
				continue
			}
			return fmt.Errorf("can't read account by address %q: %w", addr, err)
		}
		if acc == nil {
			continue
		}

		codeHash = acc.CodeHash
		if ok = accounts.IsEmptyCodeHash(codeHash); ok {
			continue
		}
		codeHashBytes = codeHash.Bytes()

		// check if we already have TEVM code
		ok, err = tx.Has(kv.ContractTEVMCode, codeHashBytes)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return fmt.Errorf("can't read code TEVM bucket by contract hash %q: %w", codeHash, err)
		}
		if err != nil || !ok {
			// doesn't have TEVM code
			continue
		}

		err = tx.Delete(kv.ContractTEVMCode, codeHashBytes)
		if err != nil {
			return fmt.Errorf("can't delete TEVM code by hash %q: %w", codeHash, err)
		}
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

// todo: TBD actual TEVM translator
func transpileCode(code []byte) ([]byte, error) {
	return append(make([]byte, 0, len(code)), code...), nil
}

func PruneTranspileStage(p *PruneState, tx kv.RwTx, cfg TranspileCfg, initialCycle bool, ctx context.Context) (err error) {
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
