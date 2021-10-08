package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

type TxPoolCfg struct {
	db        kv.RwDB
	pool      *core.TxPool
	config    core.TxPoolConfig
	startFunc func()
}

func StageTxPoolCfg(db kv.RwDB, pool *core.TxPool, config core.TxPoolConfig, startFunc func()) TxPoolCfg {
	return TxPoolCfg{
		db:        db,
		pool:      pool,
		config:    config,
		startFunc: startFunc,
	}
}

func SpawnTxPool(s *StageState, tx kv.RwTx, cfg TxPoolCfg, ctx context.Context) error {
	quitCh := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}
	if to == s.BlockNumber {
		return nil
	}

	logPrefix := s.LogPrefix()
	if to < s.BlockNumber {
		return fmt.Errorf("to (%d) < from (%d)", to, s.BlockNumber)
	}
	if cfg.pool != nil && !cfg.pool.IsStarted() {
		log.Info(fmt.Sprintf("[%s] Starting tx pool after sync", logPrefix), "from", s.BlockNumber, "to", to)
		headHash, err := rawdb.ReadCanonicalHash(tx, to)
		if err != nil {
			return err
		}
		headHeader := rawdb.ReadHeader(tx, headHash, to)
		if err := cfg.pool.Start(headHeader.GasLimit, to); err != nil {
			return fmt.Errorf(" start pool phase 1: %w", err)
		}
		if cfg.startFunc != nil {
			cfg.startFunc()
		}
	}
	if cfg.pool != nil && cfg.pool.IsStarted() && s.BlockNumber > 0 {
		if err := incrementalTxPoolUpdate(logPrefix, s.BlockNumber, to, cfg.pool, tx, quitCh); err != nil {
			return err
		}
		pending, queued := cfg.pool.Stats()
		log.Info(fmt.Sprintf("[%s] Transaction stats", logPrefix), "pending", pending, "queued", queued)
	}
	if err := s.Update(tx, to); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func incrementalTxPoolUpdate(logPrefix string, from, to uint64, pool *core.TxPool, tx kv.RwTx, quitCh <-chan struct{}) error {
	headHash, err := rawdb.ReadCanonicalHash(tx, to)
	if err != nil {
		return err
	}

	headHeader := rawdb.ReadHeader(tx, headHash, to)
	pool.ResetHead(headHeader.GasLimit, to)
	canonical := make([]common.Hash, to-from)
	currentHeaderIdx := uint64(0)

	canonicals, err := tx.Cursor(kv.HeaderCanonical)
	if err != nil {
		return err
	}
	defer canonicals.Close()
	for k, v, err := canonicals.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = canonicals.Next() {
		if err != nil {
			return err
		}
		if err := libcommon.Stopped(quitCh); err != nil {
			return err
		}

		if currentHeaderIdx >= to-from { // if header stage is ahead of body stage
			break
		}

		copy(canonical[currentHeaderIdx][:], v)
		currentHeaderIdx++
	}

	log.Trace(fmt.Sprintf("[%s] Read canonical hashes", logPrefix), "hashes", len(canonical))
	bodies, err := tx.Cursor(kv.BlockBody)
	if err != nil {
		return err
	}
	defer bodies.Close()
	for k, _, err := bodies.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, _, err = bodies.Next() {
		if err != nil {
			return err
		}
		if err := libcommon.Stopped(quitCh); err != nil {
			return err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		if blockNumber > to {
			break
		}

		if canonical[blockNumber-from-1] != blockHash {
			// non-canonical case
			continue
		}

		body := rawdb.ReadBodyWithTransactions(tx, blockHash, blockNumber)
		for _, tx := range body.Transactions {
			pool.RemoveTx(tx.Hash(), true /* outofbound */)
		}
	}
	return nil
}

func UnwindTxPool(u *UnwindState, s *StageState, tx kv.RwTx, cfg TxPoolCfg, ctx context.Context) (err error) {
	if u.UnwindPoint >= s.BlockNumber {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	quitCh := ctx.Done()

	logPrefix := s.LogPrefix()
	if cfg.pool != nil && cfg.pool.IsStarted() {
		if err := unwindTxPoolUpdate(logPrefix, u.UnwindPoint, s.BlockNumber, cfg.pool, tx, quitCh); err != nil {
			return err
		}
		pending, queued := cfg.pool.Stats()
		log.Info(fmt.Sprintf("[%s] Transaction stats", logPrefix), "pending", pending, "queued", queued)
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

func unwindTxPoolUpdate(logPrefix string, from, to uint64, pool *core.TxPool, tx kv.RwTx, quitCh <-chan struct{}) error {
	headHash, err := rawdb.ReadCanonicalHash(tx, from)
	if err != nil {
		return err
	}
	headHeader := rawdb.ReadHeader(tx, headHash, from)
	pool.ResetHead(headHeader.GasLimit, from)
	canonical := make([]common.Hash, to-from)

	canonicals, err := tx.Cursor(kv.HeaderCanonical)
	if err != nil {
		return err
	}
	defer canonicals.Close()
	for k, v, err := canonicals.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = canonicals.Next() {
		if err != nil {
			return err
		}
		if err := libcommon.Stopped(quitCh); err != nil {
			return err
		}
		blockNumber := binary.BigEndian.Uint64(k[:8])

		if blockNumber > to {
			break
		}

		copy(canonical[blockNumber-from-1][:], v)
	}
	log.Trace(fmt.Sprintf("[%s] Read canonical hashes", logPrefix), "hashes", len(canonical))
	senders := make([][]common.Address, to-from+1)
	sendersC, err := tx.Cursor(kv.Senders)
	if err != nil {
		return err
	}
	defer sendersC.Close()
	for k, v, err := sendersC.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = sendersC.Next() {
		if err != nil {
			return err
		}
		if err := libcommon.Stopped(quitCh); err != nil {
			return err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		if blockNumber > to {
			break
		}

		if canonical[blockNumber-from-1] != blockHash {
			// non-canonical case
			continue
		}
		sendersArray := make([]common.Address, len(v)/length.Addr)
		for i := 0; i < len(sendersArray); i++ {
			copy(sendersArray[i][:], v[i*length.Addr:])
		}
		senders[blockNumber-from-1] = sendersArray
	}

	var txsToInject []types.Transaction
	bodies, err := tx.Cursor(kv.BlockBody)
	if err != nil {
		return err
	}
	defer bodies.Close()
	for k, _, err := bodies.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, _, err = bodies.Next() {
		if err != nil {
			return err
		}
		if err := libcommon.Stopped(quitCh); err != nil {
			return err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		if blockNumber > to {
			break
		}

		if canonical[blockNumber-from-1] != blockHash {
			// non-canonical case
			continue
		}

		body := rawdb.ReadBodyWithTransactions(tx, blockHash, blockNumber)
		body.SendersToTxs(senders[blockNumber-from-1])
		txsToInject = append(txsToInject, body.Transactions...)
	}
	//nolint:errcheck
	log.Info(fmt.Sprintf("[%s] Injecting txs into the pool", logPrefix), "number", len(txsToInject))
	pool.AddRemotesSync(txsToInject)
	log.Info(fmt.Sprintf("[%s] Injection complete", logPrefix))
	return nil
}

func PruneTxPool(s *PruneState, tx kv.RwTx, cfg TxPoolCfg, ctx context.Context) (err error) {
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
