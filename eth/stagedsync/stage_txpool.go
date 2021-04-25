package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

type TxPoolCfg struct {
	pool      *core.TxPool
	poolStart func() error
}

func StageTxPoolCfg(pool *core.TxPool, poolStart func() error) TxPoolCfg {
	return TxPoolCfg{
		pool:      pool,
		poolStart: poolStart,
	}
}

func spawnTxPool(s *StageState, db ethdb.Database, cfg TxPoolCfg, quitCh <-chan struct{}) error {
	var tx ethdb.RwTx
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = hasTx.Tx().(ethdb.RwTx)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.(ethdb.HasRwKV).RwKV().BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}
	logPrefix := s.state.LogPrefix()
	if to < s.BlockNumber {
		return fmt.Errorf("%s: to (%d) < from (%d)", logPrefix, to, s.BlockNumber)
	}
	if cfg.pool != nil && !cfg.pool.IsStarted() {
		log.Info(fmt.Sprintf("[%s] Starting tx pool after sync", logPrefix), "from", s.BlockNumber, "to", to)
		headHash, err := rawdb.ReadCanonicalHash(tx, to)
		if err != nil {
			return err
		}
		headHeader := rawdb.ReadHeader(tx, headHash, to)
		if err := cfg.pool.Start(headHeader.GasLimit, to); err != nil {
			return fmt.Errorf("%s: start pool phase 1: %w", logPrefix, err)
		}
		if err := cfg.poolStart(); err != nil {
			return fmt.Errorf("%s: start pool phase 2: %w", logPrefix, err)
		}
	}
	if cfg.pool != nil && cfg.pool.IsStarted() && s.BlockNumber > 0 {
		if err := incrementalTxPoolUpdate(logPrefix, s.BlockNumber, to, cfg.pool, tx, quitCh); err != nil {
			return fmt.Errorf("[%s]: %w", logPrefix, err)
		}
		pending, queued := cfg.pool.Stats()
		log.Info(fmt.Sprintf("[%s] Transaction stats", logPrefix), "pending", pending, "queued", queued)
	}
	if err := s.DoneAndUpdate(tx, to); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil

}

func incrementalTxPoolUpdate(logPrefix string, from, to uint64, pool *core.TxPool, tx ethdb.RwTx, quitCh <-chan struct{}) error {
	headHash, err := rawdb.ReadCanonicalHash(tx, to)
	if err != nil {
		return err
	}

	headHeader := rawdb.ReadHeader(tx, headHash, to)
	pool.ResetHead(headHeader.GasLimit, to)
	canonical := make([]common.Hash, to-from)
	currentHeaderIdx := uint64(0)

	canonicals, err := tx.Cursor(dbutils.HeaderCanonicalBucket)
	if err != nil {
		return err
	}
	defer canonicals.Close()
	for k, v, err := canonicals.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = canonicals.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}

		if currentHeaderIdx >= to-from { // if header stage is ahead of body stage
			break
		}

		copy(canonical[currentHeaderIdx][:], v)
		currentHeaderIdx++
	}

	log.Info(fmt.Sprintf("[%s] Read canonical hashes", logPrefix), "hashes", len(canonical))
	bodies, err := tx.Cursor(dbutils.BlockBodyPrefix)
	if err != nil {
		return err
	}
	defer bodies.Close()
	for k, _, err := bodies.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, _, err = bodies.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
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

		body := rawdb.ReadBody(ethdb.NewRoTxDb(tx), blockHash, blockNumber)
		for _, tx := range body.Transactions {
			pool.RemoveTx(tx.Hash(), true /* outofbound */)
		}
	}
	return nil
}

func unwindTxPool(u *UnwindState, s *StageState, db ethdb.Database, cfg TxPoolCfg, quitCh <-chan struct{}) error {
	if u.UnwindPoint >= s.BlockNumber {
		s.Done()
		return nil
	}
	var tx ethdb.RwTx
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = hasTx.Tx().(ethdb.RwTx)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.(ethdb.HasRwKV).RwKV().BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := s.state.LogPrefix()
	if cfg.pool != nil && cfg.pool.IsStarted() {
		if err := unwindTxPoolUpdate(logPrefix, u.UnwindPoint, s.BlockNumber, cfg.pool, tx, quitCh); err != nil {
			return fmt.Errorf("[%s]: %w", logPrefix, err)
		}
		pending, queued := cfg.pool.Stats()
		log.Info(fmt.Sprintf("[%s] Transaction stats", logPrefix), "pending", pending, "queued", queued)
	}
	if err := u.Done(tx); err != nil {
		return fmt.Errorf("%s: reset: %w", logPrefix, err)
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindTxPoolUpdate(logPrefix string, from, to uint64, pool *core.TxPool, tx ethdb.RwTx, quitCh <-chan struct{}) error {
	headHash, err := rawdb.ReadCanonicalHash(tx, from)
	if err != nil {
		return err
	}
	headHeader := rawdb.ReadHeader(tx, headHash, from)
	pool.ResetHead(headHeader.GasLimit, from)
	canonical := make([]common.Hash, to-from)

	canonicals, err := tx.Cursor(dbutils.HeaderCanonicalBucket)
	if err != nil {
		return err
	}
	defer canonicals.Close()
	for k, v, err := canonicals.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = canonicals.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}
		blockNumber := binary.BigEndian.Uint64(k[:8])

		if blockNumber > to {
			break
		}

		copy(canonical[blockNumber-from-1][:], v)
	}
	log.Info(fmt.Sprintf("[%s] Read canonical hashes", logPrefix), "hashes", len(canonical))
	senders := make([][]common.Address, to-from+1)
	sendersC, err := tx.Cursor(dbutils.Senders)
	if err != nil {
		return err
	}
	defer sendersC.Close()
	for k, v, err := sendersC.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = sendersC.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
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
		sendersArray := make([]common.Address, len(v)/common.AddressLength)
		for i := 0; i < len(sendersArray); i++ {
			copy(sendersArray[i][:], v[i*common.AddressLength:])
		}
		senders[blockNumber-from-1] = sendersArray
	}

	var txsToInject []types.Transaction
	bodies, err := tx.Cursor(dbutils.BlockBodyPrefix)
	if err != nil {
		return err
	}
	defer bodies.Close()
	for k, _, err := bodies.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, _, err = bodies.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
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

		body := rawdb.ReadBody(ethdb.NewRoTxDb(tx), blockHash, blockNumber)
		body.SendersToTxs(senders[blockNumber-from-1])
		txsToInject = append(txsToInject, body.Transactions...)
	}
	//nolint:errcheck
	log.Info(fmt.Sprintf("[%s] Injecting txs into the pool", logPrefix), "number", len(txsToInject))
	pool.AddRemotesSync(txsToInject)
	log.Info(fmt.Sprintf("[%s] Injection complete", logPrefix))
	return nil
}
