package stagedsync

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func spawnTxPool(s *StageState, db *ethdb.ObjectDatabase, pool *core.TxPool, quitCh <-chan struct{}) error {
	to, err := s.ExecutionAt(db)
	if err != nil {
		return err
	}
	if to < s.BlockNumber {
		return fmt.Errorf("txPoolUpdate to (%d) < from (%d)", to, s.BlockNumber)
	}
	if to - s.BlockNumber <= 1 {
		if pool != nil && !pool.IsStarted() {
			log.Info("Starting tx pool since block numbers converged", "from", s.BlockNumber, "to", to)
			if err := pool.Start(); err != nil {
				return fmt.Errorf("txPoolUpdate start pool: %w", err)
			}
		}
	}
	if pool != nil && pool.IsStarted() && s.BlockNumber > 0 {
		if err := incrementalTxPoolUpdate(s.BlockNumber, to, pool, db, quitCh); err != nil {
			return err
		}
	}
	return s.DoneAndUpdate(db, to)
}

func incrementalTxPoolUpdate(from, to uint64, pool *core.TxPool, db *ethdb.ObjectDatabase, quitCh <-chan struct{}) error {
	headHash := rawdb.ReadCanonicalHash(db, to)
	headHeader := rawdb.ReadHeader(db, headHash, to)
	pool.ResetHead(headHeader.GasLimit, from)
	canonical := make([]common.Hash, to-from+1)
	currentHeaderIdx := uint64(0)

	if err := db.Walk(dbutils.HeaderPrefix, dbutils.EncodeBlockNumber(from+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		// Skip non relevant records
		if !dbutils.CheckCanonicalKey(k) {
			return true, nil
		}

		if currentHeaderIdx > to-from{ // if header stage is ehead of body stage
			return false, nil
		}

		copy(canonical[currentHeaderIdx][:], v)
		currentHeaderIdx++
		return true, nil
	}); err != nil {
		return err
	}
	log.Info("TxPoolUpdate: Reading canonical hashes complete", "hashes", len(canonical))
	if err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(from+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		if blockNumber > to {
			return false, nil
		}

		if canonical[blockNumber-from-1] != blockHash {
			// non-canonical case
			return true, nil
		}

		body := new(types.Body)
		if err := rlp.Decode(bytes.NewReader(v), body); err != nil {
			return false, fmt.Errorf("txPoolUpdate: invalid block body RLP: %w", err)
		}
		for _, tx := range body.Transactions {
			pool.RemoveTx(tx.Hash(), true /* outofbound */)
		}
		return true, nil
	}); err != nil {
		log.Error("TxPoolUpdate: walking over the block bodies", "error", err)
		return err
	}
	return nil
}

func unwindTxPool(u *UnwindState, s *StageState, db *ethdb.ObjectDatabase, pool *core.TxPool, quitCh <-chan struct{}) error {
	if u.UnwindPoint >= s.BlockNumber {
		s.Done()
		return nil
	}
	if pool != nil && pool.IsStarted() {
		if err := unwindTxPoolUpdate(u.UnwindPoint, s.BlockNumber, pool, db, quitCh); err != nil {
			return err
		}
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind TxPool: reset: %w", err)
	}
	return nil
}

func unwindTxPoolUpdate(from, to uint64, pool *core.TxPool, db *ethdb.ObjectDatabase, quitCh <-chan struct{}) error {
	headHash := rawdb.ReadCanonicalHash(db, from)
	headHeader := rawdb.ReadHeader(db, headHash, from)
	pool.ResetHead(headHeader.GasLimit, from)
	canonical := make([]common.Hash, to-from+1)
	currentHeaderIdx := uint64(0)

	if err := db.Walk(dbutils.HeaderPrefix, dbutils.EncodeBlockNumber(from+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		// Skip non relevant records
		if !dbutils.CheckCanonicalKey(k) {
			return true, nil
		}

		if currentHeaderIdx > to-from{ // if header stage is ehead of body stage
			return false, nil
		}

		copy(canonical[currentHeaderIdx][:], v)
		currentHeaderIdx++
		return true, nil
	}); err != nil {
		return err
	}
	log.Info("unwind TxPoolUpdate: Reading canonical hashes complete", "hashes", len(canonical))
	senders := make([][]common.Address, to-from+1)
	sendersIdx := uint64(0)
	if err := db.Walk(dbutils.Senders, dbutils.EncodeBlockNumber(from+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		if blockNumber > to {
			return false, nil
		}

		if canonical[blockNumber-from-1] != blockHash {
			// non-canonical case
			return true, nil
		}
		sendersArray := make([]common.Address, len(v)/common.AddressLength)
		for i := 0; i < len(sendersArray); i++ {
			copy(sendersArray[i][:], v[i*common.AddressLength:])
		}
		senders[sendersIdx] = sendersArray
		sendersIdx++
		return true, nil
	}); err != nil {
		log.Error("TxPoolUpdate: walking over sender", "error", err)
		return err
	}
	var txsToInject []*types.Transaction
	if err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(from+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		if blockNumber > to {
			return false, nil
		}

		if canonical[blockNumber-from-1] != blockHash {
			// non-canonical case
			return true, nil
		}

		body := new(types.Body)
		if err := rlp.Decode(bytes.NewReader(v), body); err != nil {
			return false, fmt.Errorf("unwind TxPoolUpdate: invalid block body RLP: %w", err)
		}
		body.SendersToTxs(senders[blockNumber-from-1])
		for _, tx := range body.Transactions {
			txsToInject = append(txsToInject, tx)
		}
		return true, nil
	}); err != nil {
		log.Error("unwind TxPoolUpdate: walking over the block bodies", "error", err)
		return err
	}
	//nolint:errcheck
	log.Info("unwind TxPoolUpdate: injecting txs into the pool", "number", len(txsToInject))
	pool.AddRemotesSync(txsToInject)
	log.Info("Injection complete")
	return nil
}