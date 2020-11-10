package stagedsync

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/log"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type Stage3Config struct {
	BatchSize       int
	BlockSize       int
	BufferSize      int
	ToProcess       int
	NumOfGoroutines int
	ReadChLen       int
	Now             time.Time
}

func SpawnRecoverSendersStage(cfg Stage3Config, s *StageState, db ethdb.Database, config *params.ChainConfig, toBlock uint64, tmpdir string, quitCh <-chan struct{}) error {
	headHash := rawdb.ReadHeadHeaderHash(db)
	headNumber := rawdb.ReadHeaderNumber(db, headHash)
	if s.BlockNumber == *headNumber {
		s.Done()
		return nil
	}

	batch := db.NewBatch()
	logPrefix := s.state.LogPrefix()

	if err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(s.BlockNumber), 0, func(k, v []byte) (bool, error) {
		number := binary.BigEndian.Uint64(k)
		v, err := rawdb.DecompressBlockBody(v)
		if err != nil {
			return false, err
		}
		body := new(types.Body)
		if err := rlp.Decode(bytes.NewReader(v), body); err != nil {
			return false, err
		}
		signer := types.MakeSigner(config, big.NewInt(int64(number)))
		senders := make([]byte, len(body.Transactions)*common.AddressLength)

		for i, tx := range body.Transactions {
			from, err := signer.Sender(tx)
			if err != nil {
				err = fmt.Errorf("%s: error recovering sender for tx=%x, %w", logPrefix, tx.Hash(), err)
				return false, err
			}
			if tx.Protected() && tx.ChainID().Cmp(signer.ChainID()) != 0 {
				err = fmt.Errorf("%s: invalid chainId, tx.Chain()=%d, igner.ChainID()=%d", logPrefix, tx.ChainID(), signer.ChainID())
				return false, err
			}
			copy(senders[i*common.AddressLength:], from[:])
		}
		if err := batch.Put(dbutils.Senders, dbutils.EncodeBlockNumber(number), senders); err != nil {
			return false, err
		}

		if batch.BatchSize() > batch.IdealBatchSize() {
			log.Info(fmt.Sprintf("%s: recovered senders", logPrefix), "count", number-s.BlockNumber, "number", number)
			if err := s.Update(batch, number); err != nil {
				return false, err
			}
			_, err := batch.Commit()
			return true, err
		}

		return true, nil
	}); err != nil {
		return err
	}

	_, err := batch.Commit()

	if err != nil {
		return err
	}

	return s.DoneAndUpdate(db, *headNumber)
}

func UnwindSendersStage(u *UnwindState, s *StageState, stateDB ethdb.Database) error {
	// Does not require any special processing
	mutation := stateDB.NewBatch()
	err := u.Done(mutation)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: reset: %v", logPrefix, err)
	}
	_, err = mutation.Commit()
	if err != nil {
		return fmt.Errorf("%s: failed to write db commit: %v", logPrefix, err)
	}
	return nil
}
