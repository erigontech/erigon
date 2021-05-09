package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

func SpawnPruning(s *StageState, tx ethdb.RwTx, quit <-chan struct{}, cfg ExecuteBlockCfg) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	logPrefix := s.state.LogPrefix()
	stateBucket := dbutils.PlainStateBucket
	storageKeyLength := common.AddressLength + common.IncarnationLength + common.HashLength

	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	if executionAt > params.FullImmutabilityThreshold {
		newGenesis := executionAt - params.FullImmutabilityThreshold
		log.Info(fmt.Sprintf("[%s] Wiping changesets", logPrefix), "until", newGenesis)

		maxPrunableBlock := newGenesis - 1

		// Wipe changesets
		changes, errRewind := changeset.RewindData(tx, maxPrunableBlock, 0, cfg.tmpdir, quit)
		if errRewind != nil {
			return fmt.Errorf("%s: getting rewind data: %v", logPrefix, errRewind)
		}
		if err := changes.Load(logPrefix, tx, stateBucket, func(k []byte, value []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			if len(k) == 20 {
				if len(value) > 0 {
					var acc accounts.Account
					if err := acc.DecodeForStorage(value); err != nil {
						return err
					}

					// Fetch the code hash
					recoverCodeHashPlain(&acc, tx, k)
					var address common.Address
					copy(address[:], k)
					if err := cleanupContractCodeBucket(
						logPrefix,
						tx,
						dbutils.PlainContractCodeBucket,
						acc,
						func(db ethdb.Tx, out *accounts.Account) (bool, error) {
							return rawdb.PlainReadAccount(db, address, out)
						},
						func(inc uint64) []byte { return dbutils.PlainGenerateStoragePrefix(address[:], inc) },
					); err != nil {
						return fmt.Errorf("%s: writeAccountPlain for %x: %w", logPrefix, address, err)
					}

					newV := make([]byte, acc.EncodingLengthForStorage())
					acc.EncodeForStorage(newV)
					if err := next(k, k, newV); err != nil {
						return err
					}
				} else {
					if err := next(k, k, nil); err != nil {
						return err
					}
				}
				return nil
			}
			if len(value) > 0 {
				if err := next(k, k[:storageKeyLength], value); err != nil {
					return err
				}
			} else {
				if err := next(k, k[:storageKeyLength], nil); err != nil {
					return err
				}
			}
			return nil

		}, etl.TransformArgs{Quit: quit}); err != nil {
			return err
		}

		if err := changeset.EraseRange(tx, 0, &maxPrunableBlock); err != nil {
			return fmt.Errorf("[%s] %w", logPrefix, err)
		}

		if err := s.Update(tx, maxPrunableBlock); err != nil {
			return err
		}
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	s.Done()

	return nil
}
