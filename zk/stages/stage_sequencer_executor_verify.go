package stages

import (
	"context"
	"math"
	"time"

	"bytes"
	"errors"
	"sort"

	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/log/v3"
)

type SequencerExecutorVerifyCfg struct {
	db          kv.RwDB
	verifier    *legacy_executor_verifier.LegacyExecutorVerifier
	txPool      *txpool.TxPool
	chainConfig *chain.Config
	cfgZk       *ethconfig.Zk
}

func StageSequencerExecutorVerifyCfg(
	db kv.RwDB,
	verifier *legacy_executor_verifier.LegacyExecutorVerifier,
	pool *txpool.TxPool,
	chainConfig *chain.Config,
	cfgZk *ethconfig.Zk,
) SequencerExecutorVerifyCfg {
	return SequencerExecutorVerifyCfg{
		db:          db,
		verifier:    verifier,
		txPool:      pool,
		chainConfig: chainConfig,
		cfgZk:       cfgZk,
	}
}

func SpawnSequencerExecutorVerifyStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerExecutorVerifyCfg,
	quiet bool,
) error {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting sequencer verify stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished sequencer verify stage", logPrefix))

	var err error
	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	hermezDb := hermez_db.NewHermezDb(tx)
	hermezDbReader := hermez_db.NewHermezDbReader(tx)

	// progress here is at the batch level
	progress, err := stages.GetStageProgress(tx, stages.SequenceExecutorVerify)
	if err != nil {
		return err
	}

	// progress here is at the block level
	executeProgress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}

	// we need to get the batch number for the latest block, so we can search for new batches to send for
	// verification
	latestBatch, err := hermezDb.GetBatchNoByL2Block(executeProgress)
	if err != nil {
		return err
	}

	isBatchFinished, err := hermezDb.GetIsBatchFullyProcessed(latestBatch)
	if err != nil {
		return err
	}
	// we could be running in a state with no executors so we need instant response that we are in an
	// ok state to save lag in the data stream !!Dragons: there will be no witnesses stored running in
	// this mode of operation
	canVerify := cfg.verifier.HasExecutorsUnsafe()

	// if batch was stopped intermediate and is not finished - we need to finish it first
	// this shouldn't occur since exec stage is before that and should finish the batch
	// but just in case something unexpected happens
	if !isBatchFinished {
		log.Error(fmt.Sprintf("[%s] batch %d is not fully processed in stage_execute", logPrefix, latestBatch))
		canVerify = false
	}

	if !canVerify {
		if latestBatch == injectedBatchNumber {
			return nil
		}

		if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, latestBatch); err != nil {
			return err
		}
		if freshTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}
		return nil
	}

	// get ordered promises from the verifier
	// NB: this call is where the stream write happens (so it will be delayed until this stage is run)
	responses, err := cfg.verifier.ProcessResultsSequentiallyUnsafe(tx)
	if err != nil {
		//TODO: what happen with promises if this request returns here?
		return err
	}

	for _, response := range responses {
		// ensure that the first response is the next batch based on the current stage progress
		// otherwise just return early until we get it
		if response.BatchNumber != progress+1 {
			if freshTx {
				if err = tx.Commit(); err != nil {
					return err
				}
			}
			return nil
		}

		// now check that we are indeed in a good state to continue
		if !response.Valid {
			if cfg.cfgZk.Limbo {
				log.Info(fmt.Sprintf("[%s] identified an invalid batch, entering limbo", s.LogPrefix()), "batch", response.BatchNumber)
				// we have an invalid batch, so we need to notify the txpool that these transactions are spurious
				// and need to go into limbo and then trigger a rewind.  The rewind will put all TX back into the
				// pool, but as it knows about these limbo transactions it will place them into limbo instead
				// of queueing them again

				// now we need to figure out the highest block number in the batch
				// and grab all the transaction hashes along the way to inform the
				// pool of hashes to avoid
				blockNumbers, err := hermezDb.GetL2BlockNosByBatch(response.BatchNumber)
				if err != nil {
					return err
				}
				if len(blockNumbers) == 0 {
					panic("failing to verify a batch without blocks")
				}
				sort.Slice(blockNumbers, func(i, j int) bool {
					return blockNumbers[i] < blockNumbers[j]
				})

				var lowestBlock, highestBlock *types.Block
				forkId, err := hermezDb.GetForkId(response.BatchNumber)
				if err != nil {
					return err
				}

				l1InfoTreeMinTimestamps := make(map[uint64]uint64)
				_, err = cfg.verifier.GetStreamBytes(response.BatchNumber, tx, blockNumbers, hermezDbReader, l1InfoTreeMinTimestamps, nil)
				if err != nil {
					return err
				}

				limboSendersToPreviousTxMap := make(map[string]uint32)
				limboStreamBytesBuilderHelper := newLimboStreamBytesBuilderHelper()

				limboDetails := txpool.NewLimboBatchDetails()
				limboDetails.Witness = response.Witness
				limboDetails.L1InfoTreeMinTimestamps = l1InfoTreeMinTimestamps
				limboDetails.BatchNumber = response.BatchNumber
				limboDetails.ForkId = forkId

				for _, blockNumber := range blockNumbers {
					block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
					if err != nil {
						return err
					}
					highestBlock = block
					if lowestBlock == nil {
						// capture the first block, then we can set the bad block hash in the unwind to terminate the
						// stage loop and broadcast the accumulator changes to the txpool before the next stage loop run
						lowestBlock = block
					}

					for i, transaction := range block.Transactions() {
						var b []byte
						buffer := bytes.NewBuffer(b)
						err = transaction.EncodeRLP(buffer)
						if err != nil {
							return err
						}

						signer := types.MakeSigner(cfg.chainConfig, blockNumber)
						sender, err := transaction.Sender(*signer)
						if err != nil {
							return err
						}
						senderMapKey := sender.Hex()

						blocksForStreamBytes, transactionsToIncludeByIndex := limboStreamBytesBuilderHelper.append(senderMapKey, blockNumber, i)
						streamBytes, err := cfg.verifier.GetStreamBytes(response.BatchNumber, tx, blocksForStreamBytes, hermezDbReader, l1InfoTreeMinTimestamps, transactionsToIncludeByIndex)
						if err != nil {
							return err
						}

						previousTxIndex, ok := limboSendersToPreviousTxMap[senderMapKey]
						if !ok {
							previousTxIndex = math.MaxUint32
						}

						hash := transaction.Hash()
						limboTxCount := limboDetails.AppendTransaction(buffer.Bytes(), streamBytes, hash, sender, previousTxIndex)
						limboSendersToPreviousTxMap[senderMapKey] = limboTxCount - 1

						log.Info(fmt.Sprintf("[%s] adding transaction to limbo", s.LogPrefix()), "hash", hash)
					}
				}

				limboDetails.TimestampLimit = highestBlock.Time()
				limboDetails.FirstBlockNumber = lowestBlock.NumberU64()
				cfg.txPool.ProcessLimboBatchDetails(limboDetails)

				u.UnwindTo(lowestBlock.NumberU64()-1, lowestBlock.Hash())
				cfg.verifier.CancelAllRequestsUnsafe()
				return nil
			} else {
				// this infinite loop will make the node to print the error once every minute therefore preventing it for creating new blocks
				for {
					time.Sleep(time.Minute)
					log.Error(fmt.Sprintf("[%s] identified an invalid batch with number %d", s.LogPrefix(), response.BatchNumber))
				}
			}
		}

		// all good so just update the stage progress for now
		if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, response.BatchNumber); err != nil {
			return err
		}

		// we know that if the batch has been marked as OK we can update the datastream progress to match
		// as the verifier will have handled writing to the stream
		highestBlock, err := hermezDb.GetHighestBlockInBatch(response.BatchNumber)
		if err != nil {
			return err
		}

		if err = stages.SaveStageProgress(tx, stages.DataStream, highestBlock); err != nil {
			return err
		}

		// store the witness
		errWitness := hermezDb.WriteWitness(response.BatchNumber, response.Witness)
		if errWitness != nil {
			log.Warn("Failed to write witness", "batch", response.BatchNumber, "err", errWitness)
		}

		cfg.verifier.MarkTopResponseAsProcessed(response.BatchNumber)
		progress = response.BatchNumber
	}

	// send off the new batches to the verifier to be processed
	for batch := progress + 1; batch <= latestBatch; batch++ {
		// we do not need to verify batch 1 as this is the injected batch so just updated progress and move on
		if batch == injectedBatchNumber {
			if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, injectedBatchNumber); err != nil {
				return err
			}
		} else {
			if cfg.verifier.IsRequestAddedUnsafe(batch) {
				continue
			}

			// we need the state root of the last block in the batch to send to the executor
			highestBlock, err := hermezDb.GetHighestBlockInBatch(batch)
			if err != nil {
				return err
			}
			if highestBlock == 0 {
				// maybe nothing in this batch and we know we don't handle batch 0 (genesis)
				continue
			}
			block, err := rawdb.ReadBlockByNumber(tx, highestBlock)
			if err != nil {
				return err
			}

			counters, found, err := hermezDb.GetBatchCounters(batch)
			if err != nil {
				return err
			}
			if !found {
				return errors.New("batch counters not found")
			}

			forkId, err := hermezDb.GetForkId(batch)
			if err != nil {
				return err
			}

			if forkId == 0 {
				return errors.New("the network cannot have a 0 fork id")
			}

			cfg.verifier.AddRequestUnsafe(legacy_executor_verifier.NewVerifierRequest(batch, forkId, block.Root(), counters), cfg.cfgZk.SequencerBatchSealTime)
		}
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindSequencerExecutorVerifyStage(
	u *stagedsync.UnwindState,
	s *stagedsync.StageState,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerExecutorVerifyCfg,
) (err error) {
	/*
		The "Unwinder" keeps stage's progress in blocks.
		If a stage's current progress is <= unwindPoint then the unwind is not invoked for this stage (sync.go line 386)
		For this particular case, the progress is in batches => its progress is always <= unwindPoint, because unwindPoint is in blocks
		This is not a problem, because this stage's progress actually keeps the number of last verified batch and we never unwind the last verified batch
	*/

	// freshTx := tx == nil
	// if freshTx {
	// 	tx, err = cfg.db.BeginRw(ctx)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	defer tx.Rollback()
	// }

	// logPrefix := u.LogPrefix()
	// log.Info(fmt.Sprintf("[%s] Unwind Executor Verify", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	// if err = u.Done(tx); err != nil {
	// 	return err
	// }

	// if freshTx {
	// 	if err = tx.Commit(); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func PruneSequencerExecutorVerifyStage(
	s *stagedsync.PruneState,
	tx kv.RwTx,
	cfg SequencerExecutorVerifyCfg,
	ctx context.Context,
) error {
	return nil
}
