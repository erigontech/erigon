package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/utils"
)

// we must perform execution and datastream alignment only during first run of this stage
var shouldCheckForExecutionAndDataStreamAlighmentOnNodeStart = true

func SpawnSequencingStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	cfg SequenceBlockCfg,
	historyCfg stagedsync.HistoryCfg,
	quiet bool,
) (err error) {
	roTx, err := cfg.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()

	lastBatch, err := stages.GetStageProgress(roTx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	highestBatchInDS, err := cfg.datastreamServer.GetHighestBatchNumber()
	if err != nil {
		return err
	}

	if !cfg.zk.SequencerResequence || lastBatch >= highestBatchInDS {
		if cfg.zk.SequencerResequence {
			log.Info(fmt.Sprintf("[%s] Resequencing completed. Please restart sequencer without resequence flag.", s.LogPrefix()))
			time.Sleep(10 * time.Second)
			return nil
		}

		err = sequencingStageStep(s, u, ctx, cfg, historyCfg, quiet, nil)
		if err != nil {
			return err
		}
	} else {
		log.Info(fmt.Sprintf("[%s] Last batch %d is lower than highest batch in datastream %d, resequencing...", s.LogPrefix(), lastBatch, highestBatchInDS))

		batches, err := cfg.datastreamServer.ReadBatches(lastBatch+1, highestBatchInDS)
		if err != nil {
			return err
		}

		err = cfg.datastreamServer.UnwindToBatchStart(lastBatch + 1)
		if err != nil {
			return err
		}

		log.Info(fmt.Sprintf("[%s] Resequence from batch %d to %d in data stream", s.LogPrefix(), lastBatch+1, highestBatchInDS))

		for _, batch := range batches {
			batchJob := NewResequenceBatchJob(batch)
			subBatchCount := 0
			for batchJob.HasMoreBlockToProcess() {
				if err = sequencingStageStep(s, u, ctx, cfg, historyCfg, quiet, batchJob); err != nil {
					return err
				}

				subBatchCount += 1
			}

			log.Info(fmt.Sprintf("[%s] Resequenced original batch %d with %d batches", s.LogPrefix(), batchJob.batchToProcess[0].BatchNumber, subBatchCount))
			if cfg.zk.SequencerResequenceStrict && subBatchCount != 1 {
				return fmt.Errorf("strict mode enabled, but resequenced batch %d has %d sub-batches", batchJob.batchToProcess[0].BatchNumber, subBatchCount)
			}
		}
	}

	return nil
}

func sequencingStageStep(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	cfg SequenceBlockCfg,
	historyCfg stagedsync.HistoryCfg,
	quiet bool,
	resequenceBatchJob *ResequenceBatchJob,
) (err error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting sequencing stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished sequencing stage", logPrefix))

	sdb, err := newStageDb(ctx, cfg.db)
	if err != nil {
		return err
	}
	defer sdb.tx.Rollback()

	executionAt, err := s.ExecutionAt(sdb.tx)
	if err != nil {
		return err
	}

	lastBatch, err := stages.GetStageProgress(sdb.tx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	forkId, err := prepareForkId(lastBatch, executionAt, sdb.hermezDb)
	if err != nil {
		return err
	}

	// stage loop should continue until we get the forkid from the L1 in a finalised block
	if forkId == 0 {
		log.Warn(fmt.Sprintf("[%s] ForkId is 0. Waiting for L1 to finalise a block...", logPrefix))
		time.Sleep(10 * time.Second)
		return nil
	}

	batchNumberForStateInitialization, err := prepareBatchNumber(sdb, forkId, lastBatch, cfg.zk.L1SyncStartBlock > 0)
	if err != nil {
		return err
	}

	var block *types.Block
	runLoopBlocks := true
	batchContext := newBatchContext(ctx, &cfg, &historyCfg, s, sdb)
	batchState := newBatchState(forkId, batchNumberForStateInitialization, executionAt+1, cfg.zk.HasExecutors(), cfg.zk.L1SyncStartBlock > 0, cfg.txPool, resequenceBatchJob)
	blockDataSizeChecker := newBlockDataChecker()
	streamWriter := newSequencerBatchStreamWriter(batchContext, batchState)

	// injected batch
	if executionAt == 0 {
		if err = processInjectedInitialBatch(batchContext, batchState); err != nil {
			return err
		}

		if err = cfg.datastreamServer.WriteWholeBatchToStream(logPrefix, sdb.tx, sdb.hermezDb.HermezDbReader, lastBatch, injectedBatchBatchNumber); err != nil {
			return err
		}
		if err = stages.SaveStageProgress(sdb.tx, stages.DataStream, 1); err != nil {
			return err
		}

		return sdb.tx.Commit()
	}

	if shouldCheckForExecutionAndDataStreamAlighmentOnNodeStart {
		// handle cases where the last batch wasn't committed to the data stream.
		// this could occur because we're migrating from an RPC node to a sequencer
		// or because the sequencer was restarted and not all processes completed (like waiting from remote executor)
		// we consider the data stream as verified by the executor so treat it as "safe" and unwind blocks beyond there
		// if we identify any.  During normal operation this function will simply check and move on without performing
		// any action.
		if !batchState.isAnyRecovery() {
			isUnwinding, err := alignExecutionToDatastream(batchContext, batchState, executionAt, u)
			if err != nil {
				// do not set shouldCheckForExecutionAndDataStreamAlighmentOnNodeStart=false because of the error
				return err
			}
			if isUnwinding {
				err = sdb.tx.Commit()
				if err != nil {
					// do not set shouldCheckForExecutionAndDataStreamAlighmentOnNodeStart=false because of the error
					return err
				}
				shouldCheckForExecutionAndDataStreamAlighmentOnNodeStart = false
				return nil
			}
		}
		shouldCheckForExecutionAndDataStreamAlighmentOnNodeStart = false
	}

	tryHaltSequencer(batchContext, batchState.batchNumber)

	if err := utils.UpdateZkEVMBlockCfg(cfg.chainConfig, sdb.hermezDb, logPrefix); err != nil {
		return err
	}

	batchCounters, err := prepareBatchCounters(batchContext, batchState)
	if err != nil {
		return err
	}

	if batchState.isL1Recovery() {
		if cfg.zk.L1SyncStopBatch > 0 && batchState.batchNumber > cfg.zk.L1SyncStopBatch {
			log.Info(fmt.Sprintf("[%s] L1 recovery has completed!", logPrefix), "batch", batchState.batchNumber)
			time.Sleep(1 * time.Second)
			return nil
		}

		log.Info(fmt.Sprintf("[%s] L1 recovery beginning for batch", logPrefix), "batch", batchState.batchNumber)

		// let's check if we have any L1 data to recover
		if err = batchState.batchL1RecoveryData.loadBatchData(sdb); err != nil {
			return err
		}

		if !batchState.batchL1RecoveryData.hasAnyDecodedBlocks() {
			log.Info(fmt.Sprintf("[%s] L1 recovery has completed!", logPrefix), "batch", batchState.batchNumber)
			time.Sleep(1 * time.Second)
			return nil
		}

		if handled, err := doCheckForBadBatch(batchContext, batchState, executionAt); err != nil || handled {
			return err
		}
	}

	batchTicker, logTicker, blockTicker := prepareTickers(batchContext.cfg)
	defer batchTicker.Stop()
	defer logTicker.Stop()
	defer blockTicker.Stop()

	log.Info(fmt.Sprintf("[%s] Starting batch %d...", logPrefix, batchState.batchNumber))

	for blockNumber := executionAt + 1; runLoopBlocks; blockNumber++ {
		log.Info(fmt.Sprintf("[%s] Starting block %d (forkid %v)...", logPrefix, blockNumber, batchState.forkId))
		logTicker.Reset(10 * time.Second)
		blockTicker.Reset(cfg.zk.SequencerBlockSealTime)

		if batchState.isL1Recovery() {
			blockNumbersInBatchSoFar, err := batchContext.sdb.hermezDb.GetL2BlockNosByBatch(batchState.batchNumber)
			if err != nil {
				return err
			}

			didLoadedAnyDataForRecovery := batchState.loadBlockL1RecoveryData(uint64(len(blockNumbersInBatchSoFar)))
			if !didLoadedAnyDataForRecovery {
				log.Info(fmt.Sprintf("[%s] Block %d is not part of batch %d. Stopping blocks loop", logPrefix, blockNumber, batchState.batchNumber))
				break
			}
		}

		if batchState.isResequence() {
			if !batchState.resequenceBatchJob.HasMoreBlockToProcess() {
				for streamWriter.legacyVerifier.HasPendingVerifications() {
					streamWriter.CommitNewUpdates()
					time.Sleep(1 * time.Second)
				}

				runLoopBlocks = false
				break
			}
		}

		header, parentBlock, err := prepareHeader(sdb.tx, blockNumber-1, batchState.blockState.getDeltaTimestamp(), batchState.getBlockHeaderForcedTimestamp(), batchState.forkId, batchState.getCoinbase(&cfg))
		if err != nil {
			return err
		}

		if batchDataOverflow := blockDataSizeChecker.AddBlockStartData(); batchDataOverflow {
			log.Info(fmt.Sprintf("[%s] BatchL2Data limit reached. Stopping.", logPrefix), "blockNumber", blockNumber)
			break
		}

		// timer: evm + smt
		t := utils.StartTimer("stage_sequence_execute", "evm", "smt")

		infoTreeIndexProgress, l1TreeUpdate, l1TreeUpdateIndex, l1BlockHash, ger, shouldWriteGerToContract, err := prepareL1AndInfoTreeRelatedStuff(sdb, batchState, header.Time, cfg.zk.SequencerResequenceReuseL1InfoIndex)
		if err != nil {
			return err
		}

		overflowOnNewBlock, err := batchCounters.StartNewBlock(l1TreeUpdateIndex != 0)
		if err != nil {
			return err
		}
		if (!batchState.isAnyRecovery() || batchState.isResequence()) && overflowOnNewBlock {
			break
		}

		ibs := state.New(sdb.stateReader)
		getHashFn := core.GetHashFn(header, func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(sdb.tx, hash, number) })
		coinbase := batchState.getCoinbase(&cfg)
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.engine, &coinbase, parentBlock.ExcessDataGas())
		batchState.blockState.builtBlockElements.resetBlockBuildingArrays()

		parentRoot := parentBlock.Root()
		if err = handleStateForNewBlockStarting(batchContext, ibs, blockNumber, batchState.batchNumber, header.Time, &parentRoot, l1TreeUpdate, shouldWriteGerToContract); err != nil {
			return err
		}

		// start waiting for a new transaction to arrive
		if !batchState.isAnyRecovery() {
			log.Info(fmt.Sprintf("[%s] Waiting for txs from the pool...", logPrefix))
		}

	LOOP_TRANSACTIONS:
		for {
			select {
			case <-logTicker.C:
				if !batchState.isAnyRecovery() {
					log.Info(fmt.Sprintf("[%s] Waiting some more for txs from the pool...", logPrefix))
				}
			case <-blockTicker.C:
				if !batchState.isAnyRecovery() {
					break LOOP_TRANSACTIONS
				}
			case <-batchTicker.C:
				if !batchState.isAnyRecovery() {
					runLoopBlocks = false
					break LOOP_TRANSACTIONS
				}
			default:
				if batchState.isLimboRecovery() {
					batchState.blockState.transactionsForInclusion, err = getLimboTransaction(ctx, cfg, batchState.limboRecoveryData.limboTxHash)
					if err != nil {
						return err
					}
				} else if !batchState.isL1Recovery() && !batchState.isResequence() {
					var allConditionsOK bool
					batchState.blockState.transactionsForInclusion, allConditionsOK, err = getNextPoolTransactions(ctx, cfg, executionAt, batchState.forkId, batchState.yieldedTransactions)
					if err != nil {
						return err
					}

					if len(batchState.blockState.transactionsForInclusion) == 0 {
						if allConditionsOK {
							time.Sleep(batchContext.cfg.zk.SequencerTimeoutOnEmptyTxPool)
						} else {
							time.Sleep(batchContext.cfg.zk.SequencerTimeoutOnEmptyTxPool / 5) // we do not need to sleep too long for txpool not ready
						}
					} else {
						log.Trace(fmt.Sprintf("[%s] Yielded transactions from the pool", logPrefix), "txCount", len(batchState.blockState.transactionsForInclusion))
					}
				} else if batchState.isResequence() {
					batchState.blockState.transactionsForInclusion, err = batchState.resequenceBatchJob.YieldNextBlockTransactions(zktx.DecodeTx)
					if err != nil {
						return err
					}
				}

				if len(batchState.blockState.transactionsForInclusion) == 0 {
					time.Sleep(batchContext.cfg.zk.SequencerTimeoutOnEmptyTxPool)
				} else {
					log.Trace(fmt.Sprintf("[%s] Yielded transactions from the pool", logPrefix), "txCount", len(batchState.blockState.transactionsForInclusion))
				}

				for i, transaction := range batchState.blockState.transactionsForInclusion {
					txHash := transaction.Hash()
					effectiveGas := batchState.blockState.getL1EffectiveGases(cfg, i)

					// The copying of this structure is intentional
					backupDataSizeChecker := *blockDataSizeChecker
					receipt, execResult, anyOverflow, err := attemptAddTransaction(cfg, sdb, ibs, batchCounters, &blockContext, header, transaction, effectiveGas, batchState.isL1Recovery(), batchState.forkId, l1TreeUpdateIndex, &backupDataSizeChecker)
					if err != nil {
						if batchState.isLimboRecovery() {
							panic("limbo transaction has already been executed once so they must not fail while re-executing")
						}

						if batchState.isResequence() {
							if cfg.zk.SequencerResequenceStrict {
								return fmt.Errorf("strict mode enabled, but resequenced batch %d failed to add transaction %s: %v", batchState.batchNumber, txHash, err)
							} else {
								log.Warn(fmt.Sprintf("[%s] error adding transaction to batch during resequence: %v", logPrefix, err),
									"hash", txHash,
									"to", transaction.GetTo(),
								)
								continue
							}
						}

						// if we are in recovery just log the error as a warning.  If the data is on the L1 then we should consider it as confirmed.
						// The executor/prover would simply skip a TX with an invalid nonce for example so we don't need to worry about that here.
						if batchState.isL1Recovery() {
							log.Warn(fmt.Sprintf("[%s] error adding transaction to batch during recovery: %v", logPrefix, err),
								"hash", txHash,
								"to", transaction.GetTo(),
							)
							continue
						}

						// if running in normal operation mode and error != nil then just allow the code to continue
						// It is safe because this approach ensures that the problematic transaction (the one that caused err != nil to be returned) is kept in yielded
						// Each transaction in yielded will be reevaluated at the end of each batch
					}

					switch anyOverflow {
					case overflowCounters:
						if batchState.isLimboRecovery() {
							panic("limbo transaction has already been executed once so they must not overflow counters while re-executing")
						}

						if !batchState.isL1Recovery() {
							log.Info(fmt.Sprintf("[%s] overflowed adding transaction to batch", logPrefix), "batch", batchState.batchNumber, "tx-hash", txHash, "has-any-transactions-in-this-batch", batchState.hasAnyTransactionsInThisBatch)
							/*
								There are two cases when overflow could occur.
								1. The block DOES not contains any transactions.
									In this case it means that a single tx overflow entire zk-counters.
									In this case we mark it so. Once marked it will be discarded from the tx-pool async (once the tx-pool process the creation of a new batch)
									NB: The tx SHOULD not be removed from yielded set, because if removed, it will be picked again on next block. That's why there is i++. It ensures that removing from yielded will start after the problematic tx
								2. The block contains transactions.
									In this case, we just have to remove the transaction that overflowed the zk-counters and all transactions after it, from the yielded set.
									This removal will ensure that these transaction could be added in the next block(s)
							*/
							if !batchState.hasAnyTransactionsInThisBatch {
								cfg.txPool.MarkForDiscardFromPendingBest(txHash)
								log.Info(fmt.Sprintf("single transaction %s overflow counters", txHash))
							}

							runLoopBlocks = false
							break LOOP_TRANSACTIONS
						}

						if batchState.isResequence() && cfg.zk.SequencerResequenceStrict {
							return fmt.Errorf("strict mode enabled, but resequenced batch %d overflowed counters on block %d", batchState.batchNumber, blockNumber)
						}

						break LOOP_TRANSACTIONS
					case overflowGas:
						if batchState.isAnyRecovery() {
							panic(fmt.Sprintf("block gas limit overflow in recovery block: %d", blockNumber))
						}
						log.Info(fmt.Sprintf("[%s] gas overflowed adding transaction to block", logPrefix), "block", blockNumber, "tx-hash", txHash)
						runLoopBlocks = false
						break LOOP_TRANSACTIONS
					case overflowNone:
					}

					if err == nil {
						blockDataSizeChecker = &backupDataSizeChecker
						batchState.onAddedTransaction(transaction, receipt, execResult, effectiveGas)
					}

					// We will only update the processed index in resequence job if there isn't overflow
					if batchState.isResequence() {
						batchState.resequenceBatchJob.UpdateLastProcessedTx(txHash)
					}
				}

				if batchState.isResequence() {
					if len(batchState.blockState.transactionsForInclusion) == 0 {
						// We need to jump to the next block here if there are no transactions in current block
						batchState.resequenceBatchJob.UpdateLastProcessedTx(batchState.resequenceBatchJob.CurrentBlock().L2Blockhash)
						break LOOP_TRANSACTIONS
					}

					if batchState.resequenceBatchJob.AtNewBlockBoundary() {
						// We need to jump to the next block here if we are at the end of the current block
						break LOOP_TRANSACTIONS
					} else {
						if cfg.zk.SequencerResequenceStrict {
							return fmt.Errorf("strict mode enabled, but resequenced batch %d has transactions that overflowed counters or failed transactions", batchState.batchNumber)
						}
					}
				}

				if batchState.isL1Recovery() {
					// just go into the normal loop waiting for new transactions to signal that the recovery
					// has finished as far as it can go
					if !batchState.isThereAnyTransactionsToRecover() {
						log.Info(fmt.Sprintf("[%s] L1 recovery no more transactions to recover", logPrefix))
					}

					break LOOP_TRANSACTIONS
				}

				if batchState.isLimboRecovery() {
					runLoopBlocks = false
					break LOOP_TRANSACTIONS
				}
			}
		}

		if block, err = doFinishBlockAndUpdateState(batchContext, ibs, header, parentBlock, batchState, ger, l1BlockHash, l1TreeUpdateIndex, infoTreeIndexProgress, batchCounters); err != nil {
			return err
		}

		if batchState.isLimboRecovery() {
			stateRoot := block.Root()
			cfg.txPool.UpdateLimboRootByTxHash(batchState.limboRecoveryData.limboTxHash, &stateRoot)
			return fmt.Errorf("[%s] %w: %s = %s", s.LogPrefix(), zk.ErrLimboState, batchState.limboRecoveryData.limboTxHash.Hex(), stateRoot.Hex())
		}

		t.LogTimer()
		gasPerSecond := float64(0)
		elapsedSeconds := t.Elapsed().Seconds()
		if elapsedSeconds != 0 {
			gasPerSecond = float64(block.GasUsed()) / elapsedSeconds
		}

		if gasPerSecond != 0 {
			log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions... (%d gas/s)", logPrefix, blockNumber, len(batchState.blockState.builtBlockElements.transactions), int(gasPerSecond)), "info-tree-index", infoTreeIndexProgress)
		} else {
			log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions...", logPrefix, blockNumber, len(batchState.blockState.builtBlockElements.transactions)), "info-tree-index", infoTreeIndexProgress)
		}

		// add a check to the verifier and also check for responses
		batchState.onBuiltBlock(blockNumber)

		if !batchState.isL1Recovery() {
			// commit block data here so it is accessible in other threads
			if errCommitAndStart := sdb.CommitAndStart(); errCommitAndStart != nil {
				return errCommitAndStart
			}
			defer sdb.tx.Rollback()
		}

		// do not use remote executor in l1recovery mode
		// if we need remote executor in l1 recovery then we must allow commit/start DB transactions
		useExecutorForVerification := !batchState.isL1Recovery() && batchState.hasExecutorForThisBatch
		counters, err := batchCounters.CombineCollectors(l1TreeUpdateIndex != 0)
		if err != nil {
			return err
		}
		cfg.legacyVerifier.StartAsyncVerification(batchContext.s.LogPrefix(), batchState.forkId, batchState.batchNumber, block.Root(), counters.UsedAsMap(), batchState.builtBlocks, useExecutorForVerification, batchContext.cfg.zk.SequencerBatchVerificationTimeout)

		// check for new responses from the verifier
		needsUnwind, err := updateStreamAndCheckRollback(batchContext, batchState, streamWriter, u)

		// lets commit everything after updateStreamAndCheckRollback no matter of its result unless
		// we're in L1 recovery where losing some blocks on restart doesn't matter
		if !batchState.isL1Recovery() {
			if errCommitAndStart := sdb.CommitAndStart(); errCommitAndStart != nil {
				return errCommitAndStart
			}
			defer sdb.tx.Rollback()
		}

		// check the return values of updateStreamAndCheckRollback
		if err != nil || needsUnwind {
			return err
		}
	}

	/*
		if adding something below that line we must ensure
		- it is also handled property in processInjectedInitialBatch
		- it is also handled property in alignExecutionToDatastream
		- it is also handled property in doCheckForBadBatch
		- it is unwound correctly
	*/

	// TODO: It is 99% sure that there is no need to write this in any of processInjectedInitialBatch, alignExecutionToDatastream, doCheckForBadBatch but it is worth double checknig
	// the unwind of this value is handed by UnwindExecutionStageDbWrites
	if _, err := rawdb.IncrementStateVersionByBlockNumberIfNeeded(batchContext.sdb.tx, block.NumberU64()); err != nil {
		return fmt.Errorf("writing plain state version: %w", err)
	}

	log.Info(fmt.Sprintf("[%s] Finish batch %d...", batchContext.s.LogPrefix(), batchState.batchNumber))

	return sdb.tx.Commit()
}
