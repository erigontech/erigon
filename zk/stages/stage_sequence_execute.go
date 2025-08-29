package stages

import (
	"context"
	"errors"
	"fmt"
	"github.com/erigontech/erigon/consensus/misc"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/zk"
	"github.com/erigontech/erigon/zk/hermez_db"
	"github.com/erigontech/erigon/zk/sequencer"
	zktx "github.com/erigontech/erigon/zk/tx"
	"github.com/erigontech/erigon/zk/utils"
)

var shouldCheckForExecutionAndDataStreamAlignment = true

type TxYielder interface {
	YieldNextTransaction() (types.Transaction, uint8, bool)
	AddMined(hash common.Hash)
	Discard(hash common.Hash)
	SetExecutionDetails(executionAt uint64, forkId uint64)
	BeginYielding()
	Cleanup()
}

func SpawnSequencingStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	cfg SequenceBlockCfg,
	historyCfg stagedsync.HistoryCfg,
	quiet bool,
	logger log.Logger,
) (err error) {
	roTx, err := cfg.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()

	if err := checkSmtMigration(ctx, cfg, roTx, s); err != nil {
		return err
	}

	hermezDb := hermez_db.NewHermezDbReader(roTx)
	lastSequence, err := hermezDb.GetLatestSequence()
	if err != nil {
		return fmt.Errorf("GetLatestSequence: %w", err)
	}

	lastBatch, err := stages.GetStageProgress(roTx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	if lastSequence != nil && lastBatch < lastSequence.BatchNo && !(cfg.zk.IsL1Recovery() || cfg.zk.SequencerResequence) {
		if !cfg.zk.ShadowSequencer {
			panic(fmt.Sprintf("lastBatch %d < lastSequence.BatchNo %d", lastBatch, lastSequence.BatchNo))
		}
	}

	highestBatchInDs, err := cfg.dataStreamServer.GetHighestBatchNumber()
	if err != nil {
		return err
	}

	if lastBatch < highestBatchInDs {
		if !cfg.zk.SequencerResequence {
			if err = cfg.dataStreamServer.UnwindToBatchStart(lastBatch + 1); err != nil {
				return err
			}
		} else {
			return resequence(s, u, ctx, cfg, historyCfg, lastBatch, highestBatchInDs, logger)
		}
	}

	if cfg.zk.SequencerResequence {
		log.Info(fmt.Sprintf("[%s] Resequencing completed. Please restart sequencer without resequence flag.", s.LogPrefix()))
		time.Sleep(10 * time.Minute)
		return nil
	}

	return sequencingBatchStep(s, u, ctx, cfg, historyCfg, nil, logger)
}

func sequencingBatchStep(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	cfg SequenceBlockCfg,
	historyCfg stagedsync.HistoryCfg,
	resequenceBatchJob *ResequenceBatchJob,
	logger log.Logger,
) (err error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting sequencing stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished sequencing stage", logPrefix))

	// at this point of time the datastream could not be ahead of the executor
	if err := validateIfDatastreamIsAheadOfExecution(s, ctx, cfg); err != nil {
		return err
	}

	sdb, err := newStageDb(ctx, cfg.db)
	if err != nil {
		return err
	}
	defer sdb.tx.Rollback()

	if err := cfg.infoTreeUpdater.WarmUp(sdb.tx); err != nil {
		return err
	}

	executionAt, err := s.ExecutionAt(sdb.tx)
	if err != nil {
		return err
	}

	lastBatch, err := stages.GetStageProgress(sdb.tx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	forkId, err := prepareForkId(lastBatch, executionAt, sdb.hermezDb, cfg)
	if err != nil {
		return err
	}

	// stage loop should continue until we get the forkid from the L1 in a finalised block
	if forkId == 0 {
		log.Warn(fmt.Sprintf("[%s] ForkId is 0. Waiting for L1 to finalise a block...", logPrefix))
		time.Sleep(10 * time.Second)
		return nil
	}

	batchNumberForStateInitialization, err := prepareBatchNumber(sdb, forkId, lastBatch, cfg.zk.IsL1Recovery())
	if err != nil {
		return err
	}

	var block *types.Block
	runLoopBlocks := true
	batchContext := newBatchContext(ctx, &cfg, &historyCfg, s, sdb)
	batchState := newBatchState(forkId, batchNumberForStateInitialization, executionAt+1, cfg.zk.UseExecutors(), cfg.zk.IsL1Recovery(), cfg.txPool, resequenceBatchJob)
	shouldCountersBeInfinite := cfg.zk.ShouldCountersBeUnlimited(batchState.isL1Recovery()) || cfg.chainConfig.IsNormalcy(executionAt)
	blockDataSizeChecker := NewBlockDataChecker(shouldCountersBeInfinite)
	streamWriter := newSequencerBatchStreamWriter(batchContext, batchState)

	// injected batch
	if executionAt == 0 {
		if cfg.chainConfig.DebugDisableZkevmStateChanges {
			// sealed empty block batch
			if err = processEmptyInitialBatch(batchContext, batchState); err != nil {
				return err
			}
		} else {
			if err = processInjectedInitialBatch(batchContext, batchState); err != nil {
				return err
			}
		}

		if err = cfg.dataStreamServer.WriteWholeBatchToStream(logPrefix, sdb.tx, sdb.hermezDb.HermezDbReader, lastBatch, injectedBatchBatchNumber); err != nil {
			return err
		}

		if err = stages.SaveStageProgress(sdb.tx, stages.DataStream, 1); err != nil {
			return err
		}

		return sdb.tx.Commit()
	}

	if shouldCheckForExecutionAndDataStreamAlignment {
		// handle cases where the last batch wasn't committed to the data stream.
		// this could occur because we're migrating from an RPC node to a sequencer
		// or because the sequencer was restarted and not all processes completed (like waiting from remote executor)
		// we consider the data stream as verified by the executor so treat it as "safe" and unwind blocks beyond there
		// if we identify any.  During normal operation this function will simply check and move on without performing
		// any action.
		if !batchState.isAnyRecovery() {
			isUnwinding, err := alignExecutionToDatastream(batchContext, executionAt, u)
			if err != nil {
				// do not set shouldCheckForExecutionAndDataStreamAlighment=false because of the error
				return err
			}
			if isUnwinding {
				if err := sdb.tx.Commit(); err != nil {
					// do not set shouldCheckForExecutionAndDataStreamAlighment=false because of the error
					return err
				}
				shouldCheckForExecutionAndDataStreamAlignment = false
				return nil
			}
		}
		shouldCheckForExecutionAndDataStreamAlignment = false
	}

	needsUnwind, exitStage, err := tryHaltSequencer(batchContext, batchState, streamWriter, u, executionAt)
	if needsUnwind || err != nil {
		return err
	}
	if exitStage {
		log.Info(fmt.Sprintf("[%s] Exiting stage during halted sequencer", logPrefix))
		// commit the tx so any updates to the stream etc are persisted
		return sdb.tx.Commit()
	}

	if err := utils.UpdateZkEVMBlockCfg(cfg.chainConfig, sdb.hermezDb, logPrefix, cfg.zk.LogLevel == log.LvlTrace); err != nil {
		return err
	}

	batchCounters := prepareBatchCounters(batchContext, batchState, shouldCountersBeInfinite)

	if batchState.isL1Recovery() {
		if cfg.zk.L1SyncStopBatch > 0 && batchState.batchNumber > cfg.zk.L1SyncStopBatch {
			log.Info(fmt.Sprintf("[%s] L1 recovery has completed!", logPrefix), "batch", batchState.batchNumber)
			time.Sleep(1 * time.Second)
			return nil
		}

		log.Info(fmt.Sprintf("[%s] L1 recovery beginning for batch", logPrefix), "batch", batchState.batchNumber)

		// let's check if we have any L1 data to recover
		if err := batchState.batchL1RecoveryData.loadBatchData(sdb); err != nil {
			return err
		}

		if !batchState.batchL1RecoveryData.hasAnyDecodedBlocks() {
			log.Info(fmt.Sprintf("[%s] L1 recovery has completed!", logPrefix), "batch", batchState.batchNumber)
			time.Sleep(1 * time.Second)
			return nil
		}

		bad := false
		for _, batch := range cfg.zk.BadBatches {
			if batch == batchState.batchNumber {
				bad = true
				break
			}
		}

		// if we aren't forcing a bad batch then check it
		if !bad {
			bad, err = doCheckForBadBatch(batchContext, batchState, executionAt)
			if err != nil {
				return err
			}
		}

		// write bad batch details unless config dictates we ignore them
		if bad && !cfg.zk.IgnoreBadBatchesCheck {
			return writeBadBatchDetails(batchContext, batchState, executionAt)
		}
	}

	logTicker := time.NewTicker(10 * time.Second)
	infoTreeTicker := time.NewTicker(cfg.zk.InfoTreeUpdateInterval)
	defer logTicker.Stop()
	defer infoTreeTicker.Stop()

	batchTimer := time.NewTimer(cfg.zk.SequencerBatchSealTime)

	log.Info(fmt.Sprintf("[%s] Starting batch %d...", logPrefix, batchState.batchNumber))

	// once the batch ticker has ticked we need a signal to close the batch after the next block is done
	batchTimedOut := false

	var header *types.Header

	// to avoid nonce problems when a transaction causes the batch to overflow we need to temporarily skip handling transactions from the same sender
	// until the next batch starts
	sendersToSkip := make(map[common.Address]struct{})

	// default to using the normal transaction yielder
	var yielder TxYielder = cfg.txYielder

	for blockNumber := executionAt + 1; runLoopBlocks; blockNumber++ {
		if batchTimedOut {
			log.Debug(fmt.Sprintf("[%s] Closing batch due to timeout", logPrefix))
			break
		}
		startTime := time.Now()
		log.Info(fmt.Sprintf("[%s] Starting block %d (forkid %v)...", logPrefix, blockNumber, batchState.forkId))
		logTicker.Reset(10 * time.Second)
		blockTimer := time.NewTimer(cfg.zk.SequencerBlockSealTime)
		emptyBlockTimer := time.NewTimer(cfg.zk.SequencerEmptyBlockSealTime)
		ethBlockGasPool := new(core.GasPool).AddGas(transactionGasLimit)

		if batchState.isLimboRecovery() {
			transactions, err := getLimboTransaction(ctx, cfg, batchState.limboRecoveryData.limboTxHash, executionAt)
			if err != nil {
				return err
			}
			yielder = sequencer.NewLimboTransactionYielder(transactions, *cfg.zk)
		} else if batchState.isResequence() {
			transactions, effectivePercentages, err := batchState.resequenceBatchJob.YieldNextBlockTransactions(zktx.DecodeTx)
			if err != nil {
				return err
			}
			yielder, err = sequencer.NewRecoveryTransactionYielder(transactions, effectivePercentages)
			if err != nil {
				return err
			}
		} else if batchState.isL1Recovery() {
			blockNumbersInBatchSoFar, err := batchContext.sdb.hermezDb.GetL2BlockNosByBatch(batchState.batchNumber)
			if err != nil {
				return err
			}

			decodedBatchL2Data, found := batchState.batchL1RecoveryData.getDecodedL1RecoveredBatchDataByIndex(uint64(len(blockNumbersInBatchSoFar)))
			if !found {
				log.Info(fmt.Sprintf("[%s] Block %d is not part of batch %d. Stopping blocks loop", logPrefix, blockNumber, batchState.batchNumber))
				break
			}

			yielder, err = sequencer.NewRecoveryTransactionYielder(decodedBatchL2Data.Transactions, decodedBatchL2Data.EffectiveGasPricePercentages)
			if err != nil {
				return err
			}
		}

		yielder.SetExecutionDetails(executionAt, batchState.forkId)
		yielder.BeginYielding()

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
				for {
					if pending, _ := streamWriter.legacyVerifier.HasPendingVerifications(); pending {
						if _, _, err = streamWriter.CommitNewUpdates(); err != nil {
							return fmt.Errorf("failed to commit new updates: %w", err)
						}
						time.Sleep(1 * time.Second)
					} else {
						break
					}
				}

				runLoopBlocks = false
				break
			}
		}

		var parentBlock *types.Block
		header, parentBlock, err = prepareHeader(sdb.tx, blockNumber-1, batchState.blockState.getDeltaTimestamp(), batchState.getBlockHeaderForcedTimestamp(), batchState.forkId, batchState.getCoinbase(&cfg), cfg.chainConfig, cfg.miningConfig)
		if err != nil {
			return err
		}

		if batchDataOverflow := blockDataSizeChecker.AddBlockStartData(); batchDataOverflow {
			log.Info(fmt.Sprintf("[%s] BatchL2Data limit reached. Stopping.", logPrefix), "blockNumber", blockNumber)
			break
		}

		// timer: evm + smt
		t := utils.StartTimer("stage_sequence_execute", "evm", "smt")

		infoTreeIndexProgress, l1TreeUpdate, l1TreeUpdateIndex, l1BlockHash, ger, shouldWriteGerToContract, err := prepareL1AndInfoTreeRelatedStuff(logPrefix, sdb, batchState, header.Time, cfg.zk.SequencerResequenceReuseL1InfoIndex, cfg.zk.SequencerResequenceInfoTreeOffset)
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
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.engine, &coinbase, cfg.chainConfig)
		batchState.blockState.builtBlockElements.resetBlockBuildingArrays()

		if cfg.chainConfig.IsPrague(header.Time) {
			misc.StoreBlockHashesEip2935(header, ibs, cfg.chainConfig, stagedsync.ChainReader{Cfg: *cfg.chainConfig, Db: sdb.tx, BlockReader: cfg.blockReader, Logger: logger})
		}

		parentRoot := parentBlock.Root()
		if err := handleStateForNewBlockStarting(batchContext, ibs, blockNumber, batchState.batchNumber, header.Time, &parentRoot, l1TreeUpdate, shouldWriteGerToContract); err != nil {
			return err
		}

		// start waiting for a new transaction to arrive
		if !batchState.isAnyRecovery() {
			log.Info(fmt.Sprintf("[%s] Waiting for txs from the pool...", logPrefix))
		}

		innerBreak := false
		emptyBlockOverflow := false
		sendersToTriggerStatechanges := make(map[common.Address]struct{})
		nonceTooHighSenders := make(map[common.Address][]uint64)
		yieldedSomething := false
		gasUsed := uint64(0)

	OuterLoopTransactions:
		for {
			if innerBreak {
				break
			}
			select {
			case <-logTicker.C:
				if !batchState.isAnyRecovery() {
					log.Info(fmt.Sprintf("[%s] Waiting some more for txs from the pool...", logPrefix))
				}
			default:
			}

			select {
			case <-batchTimer.C:
				if !batchState.isAnyRecovery() {
					log.Debug(fmt.Sprintf("[%s] Batch timeout reached", logPrefix))
					batchTimedOut = true
				}
			default:
			}

			select {
			case <-blockTimer.C:
				if !batchState.isAnyRecovery() {
					// no transactions or the block seal time is equal to the empty block seal time
					// break here to avoid log noise
					if len(batchState.blockState.builtBlockElements.transactions) > 0 ||
						cfg.zk.SequencerBlockSealTime == cfg.zk.SequencerEmptyBlockSealTime {
						break OuterLoopTransactions
					} else {
						log.Info(fmt.Sprintf("[%s] Block timeout reached with no transactions processed", logPrefix))
						blockTimer.Stop()
						blockTimer.Reset(cfg.zk.SequencerBlockSealTime)
					}
				}
			default:
			}

			select {
			case <-emptyBlockTimer.C:
				if len(batchState.blockState.builtBlockElements.transactions) == 0 && !batchState.isAnyRecovery() {
					log.Info(fmt.Sprintf("[%s] Empty block timeout reached with no transactions processed", logPrefix))
					emptyBlockTimer.Stop()
					break OuterLoopTransactions
				}
			default:
			}

			select {
			case <-infoTreeTicker.C:
				processedLogs, err := cfg.infoTreeUpdater.CheckForInfoTreeUpdates(logPrefix, sdb.tx)
				if err != nil {
					return err
				}
				var latestIndex uint64
				latest := cfg.infoTreeUpdater.GetLatestUpdate()
				if latest != nil {
					latestIndex = latest.Index
				}
				log.Info(fmt.Sprintf("[%s] Info tree updates", logPrefix), "count", processedLogs, "latestIndex", latestIndex)
			default:
			}

		InnerLoopTransactions:
			for {
				transaction, effectiveGas, ok := yielder.YieldNextTransaction()
				if !ok {
					if !batchState.isAnyRecovery() {
						time.Sleep(cfg.zk.SequencerTimeoutOnEmptyTxPool)
					}
					break InnerLoopTransactions
				}

				yieldedSomething = true

				// quick check if we should stop handling transactions
				select {
				case <-blockTimer.C:
					if !batchState.isAnyRecovery() {
						innerBreak = true
						break InnerLoopTransactions
					}
				default:
				}

				if cfg.zk.SequencerBlockGasLimit > 0 && gasUsed >= cfg.zk.SequencerBlockGasLimit {
					log.Info(fmt.Sprintf("[%s] Block gas limit reached. Stopping.", logPrefix), "blockNumber", blockNumber, "gasUsed", gasUsed, "gasLimit", cfg.zk.SequencerBlockGasLimit)
					innerBreak = true
					break InnerLoopTransactions
				}

				txHash := transaction.Hash()

				txSender, ok := transaction.GetSender()
				if !ok {
					signer := types.MakeSigner(cfg.chainConfig, executionAt, header.Time)
					sender, err := signer.Sender(transaction)
					if err != nil {
						log.Warn("[extractTransaction] Failed to recover sender from transaction, skipping and removing from pool",
							"error", err,
							"hash", transaction.Hash())
						batchState.blockState.transactionsToDiscard = append(batchState.blockState.transactionsToDiscard, batchState.blockState.transactionHashesToSlots[txHash])
						continue
					}

					transaction.SetSender(sender)
					txSender = sender
				}

				if _, found := sendersToSkip[txSender]; found {
					continue
				}

				if _, found := nonceTooHighSenders[txSender]; found {
					continue
				}

				// The copying of this structure is intentional
				backupDataSizeChecker := *blockDataSizeChecker
				receipt, execResult, txCounters, anyOverflow, err := attemptAddTransaction(cfg, sdb, ibs, batchCounters, &blockContext, header, transaction, effectiveGas, batchState.isL1Recovery(), batchState.forkId, l1TreeUpdateIndex, &backupDataSizeChecker, ethBlockGasPool)
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

					if errors.Is(err, core.ErrNonceTooLow) {
						log.Info(fmt.Sprintf("[%s] nonce too low detected for sender, skipping transactions for now", logPrefix), "sender", txSender.Hex(), "nonceIssue", err)
						sendersToTriggerStatechanges[txSender] = struct{}{}
						batchState.blockState.transactionsToDiscard = append(batchState.blockState.transactionsToDiscard, batchState.blockState.transactionHashesToSlots[txHash])
						yielder.Discard(txHash)
						continue
					}

					if errors.Is(err, core.ErrNonceTooHigh) {
						log.Info(fmt.Sprintf("[%s] nonce too high detected for sender, skipping transactions for now", logPrefix), "sender", txSender.Hex(), "nonceIssue", err)
						nonceTooHighSenders[txSender] = append(nonceTooHighSenders[txSender], transaction.GetNonce())
						yielder.Discard(txHash)
						continue
					}

					// if we have an error at this point something has gone wrong, either in the pool or otherwise
					// to stop the pool growing and hampering further processing of good transactions here
					// we mark it for being discarded
					log.Warn(fmt.Sprintf("[%s] error adding transaction to batch, discarding from pool", logPrefix), "hash", txHash, "err", err)
					batchState.blockState.transactionsToDiscard = append(batchState.blockState.transactionsToDiscard, batchState.blockState.transactionHashesToSlots[txHash])
					yielder.Discard(txHash)
					cfg.txPool.MarkForDiscardFromPendingBest(txHash)
				}

				switch anyOverflow {
				case overflowCounters:
					if batchState.isLimboRecovery() {
						panic("limbo transaction has already been executed once so they must not overflow counters while re-executing")
					}

					if !batchState.isL1Recovery() {
						// we need to now skip any further transactions from the same sender in this batch as we will encounter nonce problems
						if sender, ok := transaction.GetSender(); ok {
							sendersToSkip[sender] = struct{}{}
						}

						/*
							here we check if the transaction on it's own would overdflow the batch counters
							by creating a new counter collector and priming it for a single block with just this transaction
							in it.  We already have the computed execution counters so we don't need to recompute them and
							can just combine collectors as normal to see if it would overflow.

							If it does overflow then we mark the hash as a bad one and move on.  Calls to the RPC will
							check if this hash has appeared too many times and stop allowing it through if required.
						*/

						// now check if this transaction on it's own would overflow counters for the batch
						tempCounters := prepareBatchCounters(batchContext, batchState, shouldCountersBeInfinite)
						singleTxOverflow, err := tempCounters.SingleTransactionOverflowCheck(txCounters)
						if err != nil {
							return err
						}

						// if the transaction overflows or if there are no transactions in the batch and no blocks built yet
						// then we mark the transaction as bad and move on
						if singleTxOverflow || (!batchState.hasAnyTransactionsInThisBatch && len(batchState.builtBlocks) == 0) {
							ocs, _ := tempCounters.CounterStats(l1TreeUpdateIndex != 0)
							// mark the transaction to be removed from the pool
							cfg.txPool.MarkForDiscardFromPendingBest(txHash)
							counter, err := handleBadTxHashCounter(sdb.hermezDb, txHash)
							if err != nil {
								return err
							}
							log.Info(fmt.Sprintf("[%s] single transaction %s cannot fit into batch - overflow", logPrefix, txHash), "context", ocs, "times_seen", counter)
						} else {
							batchState.newOverflowTransaction()
							transactionNotAddedText := fmt.Sprintf("[%s] transaction %s was not included in this batch because it overflowed.", logPrefix, txHash)
							ocs, _ := batchCounters.CounterStats(l1TreeUpdateIndex != 0)
							log.Info(transactionNotAddedText, "Counters context:", ocs, "overflow transactions", batchState.overflowTransactions)
							if batchState.reachedOverflowTransactionLimit() || cfg.zk.SealBatchImmediatelyOnOverflow {
								log.Info(fmt.Sprintf("[%s] closing batch due to overflow counters", logPrefix), "counters: ", batchState.overflowTransactions, "immediate", cfg.zk.SealBatchImmediatelyOnOverflow)
								runLoopBlocks = false
								if len(batchState.blockState.builtBlockElements.transactions) == 0 {
									emptyBlockOverflow = true
								}
								break OuterLoopTransactions
							}
						}

						// now we have finished with logging the overflow,remove the last attempted counters as we may want to continue processing this batch with other transactions
						batchCounters.RemovePreviousTransactionCounters()

						// continue on processing other transactions and skip this one
						continue
					}

					if batchState.isResequence() && cfg.zk.SequencerResequenceStrict {
						return fmt.Errorf("strict mode enabled, but resequenced batch %d overflowed counters on block %d", batchState.batchNumber, blockNumber)
					}
				case overflowGas:
					if batchState.isAnyRecovery() {
						panic(fmt.Sprintf("block gas limit overflow in recovery block: %d", blockNumber))
					}
					log.Info(fmt.Sprintf("[%s] gas overflowed adding transaction to block", logPrefix), "block", blockNumber, "tx-hash", txHash)
					runLoopBlocks = false
					break OuterLoopTransactions
				case overflowNone:
				}

				if err == nil {
					blockDataSizeChecker = &backupDataSizeChecker
					batchState.onAddedTransaction(transaction, receipt, execResult, effectiveGas)
					yielder.AddMined(txHash)
					gasUsed += execResult.UsedGas
				}

				// We will only update the processed index in resequence job if there isn't overflow
				if batchState.isResequence() {
					batchState.resequenceBatchJob.UpdateLastProcessedTx(txHash)
				}
			}

			if batchState.isResequence() {
				if !yieldedSomething {
					// We need to jump to the next block here if there are no transactions in current block
					batchState.resequenceBatchJob.UpdateLastProcessedTx(batchState.resequenceBatchJob.CurrentBlock().L2Blockhash)
					break OuterLoopTransactions
				}

				if batchState.resequenceBatchJob.AtNewBlockBoundary() {
					// We need to jump to the next block here if we are at the end of the current block
					break OuterLoopTransactions
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

				break OuterLoopTransactions
			}

			if batchState.isLimboRecovery() {
				runLoopBlocks = false
				break OuterLoopTransactions
			}
		}

		// nonce too high transactions need moving straight to the queued pool
		if len(nonceTooHighSenders) > 0 {
			cfg.txPool.MoveFromPendingToQueued(nonceTooHighSenders)
		}

		// now trigger sender state changes in the pool where we encountered nonce issues during execution
		if len(sendersToTriggerStatechanges) > 0 {
			if err := cfg.txPool.TriggerSenderStateChanges(ctx, sdb.tx, header.GasLimit, sendersToTriggerStatechanges); err != nil {
				return err
			}
		}

		// we do not want to commit this block if it has no transactions and we detected an overflow - essentially the batch is too
		// full to get any more transactions in it and we don't want to commit an empty block
		if emptyBlockOverflow {
			log.Info(fmt.Sprintf("[%s] Block %d overflow detected with no transactions added, skipping block for next batch", logPrefix, blockNumber))
			break
		}

		// 0 TX block handling:
		// if we had some transactions yielded but didn't mine any in this block then we shouldn't commit it and move on.
		// this could happen if there were lots of nonce issues from transaction in the pool due to a failed tx processing or similar and
		// there wasn't much time left in the batch to mine any transactions
		if yieldedSomething && len(batchState.blockState.builtBlockElements.transactions) == 0 {
			log.Info(fmt.Sprintf("[%s] Skipping block: no transactions mined in block %d, skipping block for now", logPrefix, blockNumber))
			break
		}

		if block, err = doFinishBlockAndUpdateState(batchContext, ibs, header, parentBlock, batchState, ger, l1BlockHash, l1TreeUpdateIndex, infoTreeIndexProgress, batchCounters); err != nil {
			return err
		}

		// add a check to the verifier and also check for responses
		batchState.onBuiltBlock(blockNumber)

		// check if we are in limbo recovery and update the pool with the new state root for the latest transaction
		// being checked then return before committing anything about the block to the DB
		if batchState.isLimboRecovery() {
			stateRoot := block.Root()
			cfg.txPool.UpdateLimboRootByTxHash(batchState.limboRecoveryData.limboTxHash, &stateRoot)
			return fmt.Errorf("[%s] %w: %s = %s", s.LogPrefix(), zk.ErrLimboState, batchState.limboRecoveryData.limboTxHash.Hex(), stateRoot.Hex())
		}

		if !batchState.isL1Recovery() {
			// commit block data here so it is accessible in other threads
			if errCommitAndStart := sdb.CommitAndStart(); errCommitAndStart != nil {
				return errCommitAndStart
			}
			defer sdb.tx.Rollback()
		}

		// remove the decoded transactions from the cache
		for _, txHash := range batchState.blockState.transactionsToDiscard {
			cfg.decodedTxCache.Remove(txHash)
		}

		t.LogTimer()

		logBlockFinished(logPrefix, blockNumber, block.GasUsed(), len(batchState.blockState.builtBlockElements.transactions), infoTreeIndexProgress, startTime)

		// do not use remote executor in l1recovery mode
		// if we need remote executor in l1 recovery then we must allow commit/start DB transactions
		useExecutorForVerification := !batchState.isL1Recovery() && batchState.hasExecutorForThisBatch
		counters, err := batchCounters.CombineCollectors(l1TreeUpdateIndex != 0)
		if err != nil {
			return err
		}
		cfg.legacyVerifier.StartAsyncVerification(batchContext.s.LogPrefix(), batchState.forkId, batchState.batchNumber, block.Root(), counters.UsedAsMap(), batchState.builtBlocks, useExecutorForVerification, batchContext.cfg.zk.SequencerBatchVerificationTimeout, batchContext.cfg.zk.SequencerBatchVerificationRetries)

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

		if _, err := rawdb.IncrementStateVersionByBlockNumberIfNeeded(batchContext.sdb.tx, block.NumberU64()); err != nil {
			return fmt.Errorf("writing plain state version: %w", err)
		}

		// notify the done hook that we have finished processing this block - will notify subscribers etc.
		// here we -1 the block number as we know we have just created a new block so can simulate that the last block notified
		// was the previous block created
		if err := cfg.doneHook.AfterRun(batchContext.sdb.tx, block.NumberU64()-1, s.PrevUnwindPoint()); err != nil {
			return err
		}
	}

	yielder.Cleanup()

	/*
		if adding something below that line we must ensure
		- it is also handled property in processInjectedInitialBatch
		- it is also handled property in alignExecutionToDatastream
		- it is also handled property in doCheckForBadBatch
		- it is unwound correctly
	*/

	log.Info(fmt.Sprintf("[%s] Finish batch %d...", batchContext.s.LogPrefix(), batchState.batchNumber))

	return sdb.tx.Commit()
}

func logBlockFinished(logPrefix string, blockNumber, gas uint64, txAmount int, infoTreeIndexProgress uint64, startTime time.Time) {
	taken := time.Since(startTime)
	mGasPerSecond := float64(0)
	mGas := float64(gas) / 1_000_000
	elapsedSeconds := taken.Seconds()
	if elapsedSeconds != 0 {
		mGasPerSecond = mGas / elapsedSeconds
	}

	if mGasPerSecond != 0 {
		log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions... (%.1f MGas/s)", logPrefix, blockNumber, txAmount, mGasPerSecond), "gas", gas, "info-tree-index", infoTreeIndexProgress, "taken", time.Since(startTime))
	} else {
		log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions...", logPrefix, blockNumber, txAmount), "gas", gas, "info-tree-index", infoTreeIndexProgress, "taken", time.Since(startTime))
	}
}

func removeInclusionTransaction(orig []types.Transaction, index int) []types.Transaction {
	if index < 0 || index >= len(orig) {
		return orig
	}
	return append(orig[:index], orig[index+1:]...)
}

func handleBadTxHashCounter(hermezDb *hermez_db.HermezDb, txHash common.Hash) (uint64, error) {
	counter, err := hermezDb.GetBadTxHashCounter(txHash)
	if err != nil {
		return 0, err
	}
	newCounter := counter + 1
	if err = hermezDb.WriteBadTxHashCounter(txHash, newCounter); err != nil {
		return 0, fmt.Errorf("failed to write bad tx hash counter: %w", err)
	}
	return newCounter, nil
}
