package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk"
	"github.com/ledgerwatch/erigon/zk/l1_data"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/utils"
)

var SpecialZeroIndexHash = common.HexToHash("0x27AE5BA08D7291C96C8CBDDCC148BF48A6D68C7974B94356F53754EF6171D757")

func SpawnSequencingStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequenceBlockCfg,
	quiet bool,
) (err error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting sequencing stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished sequencing stage", logPrefix))

	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	sdb := newStageDb(tx)

	l1Recovery := cfg.zk.L1SyncStartBlock > 0

	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	lastBatch, err := stages.GetStageProgress(tx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	isLastBatchPariallyProcessed, err := sdb.hermezDb.GetIsBatchPartiallyProcessed(lastBatch)
	if err != nil {
		return err
	}

	forkId, err := prepareForkId(lastBatch, executionAt, sdb.hermezDb)
	if err != nil {
		return err
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(sdb.tx, hash, number) }
	hasExecutorForThisBatch := !isLastBatchPariallyProcessed && cfg.zk.HasExecutors()

	// handle case where batch wasn't closed properly
	// close it before starting a new one
	// this occurs when sequencer was switched from syncer or sequencer datastream files were deleted
	// and datastream was regenerated
	isLastEntryBatchEnd, err := cfg.datastreamServer.IsLastEntryBatchEnd()
	if err != nil {
		return err
	}

	// injected batch
	if executionAt == 0 {
		// set the block height for the fork we're running at to ensure contract interactions are correct
		if err = utils.RecoverySetBlockConfigForks(1, forkId, cfg.chainConfig, logPrefix); err != nil {
			return err
		}

		header, parentBlock, err := prepareHeader(tx, executionAt, math.MaxUint64, math.MaxUint64, forkId, cfg.zk.AddressSequencer)
		if err != nil {
			return err
		}

		getHashFn := core.GetHashFn(header, getHeader)
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.engine, &cfg.zk.AddressSequencer, parentBlock.ExcessDataGas())

		if err = processInjectedInitialBatch(ctx, cfg, s, sdb, forkId, header, parentBlock, &blockContext, l1Recovery); err != nil {
			return err
		}

		if err = cfg.datastreamServer.WriteWholeBatchToStream(logPrefix, tx, sdb.hermezDb.HermezDbReader, lastBatch, injectedBatchNumber); err != nil {
			return err
		}

		if freshTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}

		return nil
	}

	if !isLastBatchPariallyProcessed && !isLastEntryBatchEnd {
		log.Warn(fmt.Sprintf("[%s] Last batch %d was not closed properly, closing it now...", logPrefix, lastBatch))
		ler, err := utils.GetBatchLocalExitRootFromSCStorage(lastBatch, sdb.hermezDb.HermezDbReader, tx)
		if err != nil {
			return err
		}

		lastBlock, err := rawdb.ReadBlockByNumber(sdb.tx, executionAt)
		if err != nil {
			return err
		}
		root := lastBlock.Root()
		if err = cfg.datastreamServer.WriteBatchEnd(sdb.hermezDb, lastBatch, lastBatch-1, &root, &ler); err != nil {
			return err
		}
	}

	if err := utils.UpdateZkEVMBlockCfg(cfg.chainConfig, sdb.hermezDb, logPrefix); err != nil {
		return err
	}

	var header *types.Header
	var parentBlock *types.Block

	var decodedBlock zktx.DecodedBatchL2Data
	var deltaTimestamp uint64 = math.MaxUint64
	var blockTransactions []types.Transaction
	var l1EffectiveGases, effectiveGases []uint8

	batchTicker := time.NewTicker(cfg.zk.SequencerBatchSealTime)
	defer batchTicker.Stop()
	nonEmptyBatchTimer := time.NewTicker(cfg.zk.SequencerNonEmptyBatchSealTime)
	defer nonEmptyBatchTimer.Stop()

	hasAnyTransactionsInThisBatch := false

	thisBatch := lastBatch
	// if last batch finished - start a new one
	if !isLastBatchPariallyProcessed {
		thisBatch++
	}

	var intermediateUsedCounters *vm.Counters
	if isLastBatchPariallyProcessed {
		intermediateCountersMap, found, err := sdb.hermezDb.GetBatchCounters(lastBatch)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("intermediate counters not found for batch %d", lastBatch)
		}

		intermediateUsedCounters = vm.NewCountersFromUsedMap(intermediateCountersMap)
	}

	batchCounters := vm.NewBatchCounterCollector(sdb.smt.GetDepth(), uint16(forkId), cfg.zk.VirtualCountersSmtReduction, cfg.zk.ShouldCountersBeUnlimited(l1Recovery), intermediateUsedCounters)
	runLoopBlocks := true
	lastStartedBn := executionAt - 1
	yielded := mapset.NewSet[[32]byte]()

	nextBatchData := l1_data.DecodedL1Data{
		Coinbase:        cfg.zk.AddressSequencer,
		IsWorkRemaining: true,
	}

	decodedBlocksSize := uint64(0)
	limboHeaderTimestamp, limboTxHash := cfg.txPool.GetLimboTxHash(thisBatch)
	limboRecovery := limboTxHash != nil
	isAnyRecovery := l1Recovery || limboRecovery

	// if not limbo set the limboHeaderTimestamp to the "default" value for "prepareHeader" function
	if !limboRecovery {
		limboHeaderTimestamp = math.MaxUint64
	}

	if l1Recovery {
		if cfg.zk.L1SyncStopBatch > 0 && thisBatch > cfg.zk.L1SyncStopBatch {
			log.Info(fmt.Sprintf("[%s] L1 recovery has completed!", logPrefix), "batch", thisBatch)
			time.Sleep(1 * time.Second)
			return nil
		}

		// let's check if we have any L1 data to recover
		nextBatchData, err = l1_data.BreakDownL1DataByBatch(thisBatch, forkId, sdb.hermezDb.HermezDbReader)
		if err != nil {
			return err
		}

		decodedBlocksSize = uint64(len(nextBatchData.DecodedData))
		if decodedBlocksSize == 0 {
			log.Info(fmt.Sprintf("[%s] L1 recovery has completed!", logPrefix), "batch", thisBatch)
			time.Sleep(1 * time.Second)
			return nil
		}

		// now look up the index associated with this info root
		var infoTreeIndex uint64
		if nextBatchData.L1InfoRoot == SpecialZeroIndexHash {
			infoTreeIndex = 0
		} else {
			found := false
			infoTreeIndex, found, err = sdb.hermezDb.GetL1InfoTreeIndexByRoot(nextBatchData.L1InfoRoot)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("could not find L1 info tree index for root %s", nextBatchData.L1InfoRoot.String())
			}
		}

		// now let's detect a bad batch and skip it if we have to
		currentBlock, err := rawdb.ReadBlockByNumber(sdb.tx, executionAt)
		if err != nil {
			return err
		}
		badBatch, err := checkForBadBatch(thisBatch, sdb.hermezDb, currentBlock.Time(), infoTreeIndex, nextBatchData.LimitTimestamp, nextBatchData.DecodedData)
		if err != nil {
			return err
		}

		if badBatch {
			log.Info(fmt.Sprintf("[%s] Skipping bad batch %d...", logPrefix, thisBatch))
			// store the fact that this batch was invalid during recovery - will be used for the stream later
			if err = sdb.hermezDb.WriteInvalidBatch(thisBatch); err != nil {
				return err
			}
			if err = sdb.hermezDb.WriteBatchCounters(thisBatch, map[string]int{}); err != nil {
				return err
			}
			if err = sdb.hermezDb.DeleteIsBatchPartiallyProcessed(thisBatch); err != nil {
				return err
			}
			if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, thisBatch); err != nil {
				return err
			}
			if err = sdb.hermezDb.WriteForkId(thisBatch, forkId); err != nil {
				return err
			}
			if freshTx {
				if err = tx.Commit(); err != nil {
					return err
				}
			}
			return nil
		}
	}

	if !isLastBatchPariallyProcessed {
		log.Info(fmt.Sprintf("[%s] Starting batch %d...", logPrefix, thisBatch))
	} else {
		log.Info(fmt.Sprintf("[%s] Continuing unfinished batch %d from block %d", logPrefix, thisBatch, executionAt))
	}

	blockDataSizeChecker := NewBlockDataChecker()

	prevHeader := rawdb.ReadHeaderByNumber(tx, executionAt)
	batchDataOverflow := false

	var block *types.Block
	for blockNumber := executionAt + 1; runLoopBlocks; blockNumber++ {
		if l1Recovery {
			decodedBlocksIndex := blockNumber - (executionAt + 1)
			if decodedBlocksIndex == decodedBlocksSize {
				runLoopBlocks = false
				break
			}

			decodedBlock = nextBatchData.DecodedData[decodedBlocksIndex]
			deltaTimestamp = uint64(decodedBlock.DeltaTimestamp)
			l1EffectiveGases = decodedBlock.EffectiveGasPricePercentages
			blockTransactions = decodedBlock.Transactions
		}

		l1InfoIndex, err := sdb.hermezDb.GetBlockL1InfoTreeIndex(lastStartedBn)
		if err != nil {
			return err
		}

		log.Info(fmt.Sprintf("[%s] Starting block %d (forkid %v)...", logPrefix, blockNumber, forkId))

		lastStartedBn = blockNumber

		addedTransactions := []types.Transaction{}
		addedReceipts := []*types.Receipt{}
		effectiveGases = []uint8{}
		addedExecutionResults := []*core.ExecutionResult{}

		header, parentBlock, err = prepareHeader(tx, blockNumber-1, deltaTimestamp, limboHeaderTimestamp, forkId, nextBatchData.Coinbase)
		if err != nil {
			return err
		}

		// run this only once the first time, do not add it on rerun
		if batchDataOverflow = blockDataSizeChecker.AddBlockStartData(uint32(prevHeader.Time-header.Time), uint32(l1InfoIndex)); batchDataOverflow {
			log.Info(fmt.Sprintf("[%s] BatchL2Data limit reached. Stopping.", logPrefix), "blockNumber", blockNumber)
			break
		}

		// timer: evm + smt
		t := utils.StartTimer("stage_sequence_execute", "evm", "smt")

		overflowOnNewBlock, err := batchCounters.StartNewBlock(l1InfoIndex != 0)
		if err != nil {
			return err
		}
		if !isAnyRecovery && overflowOnNewBlock {
			break
		}

		infoTreeIndexProgress, l1TreeUpdate, l1TreeUpdateIndex, l1BlockHash, ger, shouldWriteGerToContract, err := prepareL1AndInfoTreeRelatedStuff(sdb, &decodedBlock, l1Recovery, header.Time)
		if err != nil {
			return err
		}

		ibs := state.New(sdb.stateReader)
		getHashFn := core.GetHashFn(header, getHeader)
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.engine, &cfg.zk.AddressSequencer, parentBlock.ExcessDataGas())

		parentRoot := parentBlock.Root()
		if err = handleStateForNewBlockStarting(
			cfg.chainConfig,
			sdb.hermezDb,
			ibs,
			blockNumber,
			thisBatch,
			header.Time,
			&parentRoot,
			l1TreeUpdate,
			shouldWriteGerToContract,
		); err != nil {
			return err
		}

		// start waiting for a new transaction to arrive
		if !isAnyRecovery {
			log.Info(fmt.Sprintf("[%s] Waiting for txs from the pool...", logPrefix))
		}

		// we don't care about defer order here we just need to make sure the tickers are stopped to
		// avoid a leak
		logTicker := time.NewTicker(10 * time.Second)
		defer logTicker.Stop()
		blockTicker := time.NewTicker(cfg.zk.SequencerBlockSealTime)
		defer blockTicker.Stop()
		var anyOverflow bool
		// start to wait for transactions to come in from the pool and attempt to add them to the current batch.  Once we detect a counter
		// overflow we revert the IBS back to the previous snapshot and don't add the transaction/receipt to the collection that will
		// end up in the finalised block
	LOOP_TRANSACTIONS:
		for {
			select {
			case <-logTicker.C:
				if !isAnyRecovery {
					log.Info(fmt.Sprintf("[%s] Waiting some more for txs from the pool...", logPrefix))
				}
			case <-blockTicker.C:
				if !isAnyRecovery {
					break LOOP_TRANSACTIONS
				}
			case <-batchTicker.C:
				if !isAnyRecovery {
					runLoopBlocks = false
					break LOOP_TRANSACTIONS
				}
			case <-nonEmptyBatchTimer.C:
				if !isAnyRecovery && hasAnyTransactionsInThisBatch {
					runLoopBlocks = false
					break LOOP_TRANSACTIONS
				}
			default:
				if limboRecovery {
					cfg.txPool.LockFlusher()
					blockTransactions, err = getLimboTransaction(cfg, limboTxHash)
					if err != nil {
						cfg.txPool.UnlockFlusher()
						return err
					}
					cfg.txPool.UnlockFlusher()
				} else if !l1Recovery {
					cfg.txPool.LockFlusher()
					blockTransactions, err = getNextPoolTransactions(cfg, executionAt, forkId, yielded)
					if err != nil {
						cfg.txPool.UnlockFlusher()
						return err
					}
					cfg.txPool.UnlockFlusher()
				}

				var receipt *types.Receipt
				var execResult *core.ExecutionResult
				for i, transaction := range blockTransactions {
					txHash := transaction.Hash()

					var effectiveGas uint8

					if l1Recovery {
						effectiveGas = l1EffectiveGases[i]
					} else {
						effectiveGas = DeriveEffectiveGasPrice(cfg, transaction)
					}

					backupDataSizeChecker := *blockDataSizeChecker
					if receipt, execResult, anyOverflow, err = attemptAddTransaction(cfg, sdb, ibs, batchCounters, &blockContext, header, transaction, effectiveGas, l1Recovery, forkId, l1InfoIndex, &backupDataSizeChecker); err != nil {
						if limboRecovery {
							panic("limbo transaction has already been executed once so they must not fail while re-executing")
						}

						// if we are in recovery just log the error as a warning.  If the data is on the L1 then we should consider it as confirmed.
						// The executor/prover would simply skip a TX with an invalid nonce for example so we don't need to worry about that here.
						if l1Recovery {
							log.Warn(fmt.Sprintf("[%s] error adding transaction to batch during recovery: %v", logPrefix, err),
								"hash", txHash,
								"to", transaction.GetTo(),
							)
							continue
						}
					}

					if anyOverflow {
						if limboRecovery {
							panic("limbo transaction has already been executed once so they must not overflow counters while re-executing")
						}

						if !l1Recovery {
							log.Info(fmt.Sprintf("[%s] overflowed adding transaction to batch", logPrefix), "batch", thisBatch, "tx-hash", txHash, "has any transactions in this batch", hasAnyTransactionsInThisBatch)
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
							if !hasAnyTransactionsInThisBatch {
								cfg.txPool.MarkForDiscardFromPendingBest(txHash)
								log.Trace(fmt.Sprintf("single transaction %s overflow counters", txHash))
							}
						}

						break LOOP_TRANSACTIONS
					}

					if err == nil {
						blockDataSizeChecker = &backupDataSizeChecker
						yielded.Remove(txHash)
						addedTransactions = append(addedTransactions, transaction)
						addedReceipts = append(addedReceipts, receipt)
						addedExecutionResults = append(addedExecutionResults, execResult)
						effectiveGases = append(effectiveGases, effectiveGas)

						hasAnyTransactionsInThisBatch = true
						nonEmptyBatchTimer.Reset(cfg.zk.SequencerNonEmptyBatchSealTime)
						log.Debug(fmt.Sprintf("[%s] Finish block %d with %s transaction", logPrefix, blockNumber, txHash.Hex()))
					}
				}

				if l1Recovery {
					// just go into the normal loop waiting for new transactions to signal that the recovery
					// has finished as far as it can go
					if len(blockTransactions) == 0 && !nextBatchData.IsWorkRemaining {
						log.Info(fmt.Sprintf("[%s] L1 recovery no more transactions to recover", logPrefix))
					}

					break LOOP_TRANSACTIONS
				}

				if limboRecovery {
					runLoopBlocks = false
					break LOOP_TRANSACTIONS
				}
			}
		}

		if err = sdb.hermezDb.WriteBlockL1InfoTreeIndex(blockNumber, l1TreeUpdateIndex); err != nil {
			return err
		}

		block, err = doFinishBlockAndUpdateState(ctx, cfg, s, sdb, ibs, header, parentBlock, forkId, thisBatch, ger, l1BlockHash, addedTransactions, addedReceipts, addedExecutionResults, effectiveGases, infoTreeIndexProgress, l1Recovery)
		if err != nil {
			return err
		}

		t.LogTimer()
		gasPerSecond := float64(0)
		elapsedSeconds := t.Elapsed().Seconds()
		if elapsedSeconds != 0 {
			gasPerSecond = float64(block.GasUsed()) / elapsedSeconds
		}

		if limboRecovery {
			stateRoot := block.Root()
			cfg.txPool.UpdateLimboRootByTxHash(limboTxHash, &stateRoot)
			return fmt.Errorf("[%s] %w: %s = %s", s.LogPrefix(), zk.ErrLimboState, limboTxHash.Hex(), stateRoot.Hex())
		} else {
			log.Debug(fmt.Sprintf("[%s] state root at block %d = %s", s.LogPrefix(), blockNumber, block.Root().Hex()))
		}

		if gasPerSecond != 0 {
			log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions... (%d gas/s)", logPrefix, blockNumber, len(addedTransactions), int(gasPerSecond)))
		} else {
			log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions...", logPrefix, blockNumber, len(addedTransactions)))
		}

		if !hasExecutorForThisBatch {
			// save counters midbatch
			// here they shouldn't add more to counters other than what they already have
			// because it would be later added twice
			counters := batchCounters.CombineCollectorsNoChanges(l1InfoIndex != 0)

			if err = sdb.hermezDb.WriteBatchCounters(thisBatch, counters.UsedAsMap()); err != nil {
				return err
			}

			if err = sdb.hermezDb.WriteIsBatchPartiallyProcessed(thisBatch); err != nil {
				return err
			}

			if err = cfg.datastreamServer.WriteBlockWithBatchStartToStream(logPrefix, tx, sdb.hermezDb, forkId, thisBatch, lastBatch, *parentBlock, *block); err != nil {
				return err
			}

			if err = tx.Commit(); err != nil {
				return err
			}
			if tx, err = cfg.db.BeginRw(ctx); err != nil {
				return err
			}
			// TODO: This creates stacked up deferrals
			defer tx.Rollback()
			sdb.SetTx(tx)

			lastBatch = thisBatch
		}
	}

	l1InfoIndex, err := sdb.hermezDb.GetBlockL1InfoTreeIndex(lastStartedBn)
	if err != nil {
		return err
	}

	counters, err := batchCounters.CombineCollectors(l1InfoIndex != 0)
	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] counters consumed", logPrefix), "batch", thisBatch, "counts", counters.UsedAsString())
	if err = sdb.hermezDb.WriteBatchCounters(thisBatch, counters.UsedAsMap()); err != nil {
		return err
	}

	if err = sdb.hermezDb.DeleteIsBatchPartiallyProcessed(thisBatch); err != nil {
		return err
	}

	// Local Exit Root (ler): read s/c storage every batch to store the LER for the highest block in the batch
	ler, err := utils.GetBatchLocalExitRootFromSCStorage(thisBatch, sdb.hermezDb.HermezDbReader, tx)
	if err != nil {
		return err
	}
	// write ler to hermezdb
	if err = sdb.hermezDb.WriteLocalExitRootForBatchNo(thisBatch, ler); err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Finish batch %d...", logPrefix, thisBatch))

	if !hasExecutorForThisBatch {
		blockRoot := block.Root()
		if err = cfg.datastreamServer.WriteBatchEnd(sdb.hermezDb, thisBatch, lastBatch, &blockRoot, &ler); err != nil {
			return err
		}
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}
