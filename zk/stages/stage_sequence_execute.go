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
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/erigon/zk/l1_data"
)

var SpecialZeroIndexHash = common.HexToHash("0x27AE5BA08D7291C96C8CBDDCC148BF48A6D68C7974B94356F53754EF6171D757")

func SpawnSequencingStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequenceBlockCfg,
	initialCycle bool,
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

	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	lastBatch, err := stages.GetStageProgress(tx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	forkId, err := prepareForkId(cfg, lastBatch, executionAt, sdb.hermezDb)
	if err != nil {
		return err
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(sdb.tx, hash, number) }

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

		if err = processInjectedInitialBatch(ctx, cfg, s, sdb, forkId, header, parentBlock, &blockContext); err != nil {
			return err
		}

		// write the batch directly to the stream
		srv := server.NewDataStreamServer(cfg.stream, cfg.chainConfig.ChainID.Uint64())
		if err = server.WriteBlocksToStream(tx, sdb.hermezDb.HermezDbReader, srv, cfg.stream, 1, 1, logPrefix); err != nil {
			return err
		}

		if freshTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}

		return nil
	}

	if err := utils.UpdateZkEVMBlockCfg(cfg.chainConfig, sdb.hermezDb, logPrefix); err != nil {
		return err
	}

	var header *types.Header
	var parentBlock *types.Block

	var addedTransactions []types.Transaction
	var addedReceipts []*types.Receipt
	var clonedBatchCounters *vm.BatchCounterCollector

	var decodedBlock zktx.DecodedBatchL2Data
	var deltaTimestamp uint64 = math.MaxUint64
	var blockTransactions []types.Transaction
	var l1EffectiveGases, effectiveGases []uint8

	batchTicker := time.NewTicker(cfg.zk.SequencerBatchSealTime)
	defer batchTicker.Stop()
	nonEmptyBatchTimer := time.NewTicker(cfg.zk.SequencerNonEmptyBatchSealTime)
	defer nonEmptyBatchTimer.Stop()

	l1Recovery := cfg.zk.L1SyncStartBlock > 0
	hasAnyTransactionsInThisBatch := false
	thisBatch := lastBatch + 1
	batchCounters := vm.NewBatchCounterCollector(sdb.smt.GetDepth(), uint16(forkId), cfg.zk.VirtualCountersSmtReduction, cfg.zk.ShouldCountersBeUnlimited(l1Recovery))
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
			if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, thisBatch); err != nil {
				return err
			}
			if err = sdb.hermezDb.WriteForkId(thisBatch, forkId); err != nil {
				return err
			}
			return nil
		}
	}

	log.Info(fmt.Sprintf("[%s] Starting batch %d...", logPrefix, thisBatch))

	for blockNumber := executionAt; runLoopBlocks; blockNumber++ {
		if l1Recovery {
			decodedBlocksIndex := blockNumber - executionAt
			if decodedBlocksIndex == decodedBlocksSize {
				runLoopBlocks = false
				break
			}

			decodedBlock = nextBatchData.DecodedData[decodedBlocksIndex]
			deltaTimestamp = uint64(decodedBlock.DeltaTimestamp)
			l1EffectiveGases = decodedBlock.EffectiveGasPricePercentages
			blockTransactions = decodedBlock.Transactions
		}

		log.Info(fmt.Sprintf("[%s] Starting block %d...", logPrefix, blockNumber+1))

		reRunBlockAfterOverflow := blockNumber == lastStartedBn
		lastStartedBn = blockNumber

		if !reRunBlockAfterOverflow {
			clonedBatchCounters = batchCounters.Clone()
			addedTransactions = []types.Transaction{}
			addedReceipts = []*types.Receipt{}
			effectiveGases = []uint8{}
			header, parentBlock, err = prepareHeader(tx, blockNumber, deltaTimestamp, limboHeaderTimestamp, forkId, nextBatchData.Coinbase)
			if err != nil {
				return err
			}
		} else {
			batchCounters = clonedBatchCounters

			// create a copy of the header otherwise the executor will return "state root mismatch error"
			header = &types.Header{
				ParentHash: header.ParentHash,
				Coinbase:   header.Coinbase,
				Difficulty: header.Difficulty,
				Number:     header.Number,
				GasLimit:   header.GasLimit,
				Time:       header.Time,
			}
		}

		l1InfoIndex, err := sdb.hermezDb.GetBlockL1InfoTreeIndex(lastStartedBn)
		if err != nil {
			return err
		}

		overflowOnNewBlock, err := batchCounters.StartNewBlock(l1InfoIndex != 0)
		if err != nil {
			return err
		}
		if !isAnyRecovery && overflowOnNewBlock {
			break
		}

		thisBlockNumber := header.Number.Uint64()

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
			thisBlockNumber,
			thisBatch,
			header.Time,
			&parentRoot,
			l1TreeUpdate,
			shouldWriteGerToContract,
		); err != nil {
			return err
		}

		if !reRunBlockAfterOverflow {
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
			reRunBlock := false
			overflow := false
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
					for i, transaction := range blockTransactions {
						var effectiveGas uint8

						if l1Recovery {
							effectiveGas = l1EffectiveGases[i]
						} else {
							effectiveGas = DeriveEffectiveGasPrice(cfg, transaction)
						}

						receipt, overflow, err = attemptAddTransaction(cfg, sdb, ibs, batchCounters, &blockContext, header, transaction, effectiveGas, l1Recovery, forkId, l1InfoIndex)
						if err != nil {
							if limboRecovery {
								panic("limbo transaction has already been executed once so they must not fail while re-executing")
							}

							// if we are in recovery just log the error as a warning.  If the data is on the L1 then we should consider it as confirmed.
							// The executor/prover would simply skip a TX with an invalid nonce for example so we don't need to worry about that here.
							if l1Recovery {
								log.Warn(fmt.Sprintf("[%s] error adding transaction to batch during recovery: %v", logPrefix, err),
									"hash", transaction.Hash(),
									"to", transaction.GetTo(),
								)
								continue
							}

							i++ // leave current tx in yielded set
							reRunBlock = true
						}
						if !reRunBlock && overflow {
							if limboRecovery {
								panic("limbo transaction has already been executed once so they must not overflow counters while re-executing")
							}

							if !l1Recovery {
								log.Info(fmt.Sprintf("[%s] overflowed adding transaction to batch", logPrefix), "batch", thisBatch, "tx-hash", transaction.Hash(), "has any transactions in this batch", hasAnyTransactionsInThisBatch)
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
									i++ // leave current tx in yielded set
									cfg.txPool.MarkForDiscardFromPendingBest(transaction.Hash())
									log.Trace(fmt.Sprintf("single transaction %s overflow counters", transaction.Hash()))
								}

								reRunBlock = true
							}
						}

						if reRunBlock {
							txSize := len(blockTransactions)
							for ; i < txSize; i++ {
								yielded.Remove(transaction.Hash())
							}
							break LOOP_TRANSACTIONS
						}

						addedTransactions = append(addedTransactions, transaction)
						addedReceipts = append(addedReceipts, receipt)
						effectiveGases = append(effectiveGases, effectiveGas)

						hasAnyTransactionsInThisBatch = true
						nonEmptyBatchTimer.Reset(cfg.zk.SequencerNonEmptyBatchSealTime)
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
			if reRunBlock {
				blockNumber-- // in order to trigger reRunBlockAfterOverflow check
				continue      // lets execute the same block again
			}
		} else {
			for idx, transaction := range addedTransactions {
				effectiveGas := effectiveGases[idx]
				receipt, innerOverflow, err := attemptAddTransaction(cfg, sdb, ibs, batchCounters, &blockContext, header, transaction, effectiveGas, false, forkId, l1InfoIndex)
				if err != nil {
					return err
				}
				if innerOverflow {
					// kill the node at this stage to prevent a batch being created that can't be proven
					panic(fmt.Sprintf("overflowed twice during execution while adding tx with index %d", idx))
				}
				addedReceipts[idx] = receipt
			}
			runLoopBlocks = false // close the batch because there are no counters left
		}

		if err = sdb.hermezDb.WriteBlockL1InfoTreeIndex(thisBlockNumber, l1TreeUpdateIndex); err != nil {
			return err
		}

		block, err := doFinishBlockAndUpdateState(ctx, cfg, s, sdb, ibs, header, parentBlock, forkId, thisBatch, ger, l1BlockHash, addedTransactions, addedReceipts, effectiveGases, infoTreeIndexProgress)
		if err != nil {
			return err
		}

		if limboRecovery {
			stateRoot := block.Root()
			cfg.txPool.UpdateLimboRootByTxHash(limboTxHash, &stateRoot)
			return fmt.Errorf("[%s] %w: %s = %s", s.LogPrefix(), zk.ErrLimboState, limboTxHash.Hex(), stateRoot.Hex())
		} else {
			log.Debug(fmt.Sprintf("[%s] state root at block %d = %s", s.LogPrefix(), thisBlockNumber, block.Root().Hex()))
		}

		for _, tx := range addedTransactions {
			log.Debug(fmt.Sprintf("[%s] Finish block %d with %s transaction", logPrefix, thisBlockNumber, tx.Hash().Hex()))
		}
		log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions...", logPrefix, thisBlockNumber, len(addedTransactions)))
	}

	l1InfoIndex, err := sdb.hermezDb.GetBlockL1InfoTreeIndex(lastStartedBn)
	if err != nil {
		return err
	}

	counters, err := batchCounters.CombineCollectors(l1InfoIndex != 0)
	if err != nil {
		return err
	}

	log.Info("counters consumed", "counts", counters.UsedAsString())
	err = sdb.hermezDb.WriteBatchCounters(thisBatch, counters.UsedAsMap())
	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Finish batch %d...", logPrefix, thisBatch))

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}
