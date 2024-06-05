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
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/utils"
)

var SpecialZeroIndexHash = common.HexToHash("0x27AE5BA08D7291C96C8CBDDCC148BF48A6D68C7974B94356F53754EF6171D757")

func SpawnSequencingStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	toBlock uint64,
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

	l1Recovery := cfg.zk.L1SyncStartBlock > 0

	// injected batch
	if executionAt == 0 {
		// set the block height for the fork we're running at to ensure contract interactions are correct
		if err = utils.RecoverySetBlockConfigForks(1, forkId, cfg.chainConfig, logPrefix); err != nil {
			return err
		}

		header, parentBlock, err := prepareHeader(tx, executionAt, math.MaxUint64, forkId, cfg.zk.AddressSequencer)
		if err != nil {
			return err
		}

		getHashFn := core.GetHashFn(header, getHeader)
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.engine, &cfg.zk.AddressSequencer, parentBlock.ExcessDataGas())

		err = processInjectedInitialBatch(ctx, cfg, s, sdb, forkId, header, parentBlock, &blockContext)
		if err != nil {
			return err
		}

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
	var decodedBlocks []zktx.DecodedBatchL2Data // only used in l1 recovery
	var blockTransactions []types.Transaction
	var l1EffectiveGases, effectiveGases []uint8

	batchTicker := time.NewTicker(cfg.zk.SequencerBatchSealTime)
	defer batchTicker.Stop()
	nonEmptyBatchTimer := time.NewTicker(cfg.zk.SequencerNonEmptyBatchSealTime)
	defer nonEmptyBatchTimer.Stop()

	hasAnyTransactionsInThisBatch := false
	thisBatch := lastBatch + 1
	batchCounters := vm.NewBatchCounterCollector(sdb.smt.GetDepth(), uint16(forkId), cfg.zk.ShouldCountersBeUnlimited(l1Recovery))
	runLoopBlocks := true
	lastStartedBn := executionAt - 1
	yielded := mapset.NewSet[[32]byte]()
	coinbase := cfg.zk.AddressSequencer
	workRemaining := true
	decodedBlocksSize := uint64(0)

	if l1Recovery {
		if cfg.zk.L1SyncStopBatch > 0 && thisBatch > cfg.zk.L1SyncStopBatch {
			log.Info(fmt.Sprintf("[%s] L1 recovery has completed!", logPrefix), "batch", thisBatch)
			time.Sleep(1 * time.Second)
			return nil
		}

		// let's check if we have any L1 data to recover
		var l1InfoRoot common.Hash
		decodedBlocks, coinbase, l1InfoRoot, workRemaining, err = getNextL1BatchData(thisBatch, forkId, sdb.hermezDb)
		if err != nil {
			return err
		}

		decodedBlocksSize = uint64(len(decodedBlocks))
		if decodedBlocksSize == 0 {
			log.Info(fmt.Sprintf("[%s] L1 recovery has completed!", logPrefix), "batch", thisBatch)
			time.Sleep(1 * time.Second)
			return nil
		}

		// now look up the index associated with this info root
		var infoTreeIndex uint64
		if l1InfoRoot == SpecialZeroIndexHash {
			infoTreeIndex = 0
		} else {
			found := false
			infoTreeIndex, found, err = sdb.hermezDb.GetL1InfoTreeIndexByRoot(l1InfoRoot)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("could not find L1 info tree index for root %s", l1InfoRoot.String())
			}
		}

		// now let's detect a bad batch and skip it if we have to
		currentBlock, err := rawdb.ReadBlockByNumber(sdb.tx, executionAt)
		if err != nil {
			return err
		}
		badBatch, err := checkForBadBatch(thisBatch, sdb.hermezDb, currentBlock.Time(), infoTreeIndex, decodedBlocks)
		if err != nil {
			return err
		}

		if badBatch {
			log.Info(fmt.Sprintf("[%s] Skipping bad batch %d...", logPrefix, thisBatch))
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

	var blockNumber uint64
	for blockNumber = executionAt; runLoopBlocks; blockNumber++ {
		if l1Recovery {
			decodedBlocksIndex := blockNumber - executionAt
			if decodedBlocksIndex == decodedBlocksSize {
				runLoopBlocks = false
				break
			}

			decodedBlock = decodedBlocks[decodedBlocksIndex]
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
			header, parentBlock, err = prepareHeader(tx, blockNumber, deltaTimestamp, forkId, coinbase)
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

		overflowOnNewBlock, err := batchCounters.StartNewBlock()
		if err != nil {
			return err
		}
		if !l1Recovery && overflowOnNewBlock {
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
			if !l1Recovery {
				log.Info(fmt.Sprintf("[%s] Waiting for txs from the pool...", logPrefix))
			}

			// we don't care about defer order here we just need to make sure the tickers are stopped to
			// avoid a leak
			logTicker := time.NewTicker(10 * time.Second)
			defer logTicker.Stop()
			blockTicker := time.NewTicker(cfg.zk.SequencerBlockSealTime)
			defer blockTicker.Stop()
			overflow := false

			// start to wait for transactions to come in from the pool and attempt to add them to the current batch.  Once we detect a counter
			// overflow we revert the IBS back to the previous snapshot and don't add the transaction/receipt to the collection that will
			// end up in the finalised block
		LOOP_TRANSACTIONS:
			for {
				select {
				case <-logTicker.C:
					log.Info(fmt.Sprintf("[%s] Waiting some more for txs from the pool...", logPrefix))
				case <-blockTicker.C:
					if !l1Recovery {
						break LOOP_TRANSACTIONS
					}
				case <-batchTicker.C:
					if !l1Recovery {
						runLoopBlocks = false
						break LOOP_TRANSACTIONS
					}
				case <-nonEmptyBatchTimer.C:
					if !l1Recovery && hasAnyTransactionsInThisBatch {
						runLoopBlocks = false
						break LOOP_TRANSACTIONS
					}
				default:
					if !l1Recovery {
						cfg.txPool.LockFlusher()
						blockTransactions, err = getNextPoolTransactions(cfg, executionAt, forkId, yielded)
						if err != nil {
							return err
						}
						cfg.txPool.UnlockFlusher()
					}

					for i, transaction := range blockTransactions {
						var receipt *types.Receipt
						var effectiveGas uint8

						if l1Recovery {
							effectiveGas = l1EffectiveGases[i]
						} else {
							effectiveGas = DeriveEffectiveGasPrice(cfg, transaction)
							effectiveGases = append(effectiveGases, effectiveGas)
						}
						effectiveGases = append(effectiveGases, effectiveGas)

						receipt, overflow, err = attemptAddTransaction(cfg, sdb, ibs, batchCounters, &blockContext, header, transaction, effectiveGas, l1Recovery, forkId)
						if err != nil {
							// if we are in recovery just log the error as a warning.  If the data is on the L1 then we should consider it as confirmed.
							// The executor/prover would simply skip a TX with an invalid nonce for example so we don't need to worry about that here.
							if l1Recovery {
								log.Warn(fmt.Sprintf("[%s] error adding transaction to batch during recovery: %v", logPrefix, err))
								continue
							}
							return err
						}
						if !l1Recovery && overflow {
							log.Info(fmt.Sprintf("[%s] overflowed adding transaction to batch", logPrefix), "batch", thisBatch, "tx-hash", transaction.Hash(), "txs before overflow", len(addedTransactions))
							/*
								There are two cases when overflow could occur.
								1. The block DOES not contains any transactions.
									In this case it means that a single tx overflow entire zk-counters.
									In this case we mark it so. Once marked it will be discarded from the tx-pool async (once the tx-pool process the creation of a new batch)
									NB: The tx SHOULD not be removed from yielded set, because if removed, it will be picked again on next block
								2. The block contains transactions.
									In this case, we just have to remove the transaction that overflowed the zk-counters and all transactions after it, from the yielded set.
									This removal will ensure that these transaction could be added in the next block(s)
							*/
							if len(addedTransactions) == 0 {
								cfg.txPool.MarkForDiscardFromPendingBest(transaction.Hash())
								log.Trace(fmt.Sprintf("single transaction %s overflow counters", transaction.Hash()))
							} else {
								txSize := len(blockTransactions)
								for ; i < txSize; i++ {
									yielded.Remove(transaction.Hash())
								}
							}

							break LOOP_TRANSACTIONS
						}

						addedTransactions = append(addedTransactions, transaction)
						addedReceipts = append(addedReceipts, receipt)

						hasAnyTransactionsInThisBatch = true
						nonEmptyBatchTimer.Reset(cfg.zk.SequencerNonEmptyBatchSealTime)
					}

					if l1Recovery {
						// just go into the normal loop waiting for new transactions to signal that the recovery
						// has finished as far as it can go
						if len(blockTransactions) == 0 && !workRemaining {
							log.Info(fmt.Sprintf("[%s] L1 recovery no more transactions to recover", logPrefix))
						}

						break LOOP_TRANSACTIONS
					}
				}
			}
			if !l1Recovery && overflow {
				blockNumber-- // in order to trigger reRunBlockAfterOverflow check
				continue      // lets execute the same block again
			}
		} else {
			for idx, transaction := range addedTransactions {
				effectiveGas := effectiveGases[idx]
				receipt, innerOverflow, err := attemptAddTransaction(cfg, sdb, ibs, batchCounters, &blockContext, header, transaction, effectiveGas, false, forkId)
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

		if err = doFinishBlockAndUpdateState(ctx, cfg, s, sdb, ibs, header, parentBlock, forkId, thisBatch, ger, l1BlockHash, addedTransactions, addedReceipts, effectiveGases, infoTreeIndexProgress); err != nil {
			return err
		}

		log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions...", logPrefix, thisBlockNumber, len(addedTransactions)))
	}

	counters, err := batchCounters.CombineCollectors()
	if err != nil {
		return err
	}

	log.Info("counters consumed", "counts", counters.UsedAsString())
	err = sdb.hermezDb.WriteBatchCounters(thisBatch, counters.UsedAsMap())
	if err != nil {
		return err
	}

	// if we do not have an executors in the zk config then we can populate the stream immediately with the latest
	// batch information
	if !cfg.zk.HasExecutors() {
		srv := server.NewDataStreamServer(cfg.stream, cfg.chainConfig.ChainID.Uint64())
		if err = server.WriteBlocksToStream(tx, sdb.hermezDb.HermezDbReader, srv, cfg.stream, executionAt+1, blockNumber, logPrefix); err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("[%s] Finish batch %d...", logPrefix, thisBatch))

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func PruneSequenceExecutionStage(s *stagedsync.PruneState, tx kv.RwTx, cfg SequenceBlockCfg, ctx context.Context, initialCycle bool) (err error) {
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	if cfg.historyV3 {
		cfg.agg.SetTx(tx)
		if initialCycle {
			if err = cfg.agg.Prune(ctx, ethconfig.HistoryV3AggregationStep/10); err != nil { // prune part of retired data, before commit
				return err
			}
		} else {
			if err = cfg.agg.PruneWithTiemout(ctx, 1*time.Second); err != nil { // prune part of retired data, before commit
				return err
			}
		}
	} else {
		if cfg.prune.History.Enabled() {
			if err = rawdb.PruneTableDupSort(tx, kv.AccountChangeSet, logPrefix, cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
			if err = rawdb.PruneTableDupSort(tx, kv.StorageChangeSet, logPrefix, cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
		}

		if cfg.prune.Receipts.Enabled() {
			if err = rawdb.PruneTable(tx, kv.Receipts, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxInt32); err != nil {
				return err
			}
			if err = rawdb.PruneTable(tx, kv.BorReceipts, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxUint32); err != nil {
				return err
			}
			// LogIndex.Prune will read everything what not pruned here
			if err = rawdb.PruneTable(tx, kv.Log, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxInt32); err != nil {
				return err
			}
		}
		if cfg.prune.CallTraces.Enabled() {
			if err = rawdb.PruneTableDupSort(tx, kv.CallTraceSet, logPrefix, cfg.prune.CallTraces.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
		}
	}

	if err = s.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
