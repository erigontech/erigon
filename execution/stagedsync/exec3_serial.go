package stagedsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/changeset"
	"os"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/seboost"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/chaos_monkey"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/shards"
)

type serialExecutor struct {
	txExecutor
	// outputs
	txCount         uint64
	blockGasUsed    uint64
	blobGasUsed     uint64
	lastBlockResult *blockResult
	worker          *exec.Worker
	seboostWriter   *seboost.Writer
	seboostReader   *seboost.Reader
}

func warmTxsHashes(block *types.Block) {
	for _, t := range block.Transactions() {
		_ = t.Hash()
	}
	_ = block.Hash()
}

func (se *serialExecutor) exec(ctx context.Context, execStage *StageState, u Unwinder,
	startBlockNum uint64, offsetFromBlockBeginning uint64, maxBlockNum uint64, blockLimit uint64,
	initialTxNum uint64, inputTxNum uint64, initialCycle bool, rwTx kv.TemporalRwTx,
	accumulator *shards.Accumulator, readAhead chan uint64, logEvery *time.Ticker) (*types.Header, kv.TemporalRwTx, error) {

	se.resetWorkers(ctx, se.rs, se.applyTx)

	havePartialBlock := false
	blockNum := startBlockNum

	var b *types.Block

	lastFrozenStep := se.applyTx.StepsInFiles(kv.CommitmentDomain)

	var lastFrozenTxNum uint64
	if lastFrozenStep > 0 {
		lastFrozenTxNum = uint64((lastFrozenStep+1)*kv.Step(se.doms.StepSize())) - 1
	}

	toBlockNum := maxBlockNum
	if blockLimit > 0 {
		toBlockNum = min(maxBlockNum, blockNum+blockLimit-1)
	}
	if maxBlockNum > blockNum+16 {
		log.Info(fmt.Sprintf("[%s] serial starting", execStage.LogPrefix()),
			"from", blockNum, "to", toBlockNum, "initialTxNum", initialTxNum,
			"initialBlockTxOffset", offsetFromBlockBeginning, "lastFrozenStep", lastFrozenStep,
			"initialCycle", initialCycle, "isForkValidation", se.isForkValidation, "isBlockProduction", se.isBlockProduction)
	}

	for ; blockNum <= maxBlockNum; blockNum++ {
		shouldGenerateChangesets := shouldGenerateChangeSets(se.cfg, blockNum, maxBlockNum, initialCycle)
		changeSet := &changeset.StateChangeSet{}
		if shouldGenerateChangesets && blockNum > 0 {
			se.doms.SetChangesetAccumulator(changeSet)
		}

		select {
		case readAhead <- blockNum:
		default:
		}

		canonicalHash, err := rawdb.ReadCanonicalHash(se.applyTx, blockNum)
		if err != nil {
			return nil, rwTx, err
		}
		var ok bool
		b, ok = exec.ReadBlockWithSendersFromGlobalReadAheader(canonicalHash)
		if b == nil || !ok {
			b, err = exec.BlockWithSenders(ctx, se.cfg.db, se.applyTx, se.cfg.blockReader, blockNum)
			if err != nil {
				return nil, rwTx, err
			}
		}
		if b == nil {
			// TODO: panic here and see that overall process deadlock
			return nil, rwTx, fmt.Errorf("nil block %d", blockNum)
		}
		go warmTxsHashes(b)

		stateCache := se.doms.GetStateCache()
		if stateCache != nil {
			stateCache.ValidateAndPrepare(b.ParentHash(), b.Hash())
		}

		txs := b.Transactions()
		header := b.HeaderNoCopy()
		getHashFnMutex := sync.Mutex{}

		// se.cfg.chainConfig.AmsterdamTime != nil && se.cfg.chainConfig.AmsterdamTime.Uint64() > 0 is
		// temporary to allow for inital non bals amsterdam testing before parallel exec is live by defualt
		if se.cfg.chainConfig.AmsterdamTime != nil && se.cfg.chainConfig.AmsterdamTime.Uint64() > 0 && se.cfg.chainConfig.IsAmsterdam(header.Time) {
			se.logger.Error(fmt.Sprintf("[%s] BLOCK PROCESSING FAILED: Amsterdam processing is not supported by serial exec", se.logPrefix), "fork-block", blockNum)
			se.logger.Error(fmt.Sprintf("[%s] Run erigon with either '--experimental.bal' or 'export ERIGON_EXEC3_PARALLEL=true'", se.logPrefix))
			return nil, rwTx, fmt.Errorf("amsterdam processing is not supported by serial exec from block: %d", blockNum)
		}

		blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
			getHashFnMutex.Lock()
			defer getHashFnMutex.Unlock()
			return se.getHeader(ctx, hash, number)
		}), se.cfg.engine, se.cfg.author, se.cfg.chainConfig)

		if accumulator != nil {
			txs, err := se.cfg.blockReader.RawTransactions(context.Background(), se.applyTx, b.NumberU64(), b.NumberU64())
			if err != nil {
				return nil, rwTx, err
			}
			accumulator.StartChange(header, txs, false)
		}

		var txTasks []exec.Task

		// load seboost txdeps for this block if available
		var blockDeps [][]int
		if se.seboostReader != nil {
			blockDeps, _ = se.seboostReader.GetDeps(blockNum)
		}

		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			// Do not oversend, wait for the result heap to go under certain size
			txTask := &exec.TxTask{
				TxNum:           inputTxNum,
				TxIndex:         txIndex,
				Header:          header,
				Uncles:          b.Uncles(),
				Txs:             txs,
				EvmBlockContext: blockContext,
				Withdrawals:     b.Withdrawals(),
				// use history reader instead of state reader to catch up to the tx where we left off
				HistoryExecution: lastFrozenTxNum > 0 && inputTxNum <= lastFrozenTxNum,
				Trace:            dbg.TraceTx(blockNum, txIndex),
				Hooks:            se.hooks,
				Logger:           se.logger,
			}

			if txTask.TxNum > 0 && txTask.TxNum <= initialTxNum {
				havePartialBlock = true
				inputTxNum++
				continue
			}

			// inject seboost txdeps if available
			if blockDeps != nil {
				depIndex := txIndex + 1 // 0=system tx, 1=user tx 0, etc.
				if depIndex >= 0 && depIndex < len(blockDeps) && len(blockDeps[depIndex]) > 0 {
					txTask.SetDependencies(blockDeps[depIndex])
					se.logger.Trace("seboost: injecting deps", "block", blockNum, "txIndex", txIndex, "depCount", len(blockDeps[depIndex]))
				}
			}

			txTasks = append(txTasks, txTask)
			inputTxNum++
		}

		start := time.Now()
		continueLoop, err := se.executeBlock(ctx, txTasks, execStage.CurrentSyncCycle.IsInitialCycle, false)

		log.Debug(fmt.Sprintf("[%s] executed block %d in %s", se.logPrefix, blockNum, time.Since(start)))
		if err != nil {
			return nil, rwTx, err
		}
		if stateCache != nil {
			stateCache.PrintStatsAndReset()
		}

		if !continueLoop {
			return b.HeaderNoCopy(), rwTx, nil
		}

		if !dbg.BatchCommitments || shouldGenerateChangesets || se.cfg.syncCfg.KeepExecutionProofs {
			if dbg.TraceBlock(blockNum) {
				fmt.Println(blockNum, "Commitment")
				se.doms.SetTrace(true, false)
			}
			// Warmup is enabled via EnableTrieWarmup at executor init
			rh, err := se.doms.ComputeCommitment(ctx, se.applyTx, true, blockNum, inputTxNum-1, se.logPrefix, nil)
			se.doms.SetTrace(false, false)

			if err != nil {
				return nil, rwTx, err
			}

			if shouldGenerateChangesets {
				se.doms.SavePastChangesetAccumulator(b.Hash(), blockNum, changeSet)
			}
			se.doms.SetChangesetAccumulator(nil)

			if !se.isBlockProduction && !bytes.Equal(rh, header.Root.Bytes()) {
				se.logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", se.logPrefix, header.Number.Uint64(), rh, header.Root.Bytes(), header.Hash()))
				return b.HeaderNoCopy(), rwTx, fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, blockNum)
			}
		}

		if dbg.StopAfterBlock > 0 && blockNum == dbg.StopAfterBlock {
			panic(fmt.Sprintf("stopping: block %d complete", blockNum))
			//return fmt.Errorf("stopping: block %d complete", blockNum)
		}

		if offsetFromBlockBeginning > 0 {
			// after history execution no offset will be required
			offsetFromBlockBeginning = 0
		}

		select {
		case <-logEvery.C:
			if !se.isApplyingBlocks {
				break
			}
			se.LogExecution()
			isBatchFull := se.readState().SizeEstimateBeforeCommitment() >= se.cfg.batchSize.Bytes()
			needCalcRoot := isBatchFull || havePartialBlock
			// If we have a partial first block it may not be validated, then we should compute root hash ASAP for fail-fast
			// this will only happen for the first executed block
			havePartialBlock = false
			if !needCalcRoot {
				break
			}
			resetExecGauges(ctx)
			ok, times, err := computeAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), rwTx, se.doms, se.cfg, execStage, false, se.logger, u, se.isBlockProduction)
			if err != nil {
				return nil, rwTx, err
			} else if !ok {
				return b.HeaderNoCopy(), rwTx, nil
			}
			resetCommitmentGauges(ctx)
			se.txExecutor.lastCommittedBlockNum.Store(b.NumberU64())
			se.txExecutor.lastCommittedTxNum.Store(inputTxNum)
			se.logger.Info(
				"periodic commit check",
				"block", b.NumberU64(),
				"txNum", inputTxNum,
				"commitment", times.ComputeCommitment,
			)
			if isBatchFull {
				return b.HeaderNoCopy(), rwTx, &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block batch is full"}
			}
		default:
		}

		select {
		case <-ctx.Done():
			return b.HeaderNoCopy(), rwTx, ctx.Err()
		default:
		}

		lastExecutedStep := kv.Step(uint64(se.lastExecutedTxNum.Load()) / se.doms.StepSize())

		// if we're in the initialCycle before we consider the blockLimit we need to make sure we keep executing
		// until we reach a transaction whose comittement which is writable to the db, otherwise the update will get lost
		if !initialCycle || lastExecutedStep > 0 && lastExecutedStep > lastFrozenStep && !dbg.DiscardCommitment() {
			if blockLimit > 0 && blockNum-startBlockNum+1 >= blockLimit && blockNum != maxBlockNum {
				return b.HeaderNoCopy(), rwTx, &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block limit reached"}
			}
		}
	}

	return b.HeaderNoCopy(), rwTx, nil
}

func (se *serialExecutor) LogExecution() {
	se.progress.LogExecution(se.rs.StateV3, se)
}

func (se *serialExecutor) LogCommitments(commitStart time.Time, committedBlocks uint64, committedTransactions uint64, committedGas uint64, stepsInDb float64, lastProgress commitment.CommitProgress) {
	se.committedGas.Add(int64(committedGas))
	se.txExecutor.lastCommittedBlockNum.Add(committedBlocks)
	se.txExecutor.lastCommittedTxNum.Add(committedTransactions)
	se.progress.LogCommitments(se.rs.StateV3, se, commitStart, stepsInDb, lastProgress)
}

func (se *serialExecutor) LogComplete(stepsInDb float64) {
	se.progress.LogComplete(se.rs.StateV3, se, stepsInDb)
}

func (se *serialExecutor) resetWorkers(ctx context.Context, rs *state.StateV3Buffered, applyTx kv.TemporalTx) (err error) {

	if se.worker == nil {
		se.taskExecMetrics = exec.NewWorkerMetrics()
		se.worker = exec.NewWorker(context.Background(), false, se.taskExecMetrics,
			se.cfg.db.(kv.TemporalRoDB), nil, se.cfg.blockReader, se.cfg.chainConfig, se.cfg.genesis, nil, se.cfg.engine, se.cfg.dirs, se.logger)
	}

	if se.applyTx != applyTx {
		if se.applyTx != nil {
			se.applyTx.Rollback()
		}

		if applyTx != nil {
			se.applyTx = applyTx
		} else {
			applyTx, err := se.cfg.db.BeginRo(ctx) //nolint
			if err != nil {
				applyTx.Rollback()
				return err
			}
			se.applyTx = applyTx.(kv.TemporalTx)
		}
	}

	se.worker.ResetState(rs, se.applyTx, nil, nil, nil)

	return nil
}

func (se *serialExecutor) executeBlock(ctx context.Context, tasks []exec.Task, isInitialCycle bool, profile bool) (cont bool, err error) {
	blockReceipts := make([]*types.Receipt, 0, len(tasks))
	var startTxIndex int

	if se.blockExecMetrics == nil {
		se.blockExecMetrics = newBlockExecMetrics()
	}

	defer func(t time.Time) {
		se.blockExecMetrics.BlockCount.Add(1)
		se.blockExecMetrics.Duration.Add(time.Since(t))
	}(time.Now())

	if len(tasks) > 0 {
		// During the first block execution, we may have half-block data in the snapshots.
		// Thus, we need to skip the first txs in the block, however, this causes the GasUsed to be incorrect.
		// So we need to skip that check for the first block, if we find half-executed data (startTxIndex>0).
		startTxIndex = max(tasks[0].(*exec.TxTask).TxIndex, 0)
	}

	// seboost: track per-transaction addresses for dependency generation.
	// We compute RAW (read-after-write) + WAW (write-after-write) deps only —
	// no read-read false deps.
	generateTxDeps := se.seboostWriter != nil && os.Getenv("SEBOOST_GENERATE") == "txdeps"
	// txAccessed[i]: all addresses accessed (read or write) by tx i — used to
	//   find whether tx i is affected by an earlier write.
	// txWritten[i]:  addresses written by tx i — used to build writersByAddr.
	var txAccessed []state.AccessSet
	var txWritten []map[accounts.Address]struct{}
	if generateTxDeps {
		txAccessed = make([]state.AccessSet, 0, len(tasks))
		txWritten = make([]map[accounts.Address]struct{}, 0, len(tasks))
	}

	var gasPool *protocol.GasPool
	for _, task := range tasks {
		txTask := task.(*exec.TxTask)

		if gasPool == nil {
			gasPool = protocol.NewGasPool(task.BlockGasLimit(), se.cfg.chainConfig.GetMaxBlobGasPerBlock(tasks[0].BlockTime()))
		}

		txTask.ResetGasPool(gasPool)
		txTask.Config = se.cfg.chainConfig
		txTask.Engine = se.cfg.engine

		result := se.worker.RunTxTask(txTask)

		// seboost: collect accessed+written addresses for this transaction.
		// accessed = all reads+writes (used to detect if tx i is affected by an earlier write)
		// written  = only writes (used to build writersByAddr — no read-read false deps)
		if generateTxDeps {
			accessed := result.AccessedAddresses
			if accessed == nil {
				accessed = state.AccessSet{}
			}
			written := result.WrittenAddresses
			if written == nil {
				written = map[accounts.Address]struct{}{}
			}
			txAccessed = append(txAccessed, accessed)
			txWritten = append(txWritten, written)
		}

		if err := func() error {
			if errors.Is(result.Err, context.Canceled) {
				return result.Err
			}
			if result.Err != nil {
				return fmt.Errorf("%w, txnIdx=%d, %v", rules.ErrInvalidBlock, txTask.TxIndex, result.Err) //same as in stage_exec.go
			}

			se.txCount++
			se.blockGasUsed += result.ExecutionResult.BlockGasUsed
			mxExecTransactions.Add(1)

			if txTask.Tx() != nil {
				se.blobGasUsed += txTask.Tx().GetBlobGas()
			}
			if txTask.IsBlockEnd() && txTask.BlockNumber() > 0 {
				//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txTask.TxNum, txTask.BlockNum)
				// End of block transaction in a block
				ibs := state.New(state.NewReaderV3(se.rs.Domains().AsGetter(se.applyTx)))
				defer ibs.Release(true)
				ibs.SetTxContext(txTask.BlockNumber(), txTask.TxIndex)
				syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
					ret, err := protocol.SysCallContract(contract, data, se.cfg.chainConfig, ibs, txTask.Header, se.cfg.engine, false /* constCall */, *se.cfg.vmConfig)
					if err != nil {
						return nil, err
					}
					result.Logs = append(result.Logs, ibs.GetRawLogs(txTask.TxIndex)...)
					return ret, err
				}

				chainReader := consensuschain.NewReader(se.cfg.chainConfig, se.applyTx, se.cfg.blockReader, se.logger)

				if se.isBlockProduction {
					_, _, err = se.cfg.engine.FinalizeAndAssemble(
						se.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles,
						blockReceipts, txTask.Withdrawals, chainReader, syscall, nil, se.logger)
				} else {
					_, err = se.cfg.engine.Finalize(
						se.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Uncles,
						blockReceipts, txTask.Withdrawals, chainReader, syscall, false, se.logger)
				}

				if err != nil {
					return fmt.Errorf("%w, txnIdx=%d, %w", rules.ErrInvalidBlock, txTask.TxIndex, err)
				}

				if !se.isBlockProduction && startTxIndex == 0 && !isInitialCycle {
					se.cfg.notifications.RecentReceipts.Add(blockReceipts, txTask.Txs, txTask.Header)
				}
				checkReceipts := !se.cfg.vmConfig.StatelessExec && se.cfg.chainConfig.IsByzantium(txTask.BlockNumber()) && !se.cfg.vmConfig.NoReceipts && !se.isBlockProduction

				if txTask.BlockNumber() > 0 && startTxIndex == 0 {
					//Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
					if err := se.getPostValidator().Process(se.blockGasUsed, se.blobGasUsed, checkReceipts, blockReceipts, txTask.Header, se.isBlockProduction, txTask.Txs, se.cfg.chainConfig, se.logger); err != nil {
						return fmt.Errorf("%w, txnIdx=%d, %w", rules.ErrInvalidBlock, txTask.TxIndex, err) //same as in stage_exec.go
					}
				}

				stateWriter := state.NewWriter(se.doms.AsPutDel(se.applyTx), nil, txTask.TxNum)

				if err = ibs.MakeWriteSet(txTask.Rules(), stateWriter); err != nil {
					panic(err)
				}
			} else if txTask.TxIndex >= 0 {
				var prev, receipt *types.Receipt
				if txTask.TxIndex > 0 && txTask.TxIndex-startTxIndex > 0 {
					prev = blockReceipts[txTask.TxIndex-startTxIndex-1]
					receipt, err = result.CreateNextReceipt(prev)
					if err != nil {
						return err
					}
				} else if txTask.TxIndex > 0 {
					// reconstruct receipt from previous receipt values
					cumGasUsed, _, logIndexAfterTx, err := rawtemporaldb.ReceiptAsOf(se.applyTx, txTask.TxNum)
					if err != nil {
						return err
					}
					receipt, err = result.CreateReceipt(int(txTask.TxNum), cumGasUsed+result.ExecutionResult.ReceiptGasUsed, logIndexAfterTx)
					if err != nil {
						return err
					}
				} else {
					receipt, err = result.CreateNextReceipt(nil)
					if err != nil {
						return err
					}
				}

				blockReceipts = append(blockReceipts, receipt)
				if hooks := result.TracingHooks(); hooks != nil && hooks.OnTxEnd != nil {
					hooks.OnTxEnd(receipt, result.Err)
				}
			} else {
				se.onBlockStart(ctx, txTask.BlockNumber(), txTask.BlockHash())
			}

			if se.cfg.syncCfg.ChaosMonkey && se.enableChaosMonkey {
				chaosErr := chaos_monkey.ThrowRandomConsensusError(false, txTask.TxIndex, se.cfg.badBlockHalt, result.Err)
				if chaosErr != nil {
					log.Warn("Monkey in a consensus")
					return chaosErr
				}
			}
			return nil
		}(); err != nil {
			if errors.Is(err, context.Canceled) {
				return false, err
			}
			se.logger.Warn(fmt.Sprintf("[%s] Execution failed", se.logPrefix),
				"block", txTask.BlockNumber(), "txNum", txTask.TxNum, "header-hash", txTask.Header.Hash().String(), "err", err, "isForkValidation", se.isForkValidation, "isBlockProduction", se.isBlockProduction)
			if se.cfg.hd != nil && se.cfg.hd.POSSync() && errors.Is(err, rules.ErrInvalidBlock) {
				se.cfg.hd.ReportBadHeaderPoS(txTask.Header.Hash(), txTask.Header.ParentHash)
			}
			if se.cfg.badBlockHalt {
				return false, err
			}

			if se.u != nil {
				unwindReason := ExecUnwind
				if errors.Is(err, rules.ErrInvalidBlock) {
					unwindReason = BadBlock(txTask.Header.Hash(), err)
				}
				if err := se.u.UnwindTo(txTask.BlockNumber()-1, unwindReason, se.applyTx); err != nil {
					return false, err
				}
			}

			return false, err
		}

		var applyReceipt *types.Receipt
		if txTask.TxIndex >= 0 && txTask.TxIndex-startTxIndex < len(blockReceipts) {
			applyReceipt = blockReceipts[txTask.TxIndex-startTxIndex]
		}

		if txTask.IsBlockEnd() {
			if se.cfg.chainConfig.Bor != nil && txTask.TxIndex >= 1 {
				// get last receipt and store the last log index + 1
				if len(blockReceipts) >= txTask.TxIndex-startTxIndex {
					applyReceipt = blockReceipts[txTask.TxIndex-startTxIndex-1]
				}

				if applyReceipt == nil {
					if startTxIndex > 0 {
						// if we're in the startup block and the last tx has been skipped we'll
						// need to run it as a historic tx to recover its logs
						prevTask := *txTask
						prevTask.HistoryExecution = true
						prevTask.ResetTx(txTask.TxNum-1, txTask.TxIndex-1)
						result := se.worker.RunTxTaskNoLock(&prevTask)
						if result.Err != nil {
							return false, fmt.Errorf("error while finding last receipt: %w", result.Err)
						}
						var cumulativeGasUsed uint64
						var logIndexAfterTx uint32
						if txTask.TxIndex > 1 {
							cumulativeGasUsed, _, logIndexAfterTx, err = rawtemporaldb.ReceiptAsOf(se.applyTx, txTask.TxNum-2)
							if err != nil {
								return false, err
							}
						}
						applyReceipt, err = result.CreateReceipt(txTask.TxIndex-1,
							cumulativeGasUsed+result.ExecutionResult.ReceiptGasUsed, logIndexAfterTx)
						if err != nil {
							return false, err
						}
					} else {
						return false, fmt.Errorf("receipt is nil but should be populated, txIndex=%d, block=%d", txTask.TxIndex-1, txTask.BlockNumber())
					}
				}
			}
		}

		if err := se.rs.ApplyTxState(ctx, se.applyTx, txTask.BlockNumber(), txTask.TxNum, state.StateUpdates{},
			txTask.BalanceIncreaseSet, applyReceipt, se.blobGasUsed, result.Logs, result.TraceFroms, result.TraceTos,
			se.cfg.chainConfig, txTask.Rules(), txTask.HistoryExecution); err != nil {
			return false, err
		}

		se.doms.SetTxNum(txTask.TxNum)
		se.lastBlockResult = &blockResult{
			BlockNum:  txTask.BlockNumber(),
			lastTxNum: txTask.TxNum,
		}
		se.lastExecutedTxNum.Store(int64(txTask.TxNum))
		se.lastExecutedBlockNum.Store(int64(txTask.BlockNumber()))

		if task.IsBlockEnd() {
			se.executedGas.Add(int64(se.blockGasUsed))
			se.blockGasUsed = 0
			se.blobGasUsed = 0
			gasPool = nil
		}
	}

	// seboost: compute RAW+WAW dependencies and write txdeps for this block.
	// dep(i,j) is set iff j < i and tx j WROTE an address that tx i accesses
	// (read or write). This eliminates read-read false dependencies.
	//
	// Block-level system addresses (coinbase, zero address) are excluded from
	// dependency tracking: every tx pays fees to coinbase and accesses it via
	// EIP-3651, which would otherwise create a false linear chain.
	if generateTxDeps && se.seboostWriter != nil && len(tasks) > 0 && len(txAccessed) > 0 {
		blockNum := tasks[0].BlockNumber()

		// Build exclusion set: addresses touched by block-level mechanism,
		// not user-to-user data flow.
		blockSysAddrs := map[accounts.Address]struct{}{
			accounts.InternAddress(tasks[0].(*exec.TxTask).Header.Coinbase): {}, // fee recipient (EIP-3651 warm)
			accounts.ZeroAddress: {}, // zero address (system txs)
		}

		deps := make(map[int]map[int]bool)
		// writersByAddr[addr] = list of tx indices that WROTE addr (excl. system addrs)
		writersByAddr := make(map[accounts.Address][]int)
		for i := range txAccessed {
			deps[i] = map[int]bool{}
			// For each address tx i accessed, check if an earlier tx wrote it.
			for addr := range txAccessed[i] {
				if _, sys := blockSysAddrs[addr]; sys {
					continue // skip system addresses
				}
				for _, j := range writersByAddr[addr] {
					deps[i][j] = true
				}
			}
			// Register tx i as a writer, excluding system addresses.
			for addr := range txWritten[i] {
				if _, sys := blockSysAddrs[addr]; sys {
					continue
				}
				writersByAddr[addr] = append(writersByAddr[addr], i)
			}
		}
		txCount := len(tasks)
		if err := se.seboostWriter.WriteBlock(blockNum, deps, txCount); err != nil {
			se.logger.Warn("seboost: failed to write txdeps", "block", blockNum, "err", err)
		}
	}

	return true, nil
}
