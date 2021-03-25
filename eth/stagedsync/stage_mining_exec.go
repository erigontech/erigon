package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

// SpawnMiningExecStage
//TODO:
// - resubmitAdjustCh - variable is not implemented
func SpawnMiningExecStage(s *StageState, tx ethdb.Database, current *miningBlock, chainConfig *params.ChainConfig, vmConfig *vm.Config, cc *core.TinyChainContext, localTxs, remoteTxs *types.TransactionsByPriceAndNonce, coinbase common.Address, noempty bool, notifier ChainEventNotifier, quit <-chan struct{}) error {
	vmConfig.NoReceipts = false
	logPrefix := s.state.LogPrefix()

	engine := cc.Engine()
	ibs := state.New(state.NewPlainStateReader(tx))
	stateWriter := state.NewPlainStateWriter(tx, tx, current.Header.Number.Uint64())
	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(current.Header.Number) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}

	// Create an empty block based on temporary copied state for
	// sealing in advance without waiting block execution finished.
	if !noempty {
		log.Info("Commit an empty block", "number", current.Header.Number)
		s.Done()
		return nil
	}

	// Short circuit if there is no available pending transactions.
	// But if we disable empty precommit already, ignore it. Since
	// empty block is necessary to keep the liveness of the network.
	if noempty {
		if !localTxs.Empty() {
			logs, err := addTransactionsToMiningBlock(current, chainConfig, vmConfig, cc, localTxs, coinbase, ibs, stateWriter, quit)
			if err != nil {
				return err
			}
			// We don't push the pendingLogsEvent while we are mining. The reason is that
			// when we are mining, the worker will regenerate a mining block every 3 seconds.
			// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.
			//if !w.isRunning() {
			NotifyPendingLogs(logPrefix, notifier, logs)
			//}
		}
		if !remoteTxs.Empty() {
			logs, err := addTransactionsToMiningBlock(current, chainConfig, vmConfig, cc, remoteTxs, coinbase, ibs, stateWriter, quit)
			if err != nil {
				return err
			}
			// We don't push the pendingLogsEvent while we are mining. The reason is that
			// when we are mining, the worker will regenerate a mining block every 3 seconds.
			// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.
			//if !w.isRunning() {
			NotifyPendingLogs(logPrefix, notifier, logs)
			//}
		}
	}

	if err := core.FinalizeBlockExecution(engine, current.Header, current.txs, current.Uncles, stateWriter, chainConfig, ibs); err != nil {
		return err
	}

	/*
		if w.isRunning() {
			if interval != nil {
				interval()
			}

			select {
			case w.taskCh <- &task{receipts: receipts, state: s, tds: w.env.tds, block: block, createdAt: time.Now(), ctx: ctx}:
				log.Warn("mining: worker task event",
					"number", block.NumberU64(),
					"hash", block.Hash().String(),
					"parentHash", block.ParentHash().String(),
				)

				log.Info("Commit new mining work", "number", block.Number(), "sealhash", w.engine.SealHash(block.Header()),
					"uncles", len(uncles), "txs", w.env.tcount,
					"gas", block.GasUsed(), "fees", totalFees(block, receipts),
					"elapsed", common.PrettyDuration(time.Since(start)))

			case <-w.exitCh:
				log.Info("Worker has exited")
			}
		}
		if update {
			w.updateSnapshot()
		}
	*/

	// hack: pretend that we are real execution stage - next stages will rely on this progress
	if err := stages.SaveStageProgress(tx, stages.Execution, current.Header.Number.Uint64()); err != nil {
		return err
	}
	s.Done()
	return nil
}

func addTransactionsToMiningBlock(current *miningBlock, chainConfig *params.ChainConfig, vmConfig *vm.Config, cc *core.TinyChainContext, txs *types.TransactionsByPriceAndNonce, coinbase common.Address, ibs *state.IntraBlockState, stateWriter state.StateWriter, quit <-chan struct{}) (types.Logs, error) {
	header := current.Header
	tcount := 0
	gasPool := new(core.GasPool).AddGas(current.Header.GasLimit)
	signer := types.NewEIP155Signer(chainConfig.ChainID)

	var coalescedLogs types.Logs
	noop := state.NewNoopWriter()

	var miningCommitTx = func(txn *types.Transaction, coinbase common.Address, vmConfig *vm.Config, chainConfig *params.ChainConfig, cc *core.TinyChainContext, ibs *state.IntraBlockState, current *miningBlock) ([]*types.Log, error) {
		snap := ibs.Snapshot()
		receipt, err := core.ApplyTransaction(chainConfig, cc, &coinbase, gasPool, ibs, noop, header, txn, &header.GasUsed, *vmConfig)
		if err != nil {
			ibs.RevertToSnapshot(snap)
			return nil, err
		}
		//if !chainConfig.IsByzantium(header.Number) {
		//	batch.Rollback()
		//}
		//fmt.Printf("Tx Hash: %x\n", txn.Hash())

		current.txs = append(current.txs, txn)
		current.receipts = append(current.receipts, receipt)
		return receipt.Logs, nil
	}

	for {
		if err := common.Stopped(quit); err != nil {
			return nil, err
		}

		// In the following three cases, we will interrupt the execution of the transaction.
		// (1) new head block event arrival, the interrupt signal is 1
		// (2) worker start or restart, the interrupt signal is 1
		// (3) worker recreate the mining block with any newly arrived transactions, the interrupt signal is 2.
		// For the first two cases, the semi-finished work will be discarded.
		// For the third case, the semi-finished work will be submitted to the consensus engine.
		//if interrupt != nil && atomic.LoadInt32(interrupt) != commitInterruptNone {
		//	// Notify resubmit loop to increase resubmitting interval due to too frequent commits.
		//	if atomic.LoadInt32(interrupt) == commitInterruptResubmit {
		//		ratio := float64(header.GasLimit-w.env.gasPool.Gas()) / float64(header.GasLimit)
		//		if ratio < 0.1 {
		//			ratio = 0.1
		//		}
		//		w.resubmitAdjustCh <- &intervalAdjust{
		//			ratio: ratio,
		//			inc:   true,
		//		}
		//	}
		//	return atomic.LoadInt32(interrupt) == commitInterruptNewHead
		//}
		// If we don't have enough gas for any further transactions then we're done
		if gasPool.Gas() < params.TxGas {
			log.Trace("Not enough gas for further transactions", "have", gasPool, "want", params.TxGas)
			break
		}
		// Retrieve the next transaction and abort if all done
		txn := txs.Peek()
		if txn == nil {
			break
		}
		// Error may be ignored here. The error has already been checked
		// during transaction acceptance is the transaction pool.
		//
		// We use the eip155 signer regardless of the env hf.
		from, _ := types.Sender(signer, txn)
		// Check whether the txn is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if txn.Protected() && !chainConfig.IsEIP155(header.Number) {
			log.Trace("Ignoring reply protected transaction", "hash", txn.Hash(), "eip155", chainConfig.EIP155Block)

			txs.Pop()
			continue
		}

		// Start executing the transaction
		ibs.Prepare(txn.Hash(), common.Hash{}, tcount)
		logs, err := miningCommitTx(txn, coinbase, vmConfig, chainConfig, cc, ibs, current)

		switch err {
		case core.ErrGasLimitReached:
			// Pop the env out-of-gas transaction without shifting in the next from the account
			log.Trace("Gas limit exceeded for env block", "sender", from)
			txs.Pop()

		case core.ErrNonceTooLow:
			// New head notification data race between the transaction pool and miner, shift
			log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", txn.Nonce())
			txs.Shift()

		case core.ErrNonceTooHigh:
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Trace("Skipping account with hight nonce", "sender", from, "nonce", txn.Nonce())
			txs.Pop()

		case nil:
			// Everything ok, collect the logs and shift in the next transaction from the same account
			coalescedLogs = append(coalescedLogs, logs...)
			tcount++
			txs.Shift()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Debug("Transaction failed, account skipped", "hash", txn.Hash(), "err", err)
			txs.Shift()
		}
	}

	/*
		// Notify resubmit loop to decrease resubmitting interval if env interval is larger
		// than the user-specified one.
		if interrupt != nil {
			w.resubmitAdjustCh <- &intervalAdjust{inc: false}
		}
	*/
	return coalescedLogs, nil

}

func NotifyPendingLogs(logPrefix string, notifier ChainEventNotifier, logs types.Logs) {
	if len(logs) == 0 {
		return
	}

	// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
	// logs by filling in the block hash when the block was mined by the local miner. This can
	// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
	cpy := make(types.Logs, len(logs))
	for i, l := range logs {
		cpy[i] = new(types.Log)
		*cpy[i] = *l
	}

	if notifier == nil {
		log.Warn(fmt.Sprintf("[%s] rpc notifier is not set, rpc daemon won't be updated about pending logs", logPrefix))
		return
	}
	notifier.OnNewPendingLogs(logs)
}
