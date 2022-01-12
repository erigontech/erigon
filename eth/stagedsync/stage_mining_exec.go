package stagedsync

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type MiningExecCfg struct {
	db          kv.RwDB
	miningState MiningState
	notifier    ChainEventNotifier
	chainConfig params.ChainConfig
	engine      consensus.Engine
	blockReader interfaces.FullBlockReader
	vmConfig    *vm.Config
	tmpdir      string
}

func StageMiningExecCfg(
	db kv.RwDB,
	miningState MiningState,
	notifier ChainEventNotifier,
	chainConfig params.ChainConfig,
	engine consensus.Engine,
	vmConfig *vm.Config,
	tmpdir string,
) MiningExecCfg {
	return MiningExecCfg{
		db:          db,
		miningState: miningState,
		notifier:    notifier,
		chainConfig: chainConfig,
		engine:      engine,
		blockReader: snapshotsync.NewBlockReader(),
		vmConfig:    vmConfig,
		tmpdir:      tmpdir,
	}
}

// SpawnMiningExecStage
//TODO:
// - resubmitAdjustCh - variable is not implemented
func SpawnMiningExecStage(s *StageState, tx kv.RwTx, cfg MiningExecCfg, quit <-chan struct{}) error {
	cfg.vmConfig.NoReceipts = false
	logPrefix := s.LogPrefix()
	current := cfg.miningState.MiningBlock
	localTxs := current.LocalTxs
	remoteTxs := current.RemoteTxs
	noempty := true

	stateReader := state.NewPlainStateReader(tx)
	ibs := state.New(stateReader)
	stateWriter := state.NewPlainStateWriter(tx, tx, current.Header.Number.Uint64())
	if cfg.chainConfig.DAOForkSupport && cfg.chainConfig.DAOForkBlock != nil && cfg.chainConfig.DAOForkBlock.Cmp(current.Header.Number) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	systemcontracts.UpgradeBuildInSystemContract(&cfg.chainConfig, current.Header.Number, ibs)

	// Create an empty block based on temporary copied state for
	// sealing in advance without waiting block execution finished.
	if !noempty {
		log.Info("Commit an empty block", "number", current.Header.Number)
		return nil
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }
	contractHasTEVM := ethdb.GetHasTEVM(tx)

	// Short circuit if there is no available pending transactions.
	// But if we disable empty precommit already, ignore it. Since
	// empty block is necessary to keep the liveness of the network.
	if noempty {
		if !localTxs.Empty() {
			logs, err := addTransactionsToMiningBlock(logPrefix, current, cfg.chainConfig, cfg.vmConfig, getHeader, contractHasTEVM, cfg.engine, localTxs, cfg.miningState.MiningConfig.Etherbase, ibs, quit)
			if err != nil {
				return err
			}
			// We don't push the pendingLogsEvent while we are mining. The reason is that
			// when we are mining, the worker will regenerate a mining block every 3 seconds.
			// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.
			//if !w.isRunning() {
			NotifyPendingLogs(logPrefix, cfg.notifier, logs)
			//}
		}
		if !remoteTxs.Empty() {
			logs, err := addTransactionsToMiningBlock(logPrefix, current, cfg.chainConfig, cfg.vmConfig, getHeader, contractHasTEVM, cfg.engine, remoteTxs, cfg.miningState.MiningConfig.Etherbase, ibs, quit)
			if err != nil {
				return err
			}
			// We don't push the pendingLogsEvent while we are mining. The reason is that
			// when we are mining, the worker will regenerate a mining block every 3 seconds.
			// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.
			//if !w.isRunning() {
			NotifyPendingLogs(logPrefix, cfg.notifier, logs)
			//}
		}
	}

	if current.Uncles == nil {
		current.Uncles = []*types.Header{}
	}
	if current.Txs == nil {
		current.Txs = []types.Transaction{}
	}
	if current.Receipts == nil {
		current.Receipts = types.Receipts{}
	}

	_, err := core.FinalizeBlockExecution(cfg.engine, stateReader, current.Header, current.Txs, current.Uncles, stateWriter, &cfg.chainConfig, ibs, current.Receipts, epochReader{tx: tx}, chainReader{config: &cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, true)
	if err != nil {
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
	return nil
}

func addTransactionsToMiningBlock(logPrefix string, current *MiningBlock, chainConfig params.ChainConfig, vmConfig *vm.Config, getHeader func(hash common.Hash, number uint64) *types.Header, contractHasTEVM func(common.Hash) (bool, error), engine consensus.Engine, txs types.TransactionsStream, coinbase common.Address, ibs *state.IntraBlockState, quit <-chan struct{}) (types.Logs, error) {
	header := current.Header
	tcount := 0
	gasPool := new(core.GasPool).AddGas(current.Header.GasLimit)
	signer := types.MakeSigner(&chainConfig, header.Number.Uint64())

	var coalescedLogs types.Logs
	noop := state.NewNoopWriter()

	var miningCommitTx = func(txn types.Transaction, coinbase common.Address, vmConfig *vm.Config, chainConfig params.ChainConfig, ibs *state.IntraBlockState, current *MiningBlock) ([]*types.Log, error) {
		snap := ibs.Snapshot()
		receipt, _, err := core.ApplyTransaction(&chainConfig, getHeader, engine, &coinbase, gasPool, ibs, noop, header, txn, &header.GasUsed, *vmConfig, contractHasTEVM)
		if err != nil {
			ibs.RevertToSnapshot(snap)
			return nil, err
		}

		//if !chainConfig.IsByzantium(header.Number) {
		//	batch.Rollback()
		//}
		//fmt.Printf("Tx Hash: %x\n", txn.Hash())

		current.Txs = append(current.Txs, txn)
		current.Receipts = append(current.Receipts, receipt)
		return receipt.Logs, nil
	}

	for {
		if err := libcommon.Stopped(quit); err != nil {
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
			log.Debug(fmt.Sprintf("[%s] Not enough gas for further transactions", logPrefix), "have", gasPool, "want", params.TxGas)
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
		from, _ := txn.Sender(*signer)
		// Check whether the txn is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if txn.Protected() && !chainConfig.IsEIP155(header.Number.Uint64()) {
			log.Debug(fmt.Sprintf("[%s] Ignoring replay protected transaction", logPrefix), "hash", txn.Hash(), "eip155", chainConfig.EIP155Block)

			txs.Pop()
			continue
		}

		// Start executing the transaction
		ibs.Prepare(txn.Hash(), common.Hash{}, tcount)
		logs, err := miningCommitTx(txn, coinbase, vmConfig, chainConfig, ibs, current)

		switch err {
		case core.ErrGasLimitReached:
			// Pop the env out-of-gas transaction without shifting in the next from the account
			log.Debug(fmt.Sprintf("[%s] Gas limit exceeded for env block", logPrefix), "sender", from)
			txs.Pop()

		case core.ErrNonceTooLow:
			// New head notification data race between the transaction pool and miner, shift
			log.Debug(fmt.Sprintf("[%s] Skipping transaction with low nonce", logPrefix), "sender", from, "nonce", txn.GetNonce())
			txs.Shift()

		case core.ErrNonceTooHigh:
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Debug(fmt.Sprintf("[%s] Skipping account with hight nonce", logPrefix), "sender", from, "nonce", txn.GetNonce())
			txs.Pop()

		case nil:
			// Everything ok, collect the logs and shift in the next transaction from the same account
			coalescedLogs = append(coalescedLogs, logs...)
			tcount++
			txs.Shift()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Debug(fmt.Sprintf("[%s] Transaction failed, account skipped", logPrefix), "hash", txn.Hash(), "err", err)
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

	if notifier == nil {
		log.Warn(fmt.Sprintf("[%s] rpc notifier is not set, rpc daemon won't be updated about pending logs", logPrefix))
		return
	}
	notifier.OnNewPendingLogs(logs)
}
