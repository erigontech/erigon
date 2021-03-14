package stagedsync

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/miner"
	"github.com/ledgerwatch/turbo-geth/params"
)

// SpawnMiningCreateBlockStage
//TODO:
// - from uncles we nned only their hashes and numbers, but whole types.Block object received
// - I don't understand meaning of `noempty` variable
// - interrupt - variable is not implemented, see miner/worker.go:798
func SpawnMiningCreateBlockStage(s *StageState, tx ethdb.Database, engine consensus.Engine, chainConfig *params.ChainConfig, txPool *core.TxPool, extra hexutil.Bytes, gasFloor, gasCeil uint64, coinbase common.Address, localUncles, remoteUncles map[common.Hash]*types.Block, noempty bool, quit <-chan struct{}) (*types.Block, error) {
	if coinbase == (common.Address{}) {
		return nil, fmt.Errorf("refusing to mine without etherbase")
	}

	chain := ChainReader{chainConfig, tx}
	var GetBlocksFromHash = func(hash common.Hash, n int) (blocks []*types.Block) {
		number := rawdb.ReadHeaderNumber(tx, hash)
		if number == nil {
			return nil
		}
		for i := 0; i < n; i++ {
			block := chain.GetBlock(hash, *number)
			if block == nil {
				break
			}
			blocks = append(blocks, block)
			hash = block.ParentHash()
			*number--
		}
		return
	}

	type envT struct {
		signer    types.Signer
		ancestors mapset.Set    // ancestor set (used for checking uncle parent validity)
		family    mapset.Set    // family set (used for checking uncle invalidity)
		uncles    mapset.Set    // uncle set
		tcount    int           // tx count in cycle
		gasPool   *core.GasPool // available gas used to pack transactions
		header    *types.Header
	}
	env := &envT{
		signer:    types.NewEIP155Signer(chainConfig.ChainID),
		ancestors: mapset.NewSet(),
		family:    mapset.NewSet(),
		uncles:    mapset.NewSet(),
	}
	// analog of miner.Worker.updateSnapshot
	var makeUncles = func(proposedUncles mapset.Set) []*types.Header {
		var uncles []*types.Header
		proposedUncles.Each(func(item interface{}) bool {
			hash, ok := item.(common.Hash)
			if !ok {
				return false
			}

			uncle, exist := localUncles[hash]
			if !exist {
				uncle, exist = remoteUncles[hash]
			}
			if !exist {
				return false
			}
			uncles = append(uncles, uncle.Header())
			return false
		})
		return uncles
	}
	var commitTransactions = func(current *envT, txs *types.TransactionsByPriceAndNonce, coinbase common.Address, interrupt *int32) bool {
		header := current.header
		if current.gasPool == nil {
			current.gasPool = new(core.GasPool).AddGas(header.GasLimit)
		}

		var coalescedLogs []*types.Log

		for {
			// In the following three cases, we will interrupt the execution of the transaction.
			// (1) new head block event arrival, the interrupt signal is 1
			// (2) worker start or restart, the interrupt signal is 1
			// (3) worker recreate the mining block with any newly arrived transactions, the interrupt signal is 2.
			// For the first two cases, the semi-finished work will be discarded.
			// For the third case, the semi-finished work will be submitted to the consensus engine.
			//if interrupt != nil && atomic.LoadInt32(interrupt) != commitInterruptNone {
			//	// Notify resubmit loop to increase resubmitting interval due to too frequent commits.
			//	if atomic.LoadInt32(interrupt) == commitInterruptResubmit {
			//		ratio := float64(header.GasLimit-w.current.gasPool.Gas()) / float64(header.GasLimit)
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
			if current.gasPool.Gas() < params.TxGas {
				log.Trace("Not enough gas for further transactions", "have", current.gasPool, "want", params.TxGas)
				break
			}
			// Retrieve the next transaction and abort if all done
			tx := txs.Peek()
			if tx == nil {
				break
			}
			// Error may be ignored here. The error has already been checked
			// during transaction acceptance is the transaction pool.
			//
			// We use the eip155 signer regardless of the current hf.
			from, _ := types.Sender(current.signer, tx)
			// Check whether the tx is replay protected. If we're not in the EIP155 hf
			// phase, start ignoring the sender until we do.
			if tx.Protected() && !chainConfig.IsEIP155(header.Number) {
				log.Trace("Ignoring reply protected transaction", "hash", tx.Hash(), "eip155", chainConfig.EIP155Block)

				txs.Pop()
				continue
			}
			// Start executing the transaction
			current.state.Prepare(tx.Hash(), common.Hash{}, current.tcount)

			logs, err := w.commitTransaction(tx, coinbase)
			switch err {
			case core.ErrGasLimitReached:
				// Pop the current out-of-gas transaction without shifting in the next from the account
				log.Trace("Gas limit exceeded for current block", "sender", from)
				txs.Pop()

			case core.ErrNonceTooLow:
				// New head notification data race between the transaction pool and miner, shift
				log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
				txs.Shift()

			case core.ErrNonceTooHigh:
				// Reorg notification data race between the transaction pool and miner, skip account =
				log.Trace("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
				txs.Pop()

			case nil:
				// Everything ok, collect the logs and shift in the next transaction from the same account
				coalescedLogs = append(coalescedLogs, logs...)
				w.current.tcount++
				txs.Shift()

			default:
				// Strange error, discard the transaction and get the next in line (note, the
				// nonce-too-high clause will prevent us from executing in vain).
				log.Debug("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)
				txs.Shift()
			}
		}

		if !w.isRunning() && len(coalescedLogs) > 0 {
			// We don't push the pendingLogsEvent while we are mining. The reason is that
			// when we are mining, the worker will regenerate a mining block every 3 seconds.
			// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.

			// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
			// logs by filling in the block hash when the block was mined by the local miner. This can
			// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
			cpy := make([]*types.Log, len(coalescedLogs))
			for i, l := range coalescedLogs {
				cpy[i] = new(types.Log)
				*cpy[i] = *l
			}
			w.pendingLogsFeed.Send(cpy)
		}
		// Notify resubmit loop to decrease resubmitting interval if current interval is larger
		// than the user-specified one.
		if interrupt != nil {
			w.resubmitAdjustCh <- &intervalAdjust{inc: false}
		}
		return false
	}

	/*
		commitTransactions(txs *types.TransactionsByPriceAndNonce, coinbase common.Address, interrupt *int32) bool {

			for {
				// In the following three cases, we will interrupt the execution of the transaction.
				// (1) new head block event arrival, the interrupt signal is 1
				// (2) worker start or restart, the interrupt signal is 1
				// (3) worker recreate the mining block with any newly arrived transactions, the interrupt signal is 2.
				// For the first two cases, the semi-finished work will be discarded.
				// For the third case, the semi-finished work will be submitted to the consensus engine.
				if interrupt != nil && atomic.LoadInt32(interrupt) != commitInterruptNone {
					// Notify resubmit loop to increase resubmitting interval due to too frequent commits.
					if atomic.LoadInt32(interrupt) == commitInterruptResubmit {
						ratio := float64(header.GasLimit-w.current.gasPool.Gas()) / float64(header.GasLimit)
						if ratio < 0.1 {
							ratio = 0.1
						}
						w.resubmitAdjustCh <- &intervalAdjust{
							ratio: ratio,
							inc:   true,
						}
					}
					return atomic.LoadInt32(interrupt) == commitInterruptNewHead
				}
				// If we don't have enough gas for any further transactions then we're done
				if w.current.gasPool.Gas() < params.TxGas {
					log.Trace("Not enough gas for further transactions", "have", w.current.gasPool, "want", params.TxGas)
					break
				}
				// Retrieve the next transaction and abort if all done
				tx := txs.Peek()
				if tx == nil {
					break
				}
				// Error may be ignored here. The error has already been checked
				// during transaction acceptance is the transaction pool.
				//
				// We use the eip155 signer regardless of the current hf.
				from, _ := types.Sender(w.current.signer, tx)
				// Check whether the tx is replay protected. If we're not in the EIP155 hf
				// phase, start ignoring the sender until we do.
				if tx.Protected() && !w.chainConfig.IsEIP155(header.Number) {
					log.Trace("Ignoring reply protected transaction", "hash", tx.Hash(), "eip155", w.chainConfig.EIP155Block)

					txs.Pop()
					continue
				}
				// Start executing the transaction
				w.current.state.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)

				logs, err := w.commitTransaction(tx, coinbase)
				switch err {
				case core.ErrGasLimitReached:
					// Pop the current out-of-gas transaction without shifting in the next from the account
					log.Trace("Gas limit exceeded for current block", "sender", from)
					txs.Pop()

				case core.ErrNonceTooLow:
					// New head notification data race between the transaction pool and miner, shift
					log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
					txs.Shift()

				case core.ErrNonceTooHigh:
					// Reorg notification data race between the transaction pool and miner, skip account =
					log.Trace("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
					txs.Pop()

				case nil:
					// Everything ok, collect the logs and shift in the next transaction from the same account
					coalescedLogs = append(coalescedLogs, logs...)
					w.current.tcount++
					txs.Shift()

				default:
					// Strange error, discard the transaction and get the next in line (note, the
					// nonce-too-high clause will prevent us from executing in vain).
					log.Debug("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)
					txs.Shift()
				}
			}

			if !w.isRunning() && len(coalescedLogs) > 0 {
				// We don't push the pendingLogsEvent while we are mining. The reason is that
				// when we are mining, the worker will regenerate a mining block every 3 seconds.
				// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.

				// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
				// logs by filling in the block hash when the block was mined by the local miner. This can
				// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
				cpy := make([]*types.Log, len(coalescedLogs))
				for i, l := range coalescedLogs {
					cpy[i] = new(types.Log)
					*cpy[i] = *l
				}
				w.pendingLogsFeed.Send(cpy)
			}
			// Notify resubmit loop to decrease resubmitting interval if current interval is larger
			// than the user-specified one.
			if interrupt != nil {
				w.resubmitAdjustCh <- &intervalAdjust{inc: false}
			}
			return false
		}
	*/
	executionAt, err := s.ExecutionAt(tx)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return nil, fmt.Errorf("%s: logs index: getting last executed block: %w", logPrefix, err)
	}
	parent, err := readBlock(executionAt, tx)
	if err != nil {
		return nil, err
	}
	if parent == nil { // todo: how to return error and don't stop TG?
		return nil, fmt.Errorf(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", executionAt)
	}

	// re-written miner/worker.go:commitNewWork
	timestamp := time.Now().Unix()
	if parent.Time() >= uint64(timestamp) {
		timestamp = int64(parent.Time() + 1)
	}
	num := parent.Number()
	env.header = &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		GasLimit:   core.CalcGasLimit(parent, gasFloor, gasCeil),
		Extra:      extra,
		Time:       uint64(timestamp),
	}

	// Only set the coinbase if our consensus engine is running (avoid spurious block rewards)
	//if w.isRunning() {
	env.header.Coinbase = coinbase
	//}

	if err := engine.Prepare(chain, env.header); err != nil {
		log.Error("Failed to prepare header for mining",
			"err", err,
			"headerNumber", env.header.Number.Uint64(),
			"headerRoot", env.header.Root.String(),
			"headerParentHash", env.header.ParentHash.String(),
			"parentNumber", parent.Number().Uint64(),
			"parentHash", parent.Hash().String(),
			"callers", debug.Callers(10))
		return nil, fmt.Errorf("mining failed")
	}

	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := chainConfig.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if env.header.Number.Cmp(daoBlock) >= 0 && env.header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if chainConfig.DAOForkSupport {
				env.header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			} else if bytes.Equal(env.header.Extra, params.DAOForkBlockExtra) {
				env.header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
	}

	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			env.family.Add(uncle.Hash())
		}
		env.family.Add(ancestor.Hash())
		env.ancestors.Add(ancestor.Hash())
	}
	commitUncle := func(env *envT, uncle *types.Header) error {
		hash := uncle.Hash()
		if env.uncles.Contains(hash) {
			return errors.New("uncle not unique")
		}
		if parent.Hash() == uncle.ParentHash {
			return errors.New("uncle is sibling")
		}
		if !env.ancestors.Contains(uncle.ParentHash) {
			return errors.New("uncle's parent unknown")
		}
		if env.family.Contains(hash) {
			return errors.New("uncle already included")
		}
		env.uncles.Add(uncle.Hash())
		return nil
	}
	// Could potentially happen if starting to mine in an odd state.
	//err := w.makeCurrent(ctx, parent, header)
	//if err != nil {
	//	log.Error("Failed to create mining context", "err", err)
	//	ctx.CancelFunc()
	//	return
	//}

	//// Create the current work task and check any fork transitions needed
	//env := w.current
	//if w.chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(header.Number) == 0 {
	//	misc.ApplyDAOHardFork(env.state)
	//}

	// Accumulate the miningUncles for the current block
	// Prefer to locally generated uncle
	uncles := make([]*types.Header, 0, 2)
	for _, blocks := range []map[common.Hash]*types.Block{localUncles, remoteUncles} {
		// Clean up stale uncle blocks first
		for hash, uncle := range blocks {
			if uncle.NumberU64()+miner.StaleThreshold <= env.header.Number.Uint64() {
				delete(blocks, hash)
			}
		}
		for hash, uncle := range blocks {
			if len(uncles) == 2 {
				break
			}
			if err = commitUncle(env, uncle.Header()); err != nil {
				log.Trace("Possible uncle rejected", "hash", hash, "reason", err)
			} else {
				log.Debug("Committing new uncle to block", "hash", hash)
				uncles = append(uncles, uncle.Header())
			}
		}
	}

	//TODO: what is it??
	//
	// Create an empty block based on temporary copied state for
	// sealing in advance without waiting block execution finished.
	//if !noempty && atomic.LoadUint32(&w.noempty) == 0 {
	//	now := time.Now()
	//	if err = w.commit(ctx, uncles, nil, false, tstart); err != nil {
	//		log.Error("Failed to commit empty block", "err", err)
	//		ctx.CancelFunc()
	//	}
	//	log.Info("Commit an empty block", "number", header.Number, "duration", time.Since(now))
	//}

	// Fill the block with all available pending transactions.
	pending, err := txPool.Pending()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pending transactions: %w", err)
	}

	// Short circuit if there is no available pending transactions.
	// But if we disable empty precommit already, ignore it. Since
	// empty block is necessary to keep the liveness of the network.
	if len(pending) == 0 && !noempty {
		return types.NewBlock(env.header, nil, makeUncles(env.uncles), nil), nil
	}

	// Split the pending transactions into locals and remotes
	localTxs, remoteTxs := make(map[common.Address]types.Transactions), pending
	for _, account := range txPool.Locals() {
		if txs := remoteTxs[account]; len(txs) > 0 {
			delete(remoteTxs, account)
			localTxs[account] = txs
		}
	}

	if len(localTxs) > 0 {
		txs := types.NewTransactionsByPriceAndNonce(env.signer, localTxs)
		if commitTransactions(txs, coinbase, 0) {
			return
		}
	}
	if len(remoteTxs) > 0 {
		txs := types.NewTransactionsByPriceAndNonce(env.signer, remoteTxs)
		if commitTransactions(txs, coinbase, 0) {
			return
		}
	}

	/*



	   	now := time.Now()
	   	if err = w.commit(ctx, uncles, w.fullTaskHook, true, tstart); err != nil {
	   		log.Error("Failed to commit block", "err", err)
	   		ctx.CancelFunc()
	   	}
	   	log.Info("Commit a block with transactions", "number", header.Number, "duration", time.Since(now))
	   }

	   // commit runs any post-transaction state modifications, assembles the final block
	   // and commits new work if consensus engine is running.
	   func (w *worker) commit(ctx consensus.Cancel, uncles []*types.Header, interval func(), update bool, start time.Time) error {
	   	// Deep copy receipts here to avoid interaction between different tasks.
	   	receipts := copyReceipts(w.current.receipts)

	   	s := &(*w.current.state)

	   	block, err := NewBlock(w.engine, s, w.current.tds, w.chain.Config(), w.current.GetHeader(), w.current.txs, uncles, w.current.receipts)
	   	if err != nil {
	   		return err
	   	}

	   	w.current.SetHeader(block.Header())

	   	if w.isRunning() {
	   		if interval != nil {
	   			interval()
	   		}

	   		select {
	   		case w.taskCh <- &task{receipts: receipts, state: s, tds: w.current.tds, block: block, createdAt: time.Now(), ctx: ctx}:
	   			log.Warn("mining: worker task event",
	   				"number", block.NumberU64(),
	   				"hash", block.Hash().String(),
	   				"parentHash", block.ParentHash().String(),
	   			)

	   			log.Info("Commit new mining work", "number", block.Number(), "sealhash", w.engine.SealHash(block.Header()),
	   				"uncles", len(uncles), "txs", w.current.tcount,
	   				"gas", block.GasUsed(), "fees", totalFees(block, receipts),
	   				"elapsed", common.PrettyDuration(time.Since(start)))

	   		case <-w.exitCh:
	   			log.Info("Worker has exited")
	   		}
	   	}
	*/

	s.Done()

	return miningBlock, nil
}
