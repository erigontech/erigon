package stagedsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
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
// - resubmitAdjustCh - variable is not implemented
func SpawnMiningCreateBlockStage(s *StageState, tx ethdb.Database, chainConfig *params.ChainConfig, vmConfig *vm.Config, cc *core.TinyChainContext, txPool *core.TxPool, extra hexutil.Bytes, gasFloor, gasCeil uint64, coinbase common.Address, localUncles, remoteUncles map[common.Hash]*types.Block, noempty bool, quit <-chan struct{}) (*types.Block, error) {
	if coinbase == (common.Address{}) {
		return nil, fmt.Errorf("refusing to mine without etherbase")
	}

	logPrefix := s.state.LogPrefix()
	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return nil, fmt.Errorf("%s: getting last executed block: %w", logPrefix, err)
	}
	parent := rawdb.ReadHeaderByNumber(tx, executionAt)
	if parent == nil { // todo: how to return error and don't stop TG?
		return nil, fmt.Errorf(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", executionAt)
	}

	engine := cc.Engine()
	blockNum := executionAt + 1
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

	batch := tx.NewBatch()
	defer batch.Rollback()

	type envT struct {
		signer    types.Signer
		ancestors mapset.Set    // ancestor set (used for checking uncle parent validity)
		family    mapset.Set    // family set (used for checking uncle invalidity)
		uncles    mapset.Set    // uncle set
		tcount    int           // tx count in cycle
		gasPool   *core.GasPool // available gas used to pack transactions

		header   *types.Header
		txs      []*types.Transaction
		receipts []*types.Receipt

		ibs         *state.IntraBlockState // apply state changes here
		stateWriter state.StateWriter
	}
	current := &envT{
		signer:      types.NewEIP155Signer(chainConfig.ChainID),
		ancestors:   mapset.NewSet(),
		family:      mapset.NewSet(),
		uncles:      mapset.NewSet(),
		ibs:         state.New(state.NewPlainStateReader(batch)),
		stateWriter: state.NewPlainStateWriter(batch, batch, blockNum),
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
	var commitTransaction = func(txn *types.Transaction, coinbase common.Address, ibs *state.IntraBlockState, stateWriter state.StateWriter) ([]*types.Log, error) {
		vmConfig.NoReceipts = false
		if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(current.header.Number) == 0 {
			misc.ApplyDAOHardFork(ibs)
		}

		header := current.header
		receipt, err := core.ApplyTransaction(chainConfig, cc, &coinbase, current.gasPool, ibs, stateWriter, header, txn, &header.GasUsed, *vmConfig)
		// batch Rollback/CommitAndBegin methods - keeps batch object valid,
		// means don't need re-create state reader or re-inject batch or create new batch
		if err != nil {
			batch.Rollback()
			return nil, err
		}
		if !chainConfig.IsByzantium(current.header.Number) {
			batch.Rollback()
		} else {
			if err = batch.CommitAndBegin(context.Background()); err != nil {
				return nil, err
			}
		}

		current.txs = append(current.txs, txn)
		current.receipts = append(current.receipts, receipt)
		return receipt.Logs, nil
	}

	var commitTransactions = func(current *envT, txs *types.TransactionsByPriceAndNonce, coinbase common.Address /*, interrupt *int32*/) bool {
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
			txn := txs.Peek()
			if txn == nil {
				break
			}
			// Error may be ignored here. The error has already been checked
			// during transaction acceptance is the transaction pool.
			//
			// We use the eip155 signer regardless of the current hf.
			from, _ := types.Sender(current.signer, txn)
			// Check whether the txn is replay protected. If we're not in the EIP155 hf
			// phase, start ignoring the sender until we do.
			if txn.Protected() && !chainConfig.IsEIP155(header.Number) {
				log.Trace("Ignoring reply protected transaction", "hash", txn.Hash(), "eip155", chainConfig.EIP155Block)

				txs.Pop()
				continue
			}

			// Start executing the transaction
			current.ibs.Prepare(txn.Hash(), common.Hash{}, current.tcount)
			logs, err := commitTransaction(txn, coinbase, current.ibs, current.stateWriter)

			switch err {
			case core.ErrGasLimitReached:
				// Pop the current out-of-gas transaction without shifting in the next from the account
				log.Trace("Gas limit exceeded for current block", "sender", from)
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
				current.tcount++
				txs.Shift()

			default:
				// Strange error, discard the transaction and get the next in line (note, the
				// nonce-too-high clause will prevent us from executing in vain).
				log.Debug("Transaction failed, account skipped", "hash", txn.Hash(), "err", err)
				txs.Shift()
			}
		}

		/*
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
		*/

		/*
			// Notify resubmit loop to decrease resubmitting interval if current interval is larger
			// than the user-specified one.
			if interrupt != nil {
				w.resubmitAdjustCh <- &intervalAdjust{inc: false}
			}
		*/
		return false
	}

	// re-written miner/worker.go:commitNewWork
	timestamp := time.Now().Unix()
	if parent.Time >= uint64(timestamp) {
		timestamp = int64(parent.Time + 1)
	}
	num := parent.Number
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		GasLimit:   core.CalcGasLimit(parent.GasUsed, parent.GasLimit, gasFloor, gasCeil),
		Extra:      extra,
		Time:       uint64(timestamp),
	}

	// Only set the coinbase if our consensus engine is running (avoid spurious block rewards)
	//if w.isRunning() {
	current.header.Coinbase = coinbase
	//}

	if err := engine.Prepare(chain, current.header); err != nil {
		log.Error("Failed to prepare header for mining",
			"err", err,
			"headerNumber", current.header.Number.Uint64(),
			"headerRoot", current.header.Root.String(),
			"headerParentHash", current.header.ParentHash.String(),
			"parentNumber", parent.Number.Uint64(),
			"parentHash", parent.Hash().String(),
			"callers", debug.Callers(10))
		return nil, fmt.Errorf("mining failed")
	}

	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := chainConfig.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if current.header.Number.Cmp(daoBlock) >= 0 && current.header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if chainConfig.DAOForkSupport {
				current.header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			} else if bytes.Equal(current.header.Extra, params.DAOForkBlockExtra) {
				current.header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
	}

	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			current.family.Add(uncle.Hash())
		}
		current.family.Add(ancestor.Hash())
		current.ancestors.Add(ancestor.Hash())
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
	//current := w.current
	//if w.chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(header.Number) == 0 {
	//	misc.ApplyDAOHardFork(current.state)
	//}

	// Accumulate the miningUncles for the current block
	// Prefer to locally generated uncle
	uncles := make([]*types.Header, 0, 2)
	for _, blocks := range []map[common.Hash]*types.Block{localUncles, remoteUncles} {
		// Clean up stale uncle blocks first
		for hash, uncle := range blocks {
			if uncle.NumberU64()+miner.StaleThreshold <= current.header.Number.Uint64() {
				delete(blocks, hash)
			}
		}
		for hash, uncle := range blocks {
			if len(uncles) == 2 {
				break
			}
			if err = commitUncle(current, uncle.Header()); err != nil {
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
		return types.NewBlock(current.header, nil, makeUncles(current.uncles), nil), nil
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
		txs := types.NewTransactionsByPriceAndNonce(current.signer, localTxs)
		if commitTransactions(current, txs, coinbase) {
			return nil, common.ErrStopped
		}
	}
	if len(remoteTxs) > 0 {
		txs := types.NewTransactionsByPriceAndNonce(current.signer, remoteTxs)
		if commitTransactions(current, txs, coinbase) {
			return nil, common.ErrStopped
		}
	}

	var NewBlock = func(engine consensus.Engine, s *state.IntraBlockState, stWriter state.StateWriter, chainConfig *params.ChainConfig, header *types.Header, txs []*types.Transaction, uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
		block, err := engine.FinalizeAndAssemble(chainConfig, header, s, txs, uncles, receipts)
		if err != nil {
			return nil, err
		}

		ctx := chainConfig.WithEIPsFlags(context.Background(), header.Number)
		if err = s.FinalizeTx(ctx, stWriter); err != nil {
			return nil, err
		}
		return block, nil
	}
	// Deep copy receipts here to avoid interaction between different tasks.
	block, err := NewBlock(engine, current.ibs, current.stateWriter, chainConfig, header, current.txs, uncles, copyReceipts(current.receipts))
	if err != nil {
		return nil, err
	}

	/*
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
		if update {
			w.updateSnapshot()
		}
	*/

	s.Done()
	return block, nil
}
