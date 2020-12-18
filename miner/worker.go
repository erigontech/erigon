// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package miner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/consensus/process"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

const (
	// resultQueueSize is the size of channel listening to sealing result.
	resultQueueSize = 10

	// txChanSize is the size of channel listening to NewTxsEvent.
	// The number is referenced from the size of tx pool.
	txChanSize = 4096

	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10

	// chainSideChanSize is the size of channel listening to ChainSideEvent.
	chainSideChanSize = 10

	// resubmitAdjustChanSize is the size of resubmitting interval adjustment channel.
	resubmitAdjustChanSize = 10

	// miningLogAtDepth is the number of confirmations before logging successful mining.
	miningLogAtDepth = 7

	// minRecommitInterval is the minimal time interval to recreate the mining block with
	// any newly arrived transactions.
	minRecommitInterval = 1 * time.Second

	// maxRecommitInterval is the maximum time interval to recreate the mining block with
	// any newly arrived transactions.
	maxRecommitInterval = 15 * time.Second

	// intervalAdjustRatio is the impact a single interval adjustment has on sealing work
	// resubmitting interval.
	intervalAdjustRatio = 0.1

	// intervalAdjustBias is applied during the new resubmit interval calculation in favor of
	// increasing upper limit or decreasing lower limit so that the limit can be reachable.
	intervalAdjustBias = 200 * 1000.0 * 1000.0

	// staleThreshold is the maximum depth of the acceptable stale block.
	staleThreshold = 7
)

// task contains all information for consensus engine sealing and result submitting.
type task struct {
	receipts  []*types.Receipt
	state     *state.IntraBlockState
	tds       *state.TrieDbState
	block     *types.Block
	createdAt time.Time
	ctx       consensus.Cancel
}

const (
	commitInterruptNone int32 = iota
	commitInterruptNewHead
	commitInterruptResubmit
)

// newWorkReq represents a request for new sealing work submitting with relative interrupt notifier.
type newWorkReq struct {
	interrupt *int32
	noempty   bool
	timestamp int64
	cancel    consensus.Cancel
}

// intervalAdjust represents a resubmitting interval adjustment.
type intervalAdjust struct {
	ratio float64
	inc   bool
}

// worker is the main object which takes care of submitting new work to consensus engine
// and gathering the sealing result.
type worker struct {
	config      *Config
	chainConfig *params.ChainConfig
	engine      *process.RemoteEngine
	eth         Backend
	chain       *core.BlockChain

	// Feeds
	pendingLogsFeed event.Feed

	// Subscriptions
	mux          *event.TypeMux
	txsCh        chan core.NewTxsEvent
	txsSub       event.Subscription
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription
	chainSideCh  chan core.ChainSideEvent
	chainSideSub event.Subscription

	// Channels
	newWorkCh          chan *newWorkReq
	taskCh             chan *task
	startCh            chan struct{}
	exitCh             chan struct{}
	resubmitIntervalCh chan time.Duration
	resubmitAdjustCh   chan *intervalAdjust

	current *environment // An environment for current running cycle.
	uncles  *miningUncles

	mu       sync.RWMutex // The lock used to protect the coinbase and extra fields
	coinbase common.Address
	extra    []byte

	snapshotMu    sync.RWMutex // The lock used to protect the block snapshot and state snapshot
	snapshotBlock *types.Block
	snapshotState *state.IntraBlockState
	snapshotTds   *state.TrieDbState

	// atomic status counters
	running int32 // The indicator whether the consensus engine is running or not.
	newTxs  int32 // New arrival transaction count since last sealing work submitting.

	// noempty is the flag used to control whether the feature of pre-seal empty
	// block is enabled. The default value is false(pre-seal is enabled by default).
	// But in some special scenario the consensus engine will seal blocks instantaneously,
	// in this case this feature will add all empty blocks into canonical chain
	// non-stop and no real transaction will be included.
	noempty uint32
	hooks

	initOnce          sync.Once
	canonicalMining   []consensus.Cancel
	canonicalMiningMu sync.Mutex
	n                 int
}

type hooks struct {
	// External functions
	isLocalBlock func(block *types.Block) bool // Function used to determine whether the specified block is mined by local miner.

	// Test hooks
	newTaskHook  func(*task)                        // Method to call upon receiving a new sealing task.
	skipSealHook func(*task) bool                   // Method to decide whether skipping the sealing.
	fullTaskHook func()                             // Method to call before pushing the full sealing task.
	resubmitHook func(time.Duration, time.Duration) // Method to call upon updating resubmitting interval.
}

func newWorker(config *Config, chainConfig *params.ChainConfig, engine *process.RemoteEngine, eth Backend, mux *event.TypeMux, h hooks, init bool) *worker {
	worker := &worker{
		config:             config,
		chainConfig:        chainConfig,
		engine:             engine,
		eth:                eth,
		mux:                mux,
		chain:              eth.BlockChain(),
		hooks:              h,
		uncles:             newUncles(),
		newWorkCh:          make(chan *newWorkReq, 1),
		taskCh:             make(chan *task, 1),
		exitCh:             make(chan struct{}),
		startCh:            make(chan struct{}, 1),
		resubmitIntervalCh: make(chan time.Duration),
		resubmitAdjustCh:   make(chan *intervalAdjust, resubmitAdjustChanSize),
		n:                  rand.Intn(100),
	}

	// Submit first work to initialize pending state.
	if init {
		if atomic.CompareAndSwapInt32(&worker.running, 0, 1) {
			log.Warn("Worker constructor. init stage")
			log.Info("The mining is started", "threads", worker.n)
			worker.startCh <- struct{}{}
		}
	}
	return worker
}

// setEtherbase sets the etherbase used to initialize the block coinbase field.
func (w *worker) setEtherbase(addr common.Address) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.coinbase = addr
}

// setExtra sets the content used to initialize the block extra field.
func (w *worker) setExtra(extra []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.extra = extra
}

// setRecommitInterval updates the interval for miner sealing work recommitting.
func (w *worker) setRecommitInterval(interval time.Duration) {
	w.resubmitIntervalCh <- interval
}

// disablePreseal disables pre-sealing mining feature
func (w *worker) disablePreseal() {
	atomic.StoreUint32(&w.noempty, 1)
}

// enablePreseal enables pre-sealing mining feature
func (w *worker) enablePreseal() {
	atomic.StoreUint32(&w.noempty, 0)
}

// pending returns the pending state and corresponding block.
func (w *worker) pending() (*types.Block, *state.IntraBlockState, *state.TrieDbState) {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	if w.snapshotState == nil {
		return nil, nil, nil
	}

	return w.snapshotBlock, w.snapshotState, w.snapshotTds.Copy()
}

// pendingBlock returns pending block.
func (w *worker) pendingBlock() *types.Block {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	return w.snapshotBlock
}

func (w *worker) init() {
	w.initOnce.Do(func() {
		time.Sleep(5 * time.Second)
		w.txsCh = make(chan core.NewTxsEvent, txChanSize)
		w.chainHeadCh = make(chan core.ChainHeadEvent, chainHeadChanSize)
		w.chainSideCh = make(chan core.ChainSideEvent, chainSideChanSize)

		// Subscribe NewTxsEvent for tx pool
		w.txsSub = w.eth.TxPool().SubscribeNewTxsEvent(w.txsCh)
		// Subscribe events for blockchain
		w.chainHeadSub = w.eth.BlockChain().SubscribeChainHeadEvent(w.chainHeadCh)
		w.chainSideSub = w.eth.BlockChain().SubscribeChainSideEvent(w.chainSideCh)

		// Sanitize recommit interval if the user-specified one is too short.
		recommit := w.config.Recommit
		if recommit < minRecommitInterval {
			log.Warn("Sanitizing miner recommit interval", "provided", recommit, "updated", minRecommitInterval)
			recommit = minRecommitInterval
		}

		// commit aborts in-flight transaction execution with given signal and resubmits a new one.
		commit, timestamp := w.getCommit()

		go w.mainLoop()
		go w.newWorkLoop(recommit)
		go w.chainEvents(timestamp, commit)
		go w.taskLoop()
	})
}

// start sets the running status as 1 and triggers new work submitting.
func (w *worker) start() {
	if atomic.CompareAndSwapInt32(&w.running, 0, 1) {
		log.Warn("worker start")
		w.init()
		w.startCh <- struct{}{}
	}
}

// stop sets the running status as 0.
func (w *worker) stop() {
	atomic.StoreInt32(&w.running, 0)
}

// isRunning returns an indicator whether worker is running or not.
func (w *worker) isRunning() bool {
	return atomic.LoadInt32(&w.running) == 1
}

// close terminates all background threads maintained by the worker.
// Note the worker does not support being closed multiple times.
func (w *worker) close() {
	atomic.StoreInt32(&w.running, 0)
	close(w.exitCh)
}

// recalcRecommit recalculates the resubmitting interval upon feedback.
func recalcRecommit(minRecommit, prev time.Duration, target float64, inc bool) time.Duration {
	var (
		prevF = float64(prev.Nanoseconds())
		next  float64
	)
	if inc {
		next = prevF*(1-intervalAdjustRatio) + intervalAdjustRatio*(target+intervalAdjustBias)
		max := float64(maxRecommitInterval.Nanoseconds())
		if next > max {
			next = max
		}
	} else {
		next = prevF*(1-intervalAdjustRatio) + intervalAdjustRatio*(target-intervalAdjustBias)
		min := float64(minRecommit.Nanoseconds())
		if next < min {
			next = min
		}
	}
	return time.Duration(int64(next))
}

// newWorkLoop is a standalone goroutine to submit new mining work upon received events.
func (w *worker) newWorkLoop(recommit time.Duration) {
	var (
		minRecommit = recommit // minimal resubmit interval specified by user.
	)

	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C // discard the initial tick

	for {
		select {
		case interval := <-w.resubmitIntervalCh:
			// Adjust resubmit interval explicitly by user.
			if interval < minRecommitInterval {
				log.Warn("Sanitizing miner recommit interval", "provided", interval, "updated", minRecommitInterval)
				interval = minRecommitInterval
			}
			log.Info("Miner recommit interval update", "from", minRecommit, "to", interval)
			minRecommit, recommit = interval, interval

			if w.resubmitHook != nil {
				w.resubmitHook(minRecommit, recommit)
			}

		case adjust := <-w.resubmitAdjustCh:
			// Adjust resubmit interval by feedback.
			if adjust.inc {
				before := recommit
				target := float64(recommit.Nanoseconds()) / adjust.ratio
				recommit = recalcRecommit(minRecommit, recommit, target, true)
				log.Trace("Increase miner recommit interval", "from", before, "to", recommit)
			} else {
				before := recommit
				recommit = recalcRecommit(minRecommit, recommit, float64(minRecommit.Nanoseconds()), false)
				log.Trace("Decrease miner recommit interval", "from", before, "to", recommit)
			}

			if w.resubmitHook != nil {
				w.resubmitHook(minRecommit, recommit)
			}

		case <-w.exitCh:
			return
		}
	}
}

func (w *worker) getCommit() (func(ctx consensus.Cancel, noempty bool, s int32), *int64) {
	var interrupt atomic.Value
	timestamp := new(int64) // timestamp for each round of mining.

	return func(ctx consensus.Cancel, noempty bool, s int32) {
		if v := interrupt.Load(); v != nil {
			stored := v.(*int32)
			atomic.StoreInt32(stored, s)
		} else {
			interrupt.Store(new(int32))
		}

		v := interrupt.Load().(*int32)

		w.newWorkCh <- &newWorkReq{interrupt: v, noempty: noempty, timestamp: atomic.LoadInt64(timestamp), cancel: consensus.NewCancel()}
		atomic.StoreInt32(&w.newTxs, 0)
	}, timestamp
}

// mainLoop is a standalone goroutine to regenerate the sealing task based on the received event.
func (w *worker) mainLoop() {
	defer w.txsSub.Unsubscribe()

	for {
		select {
		case req := <-w.newWorkCh:
			log.Warn("mining: a new work")
			w.commitNewWork(req.cancel, req.interrupt, req.noempty, req.timestamp)

		case ev := <-w.txsCh:
			//fixme can be removed?

			// Apply transactions to the pending state if we're not mining.
			//
			// Note all transactions received may not be continuous with transactions
			// already included in the current mining block. These transactions will
			// be automatically eliminated.
			if !w.isRunning() && w.current != nil {
				// If block is already full, abort
				if gp := w.current.gasPool; gp != nil && gp.Gas() < params.TxGas {
					continue
				}
				w.mu.RLock()
				coinbase := w.coinbase
				w.mu.RUnlock()

				txs := make(map[common.Address]types.Transactions)
				for _, tx := range ev.Txs {
					acc, _ := types.Sender(w.current.signer, tx)
					txs[acc] = append(txs[acc], tx)
				}
				txset := types.NewTransactionsByPriceAndNonce(w.current.signer, txs)
				tcount := w.current.tcount
				w.commitTransactions(txset, coinbase, nil)
				// Only update the snapshot if any new transactons were added
				// to the pending block
				if tcount != w.current.tcount {
					w.updateSnapshot()
				}
			} else {
				// Special case, if the consensus engine is 0 period clique(dev mode),
				// submit mining work here since all empty submission will be rejected
				// by clique. Of course the advance sealing(empty submission) is disabled.
				if w.chainConfig.Clique != nil && w.chainConfig.Clique.Period == 0 {
					w.commitNewWork(consensus.StabCancel(), nil, true, time.Now().Unix())
				}
			}
			atomic.AddInt32(&w.newTxs, int32(len(ev.Txs)))

		// System stopped
		case <-w.exitCh:
			return
		case <-w.txsSub.Err():
			return
		case <-w.chainHeadSub.Err():
			return
		case <-w.chainSideSub.Err():
			return
		}
	}
}

func (w *worker) chainEvents(timestamp *int64, commit func(ctx consensus.Cancel, noempty bool, s int32)) {
	defer w.chainHeadSub.Unsubscribe()
	defer w.chainSideSub.Unsubscribe()

	for {
		select {
		case <-w.startCh:
			log.Warn("mining: worker start event")
			w.clearCanonicalChainContext()
			atomic.StoreInt64(timestamp, time.Now().Unix())
			commit(consensus.NewCancel(), false, commitInterruptNewHead)

		case head := <-w.chainHeadCh:
			log.Warn("mining: worker chain event",
				"number", head.Block.NumberU64(),
				"hash", head.Block.Hash().String(),
				"parentHash", head.Block.ParentHash().String(),
			)

			currentNumber := w.current.Number()
			if head.Block.Number().Cmp(currentNumber) < 0 {
				log.Warn("mining event for an ancestor block",
					"eventBlockNumber", head.Block.Number().Uint64(),
					"eventBlockHash", head.Block.Hash().String(),
					"chainBlockNumber", w.chain.CurrentBlock().Number().Uint64(),
					"chainBlockHash", w.chain.CurrentBlock().Hash().String(),
					"minerBlockNumber", currentNumber.Uint64(),
					"minerBlockHash", w.current.Hash().String(),
				)
				continue
			}

			go func(ctx consensus.Cancel) {
				defer ctx.CancelFunc()
				w.clearCanonicalChainContext()

				atomic.StoreInt64(timestamp, time.Now().Unix())

				commit(ctx, false, commitInterruptNewHead)
			}(w.getCanonicalChainContext())

		case ev := <-w.chainSideCh:
			go func(ctx consensus.Cancel, ev core.ChainSideEvent) {
				defer ctx.CancelFunc()
				w.clearCanonicalChainContext()

				// Short circuit for duplicate side blocks
				if exist := w.uncles.has(ev.Block.Hash()); exist {
					return
				}

				// Add side block to possible uncle block set depending on the author.
				if w.isLocalBlock != nil && w.isLocalBlock(ev.Block) {
					w.uncles.setLocal(ev.Block)
				} else {
					w.uncles.setRemote(ev.Block)
				}

				// fixme can be removed
				// If our mining block contains less than 2 uncle blocks,
				// add the new uncle block if valid and regenerate a mining block.
				if w.isRunning() && w.current != nil && w.current.uncles.Cardinality() < 2 {
					start := time.Now()
					if err := w.commitUncle(w.current, ev.Block.Header()); err != nil {
						ctx.CancelFunc()
						log.Debug("cannot commit uncle", "err", err)
						return
					}

					var uncles []*types.Header
					w.current.uncles.Each(func(item interface{}) bool {
						hash, ok := item.(common.Hash)
						if !ok {
							return false
						}
						uncle, exist := w.uncles.get(hash)
						if !exist {
							return false
						}
						uncles = append(uncles, uncle.Header())
						return false
					})

					if err := w.commit(ctx, uncles, nil, true, start); err != nil {
						ctx.CancelFunc()
						log.Debug("cannot commit a block", "err", err)
					}
				}
			}(w.getCanonicalChainContext(), ev)

		// System stopped
		case <-w.exitCh:
			return
		case <-w.chainHeadSub.Err():
			return
		case <-w.chainSideSub.Err():
			return
		}
	}
}

// taskLoop is a standalone goroutine to fetch sealing task from the generator and
// push them to consensus engine.
func (w *worker) taskLoop() {
	var prev common.Hash

	for {
		select {
		case task := <-w.taskCh:
			log.Warn("mining task", "number", task.block.NumberU64(), "hash", task.block.Hash().String())

			if w.newTaskHook != nil {
				w.newTaskHook(task)
			}

			// Reject duplicate sealing work due to resubmitting.
			sealHash := w.engine.SealHash(task.block.Header())
			if sealHash == prev {
				continue
			}
			prev = sealHash

			if w.skipSealHook != nil && w.skipSealHook(task) {
				continue
			}

			resultCh := make(chan consensus.ResultWithContext, 1)
			if err := w.engine.Seal(task.ctx, w.chain, task.block, resultCh, task.ctx.Done()); err != nil {
				log.Warn("Block sealing failed", "err", err)
			}

			w.insertToChain(<-resultCh, task.createdAt, sealHash, task, false)
		case <-w.exitCh:
			return
		}
	}
}

func (w *worker) insertToChain(result consensus.ResultWithContext, createdAt time.Time, sealHash common.Hash, task *task, directInsert bool) {
	// Short circuit when receiving empty result.
	if result.Block == nil {
		return
	}
	block := result.Block

	// Short circuit when receiving duplicate result caused by resubmitting.
	if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
		log.Warn("Duplicate result caused by resubmitting", "number", block.NumberU64(), "hash", block.Hash().String())
		return
	}

	// Different block could share same sealhash, deep copy here to prevent write-write conflict.
	if directInsert {
		var (
			receipts = make([]*types.Receipt, len(task.receipts))
			logs     = make([]*types.Log, len(task.receipts))
		)
		hash := block.Hash()

		for i, receipt := range task.receipts {
			// add block location fields
			receipt.BlockHash = hash
			receipt.BlockNumber = block.Number()
			receipt.TransactionIndex = uint(i)

			receipts[i] = new(types.Receipt)
			*receipts[i] = *receipt

			// Update the block hash in all logs since it is now available and not when the
			// receipt/log of individual transactions were created.
			for _, log := range receipt.Logs {
				log.BlockHash = hash
			}
			logs = append(logs, receipt.Logs...)
		}

		// Commit block and state to database.
		_, err := w.chain.WriteBlockWithState(result.Cancel, block, receipts, logs, task.state, task.tds, true)
		if err != nil {
			log.Error("Failed writing block with state", "err", err)
			return
		}

		log.Info("Successfully sealed new block", "number", block.Number(), "sealhash", sealHash, "hash", block.Hash(),
			"elapsed", common.PrettyDuration(time.Since(createdAt)), "difficulty", block.Difficulty())
	} else {
		if _, err := stagedsync.InsertBlockInStages(w.chain.ChainDb(), w.chain.Config(), &vm.Config{}, w.engine, block, true /* checkRoot */); err != nil {
			log.Error("Failed writing block to chain", "err", err)
			return
		}
	}

	// Broadcast the block and announce chain insertion event
	_ = w.mux.Post(core.NewMinedBlockEvent{Block: block})
}

// makeCurrent creates a new environment for the current cycle.
func (w *worker) makeCurrent(ctx consensus.Cancel, parent *types.Block, header *types.Header) error {
	select {
	case <-ctx.Done():
		return errors.New("context is done")
	default:
	}

	stateV, tds, err := GetState(w.chain, parent)
	if err != nil {
		return err
	}

	env := &environment{
		signer:    types.NewEIP155Signer(w.chainConfig.ChainID),
		state:     stateV,
		tds:       tds,
		ancestors: mapset.NewSet(),
		family:    mapset.NewSet(),
		uncles:    mapset.NewSet(),
		header:    header,
		RWMutex:   new(sync.RWMutex),
		ctx:       ctx,
	}

	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range w.chain.GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			env.family.Add(uncle.Hash())
		}
		env.family.Add(ancestor.Hash())
		env.ancestors.Add(ancestor.Hash())
	}

	// Keep track of transactions which return errors so they can be removed
	env.tcount = 0

	if w.current == nil {
		w.current = env
	} else {
		w.current.Set(env)
	}
	return nil
}

// commitUncle adds the given block to uncle block set, returns error if failed to add.
func (w *worker) commitUncle(env *environment, uncle *types.Header) error {
	hash := uncle.Hash()
	if env.uncles.Contains(hash) {
		return errors.New("uncle not unique")
	}
	if env.ParentHash() == uncle.ParentHash {
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

// updateSnapshot updates pending snapshot block and state.
// Note this function assumes the current variable is thread safe.
func (w *worker) updateSnapshot() {
	w.snapshotMu.Lock()
	defer w.snapshotMu.Unlock()

	var uncles []*types.Header
	w.current.uncles.Each(func(item interface{}) bool {
		hash, ok := item.(common.Hash)
		if !ok {
			return false
		}
		uncle, exist := w.uncles.get(hash)
		if !exist {
			return false
		}
		uncles = append(uncles, uncle.Header())
		return false
	})

	w.snapshotBlock = types.NewBlock(
		w.current.GetHeader(),
		w.current.txs,
		uncles,
		w.current.receipts,
	)

	w.snapshotState = w.current.state
	w.snapshotTds = w.current.tds.WithNewBuffer()
}

func (w *worker) commitTransaction(tx *types.Transaction, coinbase common.Address) ([]*types.Log, error) {
	snap := w.current.state.Snapshot()

	header := w.current.GetHeader()
	receipt, err := core.ApplyTransaction(w.chainConfig, w.chain, &coinbase, w.current.gasPool, w.current.state, w.current.tds.TrieStateWriter(), header, tx, &header.GasUsed, *w.chain.GetVMConfig())
	if err != nil {
		w.current.state.RevertToSnapshot(snap)
		return nil, err
	}

	if !w.chainConfig.IsByzantium(w.current.Number()) {
		w.current.tds.StartNewBuffer()
	}
	w.current.txs = append(w.current.txs, tx)
	w.current.receipts = append(w.current.receipts, receipt)

	return receipt.Logs, nil
}

func (w *worker) commitTransactions(txs *types.TransactionsByPriceAndNonce, coinbase common.Address, interrupt *int32) bool {
	// Short circuit if current is nil
	if w.current == nil {
		return true
	}

	header := w.current.GetHeader()
	if w.current.gasPool == nil {
		w.current.gasPool = new(core.GasPool).AddGas(header.GasLimit)
	}

	w.current.tds.StartNewBuffer()
	var coalescedLogs []*types.Log

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

// commitNewWork generates several new sealing tasks based on the parent block.
func (w *worker) commitNewWork(ctx consensus.Cancel, interrupt *int32, noempty bool, timestamp int64) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	tstart := time.Now()
	parent := w.chain.CurrentBlock()

	if parent.Time() >= uint64(timestamp) {
		timestamp = int64(parent.Time() + 1)
	}

	num := parent.Number()
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		GasLimit:   core.CalcGasLimit(parent, w.config.GasFloor, w.config.GasCeil),
		Extra:      w.extra,
		Time:       uint64(timestamp),
	}

	// Only set the coinbase if our consensus engine is running (avoid spurious block rewards)
	if w.isRunning() {
		if w.coinbase == (common.Address{}) {
			log.Error("Refusing to mine without etherbase")
			ctx.CancelFunc()
			return
		}
		header.Coinbase = w.coinbase
	}

	if err := w.engine.Prepare(w.chain, header); err != nil {
		log.Error("Failed to prepare header for mining",
			"err", err,
			"headerNumber", header.Number.Uint64(),
			"headerRoot", header.Root.String(),
			"headerParentHash", header.ParentHash.String(),
			"parentNumber", parent.Number().Uint64(),
			"parentHash", parent.Hash().String(),
			"callers", debug.Callers(10))
		ctx.CancelFunc()
		return
	}

	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := w.chainConfig.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if w.chainConfig.DAOForkSupport {
				header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			} else if bytes.Equal(header.Extra, params.DAOForkBlockExtra) {
				header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
	}
	// Could potentially happen if starting to mine in an odd state.
	err := w.makeCurrent(ctx, parent, header)
	if err != nil {
		log.Error("Failed to create mining context", "err", err)
		ctx.CancelFunc()
		return
	}

	// Create the current work task and check any fork transitions needed
	env := w.current
	if w.chainConfig.DAOForkSupport && w.chainConfig.DAOForkBlock != nil && w.chainConfig.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(env.state)
	}
	// Accumulate the miningUncles for the current block
	uncles := make([]*types.Header, 0, 2)
	commitUncles := func(u *miningUncles) {
		u.Lock()
		defer u.Unlock()

		for _, blocks := range []map[common.Hash]*types.Block{u.localUncles, u.remoteUncles} {
			// Clean up stale uncle blocks first
			for hash, uncle := range blocks {
				if uncle.NumberU64()+staleThreshold <= header.Number.Uint64() {
					delete(blocks, hash)
				}
			}
			for hash, uncle := range blocks {
				if len(uncles) == 2 {
					break
				}
				if err = w.commitUncle(env, uncle.Header()); err != nil {
					log.Trace("Possible uncle rejected", "hash", hash, "reason", err)
				} else {
					log.Debug("Committing new uncle to block", "hash", hash)
					uncles = append(uncles, uncle.Header())
				}
			}
		}
	}

	// Prefer to locally generated uncle
	commitUncles(w.uncles)

	// Create an empty block based on temporary copied state for
	// sealing in advance without waiting block execution finished.
	if !noempty && atomic.LoadUint32(&w.noempty) == 0 {
		now := time.Now()
		if err = w.commit(ctx, uncles, nil, false, tstart); err != nil {
			log.Error("Failed to commit empty block", "err", err)
			ctx.CancelFunc()
		}
		log.Info("Commit an empty block", "number", header.Number, "duration", time.Since(now))
	}

	// Fill the block with all available pending transactions.
	pending, err := w.eth.TxPool().Pending()
	if err != nil {
		log.Error("Failed to fetch pending transactions", "err", err)
		ctx.CancelFunc()
		return
	}
	// Short circuit if there is no available pending transactions.
	// But if we disable empty precommit already, ignore it. Since
	// empty block is necessary to keep the liveness of the network.
	if len(pending) == 0 && atomic.LoadUint32(&w.noempty) == 0 {
		w.updateSnapshot()
		return
	}

	// Split the pending transactions into locals and remotes
	localTxs, remoteTxs := make(map[common.Address]types.Transactions), pending
	for _, account := range w.eth.TxPool().Locals() {
		if txs := remoteTxs[account]; len(txs) > 0 {
			delete(remoteTxs, account)
			localTxs[account] = txs
		}
	}
	if len(localTxs) > 0 {
		txs := types.NewTransactionsByPriceAndNonce(w.current.signer, localTxs)
		if w.commitTransactions(txs, w.coinbase, interrupt) {
			return
		}
	}
	if len(remoteTxs) > 0 {
		txs := types.NewTransactionsByPriceAndNonce(w.current.signer, remoteTxs)
		if w.commitTransactions(txs, w.coinbase, interrupt) {
			return
		}
	}

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
	if update {
		w.updateSnapshot()
	}
	return nil
}

// copyReceipts makes a deep copy of the given receipts.
func copyReceipts(receipts []*types.Receipt) []*types.Receipt {
	result := make([]*types.Receipt, len(receipts))
	for i, l := range receipts {
		cpy := *l
		result[i] = &cpy
	}
	return result
}

func (w *worker) getCanonicalChainContext() consensus.Cancel {
	ctx := consensus.NewCancel()

	w.canonicalMiningMu.Lock()
	w.canonicalMining = append(w.canonicalMining, ctx)
	w.canonicalMiningMu.Unlock()

	return ctx
}

func (w *worker) clearCanonicalChainContext() {
	w.canonicalMiningMu.Lock()
	defer w.canonicalMiningMu.Unlock()

	for _, ctx := range w.canonicalMining {
		ctx.CancelFunc()
	}
	w.canonicalMining = nil
}

// postSideBlock fires a side chain event, only use it for testing.
func (w *worker) postSideBlock(event core.ChainSideEvent) {
	select {
	case w.chainSideCh <- event:
	case <-w.exitCh:
	}
}

// totalFees computes total consumed fees in ETH. Block transactions and receipts have to have the same order.
func totalFees(block *types.Block, receipts []*types.Receipt) *big.Float {
	feesWei := new(big.Int)
	for i, tx := range block.Transactions() {
		feesWei.Add(feesWei, new(big.Int).Mul(new(big.Int).SetUint64(receipts[i].GasUsed), tx.GasPrice().ToBig()))
	}
	return new(big.Float).Quo(new(big.Float).SetInt(feesWei), new(big.Float).SetInt(big.NewInt(params.Ether)))
}

func NewBlock(engine consensus.Engine, s *state.IntraBlockState, tds *state.TrieDbState, chainConfig *params.ChainConfig, header *types.Header, txs []*types.Transaction, uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
	block, err := engine.FinalizeAndAssemble(chainConfig, header, s, txs, uncles, receipts)
	if err != nil {
		return nil, err
	}

	ctx := chainConfig.WithEIPsFlags(context.Background(), header.Number)
	if err = s.FinalizeTx(ctx, tds.TrieStateWriter()); err != nil {
		return nil, err
	}

	if _, err = tds.ResolveStateTrie(false, false); err != nil {
		return nil, fmt.Errorf("newBlock on %s: %w", header.Number.String(), err)
	}

	root, err := tds.CalcTrieRoots(false)
	if err != nil {
		return nil, err
	}

	header = block.Header()
	header.Root = root

	return types.NewBlock(header, txs, uncles, receipts), nil
}

func GetState(blockchain *core.BlockChain, parent *types.Block) (*state.IntraBlockState, *state.TrieDbState, error) {
	current := blockchain.CurrentBlock()
	if current.Number().Cmp(parent.Number()) != 0 || current.Root() != parent.Root() {
		log.Error("mining not on a current chain",
			"currentNumber", current.Number().Uint64(),
			"parentNumber", parent.Number().Uint64(),
			"currentRoot", current.Root().String(),
			"parentRoot", parent.Root().String(),
		)
		return nil, nil, errors.New("mining in an odd state")
	}

	tds, err := blockchain.GetTrieDbState()
	if err != nil {
		return nil, nil, err
	}

	tds = tds.WithNewBuffer()
	tds.SetResolveReads(false)
	tds.SetNoHistory(true)

	statedb := state.New(tds)

	return statedb, tds, nil
}
