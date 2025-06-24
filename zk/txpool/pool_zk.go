package txpool

import (
	"bytes"
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/cmp"
	"github.com/erigontech/erigon-lib/common/fixedgas"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/txpool/txpoolcfg"
	"github.com/erigontech/erigon-lib/types"
	types2 "github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/zk/utils"
	"github.com/holiman/uint256"
)

/*
here we keep the changes made to the txpool to support zk sequencing.  Designed to create a
hard compilation fail when rebasing from upstream further down the line.
*/

const (
	transactionGasLimit = utils.PreForkId7BlockGasLimit
)

func calcProtocolBaseFee(baseFee uint64) uint64 {
	return 0
}

func (p *TxPool) Trace(msg string, ctx ...interface{}) {
	if p.logLevel == log.LvlTrace {
		log.Trace(msg, ctx...)
	}
}

// onSenderStateChange is the function that recalculates ephemeral fields of transactions and determines
// which sub pool they will need to go to. Since this depends on other transactions from the same sender by with lower
// nonces, and also affect other transactions from the same sender with higher nonce, it loops through all transactions
// for a given senderID
func (p *TxPool) onSenderStateChange(senderID uint64, senderNonce uint64, senderBalance uint256.Int, byNonce *BySenderAndNonce,
	protocolBaseFee, blockGasLimit uint64, pending *PendingPool, baseFee, queued *SubPool, discard func(*metaTx, DiscardReason)) {
	noGapsNonce := senderNonce
	cumulativeRequiredBalance := uint256.NewInt(0)
	minFeeCap := uint256.NewInt(0).SetAllOne()
	minTip := uint64(math.MaxUint64)
	var toDel []*metaTx // can't delete items while iterate them
	byNonce.ascend(senderID, func(mt *metaTx) bool {
		if mt.Tx.Traced {
			log.Info(fmt.Sprintf("TX TRACING: onSenderStateChange loop iteration idHash=%x senderID=%d, senderNonce=%d, txn.nonce=%d, currentSubPool=%s", mt.Tx.IDHash, senderID, senderNonce, mt.Tx.Nonce, mt.currentSubPool))
		}
		if senderNonce > mt.Tx.Nonce {
			if mt.Tx.Traced {
				log.Info(fmt.Sprintf("TX TRACING: removing due to low nonce for idHash=%x senderID=%d, senderNonce=%d, txn.nonce=%d, currentSubPool=%s", mt.Tx.IDHash, senderID, senderNonce, mt.Tx.Nonce, mt.currentSubPool))
			}
			// del from sub-pool
			switch mt.currentSubPool {
			case PendingSubPool:
				pending.Remove(mt)
			case BaseFeeSubPool:
				baseFee.Remove(mt)
			case QueuedSubPool:
				queued.Remove(mt)
			default:
				//already removed
			}
			toDel = append(toDel, mt)
			return true
		}
		if minFeeCap.Gt(&mt.Tx.FeeCap) {
			*minFeeCap = mt.Tx.FeeCap
		}
		mt.minFeeCap = *minFeeCap
		if mt.Tx.Tip.IsUint64() {
			minTip = cmp.Min(minTip, mt.Tx.Tip.Uint64())
		}
		mt.minTip = minTip

		mt.nonceDistance = 0
		if mt.Tx.Nonce > senderNonce { // no uint underflow
			mt.nonceDistance = mt.Tx.Nonce - senderNonce
		}

		// Sender has enough balance for: gasLimit x feeCap + transferred_value
		needBalance := uint256.NewInt(mt.Tx.Gas)
		needBalance.Mul(needBalance, &mt.Tx.FeeCap)
		needBalance.Add(needBalance, &mt.Tx.Value)
		// 1. Minimum fee requirement. Set to 1 if feeCap of the transaction is no less than in-protocol
		// parameter of minimal base fee. Set to 0 if feeCap is less than minimum base fee, which means
		// this transaction will never be included into this particular chain.
		mt.subPool &^= EnoughFeeCapProtocol
		if mt.minFeeCap.CmpUint64(protocolBaseFee) >= 0 {
			mt.subPool |= EnoughFeeCapProtocol
		} else {
			mt.subPool = 0 // TODO: we immediately drop all transactions if they have no first bit - then maybe we don't need this bit at all? And don't add such transactions to queue?
			return true
		}

		// 2. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for
		// the sender is M, and there are transactions for all nonces between M and N from the same
		// sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
		mt.subPool &^= NoNonceGaps
		if noGapsNonce == mt.Tx.Nonce {
			mt.subPool |= NoNonceGaps
			noGapsNonce++
		}

		// 3. Sufficient balance for gas. Set to 1 if the balance of sender's account in the
		// state is B, nonce of the sender in the state is M, nonce of the transaction is N, and the
		// sum of feeCap x gasLimit + transferred_value of all transactions from this sender with
		// nonces N+1 ... M is no more than B. Set to 0 otherwise. In other words, this bit is
		// set if there is currently a guarantee that the transaction and all its required prior
		// transactions will be able to pay for gas.
		mt.subPool &^= EnoughBalance
		mt.cumulativeBalanceDistance = math.MaxUint64
		if mt.Tx.Nonce >= senderNonce {
			cumulativeRequiredBalance = cumulativeRequiredBalance.Add(cumulativeRequiredBalance, needBalance) // already deleted all transactions with nonce <= sender.nonce
			if senderBalance.Gt(cumulativeRequiredBalance) || senderBalance.Eq(cumulativeRequiredBalance) {
				mt.subPool |= EnoughBalance
			} else {
				if cumulativeRequiredBalance.IsUint64() && senderBalance.IsUint64() {
					mt.cumulativeBalanceDistance = cumulativeRequiredBalance.Uint64() - senderBalance.Uint64()
				}
			}
		}

		mt.subPool &^= NotTooMuchGas
		// zk: here we don't care about block limits any more and care about only the transaction gas limit in ZK
		if mt.Tx.Gas <= transactionGasLimit {
			mt.subPool |= NotTooMuchGas
		}

		if mt.Tx.Traced {
			log.Info(fmt.Sprintf("TX TRACING: onSenderStateChange loop iteration idHash=%x senderId=%d subPool=%b", mt.Tx.IDHash, mt.Tx.SenderID, mt.subPool))
		}

		// Some fields of mt might have changed, need to fix the invariants in the subpool best and worst queues
		switch mt.currentSubPool {
		case PendingSubPool:
			pending.Updated(mt)
		case BaseFeeSubPool:
			baseFee.Updated(mt)
		case QueuedSubPool:
			queued.Updated(mt)
		}
		return true
	})
	for _, mt := range toDel {
		discard(mt, NonceTooLow)
	}
}

// zk: the implementation of best here is changed only to not take into account block gas limits as we don't care about
// these in zk.  Instead we do a quick check on the transaction maximum gas in zk
func (p *TxPool) best(n uint16, txs *types.TxsRlp, tx kv.Tx, onTopOf, availableGas, availableBlobGas uint64) (bool, int, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.isDeniedYieldingTransactions() {
		p.Trace("Denied yielding transactions, cannot proceed")
		return false, 0, nil
	}

	// First wait for the corresponding block to arrive
	if p.lastSeenBlock.Load() < onTopOf {
		p.Trace("Block not yet arrived, too early to process", "lastSeenBlock", p.lastSeenBlock.Load(), "requiredBlock", onTopOf)
		return false, 0, nil
	}

	isShanghai := p.isShanghai()
	isLondon := p.isLondon()
	_ = isLondon
	best := p.pending.best

	txs.Resize(uint(cmp.Min(int(n), len(best.ms))))
	var toRemove []*metaTx
	count := 0

	p.pending.EnforceBestInvariants()

	for i := 0; count < int(n) && i < len(best.ms); i++ {
		// if we wouldn't have enough gas for a standard transaction then quit out early
		if availableGas < fixedgas.TxGas {
			break
		}

		mt := best.ms[i]
		p.Trace("Processing transaction", "txID", mt.Tx.IDHash)

		if !isLondon && mt.Tx.Type == 0x2 {
			// remove ldn txs when not in london
			toRemove = append(toRemove, mt)
			log.Info("Removing London transaction in non-London environment", "txID", mt.Tx.IDHash)
			continue
		}

		if mt.Tx.Gas > transactionGasLimit {
			// Skip transactions with very large gas limit, these shouldn't enter the pool at all
			log.Debug("found a transaction in the pending pool with too high gas for tx - clear the tx pool")
			p.Trace("Skipping transaction with too high gas", "txID", mt.Tx.IDHash, "gas", mt.Tx.Gas)
			continue
		}
		rlpTx, sender, isLocal, err := p.getRlpLocked(tx, mt.Tx.IDHash[:])
		if err != nil {
			p.Trace("Error getting RLP of transaction", "txID", mt.Tx.IDHash, "error", err)
			return false, count, err
		}
		if len(rlpTx) == 0 {
			toRemove = append(toRemove, mt)
			log.Info("Removing transaction with empty RLP", "txID", common.BytesToHash(mt.Tx.IDHash[:]))
			continue
		}

		// Skip transactions that require more blob gas than is available
		blobCount := uint64(len(mt.Tx.BlobHashes))
		if blobCount*fixedgas.BlobGasPerBlob > availableBlobGas {
			p.Trace("Skipping transaction due to insufficient blob gas", "txID", mt.Tx.IDHash, "requiredBlobGas", blobCount*fixedgas.BlobGasPerBlob, "availableBlobGas", availableBlobGas)
			continue
		}
		availableBlobGas -= blobCount * fixedgas.BlobGasPerBlob

		// make sure we have enough gas in the caller to add this transaction.
		// not an exact science using intrinsic gas but as close as we could hope for at
		// this stage
		intrinsicGas, floorGas, _ := txpoolcfg.CalcIntrinsicGas(uint64(mt.Tx.DataLen), uint64(mt.Tx.DataNonZeroLen), uint64(len(mt.Tx.Authorizations)), uint64(mt.Tx.AlAddrCount), uint64(mt.Tx.AlStorCount), mt.Tx.Creation, true, true, isShanghai, p.isPrague())
		if p.isPrague() && floorGas > intrinsicGas {
			intrinsicGas = floorGas
		}

		if intrinsicGas > availableGas {
			// we might find another TX with a low enough intrinsic gas to include so carry on
			p.Trace("Skipping transaction due to insufficient gas", "txID", mt.Tx.IDHash, "intrinsicGas", intrinsicGas, "availableGas", availableGas)
			continue
		}

		if intrinsicGas <= availableGas { // check for potential underflow
			availableGas -= intrinsicGas
		}

		p.Trace("Including transaction", "txID", mt.Tx.IDHash)
		txs.Txs[count] = rlpTx
		txs.TxIds[count] = mt.Tx.IDHash
		copy(txs.Senders.At(count), sender.Bytes())
		txs.IsLocal[count] = isLocal
		count++
	}

	txs.Resize(uint(count))
	if len(toRemove) > 0 {
		for _, mt := range toRemove {
			p.pending.Remove(mt)
			p.discardLocked(mt, UnsupportedTx)
			log.Debug("Removed transaction from pending pool", "txID", mt.Tx.IDHash)
		}
	}
	return true, count, nil
}

func (p *TxPool) ForceUpdateLatestBlock(blockNumber uint64) {
	if p != nil {
		p.lastSeenBlock.Store(blockNumber)
	}
}

// MarkForDiscardFromPendingBest function is invoked if a single tx overflow entire zk-counters.
// In this case there is nothing we can do but to mark is as such
// and on next "pool iteration" it will be discard
func (p *TxPool) MarkForDiscardFromPendingBest(txHash common.Hash) {
	p.lock.Lock()
	defer p.lock.Unlock()

	best := p.pending.best

	for i := 0; i < len(best.ms); i++ {
		mt := best.ms[i]
		if bytes.Equal(mt.Tx.IDHash[:], txHash[:]) {
			p.overflowZkCounters = append(p.overflowZkCounters, mt)
			break
		}
	}
}

func (p *TxPool) TriggerSenderStateChanges(ctx context.Context, tx kv.Tx, blockGasLimit uint64, senders map[common.Address]struct{}) error {
	if len(senders) == 0 {
		return nil
	}

	cache := p.cache()

	p.lock.Lock()
	defer p.lock.Unlock()

	sendersToUpdate := make(map[uint64]struct{})
	for sender := range senders {
		if id, ok := p.senders.senderIDs[sender]; ok {
			sendersToUpdate[id] = struct{}{}
		}
	}

	baseFee := p.pendingBaseFee.Load()

	cacheView, err := cache.View(ctx, tx)
	if err != nil {
		return err
	}

	for senderID := range sendersToUpdate {
		nonce, balance, err := p.senders.info(cacheView, senderID)
		if err != nil {
			return err
		}
		p.onSenderStateChange(senderID, nonce, balance, p.all,
			baseFee, blockGasLimit, p.pending, p.baseFee, p.queued, p.discardLocked)
	}

	return nil
}

// discards the transactions that are in overflowZkCoutners from pending
// executes the discard function on them
// deletes the tx from the sendersWithChangedState map
// deletes the discarded txs from the overflowZkCounters
func (p *TxPool) discardOverflowZkCountersFromPending(pending *PendingPool, discard func(*metaTx, DiscardReason), sendersWithChangedState map[uint64]struct{}) {
	for _, mt := range p.overflowZkCounters {
		log.Info("[tx_pool] Removing TX from pending due to counter overflow", "tx", common.BytesToHash(mt.Tx.IDHash[:]))
		pending.Remove(mt)
		discard(mt, OverflowZkCounters)
		sendersWithChangedState[mt.Tx.SenderID] = struct{}{}
		// do not hold on to the discard reason for an OOC issue
		p.discardReasonsLRU.Remove(string(mt.Tx.IDHash[:]))
	}
	p.overflowZkCounters = p.overflowZkCounters[:0]
}

func (p *TxPool) StartIfNotStarted(ctx context.Context, txPoolDb kv.RoDB, coreTx kv.Tx) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started.Load() {
		txPoolDbTx, err := txPoolDb.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer txPoolDbTx.Rollback()

		if err := p.fromDB(ctx, txPoolDbTx, coreTx); err != nil {
			return fmt.Errorf("loading txs from DB: %w", err)
		}
		if p.started.CompareAndSwap(false, true) {
			log.Info("[txpool] Start if not started")
		}
	}

	return nil
}

func markAsLocal(txSlots *types2.TxSlots) {
	for i := range txSlots.IsLocal {
		txSlots.IsLocal[i] = true
	}
}

// PreYield is a function that is called before the yield function is called.  Because the yield function is called from outside
// the txpool and relies on using the txpool db to create a View there is a potential race between the flush function running
// and the yield function starting which can result in it appearing that a transaction has no RLP because the flush has not yet
// finished
func (p *TxPool) PreYield() {
	p.flushMtx.Lock()
}

func (p *TxPool) PostYield() {
	p.flushMtx.Unlock()
}
