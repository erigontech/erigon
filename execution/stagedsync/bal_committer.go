package stagedsync

import (
	"bytes"
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// balFoldReader serves the speculative fold's state reads. Changed
// account/storage keys return the BAL's block-final values (from cs); all other
// reads — fold siblings and commitment branches — fall through to the pre-state
// base reader. This keeps the trie's account loads during storage-root
// recomputation consistent with the buffered final updates.
type balFoldReader struct {
	base  commitmentdb.StateReader
	cs    *calcState
	txNum uint64
}

func (r *balFoldReader) WithHistory() bool { return r.base.WithHistory() }

func (r *balFoldReader) CheckDataAvailable(d kv.Domain, step kv.Step) error {
	return r.base.CheckDataAvailable(d, step)
}

func (r *balFoldReader) Clone(tx kv.TemporalTx) commitmentdb.StateReader {
	return &balFoldReader{base: r.base.Clone(tx), cs: r.cs, txNum: r.txNum}
}

func (r *balFoldReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) ([]byte, kv.Step, error) {
	step := kv.Step(0)
	if stepSize > 0 {
		step = kv.Step(r.txNum / stepSize)
	}
	switch d {
	case kv.AccountsDomain:
		if len(plainKey) == 20 {
			addr := accounts.InternAddress(common.BytesToAddress(plainKey))
			if acc, ok := r.cs.accounts[addr]; ok && acc.dirty {
				return r.cs.encodeFinalAccount(acc), step, nil
			}
		}
	case kv.StorageDomain:
		if len(plainKey) == 20+32 {
			addr := accounts.InternAddress(common.BytesToAddress(plainKey[:20]))
			slot := accounts.InternKey(common.BytesToHash(plainKey[20:]))
			if dirty, ok := r.cs.storageDirty[addr]; ok && dirty[slot] {
				v := r.cs.storageState[addr][slot]
				if v.IsZero() {
					return nil, step, nil
				}
				return v.Bytes(), step, nil
			}
		}
	}
	return r.base.Read(d, plainKey, stepSize)
}

// balSeed is sent to the calculator at block dispatch (ahead of the block's
// txResults) so the commitment fold can run concurrently with execution.
type balSeed struct {
	blockNum   uint64
	blockHash  common.Hash
	stateRoot  common.Hash // header.Root — the expected post-block trie root
	firstTxNum uint64      // block's first txNum — pins the fold reader to pre-state
	lastTxNum  uint64      // block's last txNum — the commitment txNum
	accessList types.BlockAccessList
}

// balConfirm signals that the apply loop's post-execution validation
// (block validator + ProcessBAL) accepted block N — "execution confirms the
// BAL". Sent on a dedicated channel so confirms don't head-of-line block
// behind buffered txResults.
type balConfirm struct {
	blockNum uint64
}

// balFold is a speculative fold held until its block is confirmed.
type balFold struct {
	blockNum  uint64
	blockHash common.Hash
	stateRoot common.Hash
	lastTxNum uint64
	rootHash  []byte
}

// balLoop is the calculator's run loop in speculative-BAL mode. It selects
// cc.in (the txResult/blockResult/balSeed stream) and the dedicated confirm
// channel. It exits once cc.in is drained and no fold is awaiting
// confirmation. The apply loop closes balConfirm on exit, which lets the
// calculator abandon unconfirmed folds (their blocks are being discarded).
func (cc *commitmentCalculator) balLoop(ctx context.Context) {
	in := cc.in
	confirm := cc.balConfirm
	for {
		if in == nil && len(cc.heldFolds) == 0 {
			return
		}
		select {
		case result, ok := <-in:
			if !ok {
				in = nil
				continue
			}
			cc.handleMessage(ctx, result)
		case c, ok := <-confirm:
			if !ok {
				// Apply loop exited (possibly on an invalid block) without
				// confirming every fold. A still-held fold means its block is
				// being discarded: drop its deferred branch deltas (which are
				// the calculator's current pending update, since speculation is
				// one-block-deep) so they can't leak into a later batch. When
				// nothing is held the pending update belongs to a txResult-path
				// block and must survive for the batch-end flush — do not reset.
				confirm = nil
				if len(cc.heldFolds) > 0 {
					clear(cc.heldFolds)
					cc.doms.GetCommitmentContext().ResetPendingUpdates()
				}
				continue
			}
			cc.handleConfirm(ctx, c)
		}
	}
}

// foldSeed folds block N's BAL-derived write-set into the trie concurrently
// with N's execution, holding the result until confirmation. The fold runs
// lock-free (saveState disabled, branch updates deferred) so it writes nothing
// to any changeset and overlaps the apply loop's concurrent state writes.
// On a guard fallback or any error it leaves N to the txResult path.
func (cc *commitmentCalculator) foldSeed(ctx context.Context, seed *balSeed) {
	// One-block-deep invariant: fold only once the predecessor is confirmed.
	// The exec loop seeds single-block batches only, so at tip the predecessor
	// is the committed state and this holds; otherwise fall back.
	if seed.blockNum == 0 || seed.blockNum-1 != cc.lastConfirmedBlock {
		return
	}
	if err := cc.state.LazyLoadErr(); err != nil {
		return
	}
	cc.balReader.txNum = seed.firstTxNum
	cs, fallback, err := balToCalcState(seed.accessList, cc.balReader, cc.logger, cc.logPrefix)
	if err != nil {
		if cc.logger != nil {
			cc.logger.Warn("["+cc.logPrefix+"] speculative BAL fold: reduce failed, using txResult path", "block", seed.blockNum, "err", err)
		}
		return
	}
	if fallback {
		return
	}
	updates := cc.updates.NewEmpty()
	cs.FlushToUpdates(updates)
	sdCtx := cc.doms.GetCommitmentContext()
	sdCtx.SetUpdates(updates)
	// The fold's account/storage loads (during storage-root recomputation) must
	// see the block-final values, not pre-state — otherwise a changed account
	// that the trie loads for a storage re-root keeps its stale fields. Overlay
	// the BAL's final values (cs) on top of the pre-state reader.
	sdCtx.SetStateReader(&balFoldReader{base: cc.balReader, cs: cs, txNum: seed.firstTxNum})
	// saveState=false: N's changeset does not exist yet, so the state marker
	// is written at confirm instead. ComputeCommitment (via SharedDomains)
	// flushes any prior pending under a brief lock, then folds lock-free.
	rh, err := cc.doms.ComputeCommitment(ctx, cc.roTx, false, seed.blockNum, seed.lastTxNum, cc.logPrefix, nil)
	if err != nil {
		cc.publish(ctx, commitmentResult{
			blockNum: seed.blockNum,
			txNum:    seed.lastTxNum,
			err:      fmt.Errorf("commitmentCalculator: speculative BAL fold: %w", err),
		})
		return
	}
	// Stamp the held pending with the block hash so the confirm-time flush
	// routes branch deltas to the exact (BlockNum, BlockHash) changeset.
	if upd := sdCtx.PeekPendingUpdate(); upd != nil && upd.BlockNum == seed.blockNum {
		upd.BlockHash = seed.blockHash
	}
	cc.heldFolds[seed.blockNum] = &balFold{
		blockNum:  seed.blockNum,
		blockHash: seed.blockHash,
		stateRoot: seed.stateRoot,
		lastTxNum: seed.lastTxNum,
		rootHash:  append([]byte(nil), rh...),
	}
	cc.balFolded[seed.blockNum] = true
	cc.lastComputedBlock = seed.blockNum
	cc.hasComputed = true
	if cc.logger != nil {
		cc.logger.Debug("["+cc.logPrefix+"] speculative BAL fold", "block", seed.blockNum, "txNum", seed.lastTxNum, "root", fmt.Sprintf("%x", rh))
	}
}

// handleConfirm advances the confirmation watermark and, if block c.blockNum
// was folded speculatively, flushes its held result.
func (cc *commitmentCalculator) handleConfirm(ctx context.Context, c balConfirm) {
	if c.blockNum > cc.lastConfirmedBlock {
		cc.lastConfirmedBlock = c.blockNum
	}
	fold, ok := cc.heldFolds[c.blockNum]
	if !ok {
		return
	}
	delete(cc.heldFolds, c.blockNum)
	cc.confirmFold(ctx, fold)
}

// confirmFold flushes a confirmed block's held branch deltas into its (now
// saved) changeset, writes its commitment state marker into the same
// changeset, then surfaces a wrong-trie-root if the BAL-derived root disagrees
// with the header. Successful roots are silent, matching computeAndCheck.
func (cc *commitmentCalculator) confirmFold(ctx context.Context, fold *balFold) {
	cc.doms.LockChangesetAccumulator()
	err := cc.doms.FlushPendingUpdatesLocked(ctx, cc.roTx)
	if err == nil {
		prev := cc.doms.GetChangesetAccumulatorLocked()
		if cs := cc.doms.GetChangesetByHash(fold.blockNum, fold.blockHash); cs != nil {
			cc.doms.SetChangesetAccumulatorLocked(cs)
		}
		err = cc.doms.GetCommitmentContext().StoreCommitmentState(cc.roTx, fold.blockNum, fold.lastTxNum)
		cc.doms.SetChangesetAccumulatorLocked(prev)
	}
	cc.doms.UnlockChangesetAccumulator()
	if err != nil {
		cc.publish(ctx, commitmentResult{
			blockNum: fold.blockNum,
			txNum:    fold.lastTxNum,
			err:      fmt.Errorf("commitmentCalculator: confirm speculative BAL fold: %w", err),
		})
		return
	}
	if !bytes.Equal(fold.rootHash, fold.stateRoot[:]) {
		cc.publish(ctx, commitmentResult{
			blockNum: fold.blockNum,
			txNum:    fold.lastTxNum,
			rootHash: fold.rootHash,
			err:      fmt.Errorf("%w: block %d root %x expected %x", ErrWrongTrieRoot, fold.blockNum, fold.rootHash, fold.stateRoot),
		})
	}
}
