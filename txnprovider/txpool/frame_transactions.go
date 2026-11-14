// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package txpool

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/holiman/uint256"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// frameReservation prevents pooled transactions from promising the same payer
// balance more than once. Reservations are protected by the pool lock.
type frameReservation struct {
	cost       uint256.Int
	paymasters uint64
}

// State-dependent rejections expire with state; sender-policy rejections expire when
// the conflicting transaction leaves the pool. Cache access and blocker membership
// checks require p.lock; copied rejection values can be used after unlocking.
type frameRejection struct {
	generation uint64
	blocker    common.Hash
	reason     txpoolcfg.DiscardReason
}

func (r frameRejection) applies(p *TxPool, generation uint64) bool {
	if r.blocker != (common.Hash{}) {
		_, pooled := p.byHash[string(r.blocker[:])]
		return pooled
	}
	return r.generation == generation
}

func (r frameRejection) validationResult() frameValidationResult {
	return frameValidationResult{reason: r.reason, senderConflict: r.blocker != (common.Hash{})}
}

// RejectFramePrefix caches a prefix policy rejection for gossip deduplication.
// The rejection depends only on the transaction, so it remains valid across state changes.
func (p *TxPool) RejectFramePrefix(hash common.Hash) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.discardReasonsLRU.Add(string(hash[:]), txpoolcfg.InvalidFrame)
}

type frameValidationResult struct {
	validation     *protocol.FrameValidation
	reason         txpoolcfg.DiscardReason
	headChanged    bool
	senderConflict bool
}

// frameValidationBatch keeps simulation results tied to the state they used.
// They may only be applied under the pool lock while that generation still matches.
type frameValidationBatch struct {
	generation uint64
	results    map[*TxnSlot]frameValidationResult
}

// frameRevalidationBatch ties parked entries to the pool's state view while
// their backend validation runs outside the pool lock.
type frameRevalidationBatch struct {
	generation uint64
	blockHash  common.Hash
	parked     []*metaTxn
}

var errFrameStateChanged = errors.New("frame validation state changed")

// A simulation batch shares one deadline, including waits for the gate and head changes.
// Queue and state-recovery waits do not consume it; admission starts a fresh budget
// after recovery. Startup validates one restored frame per batch so earlier frames
// cannot exhaust later budgets.
const frameValidationTimeout = 5 * time.Second

func (tx *TxnSlot) hasKeyedNonce() bool {
	frame, ok := tx.Txn.(*types.FrameTransaction)
	return ok && !frame.UsesLegacyNonce()
}

// Restore frames before ordinary transactions so their parked entries protect
// nonce successors until validation finishes outside the pool lock.
// The caller must hold the pool lock to read the local transaction history.
func (p *TxPool) splitRestoredTxns(txns TxnSlots) (ordinary, frames TxnSlots) {
	for i, txn := range txns.Txns {
		// Replayed notifications must not apply admission penalties to pooled transactions.
		if p.byHash[string(txn.IDHash[:])] != nil {
			continue
		}
		isLocal := p.isLocalLRU.Contains(string(txn.IDHash[:]))
		if txn.TxType() == FrameTxnType {
			frames.Append(txn, txns.Senders.At(i), isLocal)
		} else {
			ordinary.Append(txn, txns.Senders.At(i), isLocal)
		}
	}
	return ordinary, frames
}

// Restored frames have no current validation or payer reservation. Keep them
// indexed but unselectable until revalidation succeeds; a backend failure must
// not lose transactions we already held. The caller holds the pool lock.
func (p *TxPool) restoreFramesLocked(blockNum uint64, txns TxnSlots) (parked []*metaTxn) {
	var announcements Announcements
	cells := make(blobCells)
	for i, txn := range txns.Txns {
		if p.byHash[string(txn.IDHash[:])] != nil {
			continue
		}
		sender, isLocal := txns.Senders.AddressAt(i), txns.IsLocal[i]
		txn.SenderID, txn.Traced = p.senders.getOrCreateID(sender, p.logger)
		reason := p.validateFrame(txn, isLocal)
		if reason == txpoolcfg.Success && txn.HasBlobs() {
			reason = p.validateBlobTxnAndSidecar(txn, isLocal, cells)
		}
		if reason == txpoolcfg.Success {
			txn.frameValidation = nil
			mt := newMetaTxn(txn, isLocal, blockNum)
			if reason = p.addLocked(mt, &announcements, cells); reason == txpoolcfg.NotSet {
				p.senderLastActivity[txn.SenderID] = blockNum
				p.frameAccountsToRefresh[sender] = struct{}{}
				parked = append(parked, mt)
				continue
			}
		}
		p.deletedTxns = append(p.deletedTxns, newMetaTxn(&TxnSlot{IDHash: txn.IDHash, SenderID: txn.SenderID}, false, 0))
	}
	return parked
}

func (p *TxPool) hasParkedLegacyFrame(senderID uint64) bool {
	frame := p.all.senderIDFrameTxn[senderID]
	return frame != nil && !frame.keyedNonce && frame.currentSubPool == 0
}

func (p *TxPool) validateFrame(txn *TxnSlot, isLocal bool) txpoolcfg.DiscardReason {
	if !p.isBogota() {
		return txpoolcfg.TypeNotActivated
	}
	frame := txn.Txn.(*types.FrameTransaction)
	// Bound validation work before checking attacker-supplied signatures.
	if err := protocol.ValidateFrameTransactionPrefix(frame); err != nil {
		return txpoolcfg.InvalidFrame
	}
	if err := frame.Validate(); err != nil {
		return txpoolcfg.InvalidFrame
	}
	if frame.GetFeeCap().Lt(frame.GetTipCap()) {
		return txpoolcfg.TipAboveFeeCap
	}
	if !isLocal && frame.GetFeeCap().LtUint64(p.cfg.MinFeeCap) {
		return txpoolcfg.UnderPriced
	}
	_, _, limits, err := frame.GasLimits()
	if err != nil || limits.Execution > p.blockGasLimit.Load() || limits.State > p.blockGasLimit.Load() {
		return txpoolcfg.GasLimitTooHigh
	}
	return txpoolcfg.Success
}

// prepareFrames checks pool policy under the lock, then simulates against the
// captured head without holding the lock. applyFrameValidation checks the state
// generation under the lock before these results can be used for admission.
func (p *TxPool) prepareFrames(ctx context.Context, txns TxnSlots) frameValidationBatch {
	batch, stateBlockHash := p.precheckFrames(txns)
	for _, txn := range txns.Txns {
		if result, ok := batch.results[txn]; ok && result.reason == txpoolcfg.Success {
			result = p.simulateFrameResult(ctx, txn, batch.generation, stateBlockHash)
			batch.results[txn] = result
			if result.headChanged && p.waitForStateChange(ctx, batch.generation) {
				break
			}
		}
	}
	return batch
}

func (p *TxPool) precheckFrames(txns TxnSlots) (frameValidationBatch, common.Hash) {
	p.lock.Lock()
	defer p.lock.Unlock()
	batch := frameValidationBatch{generation: p.stateGeneration, results: make(map[*TxnSlot]frameValidationResult)}
	for i, txn := range txns.Txns {
		if txn.TxType() != FrameTxnType {
			continue
		}
		if rejection, ok := p.frameRejections.Get(string(txn.IDHash[:])); ok && rejection.applies(p, batch.generation) {
			batch.results[txn] = rejection.validationResult()
			continue
		}
		senderID, _ := p.senders.getID(txns.Senders.AddressAt(i))
		batch.results[txn] = p.precheckFrame(txn, txns.IsLocal[i], senderID)
	}
	return batch, p.stateBlockHash
}

// precheckFrame requires the pool lock.
func (p *TxPool) precheckFrame(txn *TxnSlot, isLocal bool, senderID uint64) frameValidationResult {
	result := frameValidationResult{reason: p.validateFrame(txn, isLocal)}
	if result.reason == txpoolcfg.Success && senderID != 0 {
		result.reason = p.checkFrameSender(txn, senderID)
		result.senderConflict = result.reason != txpoolcfg.Success
	}
	return result
}

// Head publication can precede pool updates. Waiting on the generation also
// covers recovery and same-height reorgs without polling the backend.
func (p *TxPool) waitForStateChange(ctx context.Context, generation uint64) bool {
	return p.waitForCondition(ctx, func() bool { return generation != p.stateGeneration })
}

// applyFrameValidation requires the pool lock. Stale batches and cleared sender
// conflicts are retried while time remains. After the deadline, current prechecks
// preserve specific rejections; frames needing simulation receive a retryable rejection.
// Simulation results for the current state generation remain usable after the deadline.
func (p *TxPool) applyFrameValidation(ctx context.Context, txns TxnSlots, batch frameValidationBatch, precheckReasons []txpoolcfg.DiscardReason, stateCache kvcache.CacheView) ([]txpoolcfg.DiscardReason, error) {
	if len(batch.results) == 0 {
		return precheckReasons, nil
	}
	stale := batch.generation != p.stateGeneration
	if stale && ctx.Err() == nil {
		return nil, errFrameStateChanged
	}
	reasons := slices.Clone(precheckReasons)
	if reasons == nil {
		reasons = make([]txpoolcfg.DiscardReason, len(txns.Txns))
	}
	for i, txn := range txns.Txns {
		result, ok := batch.results[txn]
		if !ok {
			continue
		}
		if stale {
			result = p.precheckFrame(txn, txns.IsLocal[i], txn.SenderID)
			if result.reason == txpoolcfg.Success {
				result.reason = txpoolcfg.FrameValidationUnavailable
			}
		}
		if result.senderConflict && !txn.hasKeyedNonce() {
			// A consumed account nonce takes precedence over sender policy, including
			// cached conflicts. Use the same state view as the rest of admission.
			senderNonce, _, _, err := p.senders.info(stateCache, txn.SenderID)
			if err != nil {
				return nil, err
			}
			if txn.Nonce < senderNonce {
				result.reason = txpoolcfg.NonceTooLow
				p.frameRejections.Add(string(txn.IDHash[:]), frameRejection{generation: p.stateGeneration, reason: result.reason})
			}
		}
		// Pool membership can change without advancing the state generation.
		if result.senderConflict && result.reason == txpoolcfg.InvalidFrame && p.checkFrameSender(txn, txn.SenderID) == txpoolcfg.Success {
			if ctx.Err() == nil {
				return nil, errFrameStateChanged
			}
			result.reason = txpoolcfg.FrameValidationUnavailable
		}
		if result.reason == txpoolcfg.Success {
			// A pooled slot's validation must match its active reservation.
			// Revalidation updates both together; duplicate admission must preserve them.
			if pooled := p.byHash[string(txn.IDHash[:])]; pooled == nil || pooled.TxnSlot != txn {
				txn.frameValidation = result.validation
			}
			continue
		}
		reasons[i] = result.reason
		if result.reason == txpoolcfg.FrameValidationUnavailable {
			p.discardReasonsLRU.Add(string(txn.IDHash[:]), result.reason)
		}
	}
	return reasons, nil
}

func (p *TxPool) simulateFrameResult(ctx context.Context, txn *TxnSlot, generation uint64, stateBlockHash common.Hash) frameValidationResult {
	result, err := p.simulateFrame(ctx, txn, generation, stateBlockHash)
	if err != nil {
		// Backend failures do not establish invalidity. New submissions may retry
		// a head mismatch; admitted transactions remain parked.
		p.logger.Debug("[txpool] Frame validation unavailable", "hash", txn.IDHash, "err", err)
		return frameValidationResult{reason: txpoolcfg.FrameValidationUnavailable, headChanged: status.Code(err) == codes.Aborted}
	}
	return result
}

func (p *TxPool) simulateFrame(ctx context.Context, txn *TxnSlot, generation uint64, stateBlockHash common.Hash) (frameValidationResult, error) {
	if p.ethBackend == nil {
		return frameValidationResult{}, errors.New("frame validation backend is unavailable")
	}
	// Serialize expensive simulations across admission and revalidation without
	// blocking access to the pool. Waiting here consumes the batch deadline too.
	select {
	case p.frameValidationGate <- struct{}{}:
		defer func() { <-p.frameValidationGate }()
	case <-ctx.Done():
		return frameValidationResult{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return frameValidationResult{}, err
	}
	// Another request may have cached a rejection while this one waited.
	// Publish rejections before releasing the gate so its waiters can reuse them.
	hash := string(txn.IDHash[:])
	p.lock.Lock()
	if p.stateViewStale {
		p.lock.Unlock()
		return frameValidationResult{}, errFrameStateChanged
	}
	rejection, rejected := p.frameRejections.Get(hash)
	rejected = rejected && rejection.applies(p, generation)
	p.lock.Unlock()
	if rejected {
		return rejection.validationResult(), nil
	}
	var data bytes.Buffer
	if err := txn.Txn.MarshalBinary(&data); err != nil {
		return frameValidationResult{}, err
	}
	req := &remoteproto.FrameValidationRequest{Transaction: data.Bytes()}
	if stateBlockHash != (common.Hash{}) {
		req.ExpectedBlockHash = gointerfaces.ConvertHashToH256(stateBlockHash)
	}
	reply, err := p.ethBackend.FrameValidation(ctx, req)
	if err != nil {
		return frameValidationResult{}, err
	}
	if !reply.Valid {
		reason := txpoolcfg.InvalidFrame
		if reply.RejectionReason == remoteproto.FrameValidationReply_NONCE_TOO_LOW {
			reason = txpoolcfg.NonceTooLow
		}
		p.lock.Lock()
		if generation == p.stateGeneration {
			p.frameRejections.Add(hash, frameRejection{generation: generation, reason: reason})
		}
		p.lock.Unlock()
		return frameValidationResult{reason: reason}, nil
	}
	if reply.Payer.GetHi() == nil ||
		reply.MaxCost.GetHi() == nil || reply.MaxCost.GetLo() == nil ||
		reply.PayerBalance.GetHi() == nil || reply.PayerBalance.GetLo() == nil {
		return frameValidationResult{}, errors.New("invalid frame validation reply")
	}
	v := &protocol.FrameValidation{
		Payer: gointerfaces.ConvertH160toAddress(reply.Payer), Paymaster: reply.Paymaster, Deadline: reply.Deadline,
		MaxCost: *gointerfaces.ConvertH256ToUint256Int(reply.MaxCost), PayerBalance: *gointerfaces.ConvertH256ToUint256Int(reply.PayerBalance),
	}
	v.Dependencies = make([]common.Address, 0, len(reply.Dependencies)+2)
	v.Dependencies = append(v.Dependencies, txn.Txn.(*types.FrameTransaction).SenderAddress, v.Payer)
	for _, dependency := range reply.Dependencies {
		if dependency.GetHi() == nil {
			return frameValidationResult{}, errors.New("invalid frame validation dependency")
		}
		v.Dependencies = append(v.Dependencies, gointerfaces.ConvertH160toAddress(dependency))
	}
	for _, dependency := range reply.CreationNonces {
		if dependency == nil || dependency.Address.GetHi() == nil || dependency.MinNonce > dependency.MaxNonce {
			return frameValidationResult{}, errors.New("invalid frame creation nonce dependency")
		}
		address := gointerfaces.ConvertH160toAddress(dependency.Address)
		v.Dependencies = append(v.Dependencies, address)
		v.CreationNonces = append(v.CreationNonces, protocol.FrameCreationNonce{
			Address: address, MinNonce: dependency.MinNonce, MaxNonce: dependency.MaxNonce,
		})
	}
	// Root storage references are derived locally from frame calldata. The RPC
	// reply records the verifier address so code changes also trigger revalidation.
	v.RecentRoots = protocol.FrameTransactionRecentRoots(txn.Txn.(*types.FrameTransaction))
	return frameValidationResult{validation: v, reason: txpoolcfg.Success}, nil
}

// checkFrameSender requires the pool lock. A matching nonce and key set may replace the existing frame.
func (p *TxPool) checkFrameSender(txn *TxnSlot, senderID uint64) txpoolcfg.DiscardReason {
	pooled := p.all.senderIDFrameTxn[senderID]
	if pooled == nil {
		return txpoolcfg.Success
	}
	pooledFrame := pooled.TxnSlot.Txn.(*types.FrameTransaction)
	candidateFrame := txn.Txn.(*types.FrameTransaction)
	if pooled.TxnSlot.Nonce != txn.Nonce || !slices.Equal(pooledFrame.NonceKeys, candidateFrame.NonceKeys) {
		p.frameRejections.Add(string(txn.IDHash[:]), frameRejection{blocker: pooled.TxnSlot.IDHash, reason: txpoolcfg.InvalidFrame})
		return txpoolcfg.InvalidFrame
	}
	return txpoolcfg.Success
}

// checkFrameAdmission checks sender exclusivity and any payer reservation under
// the pool lock. Restored frames remain parked without a validation result.
func (p *TxPool) checkFrameAdmission(txn *TxnSlot, found *metaTxn) txpoolcfg.DiscardReason {
	// Simulation releases the pool lock, so sender conflicts must be checked again.
	if reason := p.checkFrameSender(txn, txn.SenderID); reason != txpoolcfg.Success {
		return reason
	}
	v := txn.frameValidation
	if v == nil {
		// A restored candidate cannot displace an admitted transaction before validation.
		if found != nil {
			return txpoolcfg.NotReplaced
		}
		return txpoolcfg.NotSet
	}
	return p.checkFrameReservation(v, found)
}

// checkFrameReservation requires the pool lock. Replacements and revalidation
// reuse the previous reservation, so only their net change counts against balance.
func (p *TxPool) checkFrameReservation(v *protocol.FrameValidation, previous *metaTxn) txpoolcfg.DiscardReason {
	reserved := p.frameReservations[v.Payer]
	if previous != nil {
		if old := previous.TxnSlot.frameValidation; old != nil && old.Payer == v.Payer {
			reserved.cost.Sub(&reserved.cost, &old.MaxCost)
			if old.Paymaster {
				reserved.paymasters--
			}
		}
	}
	// All coded paymasters use the non-canonical pool policy: one pending frame
	// transaction per paymaster, even when its balance could cover more.
	if v.Paymaster && reserved.paymasters != 0 {
		return txpoolcfg.InvalidFrame
	}
	var total uint256.Int
	if _, overflow := total.AddOverflow(&reserved.cost, &v.MaxCost); overflow || total.Gt(&v.PayerBalance) {
		return txpoolcfg.InsufficientFunds
	}
	// Ordinary transactions spend the same balance. Include their pooled costs,
	// except for the transaction being replaced, before reserving a frame's fees.
	funded := true
	if senderID, ok := p.senders.getID(v.Payer); ok {
		p.all.ascend(senderID, func(mt *metaTxn) bool {
			if mt == previous {
				return true
			}
			_, overflow := total.AddOverflow(&total, requiredBalance(mt.TxnSlot))
			funded = !overflow && !total.Gt(&v.PayerBalance)
			return funded
		})
	}
	if !funded {
		return txpoolcfg.InsufficientFunds
	}
	return txpoolcfg.NotSet
}

// Blocked nonce chains may exceed the queued limit, and parked frames are
// outside the sub-pools. Both still count toward total capacity.
func (p *TxPool) enforceTotalCapacity(pendingBaseFee uint64) {
	excess := len(p.byHash) - p.pending.limit - p.baseFee.limit - p.queued.limit
	if excess <= 0 {
		return
	}
	var parked []*metaTxn
	for _, mt := range p.all.senderIDFrameTxn {
		if mt.currentSubPool == 0 {
			parked = append(parked, mt)
		}
	}
	fee := *uint256.NewInt(pendingBaseFee)
	sort.Slice(parked, func(i, j int) bool { return parked[i].worse(parked[j], fee) })
	for excess > 0 && len(parked) > 0 {
		worst := parked[0]
		for _, candidate := range []*metaTxn{p.pending.Worst(), p.baseFee.Worst(), p.queued.Worst()} {
			if candidate != nil && candidate.worse(worst, fee) {
				worst = candidate
			}
		}
		reason := txpoolcfg.QueuedPoolOverflow
		switch worst.currentSubPool {
		case PendingSubPool:
			reason = txpoolcfg.PendingPoolOverflow
		case BaseFeeSubPool:
			reason = txpoolcfg.BaseFeePoolOverflow
		}
		if worst.currentSubPool == 0 {
			parked = parked[1:]
		} else {
			p.removeFromSubPool(worst, "total pool overflow")
			p.frameAccountsToRefresh[p.senders.senderID2Addr[worst.TxnSlot.SenderID]] = struct{}{}
		}
		p.discardLocked(worst, reason)
		excess--
	}
}

func (p *TxPool) reserveFrame(v *protocol.FrameValidation) {
	if v == nil {
		return
	}
	if p.frameReservations == nil {
		p.frameReservations = make(map[common.Address]frameReservation)
	}
	r := p.frameReservations[v.Payer]
	r.cost.Add(&r.cost, &v.MaxCost)
	if v.Paymaster {
		r.paymasters++
	}
	p.frameReservations[v.Payer] = r
	p.frameAccountsToRefresh[v.Payer] = struct{}{}
}

func (p *TxPool) releaseFrame(v *protocol.FrameValidation) {
	if v == nil {
		return
	}
	r := p.frameReservations[v.Payer]
	r.cost.Sub(&r.cost, &v.MaxCost)
	if v.Paymaster {
		r.paymasters--
	}
	if r.cost.IsZero() && r.paymasters == 0 {
		delete(p.frameReservations, v.Payer)
	} else {
		p.frameReservations[v.Payer] = r
	}
	p.frameAccountsToRefresh[v.Payer] = struct{}{}
}

// Frame removal and payer reservations can change ordinary transaction eligibility.
// Defer refreshes until promotion has a current state view and no sender-index
// iteration is in progress.
func (p *TxPool) refreshFrameAccounts(cacheView kvcache.CacheView) error {
	for len(p.frameAccountsToRefresh) > 0 {
		for address := range p.frameAccountsToRefresh {
			senderID, ok := p.senders.getID(address)
			if !ok || !p.all.hasTxns(senderID) {
				delete(p.frameAccountsToRefresh, address)
				continue
			}
			if err := p.refreshSender(cacheView, senderID); err != nil {
				return err
			}
		}
	}
	return nil
}

// Parked frames keep their reservations and nonce indexes. Refresh legacy-nonce
// senders so their successors also stop being selectable until validation succeeds.
func (p *TxPool) parkFrameLocked(mt *metaTxn, reason string) {
	p.removeFromSubPool(mt, reason)
	frame := mt.TxnSlot.Txn.(*types.FrameTransaction)
	if frame.UsesLegacyNonce() {
		p.frameAccountsToRefresh[frame.SenderAddress] = struct{}{}
	}
}

type bogotaActivation struct {
	active      bool
	crossedBack bool
}

func (p *TxPool) RetainBogotaActivation(changes *remoteproto.StateChangeBatch) {
	p.lock.Lock()
	defer p.lock.Unlock()
	activation := p.bogotaActivationAfter(changes)
	p.pendingActivation = &activation
}

func (p *TxPool) bogotaActivationAfter(changes *remoteproto.StateChangeBatch) bogotaActivation {
	activation := bogotaActivation{active: p.headIsBogota}
	if p.pendingActivation != nil {
		activation = *p.pendingActivation
	}
	activeBefore := activation.active
	// Forward history can overlap a subscription reset. Intermediate forward entries
	// do not prove a reorg; an explicit unwind must survive later reactivation.
	for _, change := range changes.ChangeBatch {
		active := p.chainConfig.IsBogota(change.BlockTime)
		if change.Direction == remoteproto.Direction_UNWIND && activation.active && !active {
			activation.crossedBack = true
		}
		activation.active = active
	}
	// An incomplete reset may omit the unwind entries, but its final head still
	// proves a crossing back before hardfork activation.
	if changes.StateChangesIncomplete && activeBefore && !activation.active {
		activation.crossedBack = true
	}
	return activation
}

// invalidateFrames drops expired transactions and parks those whose validation may be stale.
// The caller holds the pool lock; revalidateFrames performs the EVM work later.
func (p *TxPool) invalidateFrames(changes *remoteproto.StateChangeBatch) []*metaTxn {
	head := changes.ChangeBatch[len(changes.ChangeBatch)-1]
	if len(p.all.senderIDFrameTxn) == 0 {
		return nil
	}
	blockTime := head.BlockTime
	// Expiry uses the same block time as validation.
	pendingTime := max(blockTime+p.chainConfig.SecondsPerSlot(), uint64(time.Now().Unix()))
	pendingSlot := protocol.FrameValidationSlot(head.SlotNumber)
	touched := make(map[common.Address]struct{})
	codeChanged := make(map[common.Address]struct{})
	accountChanges := make(map[common.Address][]byte)
	nonceSlots := make(map[common.Hash]struct{})
	rootSlots := make(map[common.Hash]struct{})
	activationCrossedBack := p.bogotaActivationAfter(changes).crossedBack
	for _, batch := range changes.ChangeBatch {
		for _, change := range batch.Changes {
			address := gointerfaces.ConvertH160toAddress(change.Address)
			touched[address] = struct{}{}
			switch change.Action {
			case remoteproto.Action_CODE, remoteproto.Action_UPSERT_CODE, remoteproto.Action_REMOVE:
				codeChanged[address] = struct{}{}
			}
			if change.Action == remoteproto.Action_UPSERT || change.Action == remoteproto.Action_UPSERT_CODE {
				accountChanges[address] = change.Data
			}
			if address == types.FrameNonceManager {
				for _, storage := range change.StorageChanges {
					nonceSlots[gointerfaces.ConvertH256ToHash(storage.Location)] = struct{}{}
				}
			}
			if address == types.FrameRecentRootVerifier {
				for _, storage := range change.StorageChanges {
					rootSlots[gointerfaces.ConvertH256ToHash(storage.Location)] = struct{}{}
				}
			}
		}
	}
	var parked []*metaTxn
	for _, mt := range p.all.senderIDFrameTxn {
		frame := mt.TxnSlot.Txn.(*types.FrameTransaction)
		previous := mt.TxnSlot.frameValidation
		var roots []protocol.RecentRootReference
		if previous != nil {
			roots = previous.RecentRoots
		} else {
			// Root age and activation are independent of simulation results.
			roots = protocol.FrameTransactionRecentRoots(frame)
		}
		invalidRoots := slices.ContainsFunc(roots, func(ref protocol.RecentRootReference) bool {
			return activationCrossedBack || !ref.ValidAt(pendingSlot)
		})
		if invalidRoots {
			p.removeFromSubPool(mt, "recent root no longer usable")
			p.discardLocked(mt, txpoolcfg.FrameStateChanged)
			continue
		}
		if previous == nil {
			parked = append(parked, mt)
			continue
		}
		// Blob costs can change with the block context even when none
		// of the validation's account dependencies changed.
		affected := mt.currentSubPool == 0 || frame.GetBlobGas() != 0 || changes.StateChangesIncomplete
		// The signed deadline needs only a timestamp check. Changes to the verifier's
		// code still trigger validation through its recorded dependency below.
		if frame.Frames[0].IsExpiryVerifier() && previous.Deadline < pendingTime {
			p.removeFromSubPool(mt, "frame expired")
			p.discardLocked(mt, txpoolcfg.FrameStateChanged)
			continue
		}
		keyedNonce := !frame.UsesLegacyNonce()
		// Helper account changes matter only when code changes. Sender and payer
		// state can also affect approval or funding. Storage slots and creation
		// nonces are checked separately below.
		for _, dependency := range previous.Dependencies {
			changed := codeChanged
			if dependency == frame.SenderAddress || dependency == previous.Payer {
				changed = touched
			}
			if _, ok := changed[dependency]; ok {
				affected = true
				break
			}
		}
		if !affected {
			for _, dependency := range previous.CreationNonces {
				data, ok := accountChanges[dependency.Address]
				if !ok {
					continue
				}
				var account accounts.Account
				if len(data) == 0 || accounts.DeserialiseV3(&account, data) != nil || account.Nonce < dependency.MinNonce || account.Nonce > dependency.MaxNonce {
					affected = true
					break
				}
			}
		}
		if !affected && keyedNonce && len(nonceSlots) > 0 {
			affected = slices.ContainsFunc(frame.NonceKeys, func(key uint256.Int) bool {
				_, changed := nonceSlots[types.FrameNonceSlot(frame.SenderAddress, key)]
				return changed
			})
		}
		if !affected && len(rootSlots) > 0 {
			affected = slices.ContainsFunc(roots, func(ref protocol.RecentRootReference) bool {
				_, changed := rootSlots[ref.StorageKey]
				return changed
			})
		}
		if !affected {
			continue
		}
		p.parkFrameLocked(mt, "frame validation pending")
		parked = append(parked, mt)
	}
	return parked
}

// revalidateFrames simulates parked transactions outside the pool lock. Results
// apply only while both the state generation and the pooled entry still match.
func (p *TxPool) revalidateFrames(ctx context.Context, parked []*metaTxn, generation uint64, stateBlockHash common.Hash) error {
	if len(parked) == 0 {
		return nil
	}
	validationCtx, cancel := context.WithTimeout(ctx, frameValidationTimeout)
	defer cancel()
	for _, mt := range parked {
		result := p.simulateFrameResult(validationCtx, mt.TxnSlot, generation, stateBlockHash)
		if result.reason == txpoolcfg.FrameValidationUnavailable {
			// Keep admitted transactions parked. The next block update retries
			// them even without further dependency changes.
			continue
		}
		p.lock.Lock()
		if generation != p.stateGeneration {
			// The newer block update also picks up parked transactions.
			p.lock.Unlock()
			return nil
		}
		if p.byHash[string(mt.TxnSlot.IDHash[:])] != mt || mt.currentSubPool != 0 {
			p.lock.Unlock()
			continue
		}
		previous, validation := mt.TxnSlot.frameValidation, result.validation
		if validation != nil && p.checkFrameReservation(validation, mt) == txpoolcfg.NotSet {
			p.releaseFrame(previous)
			mt.TxnSlot.frameValidation = validation
			p.reserveFrame(validation)
			p.queued.Add(mt, "frame revalidated", p.logger)
			frame := mt.TxnSlot.Txn.(*types.FrameTransaction)
			if frame.UsesLegacyNonce() {
				p.frameAccountsToRefresh[frame.SenderAddress] = struct{}{}
			}
		} else {
			p.discardLocked(mt, txpoolcfg.FrameStateChanged)
		}
		p.lock.Unlock()
	}
	return p.withLockedState(ctx, nil, true, func(_ kv.TemporalTx, cacheView kvcache.CacheView) error {
		var announcements Announcements
		if err := p.promote(cacheView, p.pendingBaseFee.Load(), p.pendingBlobFee.Load(), &announcements); err != nil {
			return err
		}
		p.notifyPendingTxns(announcements)
		p.publishDepthMetrics()
		return nil
	})
}
