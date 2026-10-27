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
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/types"
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

type frameValidationResult struct {
	validation *protocol.FrameValidation
	reason     txpoolcfg.DiscardReason
}

// frameValidationBatch keeps simulation results tied to the state they used.
// They may only be applied under the pool lock while that generation still matches.
type frameValidationBatch struct {
	generation uint64
	results    map[*TxnSlot]frameValidationResult
}

// frameBlockUpdate is the validation work left after applying a block update
// under the pool lock. The backend work runs only after that lock is released.
type frameBlockUpdate struct {
	generation uint64
	blockHash  common.Hash
	parked     []*metaTxn
	unwound    TxnSlots
}

var errFrameStateChanged = errors.New("frame validation state changed")

// One deadline covers the whole batch, including waits for the simulation gate.
// A fresh timeout per transaction would let large batches hold up admission.
const frameValidationTimeout = 5 * time.Second

func (tx *TxnSlot) hasKeyedNonce() bool {
	frame, ok := tx.Txn.(*types.FrameTransaction)
	return ok && !frame.UsesLegacyNonce()
}

func (p *TxPool) validateFrame(txn *TxnSlot, isLocal bool) txpoolcfg.DiscardReason {
	if !p.chainConfig.IsBogota(uint64(time.Now().Unix())) {
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
	p.lock.Lock()
	batch := frameValidationBatch{generation: p.stateGeneration, results: make(map[*TxnSlot]frameValidationResult)}
	stateBlockHash := p.stateBlockHash
	for i, txn := range txns.Txns {
		if txn.TxType() != FrameTxnType {
			continue
		}
		batch.results[txn] = frameValidationResult{reason: p.validateFrame(txn, txns.IsLocal[i])}
	}
	p.lock.Unlock()
	for _, txn := range txns.Txns {
		if result, ok := batch.results[txn]; ok && result.reason == txpoolcfg.Success {
			batch.results[txn] = p.simulateFrameResult(ctx, txn, stateBlockHash)
		}
	}
	return batch
}

// applyFrameValidation requires the pool lock. Stale results trigger another
// simulation while time remains; after the batch deadline, only frame transactions
// are rejected so unrelated transactions can still be admitted.
func (p *TxPool) applyFrameValidation(ctx context.Context, txns TxnSlots, batch frameValidationBatch, precheckReasons []txpoolcfg.DiscardReason) ([]txpoolcfg.DiscardReason, error) {
	stale := len(batch.results) > 0 && batch.generation != p.stateGeneration
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
			result = frameValidationResult{reason: txpoolcfg.FrameValidationUnavailable}
		}
		if result.reason == txpoolcfg.Success {
			txn.frameValidation = result.validation
			continue
		}
		reasons[i] = result.reason
		if result.reason == txpoolcfg.FrameValidationUnavailable {
			p.discardReasonsLRU.Add(string(txn.IDHash[:]), result.reason)
		}
	}
	return reasons, nil
}

func (p *TxPool) simulateFrameResult(ctx context.Context, txn *TxnSlot, stateBlockHash common.Hash) frameValidationResult {
	validation, err := p.simulateFrame(ctx, txn, stateBlockHash)
	if err != nil {
		// Backend failures do not establish invalidity. A retryable per-transaction
		// discard lets the rest of the batch or block update continue.
		p.logger.Debug("[txpool] Frame validation unavailable", "hash", txn.IDHash, "err", err)
		return frameValidationResult{reason: txpoolcfg.FrameValidationUnavailable}
	}
	if validation == nil {
		return frameValidationResult{reason: txpoolcfg.InvalidFrame}
	}
	return frameValidationResult{validation: validation, reason: txpoolcfg.Success}
}

func (p *TxPool) simulateFrame(ctx context.Context, txn *TxnSlot, stateBlockHash common.Hash) (*protocol.FrameValidation, error) {
	if p.ethBackend == nil {
		return nil, errors.New("frame validation backend is unavailable")
	}
	// Serialize expensive simulations across admission and revalidation without
	// blocking access to the pool. Waiting here consumes the batch deadline too.
	select {
	case p.frameValidationGate <- struct{}{}:
		defer func() { <-p.frameValidationGate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var data bytes.Buffer
	if err := txn.Txn.MarshalBinary(&data); err != nil {
		return nil, err
	}
	req := &remoteproto.FrameValidationRequest{Transaction: data.Bytes()}
	if stateBlockHash != (common.Hash{}) {
		req.ExpectedBlockHash = gointerfaces.ConvertHashToH256(stateBlockHash)
	}
	reply, err := p.ethBackend.FrameValidation(ctx, req)
	if err != nil {
		return nil, err
	}
	if !reply.Valid {
		return nil, nil
	}
	if len(reply.Payer) != 20 || len(reply.MaxCost) > 32 || len(reply.PayerBalance) > 32 {
		return nil, errors.New("invalid frame validation reply")
	}
	v := &protocol.FrameValidation{Payer: common.BytesToAddress(reply.Payer), Paymaster: reply.Paymaster, Deadline: reply.Deadline}
	v.MaxCost.SetBytes(reply.MaxCost)
	v.PayerBalance.SetBytes(reply.PayerBalance)
	v.Dependencies = make([]common.Address, 0, len(reply.Dependencies)+2)
	v.Dependencies = append(v.Dependencies, txn.Txn.(*types.FrameTransaction).SenderAddress, v.Payer)
	for _, dependency := range reply.Dependencies {
		if len(dependency) != 20 {
			return nil, errors.New("invalid frame validation dependency")
		}
		v.Dependencies = append(v.Dependencies, common.BytesToAddress(dependency))
	}
	return v, nil
}

// checkFrameAdmission checks sender exclusivity and payer reservations under the
// pool lock, after simulation has established the transaction's payer and cost.
func (p *TxPool) checkFrameAdmission(txn *TxnSlot, found *metaTxn) txpoolcfg.DiscardReason {
	v := txn.frameValidation
	pendingFrame := p.all.senderIDFrameTxn[txn.SenderID]
	if v == nil || pendingFrame != nil && pendingFrame != found {
		return txpoolcfg.InvalidFrame
	}
	var previous *protocol.FrameValidation
	if found != nil {
		previous = found.TxnSlot.frameValidation
	}
	return p.checkFrameReservation(v, previous)
}

// checkFrameReservation requires the pool lock. Replacements and revalidation
// reuse the previous reservation, so only their net change counts against balance.
func (p *TxPool) checkFrameReservation(v, previous *protocol.FrameValidation) txpoolcfg.DiscardReason {
	reserved := p.frameReservations[v.Payer]
	if previous != nil && previous.Payer == v.Payer {
		reserved.cost.Sub(&reserved.cost, &previous.MaxCost)
		if previous.Paymaster {
			reserved.paymasters--
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
	return txpoolcfg.NotSet
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
}

// invalidateFrames parks transactions whose validation results may be stale.
// The caller holds the pool lock; revalidateFrames performs the EVM work later.
func (p *TxPool) invalidateFrames(changes *remoteproto.StateChangeBatch) []*metaTxn {
	touched := make(map[common.Address]struct{})
	nonceSlots := make(map[common.Hash]struct{})
	var nonceManagerAccountChanged bool
	for _, batch := range changes.ChangeBatch {
		for _, change := range batch.Changes {
			address := gointerfaces.ConvertH160toAddress(change.Address)
			touched[address] = struct{}{}
			if address == types.FrameNonceManager {
				nonceManagerAccountChanged = nonceManagerAccountChanged || change.Action != remoteproto.Action_STORAGE
				for _, storage := range change.StorageChanges {
					nonceSlots[gointerfaces.ConvertH256ToHash(storage.Location)] = struct{}{}
				}
			}
		}
	}
	var pending []*metaTxn
	for _, mt := range p.byHash {
		previous := mt.TxnSlot.frameValidation
		if previous == nil {
			continue
		}
		frame := mt.TxnSlot.Txn.(*types.FrameTransaction)
		keyedNonce := !frame.UsesLegacyNonce()
		first := &frame.Frames[0]
		// Expiry and blob costs can change with the block context even when none
		// of the validation's account dependencies changed.
		affected := mt.currentSubPool == 0 || first.Target != nil && *first.Target == types.FrameExpiryVerifier || frame.GetBlobGas() != 0
		for _, dependency := range previous.Dependencies {
			// Storage-only changes to the nonce manager need only the selected slots
			// checked below. If the manager is the sender, validation can read all its storage.
			if dependency == types.FrameNonceManager && keyedNonce && frame.SenderAddress != types.FrameNonceManager && !nonceManagerAccountChanged {
				continue
			}
			if _, ok := touched[dependency]; ok {
				affected = true
				break
			}
		}
		if !affected && keyedNonce && len(nonceSlots) > 0 {
			for _, key := range frame.NonceKeys {
				if _, ok := nonceSlots[types.FrameNonceSlot(frame.SenderAddress, key)]; ok {
					affected = true
					break
				}
			}
		}
		if !affected {
			continue
		}
		// Keep the reservation and nonce index, but prevent selection or promotion
		// until validation against the new state completes.
		p.removeFromSubPool(mt, "frame validation pending")
		pending = append(pending, mt)
	}
	return pending
}

// revalidateFrames simulates parked transactions outside the pool lock. Results
// apply only while both the state generation and the pooled entry still match.
func (p *TxPool) revalidateFrames(ctx context.Context, pending []*metaTxn, generation uint64, stateBlockHash common.Hash) {
	if len(pending) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, frameValidationTimeout)
	defer cancel()
	for _, mt := range pending {
		result := p.simulateFrameResult(ctx, mt.TxnSlot, stateBlockHash)
		p.lock.Lock()
		if generation != p.stateGeneration {
			// The newer block update also picks up parked transactions.
			p.lock.Unlock()
			return
		}
		if p.byHash[string(mt.TxnSlot.IDHash[:])] != mt || mt.currentSubPool != 0 {
			p.lock.Unlock()
			continue
		}
		previous, validation := mt.TxnSlot.frameValidation, result.validation
		if validation != nil && p.checkFrameReservation(validation, previous) == txpoolcfg.NotSet {
			p.releaseFrame(previous)
			mt.TxnSlot.frameValidation = validation
			p.reserveFrame(validation)
			p.queued.Add(mt, "frame revalidated", p.logger)
		} else {
			reason := txpoolcfg.FrameStateChanged
			if result.reason == txpoolcfg.FrameValidationUnavailable {
				reason = result.reason
			}
			p.discardLocked(mt, reason)
		}
		p.lock.Unlock()
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	var announcements Announcements
	p.promote(p.pendingBaseFee.Load(), p.pendingBlobFee.Load(), &announcements, p.logger)
	p.pending.EnforceBestInvariants()
	if announcements.Len() > 0 {
		select {
		case p.newPendingTxns <- announcements.Copy():
		default:
		}
	}
}
