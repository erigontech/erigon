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
	"math/big"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type frameReservation struct {
	cost       uint256.Int
	paymasters uint64
}

type frameValidationResult struct {
	validation *protocol.FrameValidation
	reason     txpoolcfg.DiscardReason
}

var errFrameStateChanged = errors.New("frame validation state changed")

const frameValidationTimeout = 5 * time.Second

func (tx *TxnSlot) hasKeyedNonce() bool {
	frame, ok := tx.Txn.(*types.FrameTransaction)
	return ok && !frame.UsesLegacyNonce()
}

func (p *TxPool) validateFrame(txn *TxnSlot, isLocal bool) (txpoolcfg.DiscardReason, error) {
	if !p.chainConfig.IsBogota(uint64(time.Now().Unix())) {
		return txpoolcfg.TypeNotActivated, nil
	}
	frame := txn.Txn.(*types.FrameTransaction)
	if err := protocol.ValidateFrameTransactionPrefix(frame); err != nil {
		return txpoolcfg.InvalidFrame, nil
	}
	if err := frame.Validate(); err != nil {
		return txpoolcfg.InvalidFrame, nil
	}
	if frame.GetFeeCap().Lt(frame.GetTipCap()) {
		return txpoolcfg.TipAboveFeeCap, nil
	}
	if !isLocal && frame.GetFeeCap().LtUint64(p.cfg.MinFeeCap) {
		return txpoolcfg.UnderPriced, nil
	}
	_, _, limits, err := frame.GasLimits()
	if err != nil || limits.Execution > p.blockGasLimit.Load() || limits.State > p.blockGasLimit.Load() {
		return txpoolcfg.GasLimitTooHigh, nil
	}
	return txpoolcfg.Success, nil
}

func (p *TxPool) prepareFrames(ctx context.Context, txns TxnSlots) (uint64, map[*TxnSlot]frameValidationResult) {
	results := make(map[*TxnSlot]frameValidationResult)
	p.lock.Lock()
	generation := p.stateGeneration
	stateBlockHash := p.stateBlockHash
	for i, txn := range txns.Txns {
		if txn.TxType() != FrameTxnType {
			continue
		}
		reason, _ := p.validateFrame(txn, txns.IsLocal[i])
		results[txn] = frameValidationResult{reason: reason}
	}
	p.lock.Unlock()
	for _, txn := range txns.Txns {
		if result, ok := results[txn]; ok && result.reason == txpoolcfg.Success {
			results[txn] = p.simulateFrameResult(ctx, txn, stateBlockHash)
		}
	}
	return generation, results
}

func (p *TxPool) simulateFrameResult(ctx context.Context, txn *TxnSlot, stateBlockHash common.Hash) frameValidationResult {
	validation, err := p.simulateFrame(ctx, txn, stateBlockHash)
	if err != nil {
		p.logger.Debug("[txpool] Frame validation unavailable", "hash", txn.IDHash, "err", err)
		return frameValidationResult{reason: txpoolcfg.ErrFrameValidation}
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

func (p *TxPool) checkFrameReservation(v, previous *protocol.FrameValidation) txpoolcfg.DiscardReason {
	reserved := p.frameReservations[v.Payer]
	if previous != nil && previous.Payer == v.Payer {
		reserved.cost.Sub(&reserved.cost, &previous.MaxCost)
		if previous.Paymaster {
			reserved.paymasters--
		}
	}
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
		affected := mt.currentSubPool == 0 || first.Target != nil && *first.Target == types.FrameExpiryVerifier || frame.GetBlobGas() != 0
		for _, dependency := range previous.Dependencies {
			// The sender's validation can read any of its own storage.
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
			if result.reason == txpoolcfg.ErrFrameValidation {
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

func frameReplacementFee(previous, replacement *uint256.Int, bump uint64) bool {
	if replacement.Cmp(previous) <= 0 {
		return false
	}
	percent := new(big.Int).Add(new(big.Int).SetUint64(bump), big.NewInt(100))
	required := new(big.Int).Mul(previous.ToBig(), percent)
	offered := new(big.Int).Mul(replacement.ToBig(), big.NewInt(100))
	return offered.Cmp(required) >= 0
}
