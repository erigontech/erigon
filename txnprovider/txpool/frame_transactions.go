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

func (p *TxPool) validateFrame(txn *TxnSlot, isLocal bool) (txpoolcfg.DiscardReason, error) {
	if !p.chainConfig.IsEIPEnabled(8141, uint64(time.Now().Unix())) {
		return txpoolcfg.TypeNotActivated, nil
	}
	frame := txn.Txn.(*types.FrameTransaction)
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
	if frame.GetBlobGas() != 0 {
		if reason := p.validateBlobTxn(txn, isLocal); reason != txpoolcfg.Success {
			return reason, nil
		}
	}
	validation, err := p.simulateFrame(txn)
	if err != nil {
		return txpoolcfg.InvalidFrame, err
	}
	if validation == nil {
		return txpoolcfg.InvalidFrame, nil
	}
	txn.frameValidation = validation
	return txpoolcfg.Success, nil
}

func (p *TxPool) simulateFrame(txn *TxnSlot) (*protocol.FrameValidation, error) {
	if p.ethBackend == nil {
		return nil, errors.New("frame validation backend is unavailable")
	}
	var data bytes.Buffer
	if err := txn.Txn.MarshalBinary(&data); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, err := p.ethBackend.FrameValidation(ctx, &remoteproto.FrameValidationRequest{Transaction: data.Bytes()})
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

func (p *TxPool) revalidateFrames(changes *remoteproto.StateChangeBatch) error {
	touched := make(map[common.Address]struct{})
	for _, batch := range changes.ChangeBatch {
		for _, change := range batch.Changes {
			touched[gointerfaces.ConvertH160toAddress(change.Address)] = struct{}{}
		}
	}
	for _, mt := range p.byHash {
		previous := mt.TxnSlot.frameValidation
		if previous == nil {
			continue
		}
		first := &mt.TxnSlot.Txn.(*types.FrameTransaction).Frames[0]
		affected := first.Target != nil && *first.Target == types.FrameExpiryVerifier || mt.TxnSlot.Txn.GetBlobGas() != 0
		for _, dependency := range previous.Dependencies {
			if _, ok := touched[dependency]; ok {
				affected = true
				break
			}
		}
		if !affected {
			continue
		}
		validation, err := p.simulateFrame(mt.TxnSlot)
		if err != nil {
			return err
		}
		reason := txpoolcfg.InvalidFrame
		if validation != nil {
			reason = p.checkFrameReservation(validation, previous)
		}
		if reason != txpoolcfg.NotSet {
			p.removeFromSubPool(mt, "frame revalidation")
			p.discardLocked(mt, reason)
			continue
		}
		p.releaseFrame(previous)
		mt.TxnSlot.frameValidation = validation
		p.reserveFrame(validation)
	}
	return nil
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
