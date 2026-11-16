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

package composer

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/protocol"
	core "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// executeOnBehalf is the selector of CrossChainProxy.executeOnBehalf, the
// call EEZL2 delivers through.
var executeOnBehalf = crypto.Keccak256([]byte("executeOnBehalf(address,uint64,bytes)"))[:4]

// ErrFull reports a Sync block without the gas for one more delivery.
var ErrFull = errors.New("the Sync block is full")

// Delivery is an incoming call delivered on a Sync block draft.
type Delivery struct {
	Call *protocol.IncomingCall
	// Sidecar is the call's inbound sidecar, the entry a batch carries.
	Sidecar *protocol.ExecutionEntry
	// Tx is the delivery's system transaction.
	Tx types.Transaction
}

// Deliveries delivers calls into the rollup of ctx on a fork of a Sync block
// draft. Each call is probed first: delivered with empty return data, the
// frame in which EEZL2 calls the proxy shows the target's output; the call
// is then delivered with it, and must succeed.
type Deliveries struct {
	ctx  protocol.SyncTxContext
	fork *Draft
	// Delivered are the deliveries made on fork, in order.
	Delivered []*Delivery
}

// NewDeliveries returns the deliveries of calls into ctx.RollupID on a fork of
// draft.
func NewDeliveries(ctx protocol.SyncTxContext, draft *Draft) (*Deliveries, error) {
	fork, err := draft.Fork()
	if err != nil {
		return nil, err
	}
	return &Deliveries{ctx: ctx, fork: fork}, nil
}

// Dispatch delivers c, returning the target's output. A failed state read
// returns ErrStateRead.
func (in *Deliveries) Dispatch(c *Call) ([]byte, error) {
	switch {
	case c.RollupID != in.ctx.RollupID:
		return nil, Poison(fmt.Errorf("call to rollup %d, not %d", c.RollupID, in.ctx.RollupID))
	case c.Target == in.ctx.EEZL2:
		return nil, Poison(errors.New("a call to EEZL2 is not deliverable"))
	}
	// refuse marks err final unless a state read failed.
	refuse := func(err error) error {
		if errors.Is(err, ErrStateRead) {
			return err
		}
		return Poison(err)
	}
	call := &protocol.IncomingCall{
		SourceAddress: c.Source, SourceRollupID: c.SourceRollup, TargetAddress: c.Target, RollupID: c.RollupID,
		Value: c.Value.ToBig(), Data: c.Data,
	}
	probe, err := in.delivery(call)
	if err != nil {
		return nil, refuse(err)
	}
	fork, err := in.fork.Fork()
	if err != nil {
		return nil, err
	}
	t := &probeTracer{eezl2: accounts.InternAddress(in.ctx.EEZL2)}
	if _, err := fork.Apply(probe.Tx, t.hooks()); errors.Is(err, core.ErrGasLimitReached) {
		return nil, ErrFull
	} else if err != nil {
		return nil, refuse(fmt.Errorf("probe of the delivery to %s: %w", c.Target, err))
	}
	if call.ReturnData, err = probed(t.captures); err != nil {
		return nil, Poison(fmt.Errorf("delivery to %s: %w", c.Target, err))
	}
	d, err := in.delivery(call)
	if err != nil {
		return nil, refuse(err)
	}
	// The probe passed the same gas check on the same block.
	receipt, err := in.fork.Apply(d.Tx, nil)
	if err != nil {
		return nil, refuse(fmt.Errorf("delivery to %s: %w", c.Target, err))
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return nil, Poison(fmt.Errorf("delivery to %s with its probed output failed", c.Target))
	}
	in.Delivered = append(in.Delivered, d)
	return call.ReturnData, nil
}

// delivery builds the system transaction delivering call at the fork's
// next system nonce.
func (in *Deliveries) delivery(call *protocol.IncomingCall) (*Delivery, error) {
	sidecar, err := protocol.NewInboundSidecar(call)
	if err != nil {
		return nil, err
	}
	nonce, err := in.fork.Nonce(protocol.SystemAddress)
	if err != nil {
		return nil, err
	}
	pairs, err := protocol.BuildSyncPairs(nil, []protocol.ExecutionEntry{*sidecar}, in.ctx, nonce)
	if err != nil {
		return nil, err
	}
	if len(pairs) != 1 {
		return nil, fmt.Errorf("a call lowered to %d deliveries", len(pairs))
	}
	tx, err := types.DecodeTransaction(pairs[0].SystemTx)
	if err != nil {
		return nil, err
	}
	return &Delivery{Call: call, Sidecar: sidecar, Tx: tx}, nil
}

type capture struct {
	success bool
	output  []byte
}

// probed is the target's output in a probe that captured captures: one
// successful call through the proxy.
func probed(captures []capture) ([]byte, error) {
	switch {
	case len(captures) == 0:
		return nil, errors.New("never called its proxy")
	case len(captures) > 1:
		return nil, fmt.Errorf("called proxies %d times: one call per delivery", len(captures))
	case !captures[0].success:
		return nil, fmt.Errorf("target reverted: %x", captures[0].output)
	}
	return captures[0].output, nil
}

// probeTracer captures the calls EEZL2 makes through executeOnBehalf.
type probeTracer struct {
	eezl2    accounts.Address
	frames   []bool
	captures []capture
}

func (p *probeTracer) hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnEnter: func(_ int, typ byte, from, _ accounts.Address, _ bool, input []byte, _ uint64, _ uint256.Int, _ []byte) {
			p.frames = append(p.frames, vm.OpCode(typ) == vm.CALL && from == p.eezl2 && bytes.HasPrefix(input, executeOnBehalf))
		},
		OnExit: func(_ int, output []byte, _ uint64, err error, reverted bool) {
			matched := p.frames[len(p.frames)-1]
			p.frames = p.frames[:len(p.frames)-1]
			if matched {
				p.captures = append(p.captures, capture{success: err == nil && !reverted, output: bytes.Clone(output)})
			}
		},
	}
}
