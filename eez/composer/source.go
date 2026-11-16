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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/chain"
	core "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

var (
	// ErrNoCalls refuses a transaction that calls no cross-chain proxy.
	ErrNoCalls = errors.New("the transaction calls no cross-chain proxy")
	// ErrRevertedCall refuses a transaction in which a frame enclosing a
	// cross-chain call fails: L1 would undo what the rollup kept.
	ErrRevertedCall = errors.New("a frame enclosing a cross-chain call failed")
)

// poison wraps the reason a transaction can never compose.
type poison struct{ err error }

func (p *poison) Error() string { return p.err.Error() }
func (p *poison) Unwrap() error { return p.err }

// Poison marks err as final for its transaction: retrying cannot help.
func Poison(err error) error { return &poison{err} }

// IsPoison reports an error Poison marked.
func IsPoison(err error) bool {
	_, ok := errors.AsType[*poison](err)
	return ok
}

// Call is a cross-chain call a source simulation met: a call to a proxy.
type Call struct {
	// Target is the contract the proxy stands for, on rollup RollupID.
	Target   common.Address
	RollupID uint64
	// Source is the proxy's caller, on rollup SourceRollup.
	Source       common.Address
	SourceRollup uint64
	Value        uint256.Int
	Data         []byte
	// Return is the target's output.
	Return []byte
}

// Dispatcher runs c on its target rollup and returns the target's output.
type Dispatcher func(c *Call) ([]byte, error)

// Source simulates the held transactions of one source chain on a fork of
// its state at a block: a call to a proxy registered with manager runs on
// its rollup through the dispatcher, and returns there without running the
// proxy, moving value or spending gas. A transaction it keeps leaves its
// writes for the next.
type Source struct {
	config    *chain.Config
	header    *types.Header
	ibs       *state.IntraBlockState
	reads     *reads
	blockHash func(uint64) (common.Hash, error)
	manager   accounts.Address
	rollup    uint64
	index     int
}

// NewSource returns the simulation of the chain of config in the context of
// the block of header, on the state reader reads and the block hashes
// blockHash returns; rollup is the chain's rollup id.
func NewSource(config *chain.Config, header *types.Header, reader state.StateReader, blockHash func(uint64) (common.Hash, error), manager common.Address, rollup uint64) *Source {
	r := &reads{StateReader: reader}
	return &Source{config: config, header: header, ibs: state.New(r), reads: r, blockHash: r.blockHash(blockHash), manager: accounts.InternAddress(manager), rollup: rollup}
}

// Nonce returns addr's nonce in the simulation.
func (s *Source) Nonce(addr common.Address) (uint64, error) {
	n, err := s.ibs.GetNonce(accounts.InternAddress(addr))
	if failed := s.reads.failed(); failed != nil {
		return 0, failed
	}
	return n, err
}

// Run simulates tx, dispatching each proxy call it makes, and returns the
// calls in order. It keeps tx's writes when accept, if set, takes the calls,
// and returns accept's error otherwise. Errors from the dispatcher come back
// as they are, and a failed state read as ErrStateRead; a transaction
// without calls or with a failed frame around one is poison.
func (s *Source) Run(tx types.Transaction, dispatch Dispatcher, accept func([]*Call) error) ([]*Call, error) {
	var (
		calls   []*Call
		failure error
		starts  []int
		spanned bool
	)
	hook := func(evm *vm.EVM, depth int, caller, addr accounts.Address, input []byte, value uint256.Int) ([]byte, bool, error) {
		if failure != nil {
			return nil, false, nil
		}
		word, err := evm.IntraBlockState().GetState(s.manager, accounts.InternKey(protocol.ProxyKey(addr.Value())))
		if err != nil {
			failure = err
			return nil, true, vm.ErrExecutionReverted
		}
		p, ok := protocol.DecodeProxy(word.Bytes32())
		if !ok {
			return nil, false, nil
		}
		c := &Call{Target: p.Address, RollupID: p.RollupID, Source: caller.Value(), SourceRollup: s.rollup, Value: value, Data: bytes.Clone(input)}
		out, err := dispatch(c)
		if err != nil {
			failure = err
			return nil, true, vm.ErrExecutionReverted
		}
		c.Return = out
		calls = append(calls, c)
		return out, true, nil
	}
	tracer := &tracing.Hooks{
		OnEnter: func(int, byte, accounts.Address, accounts.Address, bool, []byte, uint64, uint256.Int, []byte) {
			starts = append(starts, len(calls))
		},
		OnExit: func(_ int, _ []byte, _ uint64, err error, reverted bool) {
			start := starts[len(starts)-1]
			starts = starts[:len(starts)-1]
			if (reverted || err != nil) && len(calls) > start {
				spanned = true
			}
		},
	}
	evm := core.CreateEVM(s.config, s.blockHash, nil, accounts.InternAddress(s.header.Coinbase), s.ibs, s.header, vm.Config{CallOverride: hook, Tracer: tracer})
	rules := evm.ChainRules()
	msg, err := tx.AsMessage(*types.MakeSigner(s.config, s.header.Number.Uint64(), s.header.Time), s.header.BaseFee, rules)
	if err != nil {
		return nil, Poison(err)
	}
	snapshot := s.ibs.PushSnapshot()
	s.ibs.SetTxContext(s.header.Number.Uint64(), s.index)
	evm.Reset(core.NewEVMTxContext(msg), s.ibs)
	gp := new(core.GasPool).AddGas(s.header.GasLimit).AddBlobGas(s.config.GetMaxBlobGasPerBlock(s.header.Time))
	_, err = core.ApplyMessage(evm, msg, gp, true, false, nil)
	switch {
	case s.reads.failed() != nil:
		err = s.reads.failed()
	case failure != nil:
		err = failure
	case err != nil:
		err = Poison(fmt.Errorf("transaction invalid on the source chain: %w", err))
	case spanned:
		err = Poison(ErrRevertedCall)
	case len(calls) == 0:
		err = Poison(ErrNoCalls)
	case accept != nil:
		err = accept(calls)
	}
	if err != nil {
		s.ibs.RevertToSnapshot(snapshot, nil)
		return nil, err
	}
	s.ibs.PopSnapshot(snapshot)
	if err := s.ibs.FinalizeTx(rules, state.NewNoopWriter()); err != nil {
		return nil, err
	}
	s.index++
	return calls, nil
}
