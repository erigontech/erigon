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
	core "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// errPostingClosed refuses an outbound call after a trigger ran: the
// posting precedes the triggers of its bundle.
var errPostingClosed = errors.New("the posting ran before the triggers")

var (
	// ErrFunding refuses a call whose value the manager cannot pay now.
	ErrFunding = errors.New("the manager cannot pay the call's value")
	// ErrToManager refuses a call to the manager, which the manager refuses.
	ErrToManager = errors.New("a call to the manager")
	// ErrCallFailed refuses a call that fails on its target chain.
	ErrCallFailed = errors.New("the call failed on its target chain")
	// ErrReentry refuses a call whose target calls a cross-chain proxy.
	ErrReentry = errors.New("the target calls a cross-chain proxy")
)

const (
	// viewGas bounds a manager's view frame.
	viewGas = 1_000_000
	// frameGas bounds a frame that writes, within the block's gas limit.
	frameGas = 30_000_000
)

// postingPhase is where a source is in its posting.
type postingPhase uint8

const (
	// postingIdle is before any outbound call.
	postingIdle postingPhase = iota
	// postingOpen runs outbound calls in the posting's transaction.
	postingOpen
	// postingClosed is after a trigger ran.
	postingClosed
)

// PostedBy sets the posting's sender, from, its tip and its gas limit:
// outbound calls run with from as their origin, the block's base fee plus
// tip as their gas price, and within gasLimit, which the posting buys from
// from's balance as it opens.
func (s *Source) PostedBy(from common.Address, tip *uint256.Int, gasLimit uint64) {
	s.origin = accounts.InternAddress(from)
	s.gasPrice.Add(s.header.BaseFee, tip)
	s.gasLimit = gasLimit
}

// Atomic runs f as the next entry of the posting, the one L1 transaction
// that runs a batch's outbound calls in order: transient storage and warm
// accounts carry from one entry to the next. The writes f makes through
// Execute stay when it returns nil. Once Run ran, it refuses with
// errPostingClosed.
func (s *Source) Atomic(f func() error) error {
	switch s.posting {
	case postingClosed:
		return errPostingClosed
	case postingIdle:
		s.ibs.Prepare(s.rules, s.origin, accounts.InternAddress(s.header.Coinbase), s.manager, vm.ActivePrecompiles(s.rules), nil)
		if err := s.buyGas(); err != nil {
			return err
		}
		s.posting = postingOpen
	}
	return s.entry(f)
}

// buyGas debits the posting's gas limit at its price from its sender, as L1
// does before the posting runs. The refund of the gas it leaves is not
// modelled: the posting's gas is known only once its batch is.
func (s *Source) buyGas() error {
	var fee uint256.Int
	if fee.Mul(uint256.NewInt(s.gasLimit), &s.gasPrice); fee.IsZero() {
		return nil
	}
	balance, err := s.ibs.GetBalance(s.origin)
	if failed := s.reads.failed(); failed != nil {
		return failed
	}
	if err != nil {
		return err
	}
	if balance.Lt(&fee) {
		return fmt.Errorf("the poster's balance %s does not buy the posting's gas, %s", &balance, &fee)
	}
	return s.ibs.SubBalance(s.origin, fee, tracing.BalanceDecreaseGasBuy)
}

// closePosting ends the posting, which precedes the triggers.
func (s *Source) closePosting() error {
	open := s.posting == postingOpen
	s.posting = postingClosed
	if !open {
		return nil
	}
	return s.finalize()
}

// entry runs f, undoing its writes when it fails.
func (s *Source) entry(f func() error) error {
	snapshot, spent := s.ibs.PushSnapshot(), s.spent
	err := f()
	if failed := s.reads.failed(); failed != nil {
		err = failed
	}
	if err != nil {
		s.ibs.RevertToSnapshot(snapshot, nil)
		s.spent = spent
		return err
	}
	s.ibs.PopSnapshot(snapshot)
	return nil
}

// finalize ends the current transaction.
func (s *Source) finalize() error {
	if err := s.ibs.FinalizeTx(s.rules, state.NewNoopWriter()); err != nil {
		return err
	}
	s.index++
	return nil
}

// Execute runs c, a call from a rollup to a contract of this chain, as the
// manager delivers it: through the proxy of c's source, created first when
// missing, from the manager with c's value. It returns the target's output
// and sets c.Gas to the gas the call used, the proxy's creation included;
// its writes are the enclosing transaction's, Atomic's. A value the manager
// cannot pay returns ErrFunding. A call to the manager, a failed call and a
// target calling a proxy are poison.
func (s *Source) Execute(c *Call) ([]byte, error) {
	manager := s.manager.Value()
	if c.Target == manager {
		return nil, Poison(ErrToManager)
	}
	out, err := s.view(protocol.EncodeComputeCrossChainProxyAddress(c.Source, c.SourceRollup))
	if err != nil {
		return nil, err
	}
	proxy, err := protocol.DecodeAddressReturn(out)
	if err != nil {
		return nil, Poison(err)
	}
	_, registered, err := s.proxyAt(proxy)
	if err != nil {
		return nil, err
	}
	// A frame failing on what the entries before left the posting may run
	// in a posting of its own: it waits.
	before := s.spent
	refuse := func(err error) error {
		if s.frameGas(before) < s.frameGas(0) {
			return fmt.Errorf("%w: %w", errOverBudget, err)
		}
		return Poison(err)
	}
	var created uint64
	if !registered {
		// The posting creates it, in the call's entry.
		if _, created, err = s.frame(common.Address{}, manager, protocol.EncodeCreateCrossChainProxy(c.Source, c.SourceRollup), uint256.Int{}, nil, nil); err != nil {
			return nil, refuse(fmt.Errorf("create the proxy of %s on rollup %d: %w", c.Source, c.SourceRollup, err))
		}
		s.spent += created
	}
	data, err := protocol.EncodeExecuteOnBehalf(c.Target, 0, c.Data)
	if err != nil {
		return nil, Poison(err)
	}
	// The top frame is the manager's call to the proxy itself.
	isProxy := func(depth int, addr accounts.Address) bool {
		if depth == 0 {
			return false
		}
		_, ok, err := s.proxyAt(addr.Value())
		return err == nil && ok
	}
	hook := func(_ *vm.EVM, depth int, _, addr accounts.Address, _ []byte, _ uint256.Int) ([]byte, bool, error) {
		if !isProxy(depth, addr) {
			return nil, false, nil
		}
		return nil, true, vm.ErrExecutionReverted
	}
	// The override skips static frames; entering a proxy from one is poison
	// as well.
	reentered := false
	tracer := &tracing.Hooks{OnEnter: func(depth int, _ byte, _, to accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
		if isProxy(depth, to) {
			reentered = true
		}
	}}
	ret, used, err := s.frame(manager, proxy, data, c.Value, hook, tracer)
	switch {
	case errors.Is(err, vm.ErrInsufficientBalance):
		return nil, fmt.Errorf("%w: %w", ErrFunding, err)
	case reentered:
		return nil, Poison(ErrReentry)
	case err != nil:
		return nil, refuse(fmt.Errorf("%w: %w %x", ErrCallFailed, err, ret))
	}
	s.spent += used
	c.Gas = created + used
	return ret, nil
}

// frameGas is the gas a frame gets once the posting's entries spent spent:
// at most what the posting's gas limit leaves, less the 1/64 the posting's
// self-call for the entry and the manager's call to the proxy each
// withhold. The posting's calldata and the work before its entries are
// priced only with its batch: it is an upper bound.
func (s *Source) frameGas(spent uint64) uint64 {
	gas := min(frameGas, s.header.GasLimit)
	if s.gasLimit == 0 {
		return gas
	}
	left := s.gasLimit - min(spent, s.gasLimit)
	left -= left / 64
	left -= left / 64
	return min(gas, left)
}

// view runs a read-only frame of the manager.
func (s *Source) view(data []byte) ([]byte, error) {
	evm := core.CreateEVM(s.config, s.blockHash, nil, accounts.InternAddress(s.header.Coinbase), s.ibs, s.header, vm.Config{NoBaseFee: true})
	evm.Reset(evmtypes.TxContext{}, s.ibs)
	ret, _, _, err := evm.StaticCall(accounts.ZeroAddress, s.manager, data, mdgas.MdGas{Execution: min(viewGas, s.header.GasLimit)})
	if err != nil {
		return nil, Poison(fmt.Errorf("manager view failed: %w", err))
	}
	return ret, nil
}

// frame calls `to` from `from` with value inside the posting, as a
// contract would: no intrinsic gas, nonce or fee.
func (s *Source) frame(from, to common.Address, data []byte, value uint256.Int, override vm.CallOverride, tracer *tracing.Hooks) ([]byte, uint64, error) {
	gas := s.frameGas(s.spent)
	evm := core.CreateEVM(s.config, s.blockHash, nil, accounts.InternAddress(s.header.Coinbase), s.ibs, s.header, vm.Config{NoBaseFee: true, CallOverride: override, Tracer: tracer})
	evm.Reset(evmtypes.TxContext{Origin: s.origin, GasPrice: s.gasPrice}, s.ibs)
	ret, left, _, err := evm.Call(accounts.InternAddress(from), accounts.InternAddress(to), data, mdgas.MdGas{Execution: gas}, value, false)
	// A top frame's output lives in the state's buffer, which the next one
	// overwrites.
	return bytes.Clone(ret), gas - left.Total(), err
}
