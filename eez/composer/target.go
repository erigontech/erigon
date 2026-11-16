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
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

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

// Atomic runs f as one transaction on the simulation's state, sent by the
// manager: the writes it makes through Execute stay when it returns nil.
func (s *Source) Atomic(f func() error) error {
	// One access list for the transaction: a snapshot reverts against it.
	s.ibs.Prepare(s.rules, s.manager, accounts.InternAddress(s.header.Coinbase), accounts.NilAddress, vm.ActivePrecompiles(s.rules), nil)
	return s.atomic(f)
}

func (s *Source) atomic(f func() error) error {
	snapshot := s.ibs.PushSnapshot()
	err := f()
	if failed := s.reads.failed(); failed != nil {
		err = failed
	}
	if err != nil {
		s.ibs.RevertToSnapshot(snapshot, nil)
		return err
	}
	s.ibs.PopSnapshot(snapshot)
	if err := s.ibs.FinalizeTx(s.rules, state.NewNoopWriter()); err != nil {
		return err
	}
	s.index++
	return nil
}

// Execute runs c, a call from a rollup to a contract of this chain, as the
// manager delivers it: through the proxy of c's source, created first when
// missing, from the manager with c's value. It returns the target's output
// and sets c.Gas to the gas the call used; its writes are the enclosing
// transaction's, Atomic's. A value the manager cannot pay returns
// ErrFunding. A call to the manager, a failed call and a target calling a
// proxy are poison.
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
	word, err := s.ibs.GetState(s.manager, accounts.InternKey(protocol.ProxyKey(proxy)))
	if err != nil {
		return nil, err
	}
	if _, ok := protocol.DecodeProxy(word.Bytes32()); !ok {
		if _, _, err := s.frame(common.Address{}, manager, protocol.EncodeCreateCrossChainProxy(c.Source, c.SourceRollup), uint256.Int{}, frameGas, nil); err != nil {
			return nil, Poison(fmt.Errorf("create the proxy of %s on rollup %d: %w", c.Source, c.SourceRollup, err))
		}
	}
	data, err := protocol.EncodeExecuteOnBehalf(c.Target, 0, c.Data)
	if err != nil {
		return nil, Poison(err)
	}
	reentered := false
	// The top frame is the manager's call to the proxy itself.
	hook := func(evm *vm.EVM, depth int, _, addr accounts.Address, _ []byte, _ uint256.Int) ([]byte, bool, error) {
		if depth == 0 {
			return nil, false, nil
		}
		word, err := evm.IntraBlockState().GetState(s.manager, accounts.InternKey(protocol.ProxyKey(addr.Value())))
		if err != nil {
			return nil, false, nil
		}
		if _, ok := protocol.DecodeProxy(word.Bytes32()); !ok {
			return nil, false, nil
		}
		reentered = true
		return nil, true, vm.ErrExecutionReverted
	}
	ret, used, err := s.frame(manager, proxy, data, c.Value, frameGas, hook)
	switch {
	case errors.Is(err, vm.ErrInsufficientBalance):
		return nil, fmt.Errorf("%w: %w", ErrFunding, err)
	case reentered:
		return nil, Poison(ErrReentry)
	case err != nil:
		return nil, Poison(fmt.Errorf("%w: %w %x", ErrCallFailed, err, ret))
	}
	c.Gas = used
	return ret, nil
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

// frame calls `to` from `from` with value inside the current transaction,
// as a contract would: no intrinsic gas, nonce or fee.
func (s *Source) frame(from, to common.Address, data []byte, value uint256.Int, gas uint64, override vm.CallOverride) ([]byte, uint64, error) {
	gas = min(gas, s.header.GasLimit)
	evm := core.CreateEVM(s.config, s.blockHash, nil, accounts.InternAddress(s.header.Coinbase), s.ibs, s.header, vm.Config{NoBaseFee: true, CallOverride: override})
	evm.Reset(evmtypes.TxContext{Origin: accounts.InternAddress(from)}, s.ibs)
	ret, left, _, err := evm.Call(accounts.InternAddress(from), accounts.InternAddress(to), data, mdgas.MdGas{Execution: gas}, value, false)
	// A top frame's output lives in the state's buffer, which the next one
	// overwrites.
	return bytes.Clone(ret), gas - left.Total(), err
}
