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

package protocol

import (
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// SystemTxEngineFor returns engine as a SystemTxEngine when txn is one of its
// system transactions, and nil otherwise.
func SystemTxEngineFor(engine rules.EngineReader, txn types.Transaction, header *types.Header) (rules.SystemTxEngine, error) {
	ste, ok := engine.(rules.SystemTxEngine)
	if !ok {
		return nil, nil
	}
	isSystemTx, err := ste.IsSystemTransaction(txn, header)
	if err != nil || !isSystemTx {
		return nil, err
	}
	return ste, nil
}

// ApplySystemTransaction runs a consensus system transaction as a free call,
// outside the block gas pool and without intrinsic gas: the engine applies its
// state effect, then the sender nonce is bumped and the call made. A reverted
// call is an error, since it makes the block invalid.
func ApplySystemTransaction(engine rules.SystemTxEngine, evm *vm.EVM, ibs *state.IntraBlockState, header *types.Header, txn types.Transaction, msg *types.Message) (*evmtypes.ExecutionResult, error) {
	if err := engine.ApplySystemTx(txn, ibs, header); err != nil {
		return nil, err
	}

	from := msg.From()
	nonce, err := ibs.GetNonce(from)
	if err != nil {
		return nil, err
	}
	if err := ibs.SetNonce(from, nonce+1, tracing.NonceChangeEoACall); err != nil {
		return nil, err
	}

	rules := evm.ChainRules()
	if rules.IsCancun {
		ibs.Prepare(rules, from, evm.Context.Coinbase, msg.To(), vm.ActivePrecompiles(rules), msg.AccessList())
	}

	_, _, gasUsed, err := evm.Call(from, msg.To(), msg.Data(), mdgas.MdGas{Execution: msg.Gas()}, *msg.Value(), false)
	if err != nil {
		return nil, err
	}
	return &evmtypes.ExecutionResult{ReceiptGasUsed: gasUsed.Total(), BlockExecutionGasUsed: gasUsed.Total()}, nil
}
