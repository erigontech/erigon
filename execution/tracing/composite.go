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

package tracing

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// NewComposite returns a single *Hooks that dispatches every event to all
// non-nil hooks in the provided slice.
//
// Rules:
//   - nil entries are ignored.
//   - Zero non-nil hooks → nil returned.
//   - One non-nil hook  → returned as-is (no wrapper).
//   - Two or more       → new *Hooks where each field calls all implementations.
func NewComposite(hooks ...*Hooks) *Hooks {
	var valid []*Hooks
	for _, h := range hooks {
		if h != nil {
			valid = append(valid, h)
		}
	}
	switch len(valid) {
	case 0:
		return nil
	case 1:
		return valid[0]
	}

	var (
		onTxStart        []TxStartHook
		onTxEnd          []TxEndHook
		onEnter          []EnterHook
		onExit           []ExitHook
		onOpcode         []OpcodeHook
		onFault          []FaultHook
		onGasChange      []GasChangeHook
		onBlockInit      []BlockchainInitHook
		onBlockStart     []BlockStartHook
		onBlockEnd       []BlockEndHook
		onGenesis        []GenesisBlockHook
		onSysCallStart   []OnSystemCallStartHook
		onSysCallEnd     []OnSystemCallEndHook
		onBalChange      []BalanceChangeHook
		onNonceChange    []NonceChangeHook
		onCodeChange     []CodeChangeHook
		onStorageChange  []StorageChangeHook
		onLog            []LogHook
		flush            []func(types.Transaction)
	)

	for _, h := range valid {
		if h.OnTxStart != nil {
			onTxStart = append(onTxStart, h.OnTxStart)
		}
		if h.OnTxEnd != nil {
			onTxEnd = append(onTxEnd, h.OnTxEnd)
		}
		if h.OnEnter != nil {
			onEnter = append(onEnter, h.OnEnter)
		}
		if h.OnExit != nil {
			onExit = append(onExit, h.OnExit)
		}
		if h.OnOpcode != nil {
			onOpcode = append(onOpcode, h.OnOpcode)
		}
		if h.OnFault != nil {
			onFault = append(onFault, h.OnFault)
		}
		if h.OnGasChange != nil {
			onGasChange = append(onGasChange, h.OnGasChange)
		}
		if h.OnBlockchainInit != nil {
			onBlockInit = append(onBlockInit, h.OnBlockchainInit)
		}
		if h.OnBlockStart != nil {
			onBlockStart = append(onBlockStart, h.OnBlockStart)
		}
		if h.OnBlockEnd != nil {
			onBlockEnd = append(onBlockEnd, h.OnBlockEnd)
		}
		if h.OnGenesisBlock != nil {
			onGenesis = append(onGenesis, h.OnGenesisBlock)
		}
		if h.OnSystemCallStart != nil {
			onSysCallStart = append(onSysCallStart, h.OnSystemCallStart)
		}
		if h.OnSystemCallEnd != nil {
			onSysCallEnd = append(onSysCallEnd, h.OnSystemCallEnd)
		}
		if h.OnBalanceChange != nil {
			onBalChange = append(onBalChange, h.OnBalanceChange)
		}
		if h.OnNonceChange != nil {
			onNonceChange = append(onNonceChange, h.OnNonceChange)
		}
		if h.OnCodeChange != nil {
			onCodeChange = append(onCodeChange, h.OnCodeChange)
		}
		if h.OnStorageChange != nil {
			onStorageChange = append(onStorageChange, h.OnStorageChange)
		}
		if h.OnLog != nil {
			onLog = append(onLog, h.OnLog)
		}
		if h.Flush != nil {
			flush = append(flush, h.Flush)
		}
	}

	out := &Hooks{}

	if len(onTxStart) > 0 {
		fs := onTxStart
		out.OnTxStart = func(vm *VMContext, txn types.Transaction, from accounts.Address) {
			for _, f := range fs {
				f(vm, txn, from)
			}
		}
	}
	if len(onTxEnd) > 0 {
		fs := onTxEnd
		out.OnTxEnd = func(receipt *types.Receipt, err error) {
			for _, f := range fs {
				f(receipt, err)
			}
		}
	}
	if len(onEnter) > 0 {
		fs := onEnter
		out.OnEnter = func(depth int, typ byte, from, to accounts.Address, precompile bool, input []byte, gas uint64, value uint256.Int, code []byte) {
			for _, f := range fs {
				f(depth, typ, from, to, precompile, input, gas, value, code)
			}
		}
	}
	if len(onExit) > 0 {
		fs := onExit
		out.OnExit = func(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
			for _, f := range fs {
				f(depth, output, gasUsed, err, reverted)
			}
		}
	}
	if len(onOpcode) > 0 {
		fs := onOpcode
		out.OnOpcode = func(pc uint64, op byte, gas, cost uint64, scope OpContext, rData []byte, depth int, err error) {
			for _, f := range fs {
				f(pc, op, gas, cost, scope, rData, depth, err)
			}
		}
	}
	if len(onFault) > 0 {
		fs := onFault
		out.OnFault = func(pc uint64, op byte, gas, cost uint64, scope OpContext, depth int, err error) {
			for _, f := range fs {
				f(pc, op, gas, cost, scope, depth, err)
			}
		}
	}
	if len(onGasChange) > 0 {
		fs := onGasChange
		out.OnGasChange = func(old, new uint64, reason GasChangeReason) {
			for _, f := range fs {
				f(old, new, reason)
			}
		}
	}
	if len(onBlockInit) > 0 {
		fs := onBlockInit
		out.OnBlockchainInit = func(cfg *chain.Config) {
			for _, f := range fs {
				f(cfg)
			}
		}
	}
	if len(onBlockStart) > 0 {
		fs := onBlockStart
		out.OnBlockStart = func(event BlockEvent) {
			for _, f := range fs {
				f(event)
			}
		}
	}
	if len(onBlockEnd) > 0 {
		fs := onBlockEnd
		out.OnBlockEnd = func(err error) {
			for _, f := range fs {
				f(err)
			}
		}
	}
	if len(onGenesis) > 0 {
		fs := onGenesis
		out.OnGenesisBlock = func(genesis *types.Block, alloc types.GenesisAlloc) {
			for _, f := range fs {
				f(genesis, alloc)
			}
		}
	}
	if len(onSysCallStart) > 0 {
		fs := onSysCallStart
		out.OnSystemCallStart = func() {
			for _, f := range fs {
				f()
			}
		}
	}
	if len(onSysCallEnd) > 0 {
		fs := onSysCallEnd
		out.OnSystemCallEnd = func() {
			for _, f := range fs {
				f()
			}
		}
	}
	if len(onBalChange) > 0 {
		fs := onBalChange
		out.OnBalanceChange = func(addr accounts.Address, prev, new uint256.Int, reason BalanceChangeReason) {
			for _, f := range fs {
				f(addr, prev, new, reason)
			}
		}
	}
	if len(onNonceChange) > 0 {
		fs := onNonceChange
		out.OnNonceChange = func(addr accounts.Address, prev, new uint64) {
			for _, f := range fs {
				f(addr, prev, new)
			}
		}
	}
	if len(onCodeChange) > 0 {
		fs := onCodeChange
		out.OnCodeChange = func(addr accounts.Address, prevHash accounts.CodeHash, prevCode []byte, hash accounts.CodeHash, code []byte) {
			for _, f := range fs {
				f(addr, prevHash, prevCode, hash, code)
			}
		}
	}
	if len(onStorageChange) > 0 {
		fs := onStorageChange
		out.OnStorageChange = func(addr accounts.Address, slot accounts.StorageKey, prev, new uint256.Int) {
			for _, f := range fs {
				f(addr, slot, prev, new)
			}
		}
	}
	if len(onLog) > 0 {
		fs := onLog
		out.OnLog = func(log *types.Log) {
			for _, f := range fs {
				f(log)
			}
		}
	}
	if len(flush) > 0 {
		fs := flush
		out.Flush = func(tx types.Transaction) {
			for _, f := range fs {
				f(tx)
			}
		}
	}

	return out
}
