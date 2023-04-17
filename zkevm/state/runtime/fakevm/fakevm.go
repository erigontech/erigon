package fakevm

import (
	"sync/atomic"

	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
)

// MemoryItemSize is the memory item size.
const MemoryItemSize int = 32

// FakeEVM represents the fake EVM.
type FakeEVM struct {
	// Context provides auxiliary blockchain related information
	Context evmtypes.BlockContext
	evmtypes.TxContext
	// StateDB gives access to the underlying state
	StateDB FakeDB
	// chainConfig contains information about the current chain
	chainConfig *chain.Config
	// chain rules contains the chain rules for the current epoch
	chainRules *chain.Rules
	// virtual machine configuration options used to initialise the
	// global (to this context) ethereum virtual machine
	// used throughout the execution of the tx.
	Config Config
	// abort is used to abort the EVM calling operations
	// NOTE: must be set atomically
	abort int32
}

// NewFakeEVM returns a new EVM. The returned EVM is not thread safe and should
// only ever be used *once*.
// func NewFakeEVM(blockCtx vm.BlockContext, txCtx vm.TxContext, statedb runtime.FakeDB, chainConfig *params.ChainConfig, config Config) *FakeEVM {
func NewFakeEVM(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, chainConfig *chain.Config, config Config) *FakeEVM {
	evm := &FakeEVM{
		Context:     blockCtx,
		TxContext:   txCtx,
		Config:      config,
		chainConfig: chainConfig,
		chainRules:  chainConfig.Rules(blockCtx.BlockNumber, blockCtx.Time),
	}
	return evm
}

// SetStateDB is the StateDB setter.
func (evm *FakeEVM) SetStateDB(stateDB FakeDB) {
	evm.StateDB = stateDB
}

// Cancel cancels any running EVM operation. This may be called concurrently and
// it's safe to be called multiple times.
func (evm *FakeEVM) Cancel() {
	atomic.StoreInt32(&evm.abort, 1)
}

// ChainConfig returns the environment's chain configuration
func (evm *FakeEVM) ChainConfig() *chain.Config { return evm.chainConfig }

// ScopeContext contains the things that are per-call, such as stack and memory,
// but not transients like pc and gas
type ScopeContext struct {
	Memory   *Memory
	Stack    *Stack
	Contract *vm.Contract
}
