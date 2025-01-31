// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"context"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/arb/arbitrum_types"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/event"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/services"
)

// Installs an Arbitrum TxProcessor, enabling ArbOS for this state transition (see vm/evm_arbitrum.go)
var ReadyEVMForL2 func(evm *vm.EVM, msg *types.Message)

// Allows ArbOS to swap out or return early from an RPC message to support the NodeInterface virtual contract
var InterceptRPCMessage = func(
	msg *types.Message,
	ctx context.Context,
	statedb *state.IntraBlockState,
	header *types.Header,
	backend NodeInterfaceBackendAPI,
	blockCtx *evmtypes.BlockContext,
) (*types.Message, *evmtypes.ExecutionResult, error) {
	return msg, nil, nil
}

// Gets ArbOS's maximum intended gas per second
var GetArbOSSpeedLimitPerSecond func(statedb state.IntraBlockStateArbitrum) (uint64, error)

// Allows ArbOS to update the gas cap so that it ignores the message's specific L1 poster costs.
var InterceptRPCGasCap = func(gascap *uint64, msg *types.Message, header *types.Header, statedb *state.IntraBlockState) {}

// Renders a solidity error in human-readable form
var RenderRPCError func(data []byte) error

type NodeInterfaceBackendAPI interface {
	ChainConfig() *chain.Config
	// CurrentHeader() *types.Header
	CurrentBlock() *types.Block
	BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error)
	HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error)
	GetLogs(ctx context.Context, blockHash common.Hash, number uint64) ([][]*types.Log, error)
	GetEVM(ctx context.Context, msg *types.Message, state *state.IntraBlockState, header *types.Header, vmConfig *vm.Config, blockCtx *evmtypes.BlockContext) *vm.EVM
}

// Arbitrum widely uses BlockChain structure so better to wrap interface here
type BlockChain interface {
	services.FullBlockReader
	ChainReader() consensus.ChainHeaderReader // may be useful more than embedding of FullBlockReader itself

	// Config retrieves the chain's fork configuration.
	Config() *chain.Config

	// Stop stops the blockchain service. If any imports are currently in progress
	// it will abort them using the procInterrupt.
	Stop()

	// State returns a new mutable state based on the current HEAD block.
	State() (state.IntraBlockStateArbitrum, error)

	// StateAt returns a new mutable state based on a particular point in time.
	StateAt(root common.Hash) (state.IntraBlockStateArbitrum, error)

	ClipToPostNitroGenesis(blockNum rpc.BlockNumber) (rpc.BlockNumber, rpc.BlockNumber)

	RecoverState(block *types.Block) error

	ReorgToOldBlock(newHead *types.Block) error

	// WriteBlockAndSetHeadWithTime also counts processTime, which will cause intermittent TrieDirty cache writes
	WriteBlockAndSetHeadWithTime(block *types.Block, receipts []*types.Receipt, logs []*types.Log, state state.IntraBlockStateArbitrum, emitHeadEvent bool, processTime time.Duration) (status WriteStatus, err error)

	GetReceiptsByHash(hash common.Hash) types.Receipts
	// StateCache returns the caching database underpinning the blockchain instance.
	StateCache() kv.RwDB

	ResetWithGenesisBlock(gb *types.Block)
	SubscribeNewTxsEvent(ch chan<- NewTxsEvent) event.Subscription
	EnqueueL2Message(ctx context.Context, tx types.Transaction, options *arbitrum_types.ConditionalOptions) error

	// GetVMConfig returns the block chain VM config.
	GetVMConfig() *vm.Config

	Engine() consensus.Engine

	GetTd(common.Hash, uint64) *big.Int
	// Processor returns the current processor.
	Processor() Processor
}

// Processor is an interface for processing blocks using a given initial state.
type Processor interface {
	// Process processes the state changes according to the Ethereum rules by running
	// the transaction messages using the statedb and applying any rewards to both
	// the processor (coinbase) and any included uncles.
	Process(block *types.Block, statedb state.IntraBlockStateArbitrum, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error)
}

// func (b *Backend) ResetWithGenesisBlock(gb *types.Block) {
// 	b.arb.BlockChain().ResetWithGenesisBlock(gb)
// }

// func (b *Backend) EnqueueL2Message(ctx context.Context, tx *types.Transaction, options *arbitrum_types.ConditionalOptions) error {
// 	return b.arb.PublishTransaction(ctx, tx, options)
// }

// func (b *Backend) SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription {
// 	return b.scope.Track(b.txFeed.Subscribe(ch))
// }

// Processor returns the current processor.
// func (bc *BlockChain) Processor() Processor {
// 	return bc.processor
// }

// // State returns a new mutable state based on the current HEAD block.
// func (bc *BlockChain) State() (*state.StateDB, error) {
// 	return bc.StateAt(bc.CurrentBlock().Root)
// }

// // StateAt returns a new mutable state based on a particular point in time.
// func (bc *BlockChain) StateAt(root common.Hash) (*state.StateDB, error) {
// 	return state.New(root, bc.stateCache, bc.snaps)
// }
