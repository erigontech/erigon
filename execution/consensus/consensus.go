// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package consensus implements different Ethereum consensus engines.
package consensus

import (
	"math/big"

	"github.com/holiman/uint256"

	common "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// ChainHeaderReader defines a small collection of methods needed to access the local
// blockchain during header verification.
//
//go:generate mockgen -typed=true -destination=./chain_header_reader_mock.go -package=consensus . ChainHeaderReader
type ChainHeaderReader interface {
	// Config retrieves the blockchain's chain configuration.
	Config() *chain.Config

	// CurrentHeader retrieves the current header from the local chain.
	CurrentHeader() *types.Header

	// CurrentFinalizedHeader retrieves the current finalized header from the local chain.
	CurrentFinalizedHeader() *types.Header

	// CurrentSafeHeader retrieves the current safe header from the local chain.
	CurrentSafeHeader() *types.Header

	// GetHeader retrieves a block header from the database by hash and number.
	GetHeader(hash common.Hash, number uint64) *types.Header

	// GetHeaderByNumber retrieves a block header from the database by number.
	GetHeaderByNumber(number uint64) *types.Header

	// GetHeaderByHash retrieves a block header from the database by its hash.
	GetHeaderByHash(hash common.Hash) *types.Header

	// GetTd retrieves the total difficulty from the database by hash and number.
	GetTd(hash common.Hash, number uint64) *big.Int

	// Number of blocks frozen in the block snapshots
	FrozenBlocks() uint64
	FrozenBorBlocks(align bool) uint64
}

// ChainReader defines a small collection of methods needed to access the local
// blockchain during header and/or uncle verification.
//
//go:generate mockgen -typed=true -destination=./chain_reader_mock.go -package=consensus . ChainReader
type ChainReader interface {
	ChainHeaderReader
	// GetBlock retrieves a block from the database by hash and number.
	GetBlock(hash common.Hash, number uint64) *types.Block
	HasBlock(hash common.Hash, number uint64) bool
}

type SystemCall func(contract common.Address, data []byte) ([]byte, error)

// Use more options to call contract
type SysCallCustom func(contract common.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error)
type Call func(contract common.Address, data []byte) ([]byte, error)

// RewardKind - The kind of block reward.
// Depending on the consensus engine the allocated block reward might have
// different semantics which could lead e.g. to different reward values.
type RewardKind uint16

const (
	// RewardAuthor - attributed to the block author.
	RewardAuthor RewardKind = 0
	// RewardEmptyStep - attributed to the author(s) of empty step(s) included in the block (AuthorityRound engine).
	RewardEmptyStep RewardKind = 1
	// RewardExternal - attributed by an external protocol (e.g. block reward contract).
	RewardExternal RewardKind = 2
	// RewardUncle - attributed to the block uncle(s) with given difference.
	RewardUncle RewardKind = 3
)

type Reward struct {
	Beneficiary common.Address
	Kind        RewardKind
	Amount      uint256.Int
}

// Engine is an algorithm agnostic consensus engine.
type Engine interface {
	EngineReader
	EngineWriter
}

// EngineReader are read-only methods of the consensus engine
// All of these methods should have thread-safe implementations
type EngineReader interface {
	// Author retrieves the Ethereum address of the account that minted the given
	// block, which may be different from the header's coinbase if a consensus
	// engine is based on signatures.
	Author(header *types.Header) (common.Address, error)

	// Dependencies retrives the dependencies between transactions
	// included in the block accosiated with this header a nil return
	// implies no dependencies are known
	TxDependencies(header *types.Header) [][]int

	// Service transactions are free and don't pay baseFee after EIP-1559
	IsServiceTransaction(sender common.Address, syscall SystemCall) bool

	Type() chain.ConsensusName

	CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall SystemCall,
	) ([]Reward, error)

	GetTransferFunc() evmtypes.TransferFunc

	GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc

	// Close terminates any background threads, DB's etc maintained by the consensus engine.
	Close() error
}

// EngineWriter are write methods of the consensus engine
type EngineWriter interface {
	// VerifyHeader checks whether a header conforms to the consensus rules of a
	// given engine. Verifying the seal may be done optionally here, or explicitly
	// via the VerifySeal method.
	VerifyHeader(chain ChainHeaderReader, header *types.Header, seal bool) error

	// VerifyUncles verifies that the given block's uncles conform to the consensus
	// rules of a given engine.
	VerifyUncles(chain ChainReader, header *types.Header, uncles []*types.Header) error

	// Prepare initializes the consensus fields of a block header according to the
	// rules of a particular engine. The changes are executed inline.
	Prepare(chain ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error

	// Initialize runs any pre-transaction state modifications (e.g. epoch start)
	Initialize(config *chain.Config, chain ChainHeaderReader, header *types.Header,
		state *state.IntraBlockState, syscall SysCallCustom, logger log.Logger, tracer *tracing.Hooks)

	// Finalize runs any post-transaction state modifications (e.g. block rewards)
	// but does not assemble the block.
	Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
		txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chain ChainReader, syscall SystemCall, skipReceiptsEval bool, logger log.Logger,
	) (types.FlatRequests, error)

	// FinalizeAndAssemble runs any post-transaction state modifications (e.g. block
	// rewards) and assembles the final block.
	//
	// Note: The block header and state database might be updated to reflect any
	// consensus rules that happen at finalization (e.g. block rewards).
	FinalizeAndAssemble(config *chain.Config, header *types.Header, state *state.IntraBlockState,
		txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chain ChainReader, syscall SystemCall, call Call, logger log.Logger,
	) (*types.Block, types.FlatRequests, error)

	// Seal generates a new sealing request for the given input block and pushes
	// the result into the given channel.
	//
	// Note, the method returns immediately and will send the result async. More
	// than one result may also be returned depending on the consensus algorithm.
	Seal(chain ChainHeaderReader, block *types.BlockWithReceipts, results chan<- *types.BlockWithReceipts, stop <-chan struct{}) error

	// SealHash returns the hash of a block prior to it being sealed.
	SealHash(header *types.Header) common.Hash

	// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
	// that a new block should have.
	CalcDifficulty(chain ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64,
		parentHash, parentUncleHash common.Hash, parentAuRaStep uint64) *big.Int

	// APIs returns the RPC APIs this consensus engine provides.
	APIs(chain ChainHeaderReader) []rpc.API
}

// PoW is a consensus engine based on proof-of-work.
type PoW interface {
	Engine

	// Hashrate returns the current mining hashrate of a PoW consensus engine.
	Hashrate() float64
}

// Transfer subtracts amount from sender and adds amount to recipient using the given Db
func Transfer(db evmtypes.IntraBlockState, sender, recipient common.Address, amount *uint256.Int, bailout bool) error {
	if !bailout {
		err := db.SubBalance(sender, *amount, tracing.BalanceChangeTransfer)
		if err != nil {
			return err
		}
	}
	return db.AddBalance(recipient, *amount, tracing.BalanceChangeTransfer)
}
