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

package ethash

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/consensus/misc"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
)

// Ethash proof-of-work protocol constants.
var (
	maxUncles                     = 2         // Maximum number of uncles allowed in a single block
	allowedFutureBlockTimeSeconds = int64(15) // Max seconds from current time allowed for blocks, before they're considered future blocks

)

// Various error messages to mark blocks invalid. These should be private to
// prevent engine specific errors from being referenced in the remainder of the
// codebase, inherently breaking if the engine is swapped out. Please put common
// error types into the consensus package.
var (
	errOlderBlockTime = errors.New("timestamp older than parent")
	errInvalidPoW     = errors.New("invalid proof-of-work")
)

// Ethash is a consensus engine based on proof-of-work implementing the ethash
// algorithm.
type BaseFake struct {
}

// Type returns underlying consensus engine
func (BaseFake) Type() chain.ConsensusName {
	return chain.EtHashConsensus
}

// Author implements consensus.Engine, returning the header's coinbase as the
// proof-of-work verified author of the block.
// This is thread-safe (only access the header.Coinbase)
func (BaseFake) Author(header *types.Header) (libcommon.Address, error) {
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules of the
// stock Ethereum ethash engine.
func (b BaseFake) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	// Short circuit if the header is known, or its parent not
	number := header.Number.Uint64()
	if chain.GetHeader(header.Hash(), number) != nil {
		return nil
	}
	if number == 0 {
		return nil
	}
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		log.Error("consensus.ErrUnknownAncestor", "parentNum", number-1, "hash", header.ParentHash.String())
		return consensus.ErrUnknownAncestor
	}
	// Sanity checks passed, do a proper verification
	return b.verifyHeader(chain, header, parent, false, seal)
}

// VerifyUncles verifies that the given block's uncles conform to the consensus
// rules of the stock Ethereum ethash engine.
func (BaseFake) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	return nil
}

func (BaseFake) VerifyUncle(chain consensus.ChainHeaderReader, header *types.Header, uncle *types.Header, uncles mapset.Set[libcommon.Hash], ancestors map[libcommon.Hash]*types.Header, seal bool) error {
	return nil
}

func VerifyHeaderBasics(chain consensus.ChainHeaderReader, header, parent *types.Header, checkTimestamp, skipGasLimit bool) error {
	// Ensure that the header's extra-data section is of a reasonable size
	if uint64(len(header.Extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("extra-data too long: %d > %d", len(header.Extra), params.MaximumExtraDataSize)
	}
	// Verify the header's timestamp
	if checkTimestamp {
		unixNow := time.Now().Unix()
		if header.Time > uint64(unixNow+allowedFutureBlockTimeSeconds) {
			return consensus.ErrFutureBlock
		}
	}
	if header.Time <= parent.Time {
		return errOlderBlockTime
	}
	// Verify that the gas limit is <= 2^63-1
	if header.GasLimit > params.MaxGasLimit {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, params.MaxGasLimit)
	}
	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
	}
	// Verify the block's gas usage and (if applicable) verify the base fee.
	if !chain.Config().IsLondon(header.Number.Uint64()) {
		// Verify BaseFee not present before EIP-1559 fork.
		if header.BaseFee != nil {
			return fmt.Errorf("invalid baseFee before fork: have %d, expected 'nil'", header.BaseFee)
		}
		if !skipGasLimit {
			if err := misc.VerifyGaslimit(parent.GasLimit, header.GasLimit); err != nil {
				return err
			}
		}
	} else if err := misc.VerifyEip1559Header(chain.Config(), parent, header, skipGasLimit); err != nil {
		// Verify the header's EIP-1559 attributes.
		return err
	}

	if err := misc.VerifyAbsenceOfCancunHeaderFields(header); err != nil {
		return err
	}

	// Verify that the block number is parent's +1
	if diff := new(big.Int).Sub(header.Number, parent.Number); diff.Cmp(big.NewInt(1)) != 0 {
		return consensus.ErrInvalidNumber
	}

	if header.WithdrawalsHash != nil {
		return consensus.ErrUnexpectedWithdrawals
	}

	if header.RequestsRoot != nil {
		return consensus.ErrUnexpectedRequests
	}

	// If all checks passed, validate any special fields for hard forks
	if err := misc.VerifyDAOHeaderExtraData(chain.Config(), header); err != nil {
		return err
	}
	return nil
}

// verifyHeader checks whether a header conforms to the consensus rules of the
// stock Ethereum ethash engine.
// See YP section 4.3.4. "Block Header Validity"
func (ethash BaseFake) verifyHeader(chain consensus.ChainHeaderReader, header, parent *types.Header, uncle bool, seal bool) error {
	if err := VerifyHeaderBasics(chain, header, parent, !uncle /*checkTimestamp*/, false /*skipGasLimit*/); err != nil {
		return err
	}
	return nil
}

func (ethash BaseFake) GenerateSeal(chain consensus.ChainHeaderReader, currnt, parent *types.Header, call consensus.Call) []byte {
	return nil
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns
// the difficulty that a new block should have when created at time
// given the parent block's time and difficulty.
func (ethash BaseFake) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, _, parentUncleHash libcommon.Hash, _ uint64) *big.Int {
	return common.Big0
}

// VerifySeal implements consensus.Engine, checking whether the given block satisfies
// the PoW difficulty requirements.
func (ethash BaseFake) VerifySeal(_ consensus.ChainHeaderReader, header *types.Header) error {
	return nil
}

// Prepare implements consensus.Engine, initializing the difficulty field of a
// header to conform to the ethash protocol. The changes are done inline.
func (ethash BaseFake) Prepare(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	return nil
}

func (ethash BaseFake) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, syscall consensus.SysCallCustom, logger log.Logger, tracer *tracing.Hooks) {
	if config.DAOForkBlock != nil && config.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(state)
	}
}

// Finalize implements consensus.Engine, accumulating the block and uncle rewards,
// setting the final state on the header
func (ethash BaseFake) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal, requests types.Requests,
	chain consensus.ChainReader, syscall consensus.SystemCall, logger log.Logger,
) (types.Transactions, types.Receipts, types.Requests, error) {
	// Accumulate any block and uncle rewards and commit the final state root
	return txs, r, nil, nil
}

// FinalizeAndAssemble implements consensus.Engine, accumulating the block and
// uncle rewards, setting the final state and assembling the block.
func (ethash BaseFake) FinalizeAndAssemble(chainConfig *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal, requests types.Requests,
	chain consensus.ChainReader, syscall consensus.SystemCall, call consensus.Call, logger log.Logger,
) (*types.Block, types.Transactions, types.Receipts, error) {

	// Finalize block
	outTxs, outR, _, err := ethash.Finalize(chainConfig, header, state, txs, uncles, r, withdrawals, requests, chain, syscall, logger)
	if err != nil {
		return nil, nil, nil, err
	}
	// Header seems complete, assemble into a block and return
	return types.NewBlock(header, outTxs, uncles, outR, withdrawals, requests), outTxs, outR, nil
}

// AccumulateRewards returns rewards for a given block. The mining reward consists
// of the static blockReward plus a reward for each included uncle (if any). Individual
// uncle rewards are also returned in an array.
func AccumulateRewards(config *chain.Config, header *types.Header, uncles []*types.Header) (uint256.Int, []uint256.Int) {
	return *uint256.NewInt(0), nil
}

func (BaseFake) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall consensus.SystemCall) ([]consensus.Reward, error) {
	return nil, nil
}

type BaseFakeConfig struct {
}
