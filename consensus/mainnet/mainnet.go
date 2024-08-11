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

package mainnet

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

	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/consensus/misc"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc"
)

var (
	// b proof-of-work protocol constants.
	maxUncles                     = 2         // Maximum number of uncles allowed in a single block
	allowedFutureBlockTimeSeconds = int64(15) // Max seconds from current time allowed for blocks, before they're considered future blocks

	// Various error messages to mark blocks invalid.
	errOlderBlockTime   = errors.New("timestamp older than parent")
	errInvalidPoW       = errors.New("invalid proof-of-work")
	ErrInvalidDumpMagic = errors.New("invalid dump magic")

	// Block rewards based on block progression
	FrontierBlockReward       = uint256.NewInt(5e+18) // Block reward in wei for successfully mining a block
	ByzantiumBlockReward      = uint256.NewInt(3e+18) // Block reward in wei for successfully mining a block upward from Byzantium
	ConstantinopleBlockReward = uint256.NewInt(2e+18) // Block reward in wei for successfully mining a block upward from Constantinople
)

// b is a consensus engine based on proof-of-work implementing the b
// algorithm.
type baseMainnet struct {
}

// Type returns underlying consensus engine
func (baseMainnet) Type() chain.ConsensusName {
	return chain.EtHashConsensus
}

// Author implements consensus.Engine, returning the header's coinbase as the
// proof-of-work verified author of the block.
// This is thread-safe (only access the header.Coinbase)
func (baseMainnet) Author(header *types.Header) (libcommon.Address, error) {
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules of the
// stock Ethereum b engine.
func (b baseMainnet) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
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
// rules of the stock Ethereum b engine.
func (baseMainnet) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	return nil
}

func (baseMainnet) VerifyUncle(chain consensus.ChainHeaderReader, header *types.Header, uncle *types.Header, uncles mapset.Set[libcommon.Hash], ancestors map[libcommon.Hash]*types.Header, seal bool) error {
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
// stock Ethereum b engine.
// See YP section 4.3.4. "Block Header Validity"
func (b baseMainnet) verifyHeader(chain consensus.ChainHeaderReader, header, parent *types.Header, uncle bool, seal bool) error {
	if err := VerifyHeaderBasics(chain, header, parent, !uncle /*checkTimestamp*/, false /*skipGasLimit*/); err != nil {
		return err
	}
	return nil
}

func (b baseMainnet) GenerateSeal(chain consensus.ChainHeaderReader, currnt, parent *types.Header, call consensus.Call) []byte {
	return nil
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns
// the difficulty that a new block should have when created at time
// given the parent block's time and difficulty.
func (b baseMainnet) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, _, parentUncleHash libcommon.Hash, _ uint64) *big.Int {
	return common.Big1 // Just a placeholder, guaranteed to be valid since td will always increase
}

// VerifySeal implements consensus.Engine, checking whether the given block satisfies
// the PoW difficulty requirements.
func (b baseMainnet) VerifySeal(_ consensus.ChainHeaderReader, header *types.Header) error {
	return nil
}

// Prepare implements consensus.Engine, initializing the difficulty field of a
// header to conform to the b protocol. The changes are done inline.
func (b baseMainnet) Prepare(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	return nil
}

func (b baseMainnet) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, syscall consensus.SysCallCustom, logger log.Logger, tracer *tracing.Hooks) {
	if config.DAOForkBlock != nil && config.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(state)
	}
}

// Finalize implements consensus.Engine, accumulating the block and uncle rewards,
// setting the final state on the header
func (b *baseMainnet) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal, requests types.Requests,
	chain consensus.ChainReader, syscall consensus.SystemCall, logger log.Logger,
) (types.Transactions, types.Receipts, types.Requests, error) {
	// Accumulate any block and uncle rewards and commit the final state root
	accumulateRewards(config, state, header, uncles)
	return txs, r, nil, nil
}

// FinalizeAndAssemble implements consensus.Engine, accumulating the block and
// uncle rewards, setting the final state and assembling the block.
func (b baseMainnet) FinalizeAndAssemble(chainConfig *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal, requests types.Requests,
	chain consensus.ChainReader, syscall consensus.SystemCall, call consensus.Call, logger log.Logger,
) (*types.Block, types.Transactions, types.Receipts, error) {

	// Finalize block
	outTxs, outR, _, err := b.Finalize(chainConfig, header, state, txs, uncles, r, withdrawals, requests, chain, syscall, logger)
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
	// Select the correct block reward based on chain progression
	blockReward := FrontierBlockReward
	if config.IsByzantium(header.Number.Uint64()) {
		blockReward = ByzantiumBlockReward
	}
	if config.IsConstantinople(header.Number.Uint64()) {
		blockReward = ConstantinopleBlockReward
	}
	// Accumulate the rewards for the miner and any included uncles
	uncleRewards := []uint256.Int{}
	reward := new(uint256.Int).Set(blockReward)
	r := new(uint256.Int)
	headerNum, _ := uint256.FromBig(header.Number)
	for _, uncle := range uncles {
		uncleNum, _ := uint256.FromBig(uncle.Number)
		r.Add(uncleNum, u256.Num8)
		r.Sub(r, headerNum)
		r.Mul(r, blockReward)
		r.Div(r, u256.Num8)
		uncleRewards = append(uncleRewards, *r)

		r.Div(blockReward, u256.Num32)
		reward.Add(reward, r)
	}
	return *reward, uncleRewards
}

// accumulateRewards retrieves rewards for a block and applies them to the coinbase accounts for miner and uncle miners
func accumulateRewards(config *chain.Config, state *state.IntraBlockState, header *types.Header, uncles []*types.Header) {
	minerReward, uncleRewards := AccumulateRewards(config, header, uncles)
	for i, uncle := range uncles {
		if i < len(uncleRewards) {
			state.AddBalance(uncle.Coinbase, &uncleRewards[i], tracing.BalanceIncreaseRewardMineUncle)
		}
	}
	state.AddBalance(header.Coinbase, &minerReward, tracing.BalanceIncreaseRewardMineBlock)
}

func (baseMainnet) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall consensus.SystemCall) ([]consensus.Reward, error) {
	minerReward, uncleRewards := AccumulateRewards(config, header, uncles)
	rewards := make([]consensus.Reward, 1+len(uncles))
	rewards[0].Beneficiary = header.Coinbase
	rewards[0].Kind = consensus.RewardAuthor
	rewards[0].Amount = minerReward
	for i, uncle := range uncles {
		rewards[i+1].Beneficiary = uncle.Coinbase
		rewards[i+1].Kind = consensus.RewardUncle
		rewards[i+1].Amount = uncleRewards[i]
	}
	return rewards, nil
}

type MainnetConfig struct {
}

// Seal implements consensus.Engine, attempting to find a nonce that satisfies
// the block's difficulty requirements.
func (baseMainnet) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	return nil
}

// SealHash returns the hash of a block prior to it being sealed.
func (baseMainnet) SealHash(header *types.Header) libcommon.Hash {
	return header.Hash()
}

// APIs implements consensus.Engine, returning the user facing RPC API to allow
// controlling the signer voting.
func (c baseMainnet) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{
		//{
		//Namespace: "clique",
		//Version:   "1.0",
		//Service:   &API{chain: chain, clique: c},
		//Public:    false,
		//}
	}
}

func (baseMainnet) Close() error {
	return nil
}

func (baseMainnet) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return nil
}

func (baseMainnet) GetTransferFunc() evmtypes.TransferFunc {
	return consensus.Transfer
}

func (baseMainnet) IsServiceTransaction(sender libcommon.Address, syscall consensus.SystemCall) bool {
	return false
}

type MainnetConsensus struct {
	baseMainnet
	fakeFail  uint64        // Block number which fails PoW check even in fake mode
	fakeDelay time.Duration // Time delay to sleep for before returning from verify
}

// NewFakeFailer creates an b consensus engine with a fake PoW scheme that
// accepts all blocks as valid apart from the single one specified, though they
// still have to conform to the Ethereum consensus rules.
func NewFakeFailer(fail uint64) *MainnetConsensus {
	return &MainnetConsensus{fakeFail: fail}
}

// NewMainnetConsensus creates an b consensus engine with a fake PoW scheme
// that accepts all blocks' seal as valid, though they still have to conform to
// the Ethereum consensus rules.
func NewMainnetConsensus() *MainnetConsensus {
	return &MainnetConsensus{}
}

// NewFakeDelayer creates an b consensus engine with a fake PoW scheme that
// accepts all blocks as valid, but delays verifications by some time, though
// they still have to conform to the Ethereum consensus rules.
func NewFakeDelayer(delay time.Duration) *MainnetConsensus {
	return &MainnetConsensus{
		fakeDelay: delay,
	}
}

func (f *MainnetConsensus) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	err := f.baseMainnet.VerifyHeader(chain, header, false)
	if err != nil {
		return err
	}

	if seal {
		return f.VerifySeal(chain, header)
	}

	return nil
}

func (f *MainnetConsensus) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	return nil
}

func (f *MainnetConsensus) VerifyUncle(chain consensus.ChainHeaderReader, block *types.Header, uncle *types.Header, uncles mapset.Set[libcommon.Hash], ancestors map[libcommon.Hash]*types.Header, seal bool) error {
	err := f.baseMainnet.VerifyUncle(chain, block, uncle, uncles, ancestors, false)
	if err != nil {
		return err
	}

	if seal {
		return f.VerifySeal(chain, uncle)
	}
	return nil
}

// If we're running a fake PoW, accept any seal as valid
func (f *MainnetConsensus) VerifySeal(_ consensus.ChainHeaderReader, header *types.Header) error {
	if f.fakeDelay > 0 {
		time.Sleep(f.fakeDelay)
	}

	if f.fakeFail == header.Number.Uint64() {
		return errInvalidPoW
	}

	return nil
}

// If we're running a fake PoW, simply return a 0 nonce immediately
func (f *MainnetConsensus) Seal(_ consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	header := block.Header()
	header.Nonce, header.MixDigest = types.BlockNonce{}, libcommon.Hash{}

	select {
	case results <- block.WithSeal(header):
	default:
		log.Warn("Sealing result is not read by miner", "mode", "fake", "sealhash", block.Hash())
	}

	return nil
}

type FullFakeb MainnetConsensus

// NewFullFaker creates an b consensus engine with a full fake scheme that
// accepts all blocks as valid, without checking any consensus rules whatsoever.
func NewFullFaker() *FullFakeb {
	return &FullFakeb{}
}

// If we're running a full engine faking, accept any input as valid
func (f *FullFakeb) VerifyHeader(_ consensus.ChainHeaderReader, _ *types.Header, _ bool) error {
	return nil
}

func (f *FullFakeb) VerifyUncles(_ consensus.ChainReader, _ *types.Header, _ []*types.Header) error {
	return nil
}
