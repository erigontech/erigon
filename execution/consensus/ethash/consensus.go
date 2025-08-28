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
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/ethash/ethashcfg"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// Ethash proof-of-work protocol constants.
var (
	FrontierBlockReward           = uint256.NewInt(5e+18) // Block reward in wei for successfully mining a block
	ByzantiumBlockReward          = uint256.NewInt(3e+18) // Block reward in wei for successfully mining a block upward from Byzantium
	ConstantinopleBlockReward     = uint256.NewInt(2e+18) // Block reward in wei for successfully mining a block upward from Constantinople
	maxUncles                     = 2                     // Maximum number of uncles allowed in a single block
	allowedFutureBlockTimeSeconds = int64(15)             // Max seconds from current time allowed for blocks, before they're considered future blocks

	// calcDifficultyEip5133 is the difficulty adjustment algorithm as specified by EIP 5133.
	// It offsets the bomb a total of 11.4M blocks.
	// Specification EIP-5133: https://eips.ethereum.org/EIPS/eip-5133
	calcDifficultyEip5133 = makeDifficultyCalculator(11400000)

	// calcDifficultyEip4345 is the difficulty adjustment algorithm as specified by EIP 4345.
	// It offsets the bomb a total of 10.7M blocks.
	// Specification EIP-4345: https://eips.ethereum.org/EIPS/eip-4345
	calcDifficultyEip4345 = makeDifficultyCalculator(10700000)

	// calcDifficultyEip3554 is the difficulty adjustment algorithm as specified by EIP 3554.
	// It offsets the bomb a total of 9.7M blocks.
	// Specification EIP-3554: https://eips.ethereum.org/EIPS/eip-3554
	calcDifficultyEip3554 = makeDifficultyCalculator(9700000)

	// calcDifficultyEip2384 is the difficulty adjustment algorithm as specified by EIP 2384.
	// It offsets the bomb 4M blocks from Constantinople, so in total 9M blocks.
	// Specification EIP-2384: https://eips.ethereum.org/EIPS/eip-2384
	calcDifficultyEip2384 = makeDifficultyCalculator(9000000)

	// calcDifficultyConstantinople is the difficulty adjustment algorithm for Constantinople.
	// It returns the difficulty that a new block should have when created at time given the
	// parent block's time and difficulty. The calculation uses the Byzantium rules, but with
	// bomb offset 5M.
	// Specification EIP-1234: https://eips.ethereum.org/EIPS/eip-1234
	calcDifficultyConstantinople = makeDifficultyCalculator(5000000)

	// calcDifficultyByzantium is the difficulty adjustment algorithm. It returns
	// the difficulty that a new block should have when created at time given the
	// parent block's time and difficulty. The calculation uses the Byzantium rules.
	// Specification EIP-649: https://eips.ethereum.org/EIPS/eip-649
	calcDifficultyByzantium = makeDifficultyCalculator(3000000)
)

// Various error messages to mark blocks invalid. These should be private to
// prevent engine specific errors from being referenced in the remainder of the
// codebase, inherently breaking if the engine is swapped out. Please put common
// error types into the consensus package.
var (
	errOlderBlockTime    = errors.New("timestamp older than parent")
	errTooManyUncles     = errors.New("too many uncles")
	errDuplicateUncle    = errors.New("duplicate uncle")
	errUncleIsAncestor   = errors.New("uncle is ancestor")
	errDanglingUncle     = errors.New("uncle's parent is not ancestor")
	errInvalidDifficulty = errors.New("non-positive difficulty")
	errInvalidMixDigest  = errors.New("invalid mix digest")
	errInvalidPoW        = errors.New("invalid proof-of-work")
)

// Type returns underlying consensus engine
func (ethash *Ethash) Type() chain.ConsensusName {
	return chain.EtHashConsensus
}

// Author implements consensus.Engine, returning the header's coinbase as the
// proof-of-work verified author of the block.
// This is thread-safe (only access the header.Coinbase)
func (ethash *Ethash) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules of the
// stock Ethereum ethash engine.
func (ethash *Ethash) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
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
	return ethash.verifyHeader(chain, header, parent, false, seal)
}

// VerifyUncles verifies that the given block's uncles conform to the consensus
// rules of the stock Ethereum ethash engine.
func (ethash *Ethash) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	// Verify that there are at most 2 uncles included in this block
	if len(uncles) > maxUncles {
		return errTooManyUncles
	}
	if len(uncles) == 0 {
		return nil
	}
	uncleBlocks, ancestors := getUncles(chain, header)

	// Verify each of the uncles that it's recent, but not an ancestor
	for _, uncle := range uncles {
		if err := ethash.VerifyUncle(chain, header, uncle, uncleBlocks, ancestors, true); err != nil {
			return err
		}
	}
	return nil
}

func getUncles(chain consensus.ChainReader, header *types.Header) (mapset.Set[common.Hash], map[common.Hash]*types.Header) {
	// Gather the set of past uncles and ancestors
	uncles, ancestors := mapset.NewSet[common.Hash](), make(map[common.Hash]*types.Header)

	number, parent := header.Number.Uint64()-1, header.ParentHash
	for i := 0; i < 7; i++ {
		ancestorHeader := chain.GetHeader(parent, number)
		if ancestorHeader == nil {
			break
		}
		ancestors[parent] = ancestorHeader
		// If the ancestor doesn't have any uncles, we don't have to iterate them
		if ancestorHeader.UncleHash != empty.UncleHash {
			// Need to add those uncles to the blacklist too
			ancestor := chain.GetBlock(parent, number)
			if ancestor == nil {
				break
			}
			for _, uncle := range ancestor.Uncles() {
				uncles.Add(uncle.Hash())
			}
		}
		parent, number = ancestorHeader.ParentHash, number-1
	}
	ancestors[header.Hash()] = header
	uncles.Add(header.Hash())
	return uncles, ancestors
}

func (ethash *Ethash) VerifyUncle(chain consensus.ChainHeaderReader, header *types.Header, uncle *types.Header, uncles mapset.Set[common.Hash], ancestors map[common.Hash]*types.Header, seal bool) error {
	// Make sure every uncle is rewarded only once
	hash := uncle.Hash()
	if uncles.Contains(hash) {
		return errDuplicateUncle
	}
	uncles.Add(hash)

	// Make sure the uncle has a valid ancestry
	if ancestors[hash] != nil {
		return errUncleIsAncestor
	}
	if ancestors[uncle.ParentHash] == nil || uncle.ParentHash == header.ParentHash {
		return errDanglingUncle
	}

	return ethash.verifyHeader(chain, uncle, ancestors[uncle.ParentHash], true, seal)
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
	if header.GasLimit > params.MaxBlockGasLimit {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, params.MaxBlockGasLimit)
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

	if header.RequestsHash != nil {
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
func (ethash *Ethash) verifyHeader(chain consensus.ChainHeaderReader, header, parent *types.Header, uncle bool, seal bool) error {
	if err := VerifyHeaderBasics(chain, header, parent, !uncle /*checkTimestamp*/, false /*skipGasLimit*/); err != nil {
		return err
	}
	// Verify the block's difficulty based on its timestamp and parent's difficulty
	expected := ethash.CalcDifficulty(chain, header.Time, parent.Time, parent.Difficulty, parent.Number.Uint64(), parent.Hash(), parent.UncleHash, parent.AuRaStep)
	if expected.Cmp(header.Difficulty) != 0 {
		return fmt.Errorf("invalid difficulty: have %v, want %v", header.Difficulty, expected)
	}
	// Verify the engine specific seal securing the block
	if seal {
		if err := ethash.VerifySeal(nil, header); err != nil {
			return err
		}
	}
	return nil
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns
// the difficulty that a new block should have when created at time
// given the parent block's time and difficulty.
func (ethash *Ethash) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, _, parentUncleHash common.Hash, _ uint64) *big.Int {
	return CalcDifficulty(chain.Config(), time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns
// the difficulty that a new block should have when created at time
// given the parent block's time and difficulty.
func CalcDifficulty(config *chain.Config, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentUncleHash common.Hash) *big.Int {
	next := parentNumber + 1
	switch {
	case config.IsGrayGlacier(next):
		return calcDifficultyEip5133(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	case config.IsArrowGlacier(next):
		return calcDifficultyEip4345(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	case config.IsLondon(next):
		return calcDifficultyEip3554(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	case config.IsMuirGlacier(next):
		return calcDifficultyEip2384(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	case config.IsConstantinople(next):
		return calcDifficultyConstantinople(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	case config.IsByzantium(next):
		return calcDifficultyByzantium(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	case config.IsHomestead(next):
		return calcDifficultyHomestead(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	default:
		return calcDifficultyFrontier(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	}
}

// Some weird constants to avoid constant memory allocs for them.
var (
	expDiffPeriod = big.NewInt(100000)
	big1          = big.NewInt(1)
	big2          = big.NewInt(2)
	big9          = big.NewInt(9)
	big10         = big.NewInt(10)
	bigMinus99    = big.NewInt(-99)
)

// makeDifficultyCalculator creates a difficultyCalculator with the given bomb-delay.
// the difficulty is calculated with Byzantium rules, which differs from Homestead in
// how uncles affect the calculation
func makeDifficultyCalculator(bombDelay uint64) func(time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentUncleHash common.Hash) *big.Int {
	// Note, the calculations below looks at the parent number, which is 1 below
	// the block number. Thus we remove one from the delay given
	bombDelayFromParent := bombDelay - 1
	return func(time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentUncleHash common.Hash) *big.Int {
		// https://github.com/ethereum/EIPs/issues/100.
		// algorithm:
		// diff = (parent_diff +
		//         (parent_diff / 2048 * max((2 if len(parent.uncles) else 1) - ((timestamp - parent.timestamp) // 9), -99))
		//        ) + 2^(periodCount - 2)

		bigTime := new(big.Int).SetUint64(time)
		bigParentTime := new(big.Int).SetUint64(parentTime)

		// holds intermediate values to make the algo easier to read & audit
		x := new(big.Int)
		y := new(big.Int)

		// (2 if len(parent_uncles) else 1) - (block_timestamp - parent_timestamp) // 9
		x.Sub(bigTime, bigParentTime)
		x.Div(x, big9)
		if parentUncleHash == empty.UncleHash {
			x.Sub(big1, x)
		} else {
			x.Sub(big2, x)
		}
		// max((2 if len(parent_uncles) else 1) - (block_timestamp - parent_timestamp) // 9, -99)
		if x.Cmp(bigMinus99) < 0 {
			x.Set(bigMinus99)
		}
		// parent_diff + (parent_diff / 2048 * max((2 if len(parent.uncles) else 1) - ((timestamp - parent.timestamp) // 9), -99))
		y.Div(parentDifficulty, params.DifficultyBoundDivisor)
		x.Mul(y, x)
		x.Add(parentDifficulty, x)

		// minimum difficulty can ever be (before exponential factor)
		if x.Cmp(params.MinimumDifficulty) < 0 {
			x.Set(params.MinimumDifficulty)
		}
		// calculate a fake block number for the ice-age delay
		// Specification: https://eips.ethereum.org/EIPS/eip-1234
		fakeBlockNumber := uint64(0)
		if parentNumber >= bombDelayFromParent {
			fakeBlockNumber = parentNumber - bombDelayFromParent
		}
		// for the exponential factor
		periodCount := new(big.Int).SetUint64(fakeBlockNumber)
		periodCount.Div(periodCount, expDiffPeriod)

		// the exponential factor, commonly referred to as "the bomb"
		// diff = diff + 2^(periodCount - 2)
		if periodCount.Cmp(big1) > 0 {
			y.Sub(periodCount, big2)
			y.Exp(big2, y, nil)
			x.Add(x, y)
		}
		return x
	}
}

// calcDifficultyHomestead is the difficulty adjustment algorithm. It returns
// the difficulty that a new block should have when created at time given the
// parent block's time and difficulty. The calculation uses the Homestead rules.
func calcDifficultyHomestead(time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, _ common.Hash) *big.Int {
	// https://github.com/ethereum/EIPs/blob/master/EIPS/eip-2.md
	// algorithm:
	// diff = (parent_diff +
	//         (parent_diff / 2048 * max(1 - (block_timestamp - parent_timestamp) // 10, -99))
	//        ) + 2^(periodCount - 2)

	bigTime := new(big.Int).SetUint64(time)
	bigParentTime := new(big.Int).SetUint64(parentTime)

	// holds intermediate values to make the algo easier to read & audit
	x := new(big.Int)
	y := new(big.Int)

	// 1 - (block_timestamp - parent_timestamp) // 10
	x.Sub(bigTime, bigParentTime)
	x.Div(x, big10)
	x.Sub(big1, x)

	// max(1 - (block_timestamp - parent_timestamp) // 10, -99)
	if x.Cmp(bigMinus99) < 0 {
		x.Set(bigMinus99)
	}
	// (parent_diff + parent_diff // 2048 * max(1 - (block_timestamp - parent_timestamp) // 10, -99))
	y.Div(parentDifficulty, params.DifficultyBoundDivisor)
	x.Mul(y, x)
	x.Add(parentDifficulty, x)

	// minimum difficulty can ever be (before exponential factor)
	if x.Cmp(params.MinimumDifficulty) < 0 {
		x.Set(params.MinimumDifficulty)
	}
	// for the exponential factor
	periodCount := new(big.Int).SetUint64(parentNumber + 1)
	periodCount.Div(periodCount, expDiffPeriod)

	// the exponential factor, commonly referred to as "the bomb"
	// diff = diff + 2^(periodCount - 2)
	if periodCount.Cmp(big1) > 0 {
		y.Sub(periodCount, big2)
		y.Exp(big2, y, nil)
		x.Add(x, y)
	}
	return x
}

// calcDifficultyFrontier is the difficulty adjustment algorithm. It returns the
// difficulty that a new block should have when created at time given the parent
// block's time and difficulty. The calculation uses the Frontier rules.
func calcDifficultyFrontier(time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, _ common.Hash) *big.Int {
	diff := new(big.Int)
	adjust := new(big.Int).Div(parentDifficulty, params.DifficultyBoundDivisor)
	bigTime := new(big.Int)
	bigParentTime := new(big.Int)

	bigTime.SetUint64(time)
	bigParentTime.SetUint64(parentTime)

	if bigTime.Sub(bigTime, bigParentTime).Cmp(params.DurationLimit) < 0 {
		diff.Add(parentDifficulty, adjust)
	} else {
		diff.Sub(parentDifficulty, adjust)
	}
	if diff.Cmp(params.MinimumDifficulty) < 0 {
		diff.Set(params.MinimumDifficulty)
	}

	periodCount := new(big.Int).SetUint64(parentNumber + 1)
	periodCount.Div(periodCount, expDiffPeriod)
	if periodCount.Cmp(big1) > 0 {
		// diff = diff + 2^(periodCount - 2)
		expDiff := periodCount.Sub(periodCount, big2)
		expDiff.Exp(big2, expDiff, nil)
		diff.Add(diff, expDiff)
		diff = math.BigMax(diff, params.MinimumDifficulty)
	}
	return diff
}

// VerifySeal implements consensus.Engine, checking whether the given block satisfies
// the PoW difficulty requirements.
func (ethash *Ethash) VerifySeal(_ consensus.ChainHeaderReader, header *types.Header) error {
	return ethash.verifySeal(header, false)
}

// Exported for fuzzing
var FrontierDifficultyCalulator = calcDifficultyFrontier
var HomesteadDifficultyCalulator = calcDifficultyHomestead
var DynamicDifficultyCalculator = makeDifficultyCalculator

// verifySeal checks whether a block satisfies the PoW difficulty requirements,
// either using the usual ethash cache for it, or alternatively using a full DAG
// to make remote mining fast.
func (ethash *Ethash) verifySeal(header *types.Header, fulldag bool) error { //nolint:unparam
	// If we're running a shared PoW, delegate verification to it
	if ethash.shared != nil {
		return ethash.shared.verifySeal(header, fulldag)
	}

	// Ensure that we have a valid difficulty for the block
	if header.Difficulty.Sign() <= 0 {
		return errInvalidDifficulty
	}
	// Recompute the digest and PoW values
	number := header.Number.Uint64()

	var (
		digest []byte
		result []byte
	)
	// If fast-but-heavy PoW verification was requested, use an ethash dataset
	if fulldag {
		dataset := ethash.dataset(number, true)
		if dataset.generated() {
			digest, result = hashimotoFull(dataset.dataset, ethash.SealHash(header).Bytes(), header.Nonce.Uint64())

			// Datasets are unmapped in a finalizer. Ensure that the dataset stays alive
			// until after the call to hashimotoFull so it's not unmapped while being used.
			runtime.KeepAlive(dataset)
		} else {
			// Dataset not yet generated, don't hang, use a cache instead
			fulldag = false
		}
	}
	// If slow-but-light PoW verification was requested (or DAG not yet ready), use an ethash cache
	if !fulldag {
		cache := ethash.cache(number)

		size := datasetSize(number)
		if ethash.config.PowMode == ethashcfg.ModeTest {
			size = 32 * 1024
		}
		digest, result = hashimotoLight(size, cache.cache, ethash.SealHash(header).Bytes(), header.Nonce.Uint64())

		// Caches are unmapped in a finalizer. Ensure that the cache stays alive
		// until after the call to hashimotoLight so it's not unmapped while being used.
		runtime.KeepAlive(cache)
	}
	// Verify the calculated values against the ones provided in the header
	if !bytes.Equal(header.MixDigest[:], digest) {
		return errInvalidMixDigest
	}
	target := new(big.Int).Div(two256, header.Difficulty)
	if new(big.Int).SetBytes(result).Cmp(target) > 0 {
		return errInvalidPoW
	}
	return nil
}

// Prepare implements consensus.Engine, initializing the difficulty field of a
// header to conform to the ethash protocol. The changes are done inline.
func (ethash *Ethash) Prepare(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	parent := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	header.Difficulty = ethash.CalcDifficulty(chain, header.Time, parent.Time, parent.Difficulty, parent.Number.Uint64(), parent.Hash(), parent.UncleHash, parent.AuRaStep)
	return nil
}

func (ethash *Ethash) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, syscall consensus.SysCallCustom, logger log.Logger, tracer *tracing.Hooks) {
	if config.DAOForkBlock != nil && config.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(state)
	}
}

// Finalize implements consensus.Engine, accumulating the block and uncle rewards,
// setting the final state on the header
func (ethash *Ethash) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainReader, syscall consensus.SystemCall, skipReceiptsEval bool, logger log.Logger,
) (types.FlatRequests, error) {
	// Accumulate any block and uncle rewards and commit the final state root
	accumulateRewards(config, state, header, uncles)
	return nil, nil
}

// FinalizeAndAssemble implements consensus.Engine, accumulating the block and
// uncle rewards, setting the final state and assembling the block.
func (ethash *Ethash) FinalizeAndAssemble(chainConfig *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainReader, syscall consensus.SystemCall, call consensus.Call, logger log.Logger,
) (*types.Block, types.FlatRequests, error) {

	// Finalize block
	_, err := ethash.Finalize(chainConfig, header, state, txs, uncles, r, withdrawals, chain, syscall, false, logger)
	if err != nil {
		return nil, nil, err
	}
	// Header seems complete, assemble into a block and return
	return types.NewBlock(header, txs, uncles, r, withdrawals), nil, nil
}

// SealHash returns the hash of a block prior to it being sealed.
func (ethash *Ethash) SealHash(header *types.Header) (hash common.Hash) {
	hasher := sha3.NewLegacyKeccak256()

	enc := []interface{}{
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra,
	}
	if header.BaseFee != nil {
		enc = append(enc, header.BaseFee)
	}
	rlp.Encode(hasher, enc)
	hasher.Sum(hash[:0])
	return hash
}

func (ethash *Ethash) IsServiceTransaction(sender common.Address, syscall consensus.SystemCall) bool {
	return false
}

func (ethash *Ethash) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, _ consensus.SystemCall,
) ([]consensus.Reward, error) {
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
		r.Rsh(r, 3) // ÷8
		uncleRewards = append(uncleRewards, *r)

		r.Rsh(blockReward, 5) // ÷32
		reward.Add(reward, r)
	}
	return *reward, uncleRewards
}

// accumulateRewards retrieves rewards for a block and applies them to the coinbase accounts for miner and uncle miners
func accumulateRewards(config *chain.Config, state *state.IntraBlockState, header *types.Header, uncles []*types.Header) {
	minerReward, uncleRewards := AccumulateRewards(config, header, uncles)
	for i, uncle := range uncles {
		if i < len(uncleRewards) {
			state.AddBalance(uncle.Coinbase, uncleRewards[i], tracing.BalanceIncreaseRewardMineUncle)
		}
	}
	state.AddBalance(header.Coinbase, minerReward, tracing.BalanceIncreaseRewardMineBlock)
}
