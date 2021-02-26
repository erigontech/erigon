// Copyright 2017 The go-ethereum Authors
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

package ethash

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/common/u256"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// Ethash proof-of-work protocol constants.
var (
	FrontierBlockReward       = uint256.NewInt().SetUint64(5e+18) // Block reward in wei for successfully mining a block
	ByzantiumBlockReward      = uint256.NewInt().SetUint64(3e+18) // Block reward in wei for successfully mining a block upward from Byzantium
	ConstantinopleBlockReward = uint256.NewInt().SetUint64(2e+18) // Block reward in wei for successfully mining a block upward from Constantinople
	maxUncles                 = 2                                 // Maximum number of uncles allowed in a single block
	allowedFutureBlockTime    = 15 * time.Second                  // Max time from current time allowed for blocks, before they're considered future blocks

	// calcDifficultyEip2384 is the difficulty adjustment algorithm as specified by EIP 2384.
	// It offsets the bomb 4M blocks from Constantinople, so in total 9M blocks.
	// Specification EIP-2384: https://eips.ethereum.org/EIPS/eip-2384
	calcDifficultyEip2384 = makeDifficultyCalculator(big.NewInt(9000000))

	// calcDifficultyConstantinople is the difficulty adjustment algorithm for Constantinople.
	// It returns the difficulty that a new block should have when created at time given the
	// parent block's time and difficulty. The calculation uses the Byzantium rules, but with
	// bomb offset 5M.
	// Specification EIP-1234: https://eips.ethereum.org/EIPS/eip-1234
	calcDifficultyConstantinople = makeDifficultyCalculator(big.NewInt(5000000))

	// calcDifficultyByzantium is the difficulty adjustment algorithm. It returns
	// the difficulty that a new block should have when created at time given the
	// parent block's time and difficulty. The calculation uses the Byzantium rules.
	// Specification EIP-649: https://eips.ethereum.org/EIPS/eip-649
	calcDifficultyByzantium = makeDifficultyCalculator(big.NewInt(3000000))
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

// Author implements consensus.Engine, returning the header's coinbase as the
// proof-of-work verified author of the block.
func (ethash *Ethash) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules of the
// stock Ethereum ethash engine.
func (ethash *Ethash) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	// Short circuit if the header is known, or its parent not
	number := header.Number.Uint64()
	if chain.GetHeader(header.HashCache(), number) != nil {
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

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers
// concurrently. The method returns a quit channel to abort the operations and
// a results channel to retrieve the async verifications.
func (ethash *Ethash) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool) (func(), <-chan error) {
	if len(headers) == 0 {
		return func() {}, make(chan error)
	}

	// Spawn as many workers as allowed threads
	workers := runtime.GOMAXPROCS(0)
	if len(headers) < workers {
		workers = len(headers)
	}

	// Create a task channel and spawn the verifiers
	var (
		done   = make(chan int, workers)
		errors = make([]error, len(headers))
		abort  = make(chan struct{})
	)

	wg := sync.WaitGroup{}
	cancel := func() {
		close(abort)
		wg.Wait()
	}

	input := new(int64)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var index int64
			for {
				index = atomic.AddInt64(input, 1) - 1
				if int(index) > len(headers)-1 {
					return
				}

				errors[index] = ethash.verifyHeaderWorker(chain, headers, seals, int(index))

				select {
				case done <- int(index):
				case <-abort:
					return
				}
			}
		}()
	}

	errorsOut := make(chan error, len(headers))
	go func() {
		var (
			out     = 0
			checked = make([]bool, len(headers))
		)
		for {
			select {
			case index := <-done:
				for checked[index] = true; checked[out]; out++ {
					errorsOut <- errors[out]
					if out == len(headers)-1 {
						close(errorsOut)
						return
					}
				}
			case <-abort:
				return
			}
		}
	}()

	return cancel, errorsOut
}

func (ethash *Ethash) verifyHeaderWorker(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool, index int) error {
	var parent *types.Header
	if index == 0 {
		parent = chain.GetHeader(headers[0].ParentHash, headers[0].Number.Uint64()-1)
	} else if headers[index-1].HashCache() == headers[index].ParentHash {
		parent = headers[index-1]
	}
	if parent == nil {
		if index-1 >= 0 && index-1 <= len(headers)-1 {
			log.Error("consensus.ErrUnknownAncestor", "index", index, "headers", len(headers), "index-1", headers[index-1].Number.Uint64(), "index", headers[index].Number.Uint64())
		} else {
			log.Error("consensus.ErrUnknownAncestor", "index", index, "headers", len(headers))
		}

		return consensus.ErrUnknownAncestor
	}
	if chain.GetHeader(headers[index].HashCache(), headers[index].Number.Uint64()) != nil {
		return nil // known block
	}
	return ethash.verifyHeader(chain, headers[index], parent, false, seals[index])
}

// VerifyUncles verifies that the given block's uncles conform to the consensus
// rules of the stock Ethereum ethash engine.
func (ethash *Ethash) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	// Verify that there are at most 2 uncles included in this block
	if len(block.Uncles()) > maxUncles {
		return errTooManyUncles
	}
	if len(block.Uncles()) == 0 {
		return nil
	}
	uncles, ancestors := getUncles(chain, block)

	// Verify each of the uncles that it's recent, but not an ancestor
	for _, uncle := range block.Uncles() {
		if err := ethash.VerifyUncle(chain, block, uncle, uncles, ancestors, true); err != nil {
			return err
		}
	}
	return nil
}

func getUncles(chain consensus.ChainReader, block *types.Block) (mapset.Set, map[common.Hash]*types.Header) {
	// Gather the set of past uncles and ancestors
	uncles, ancestors := mapset.NewSet(), make(map[common.Hash]*types.Header)

	number, parent := block.NumberU64()-1, block.ParentHash()
	for i := 0; i < 7; i++ {
		ancestor := chain.GetBlock(parent, number)
		if ancestor == nil {
			break
		}
		ancestors[ancestor.HashCache()] = ancestor.Header()
		for _, uncle := range ancestor.Uncles() {
			uncles.Add(uncle.HashCache())
		}
		parent, number = ancestor.ParentHash(), number-1
	}
	ancestors[block.HashCache()] = block.Header()
	uncles.Add(block.HashCache())
	return uncles, ancestors
}

func (ethash *Ethash) VerifyUncle(chain consensus.ChainHeaderReader, block *types.Block, uncle *types.Header, uncles mapset.Set, ancestors map[common.Hash]*types.Header, seal bool) error {
	// Make sure every uncle is rewarded only once
	hash := uncle.HashCache()
	if uncles.Contains(hash) {
		return errDuplicateUncle
	}
	uncles.Add(hash)

	// Make sure the uncle has a valid ancestry
	if ancestors[hash] != nil {
		return errUncleIsAncestor
	}
	if ancestors[uncle.ParentHash] == nil || uncle.ParentHash == block.ParentHash() {
		return errDanglingUncle
	}

	return ethash.verifyHeader(chain, uncle, ancestors[uncle.ParentHash], true, seal)
}

// verifyHeader checks whether a header conforms to the consensus rules of the
// stock Ethereum ethash engine.
// See YP section 4.3.4. "Block Header Validity"
func (ethash *Ethash) verifyHeader(chain consensus.ChainHeaderReader, header, parent *types.Header, uncle bool, seal bool) error {
	// Ensure that the header's extra-data section is of a reasonable size
	if uint64(len(header.Extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("extra-data too long: %d > %d", len(header.Extra), params.MaximumExtraDataSize)
	}
	// Verify the header's timestamp
	if !uncle {
		if header.Time > uint64(time.Now().Add(allowedFutureBlockTime).Unix()) {
			return consensus.ErrFutureBlock
		}
	}
	if header.Time <= parent.Time {
		return errOlderBlockTime
	}
	// Verify the block's difficulty based on its timestamp and parent's difficulty
	expected := ethash.CalcDifficulty(chain, header.Time, parent.Time, parent.Difficulty, parent.Number, parent.HashCache(), parent.UncleHash)

	if expected.Cmp(header.Difficulty) != 0 {
		return fmt.Errorf("invalid difficulty: have %v, want %v", header.Difficulty, expected)
	}
	// Verify that the gas limit is <= 2^63-1
	cap := uint64(0x7fffffffffffffff)
	if header.GasLimit > cap {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, cap)
	}
	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
	}

	// Verify that the gas limit remains within allowed bounds
	diff := int64(parent.GasLimit) - int64(header.GasLimit)
	if diff < 0 {
		diff *= -1
	}
	limit := parent.GasLimit / params.GasLimitBoundDivisor

	if uint64(diff) >= limit || header.GasLimit < params.MinGasLimit {
		return fmt.Errorf("invalid gas limit: have %d, want %d += %d", header.GasLimit, parent.GasLimit, limit)
	}
	// Verify that the block number is parent's +1
	if diff := new(big.Int).Sub(header.Number, parent.Number); diff.Cmp(big.NewInt(1)) != 0 {
		return consensus.ErrInvalidNumber
	}
	// Verify the engine specific seal securing the block
	if seal {
		if err := ethash.VerifySeal(nil, header); err != nil {
			return err
		}
	}
	// If all checks passed, validate any special fields for hard forks
	if err := misc.VerifyDAOHeaderExtraData(chain.Config(), header); err != nil {
		return err
	}
	if err := misc.VerifyForkHashes(chain.Config(), header, uncle); err != nil {
		return err
	}
	return nil
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns
// the difficulty that a new block should have when created at time
// given the parent block's time and difficulty.
func (ethash *Ethash) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty, parentNumber *big.Int, _, parentUncleHash common.Hash) *big.Int {
	return CalcDifficulty(chain.Config(), time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns
// the difficulty that a new block should have when created at time
// given the parent block's time and difficulty.
func CalcDifficulty(config *params.ChainConfig, time, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentUncleHash common.Hash) *big.Int {
	next := new(big.Int).Add(parentNumber, big1)
	switch {
	case config.IsMuirGlacier(next):
		return calcDifficultyEip2384(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	case config.IsConstantinople(next):
		return calcDifficultyConstantinople(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	case config.IsByzantium(next):
		return calcDifficultyByzantium(time, parentTime, parentDifficulty, parentNumber, parentUncleHash)
	case config.IsHomestead(next):
		return calcDifficultyHomestead(time, parentTime, parentDifficulty, parentNumber)
	default:
		return calcDifficultyFrontier(time, parentTime, parentDifficulty, parentNumber)
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
func makeDifficultyCalculator(bombDelay *big.Int) func(time, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentUncleHash common.Hash) *big.Int {
	// Note, the calculations below looks at the parent number, which is 1 below
	// the block number. Thus we remove one from the delay given
	bombDelayFromParent := new(big.Int).Sub(bombDelay, big1)
	return func(time, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentUncleHash common.Hash) *big.Int {
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
		if parentUncleHash == types.EmptyUncleHash {
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
		fakeBlockNumber := new(big.Int)
		if parentNumber.Cmp(bombDelayFromParent) >= 0 {
			fakeBlockNumber = fakeBlockNumber.Sub(parentNumber, bombDelayFromParent)
		}
		// for the exponential factor
		periodCount := fakeBlockNumber
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
func calcDifficultyHomestead(time, parentTime uint64, parentDifficulty, parentNumber *big.Int) *big.Int {
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
	periodCount := new(big.Int).Add(parentNumber, big1)
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
func calcDifficultyFrontier(time, parentTime uint64, parentDifficulty, parentNumber *big.Int) *big.Int {
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

	periodCount := new(big.Int).Add(parentNumber, big1)
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
		if ethash.config.PowMode == ModeTest {
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
func (ethash *Ethash) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {
	parent := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	header.Difficulty = ethash.CalcDifficulty(chain, header.Time, parent.Time, parent.Difficulty, parent.Number, parent.HashCache(), parent.UncleHash)
	return nil
}

// Finalize implements consensus.Engine, accumulating the block and uncle rewards,
// setting the final state on the header
func (ethash *Ethash) Finalize(config *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction, uncles []*types.Header) {
	// Accumulate any block and uncle rewards and commit the final state root
	accumulateRewards(config, state, header, uncles)
}

// FinalizeAndAssemble implements consensus.Engine, accumulating the block and
// uncle rewards, setting the final state and assembling the block.
func (ethash *Ethash) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction, uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
	// Accumulate any block and uncle rewards and commit the final state root
	accumulateRewards(chainConfig, state, header, uncles)
	// Header seems complete, assemble into a block and return
	return types.NewBlock(header, txs, uncles, receipts), nil
}

// SealHash returns the hash of a block prior to it being sealed.
func (ethash *Ethash) SealHash(header *types.Header) (hash common.Hash) {
	hasher := sha3.NewLegacyKeccak256()

	rlp.Encode(hasher, []interface{}{
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
	})
	hasher.Sum(hash[:0])
	return hash
}

// AccumulateRewards returns rewards for a given block. The mining reward consists
// of the static blockReward plus a reward for each included uncle (if any). Individual
// uncle rewards are also returned in an array.
func AccumulateRewards(config *params.ChainConfig, header *types.Header, uncles []*types.Header) (uint256.Int, []uint256.Int) {
	// Select the correct block reward based on chain progression
	blockReward := FrontierBlockReward
	if config.IsByzantium(header.Number) {
		blockReward = ByzantiumBlockReward
	}
	if config.IsConstantinople(header.Number) {
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

// accumulateRewards retreives rewards for a block and applies them to the coinbase accounts for miner and uncle miners
func accumulateRewards(config *params.ChainConfig, state *state.IntraBlockState, header *types.Header, uncles []*types.Header) {
	minerReward, uncleRewards := AccumulateRewards(config, header, uncles)
	for i, uncle := range uncles {
		if i < len(uncleRewards) {
			state.AddBalance(uncle.Coinbase, &uncleRewards[i])
		}
	}
	state.AddBalance(header.Coinbase, &minerReward)
}
