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

// Package clique implements the proof-of-authority consensus engine.
package aura

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura/aurainterfaces"
	"github.com/ledgerwatch/erigon/consensus/aura/contracts"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/secp256k1"
	"go.uber.org/atomic"
)

type StepDurationInfo struct {
	TransitionStep      uint64
	TransitionTimestamp uint64
	StepDuration        uint64
}

type Step struct {
	calibrate bool // whether calibration is enabled.
	inner     *atomic.Uint64
	// Planned durations of steps.
	durations []StepDurationInfo
}

func (s *Step) doCalibrate() {
	if s.calibrate {
		if !s.optCalibrate() {
			ctr := s.inner.Load()
			panic(fmt.Errorf("step counter under- or overflow: %d", ctr))
		}
	}
}

// optCalibrate Calibrates the AuRa step number according to the current time.
func (s *Step) optCalibrate() bool {
	now := time.Now().Second()
	var info StepDurationInfo
	i := 0
	for _, d := range s.durations {
		if d.TransitionTimestamp >= uint64(now) {
			break
		}
		info = d
		i++
	}
	if i == 0 {
		panic("durations cannot be empty")
	}

	if uint64(now) < info.TransitionTimestamp {
		return false
	}

	newStep := (uint64(now)-info.TransitionTimestamp)/info.StepDuration + info.TransitionStep
	s.inner.Store(newStep)
	return true
}

type PermissionedStep struct {
	inner      *Step
	canPropose *atomic.Bool
}

//nolint
type EpochManager struct {
	epochTransitionHash   common.Hash // H256,
	epochTransitionNumber uint64      // BlockNumber
	finalityChecker       RollingFinality
	force                 bool
}

//nolint
type unAssembledHeader struct {
	h common.Hash // H256
	n uint64      // BlockNumber
	a []common.Address
}

// RollingFinality checker for authority round consensus.
// Stores a chain of unfinalized hashes that can be pushed onto.
//nolint
type RollingFinality struct {
	headers    []unAssembledHeader //nolint
	signers    SimpleList
	signCount  map[common.Address]uint
	lastPushed *common.Hash // Option<H256>,
	// First block for which a 2/3 quorum (instead of 1/2) is required.
	twoThirdsMajorityTransition uint64 //BlockNumber
}

// AuRa
//nolint
type AuRa struct {
	db     ethdb.RwKV // Database to store and retrieve snapshot checkpoints
	exitCh chan struct{}
	lock   sync.RWMutex // Protects the signer fields

	step              PermissionedStep
	OurSigningAddress common.Address // Same as Etherbase in Mining
	cfg               AuthorityRoundParams
	EmptyStepsSet     *EmptyStepSet

	//Validators                     ValidatorSet
	//ValidateScoreTransition        uint64
	//ValidateStepTransition         uint64
	//EpochManager                   EpochManager // Mutex<EpochManager>,
	//immediateTransitions           bool
	//blockReward                    map[uint64]*uint256.Int
	//blockRewardContractTransitions BlockRewardContractList
	//maximumUncleCountTransition    uint64
	//maximumUncleCount              uint
	//emptyStepsTransition           uint64
	//strictEmptyStepsTransition     uint64
	//twoThirdsMajorityTransition    uint64 //  BlockNumber
	//maximumEmptySteps              uint
	////machine: EthereumMachine,
	//// History of step hashes recently received from peers.
	//receivedStepHashes map[uint64]map[common.Address]common.Hash // RwLock<BTreeMap<(u64, Address), H256>>
	//// If set, enables random number contract integration. It maps the transition block to the contract address.
	//randomnessContractAddress map[uint64]common.Address
	//// The addresses of contracts that determine the block gas limit.
	//blockGasLimitContractTransitions map[uint64]common.Address
	//// Memoized gas limit overrides, by block hash.
	//gasLimitOverrideCache *GasLimitOverride //Mutex<LruCache<H256, Option<U256>>>,
	//// The block number at which the consensus engine switches from AuRa to AuRa with POSDAO
	//// modifications. For details about POSDAO, see the whitepaper:
	//// https://www.xdaichain.com/for-validators/posdao-whitepaper
	//posdaoTransition *uint64 // Option<BlockNumber>,
}

type GasLimitOverride struct {
	cache *lru.Cache
}

func NewGasLimitOverride() *GasLimitOverride {
	// The number of recent block hashes for which the gas limit override is memoized.
	const GasLimitOverrideCacheCapacity = 10

	cache, err := lru.New(GasLimitOverrideCacheCapacity)
	if err != nil {
		panic("error creating prefetching cache for blocks")
	}
	return &GasLimitOverride{cache: cache}
}

func (pb *GasLimitOverride) Pop(hash common.Hash) *uint256.Int {
	if val, ok := pb.cache.Get(hash); ok && val != nil {
		pb.cache.Remove(hash)
		if v, ok := val.(*uint256.Int); ok {
			return v
		}
	}
	return nil
}

func (pb *GasLimitOverride) Add(hash common.Hash, b *uint256.Int) {
	if b == nil {
		return
	}
	pb.cache.ContainsOrAdd(hash, b)
}

// NewAuRa creates a Clique proof-of-authority consensus engine with the initial
// signers set to the ones provided by the user.
func NewAuRa(cfg *params.ChainConfig, db ethdb.RwKV, ourSigningAddress common.Address, auraParams AuthorityRoundParams) (*AuRa, error) {
	config := cfg.Aura

	if _, ok := auraParams.StepDurations[0]; !ok {
		return nil, fmt.Errorf("authority Round step 0 duration is undefined")
	}
	for _, v := range auraParams.StepDurations {
		if v == 0 {
			return nil, fmt.Errorf("authority Round step 0 duration is undefined")
		}
	}
	if _, ok := auraParams.StepDurations[0]; !ok {
		return nil, fmt.Errorf("authority Round step duration cannot be 0")
	}
	//shouldTimeout := auraParams.StartStep == nil
	initialStep := uint64(0)
	if auraParams.StartStep != nil {
		initialStep = *auraParams.StartStep
	}
	var durations []StepDurationInfo
	durInfo := StepDurationInfo{
		TransitionStep:      0,
		TransitionTimestamp: 0,
		StepDuration:        auraParams.StepDurations[0],
	}
	durations = append(durations, durInfo)
	var i = 0
	for time, dur := range auraParams.StepDurations {
		if i == 0 { // skip first
			i++
			continue
		}

		step, t, ok := nextStepTimeDuration(durInfo, time)
		if !ok {
			return nil, fmt.Errorf("timestamp overflow")
		}
		durInfo.TransitionStep = step
		durInfo.TransitionTimestamp = t
		durInfo.StepDuration = dur
		durations = append(durations, durInfo)
	}
	step := &Step{
		inner:     atomic.NewUint64(initialStep),
		calibrate: auraParams.StartStep == nil,
		durations: durations,
	}
	step.doCalibrate()

	/*
		    let engine = Arc::new(AuthorityRound {
		        transition_service: IoService::<()>::start("AuRa")?,
		        step: Arc::new(PermissionedStep {
		            inner: step,
		            can_propose: AtomicBool::new(true),
		        }),
		        client: Arc::new(RwLock::new(None)),
		        signer: RwLock::new(None),
		        validators: our_params.validators,
		        validate_score_transition: our_params.validate_score_transition,
		        validate_step_transition: our_params.validate_step_transition,
		        empty_steps: Default::default(),
		        epoch_manager: Mutex::new(EpochManager::blank(
		            our_params.two_thirds_majority_transition,
		        )),
		        immediate_transitions: our_params.immediate_transitions,
		        block_reward: our_params.block_reward,
		        block_reward_contract_transitions: our_params.block_reward_contract_transitions,
		        maximum_uncle_count_transition: our_params.maximum_uncle_count_transition,
		        maximum_uncle_count: our_params.maximum_uncle_count,
		        empty_steps_transition: our_params.empty_steps_transition,
		        maximum_empty_steps: our_params.maximum_empty_steps,
		        two_thirds_majority_transition: our_params.two_thirds_majority_transition,
		        machine: machine,
		        received_step_hashes: RwLock::new(Default::default()),
		        gas_limit_override_cache: Mutex::new(LruCache::new(GAS_LIMIT_OVERRIDE_CACHE_CAPACITY)),
		    })
			// Do not initialize timeouts for tests.
		    if should_timeout {
		        let handler = TransitionHandler {
		            step: engine.step.clone(),
		            client: engine.client.clone(),
		        };
		        engine
		            .transition_service
		            .register_handler(Arc::new(handler))?;
		    }
	*/

	exitCh := make(chan struct{})

	c := &AuRa{
		db:                db,
		exitCh:            exitCh,
		step:              PermissionedStep{inner: step, canPropose: atomic.NewBool(true)},
		OurSigningAddress: ourSigningAddress,
		cfg:               auraParams,
	}
	_ = config

	return c, nil
}

// A helper accumulator function mapping a step duration and a step duration transition timestamp
// to the corresponding step number and the correct starting second of the step.
func nextStepTimeDuration(info StepDurationInfo, time uint64) (uint64, uint64, bool) {
	stepDiff := time + info.StepDuration
	if stepDiff < 1 {
		return 0, 0, false
	}
	stepDiff -= 1
	if stepDiff < info.TransitionTimestamp {
		return 0, 0, false
	}
	stepDiff -= info.TransitionTimestamp
	if info.StepDuration == 0 {
		return 0, 0, false
	}
	stepDiff /= info.StepDuration
	timeDiff := stepDiff * info.StepDuration
	return info.TransitionStep + stepDiff, info.TransitionTimestamp + timeDiff, true
}

// Author implements consensus.Engine, returning the Ethereum address recovered
// from the signature in the header's extra-data section.
func (c *AuRa) Author(header *types.Header) (common.Address, error) {
	return common.Address{}, nil
	//return ecrecover(header, c.signatures)
}

// VerifyHeader checks whether a header conforms to the consensus rules.
func (c *AuRa) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, _ bool) error {
	return nil
	//return c.verifyHeader(chain, header, nil)
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers. The
// method returns a quit channel to abort the operations and a results channel to
// retrieve the async verifications (the order is that of the input slice).
func (c *AuRa) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, _ []bool) error {
	return nil
	//if len(headers) == 0 {
	//	return nil
	//}
	//for i, header := range headers {
	//	if err := c.verifyHeader(chain, header, headers[:i]); err != nil {
	//		return err
	//	}
	//}
	//return nil
}

// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
func (c *AuRa) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	return nil
	//if len(uncles) > 0 {
	//	return errors.New("uncles not allowed")
	//}
	//return nil
}

// VerifySeal implements consensus.Engine, checking whether the signature contained
// in the header satisfies the consensus protocol requirements.
func (c *AuRa) VerifySeal(chain consensus.ChainHeaderReader, header *types.Header) error {
	return nil
	//snap, err := c.Snapshot(chain, header.Number.Uint64(), header.Hash(), nil)
	//if err != nil {
	//	return err
	//}
	//return c.verifySeal(chain, header, snap)
}

// Prepare implements consensus.Engine, preparing all the consensus fields of the
// header for running the transactions on top.
func (c *AuRa) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {
	return nil
	/// If the block isn't a checkpoint, cast a random vote (good enough for now)
	//header.Coinbase = common.Address{}
	//header.Nonce = types.BlockNonce{}
	//
	//number := header.Number.Uint64()
	/// Assemble the voting snapshot to check which votes make sense
	//snap, err := c.Snapshot(chain, number-1, header.ParentHash, nil)
	//if err != nil {
	//	return err
	//}
	//if number%c.config.Epoch != 0 {
	//	c.lock.RLock()
	//
	//	// Gather all the proposals that make sense voting on
	//	addresses := make([]common.Address, 0, len(c.proposals))
	//	for address, authorize := range c.proposals {
	//		if snap.validVote(address, authorize) {
	//			addresses = append(addresses, address)
	//		}
	//	}
	//	// If there's pending proposals, cast a vote on them
	//	if len(addresses) > 0 {
	//		header.Coinbase = addresses[rand.Intn(len(addresses))]
	//		if c.proposals[header.Coinbase] {
	//			copy(header.Nonce[:], NonceAuthVote)
	//		} else {
	//			copy(header.Nonce[:], nonceDropVote)
	//		}
	//	}
	//	c.lock.RUnlock()
	//}
	/// Set the correct difficulty
	//header.Difficulty = calcDifficulty(snap, c.signer)
	//
	/// Ensure the extra data has all its components
	//if len(header.Extra) < ExtraVanity {
	//	header.Extra = append(header.Extra, bytes.Repeat([]byte{0x00}, ExtraVanity-len(header.Extra))...)
	//}
	//header.Extra = header.Extra[:ExtraVanity]
	//
	//if number%c.config.Epoch == 0 {
	//	for _, signer := range snap.GetSigners() {
	//		header.Extra = append(header.Extra, signer[:]...)
	//	}
	//}
	//header.Extra = append(header.Extra, make([]byte, ExtraSeal)...)
	//
	/// Mix digest is reserved for now, set to empty
	//header.MixDigest = common.Hash{}
	//
	/// Ensure the timestamp has the correct delay
	//parent := chain.GetHeader(header.ParentHash, number-1)
	//if parent == nil {
	//	return consensus.ErrUnknownAncestor
	//}
	//header.Time = parent.Time + c.config.Period
	//
	//now := uint64(time.Now().Unix())
	//if header.Time < now {
	//	header.Time = now
	//}
	//
	//return nil
}

// Finalize implements consensus.Engine, ensuring no uncles are set, nor block
// rewards given.
func (c *AuRa) Finalize(cc *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
	// accumulateRewards retreives rewards for a block and applies them to the coinbase accounts for miner and uncle miners
	beneficiaries, _, rewards, err := AccumulateRewards(cc, c, header, uncles, syscall)
	if err != nil {
		log.Error("accumulateRewards", "err", err)
		return
	}
	for i := range beneficiaries {
		state.AddBalance(beneficiaries[i], rewards[i])
	}
}

// FinalizeAndAssemble implements consensus.Engine, ensuring no uncles are set,
// nor block rewards given, and returns the final block.
func (c *AuRa) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, receipts []*types.Receipt, syscall consensus.SystemCall) (*types.Block, error) {
	// No block rewards in PoA, so the state remains as is and uncles are dropped
	//header.UncleHash = types.CalcUncleHash(nil)

	// Assemble and return the final block for sealing
	return types.NewBlock(header, txs, nil, receipts), nil
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (c *AuRa) Authorize(signer common.Address, signFn clique.SignerFn) {
	c.lock.Lock()
	defer c.lock.Unlock()

	//c.signer = signer
	//c.signFn = signFn
}

// Seal implements consensus.Engine, attempting to create a sealed block using
// the local signing credentials.
func (c *AuRa) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	return nil
	//header := block.Header()
	//
	/// Sealing the genesis block is not supported
	//number := header.Number.Uint64()
	//if number == 0 {
	//	return errUnknownBlock
	//}
	/// For 0-period chains, refuse to seal empty blocks (no reward but would spin sealing)
	//if c.config.Period == 0 && len(block.Transactions()) == 0 {
	//	log.Info("Sealing paused, waiting for transactions")
	//	return nil
	//}
	/// Don't hold the signer fields for the entire sealing procedure
	//c.lock.RLock()
	//signer, signFn := c.signer, c.signFn
	//c.lock.RUnlock()
	//
	/// Bail out if we're unauthorized to sign a block
	//snap, err := c.Snapshot(chain, number-1, header.ParentHash, nil)
	//if err != nil {
	//	return err
	//}
	//if _, authorized := snap.Signers[signer]; !authorized {
	//	return ErrUnauthorizedSigner
	//}
	/// If we're amongst the recent signers, wait for the next block
	//for seen, recent := range snap.Recents {
	//	if recent == signer {
	//		// Signer is among RecentsRLP, only wait if the current block doesn't shift it out
	//		if limit := uint64(len(snap.Signers)/2 + 1); number < limit || seen > number-limit {
	//			log.Info("Signed recently, must wait for others")
	//			return nil
	//		}
	//	}
	//}
	/// Sweet, the protocol permits us to sign the block, wait for our time
	//delay := time.Unix(int64(header.Time), 0).Sub(time.Now()) // nolint: gosimple
	//if header.Difficulty.Cmp(diffNoTurn) == 0 {
	//	// It's not our turn explicitly to sign, delay it a bit
	//	wiggle := time.Duration(len(snap.Signers)/2+1) * wiggleTime
	//	delay += time.Duration(rand.Int63n(int64(wiggle)))
	//
	//	log.Trace("Out-of-turn signing requested", "wiggle", common.PrettyDuration(wiggle))
	//}
	/// Sign all the things!
	//sighash, err := signFn(signer, accounts.MimetypeClique, CliqueRLP(header))
	//if err != nil {
	//	return err
	//}
	//copy(header.Extra[len(header.Extra)-ExtraSeal:], sighash)
	/// Wait until sealing is terminated or delay timeout.
	//log.Trace("Waiting for slot to sign and propagate", "delay", common.PrettyDuration(delay))
	//go func() {
	//	select {
	//	case <-stop:
	//		return
	//	case <-time.After(delay):
	//	}
	//
	//	select {
	//	case results <- block.WithSeal(header):
	//	default:
	//		log.Warn("Sealing result is not read by miner", "sealhash", SealHash(header))
	//	}
	//}()
	//
	//return nil
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have:
// * DIFF_NOTURN(2) if BLOCK_NUMBER % SIGNER_COUNT != SIGNER_INDEX
// * DIFF_INTURN(1) if BLOCK_NUMBER % SIGNER_COUNT == SIGNER_INDEX
func (c *AuRa) CalcDifficulty(chain consensus.ChainHeaderReader, _, _ uint64, _ *big.Int, parentNumber uint64, parentHash, _ common.Hash) *big.Int {
	return nil
	//snap, err := c.Snapshot(chain, parentNumber, parentHash, nil)
	//if err != nil {
	//	return nil
	//}
	//return calcDifficulty(snap, c.signer)
}

func (c *AuRa) SealHash(header *types.Header) common.Hash {
	return clique.SealHash(header)
}

// Close implements consensus.Engine. It's a noop for clique as there are no background threads.
func (c *AuRa) Close() error {
	common.SafeClose(c.exitCh)
	return nil
}

// APIs implements consensus.Engine, returning the user facing RPC API to allow
// controlling the signer voting.
func (c *AuRa) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{
		//{
		//Namespace: "clique",
		//Version:   "1.0",
		//Service:   &API{chain: chain, clique: c},
		//Public:    false,
		//}
	}
}

func (c *AuRa) EmptySteps(fromStep, toStep uint64, parentHash common.Hash) []EmptyStep {
	from := EmptyStep{step: fromStep + 1, parentHash: parentHash}
	to := EmptyStep{step: toStep}
	res := []EmptyStep{}
	if to.Less(&from) {
		return res
	}

	c.EmptyStepsSet.Sort()
	c.EmptyStepsSet.ForEach(func(i int, step *EmptyStep) {
		if step.Less(&from) || (&to).Less(step) {
			return
		}
		if step.parentHash != parentHash {
			return
		}
		res = append(res, *step)
	})
	return res
}

// AccumulateRewards returns rewards for a given block. The mining reward consists
// of the static blockReward plus a reward for each included uncle (if any). Individual
// uncle rewards are also returned in an array.
func AccumulateRewards(_ *params.ChainConfig, aura *AuRa, header *types.Header, _ []*types.Header, syscall consensus.SystemCall) (beneficiaries []common.Address, rewardKind []aurainterfaces.RewardKind, rewards []*uint256.Int, err error) {
	if header.Number.Uint64() == aura.cfg.TwoThirdsMajorityTransition {
		log.Info("Transitioning to 2/3 quorum", "block", aura.cfg.TwoThirdsMajorityTransition)
	}
	if header.Number.Uint64() >= aura.cfg.EmptyStepsTransition {
		var emptySteps []EmptyStep
		if len(header.Seal) == 0 {
			// this is a new block, calculate rewards based on the empty steps messages we have accumulated
			if err = aura.db.View(context.Background(), func(tx ethdb.Tx) error {
				parent := rawdb.ReadHeader(tx, header.ParentHash, header.Number.Uint64())
				if parent == nil {
					return fmt.Errorf("parent not found: %d,%x\n", header.Number.Uint64(), header.ParentHash)
				}
				parentStep, err := headerStep(parent, aura.cfg.EmptyStepsTransition)
				if err != nil {
					return err
				}
				currentStep := aura.step.inner.inner.Load()
				emptySteps = aura.EmptySteps(parentStep, currentStep, parent.Hash())

				return nil
			}); err != nil {
				return
			}
		} else {
			// we're verifying a block, extract empty steps from the seal
			emptySteps, err = headerEmptySteps(header)
			if err != nil {
				return
			}
		}

		for _, s := range emptySteps {
			var author common.Address
			author, err = s.author()
			if err != nil {
				return
			}

			beneficiaries = append(beneficiaries, author)
			rewardKind = append(rewardKind, aurainterfaces.RewardEmptyStep)
		}
	}

	beneficiaries = append(beneficiaries, header.Coinbase)
	rewardKind = append(rewardKind, aurainterfaces.RewardAuthor)

	rewardContractAddress := aura.cfg.BlockRewardContractTransitions.GreaterOrEqual(header.Number.Uint64())
	if rewardContractAddress != nil {
		beneficiaries, rewards = callBlockRewardAbi(rewardContractAddress.Address, syscall, beneficiaries, rewardKind)
		rewardKind = rewardKind[:len(beneficiaries)]
		for i := 0; i < len(rewardKind); i++ {
			rewardKind[i] = aurainterfaces.RewardExternal
		}
	} else {
		// find: n <= header.number
		var foundNum uint64
		var found bool
		for n := range aura.cfg.BlockReward {
			if n > header.Number.Uint64() {
				continue
			}
			if n > foundNum {
				found = true
				foundNum = n
			}
		}
		if !found {
			panic("Current block's reward is not found; this indicates a chain config error")
		}
		reward := aura.cfg.BlockReward[foundNum]
		rewards = append(rewards, reward)
	}

	//err = aura.Validators.onCloseBlock(header, aura.OurSigningAddress)
	//if err != nil {
	//	return
	//}
	return
}

func callBlockRewardAbi(contractAddr common.Address, syscall consensus.SystemCall, beneficiaries []common.Address, rewardKind []aurainterfaces.RewardKind) ([]common.Address, []*uint256.Int) {
	castedKind := make([]uint16, len(rewardKind))
	for i := range rewardKind {
		castedKind[i] = uint16(rewardKind[i])
	}
	packed, err := blockRewardAbi().Pack("reward", beneficiaries, castedKind)
	if err != nil {
		panic(err)
	}
	out, err := syscall(contractAddr, packed)
	if err != nil {
		panic(err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	res, err := blockRewardAbi().Unpack("reward", out)
	if err != nil {
		panic(err)
	}
	_ = res[0]
	_ = res[1]
	fmt.Printf("aaaaa: %#v, %#v\n", res[0], res[1])
	return nil, nil
}

func blockRewardAbi() abi.ABI {
	a, err := abi.JSON(bytes.NewReader(contracts.BlockReward))
	if err != nil {
		panic(err)
	}
	return a
}

// An empty step message that is included in a seal, the only difference is that it doesn't include
// the `parent_hash` in order to save space. The included signature is of the original empty step
// message, which can be reconstructed by using the parent hash of the block in which this sealed
// empty message is inc    luded.
type SealedEmptyStep struct {
	signature []byte // H520
	step      uint64
}

// extracts the empty steps from the header seal. should only be called when there are 3 fields in the seal
// (i.e. header.number() >= self.empty_steps_transition).
func headerEmptySteps(header *types.Header) ([]EmptyStep, error) {
	s := headerEmptyStepsRaw(header)
	sealedSteps := []SealedEmptyStep{}
	err := rlp.DecodeBytes(s, &sealedSteps)
	if err != nil {
		return nil, err
	}
	steps := make([]EmptyStep, len(sealedSteps))
	for i := range sealedSteps {
		steps[i] = newEmptyStepFromSealed(sealedSteps[i], header.ParentHash)
	}
	return steps, nil
}

func newEmptyStepFromSealed(step SealedEmptyStep, parentHash common.Hash) EmptyStep {
	return EmptyStep{
		signature:  step.signature,
		step:       step.step,
		parentHash: parentHash,
	}
}

// extracts the raw empty steps vec from the header seal. should only be called when there are 3 fields in the seal
// (i.e. header.number() >= self.empty_steps_transition)
func headerEmptyStepsRaw(header *types.Header) []byte {
	if len(header.Seal) < 3 {
		panic("was checked with verify_block_basic; has 3 fields; qed")
	}
	return header.Seal[2]
}

func headerStep(header *types.Header, emptyStepsTransition uint64) (uint64, error) {
	if len(header.Seal) == 0 {
		panic(fmt.Errorf("was either checked with verify_block_basic or is genesis; has %v fields; qed (Make sure the spec file has a correct genesis seal)", headerExpectedSealFields(header, emptyStepsTransition)))
	}
	var val uint64
	err := rlp.DecodeBytes(header.Seal[0], &val)
	return val, err
}

func headerExpectedSealFields(header *types.Header, emptyStepsTransition uint64) uint {
	if header.Number.Uint64() >= emptyStepsTransition {
		return 3
	}
	return 2
}

// A message broadcast by authorities when it's their turn to seal a block but there are no
// transactions. Other authorities accumulate these messages and later include them in the seal as
// proof.
//
// An empty step message is created _instead of_ a block if there are no pending transactions.
// It cannot itself be a parent, and `parent_hash` always points to the most recent block. E.g.:
// * Validator A creates block `bA`.
// * Validator B has no pending transactions, so it signs an empty step message `mB`
//   instead whose hash points to block `bA`.
// * Validator C also has no pending transactions, so it also signs an empty step message `mC`
//   instead whose hash points to block `bA`.
// * Validator D creates block `bD`. The parent is block `bA`, and the header includes `mB` and `mC`.
type EmptyStep struct {
	// The signature of the other two fields, by the message's author.
	signature []byte // H520
	// This message's step number.
	step uint64
	// The hash of the most recent block.
	parentHash common.Hash //     H256
}

func (s *EmptyStep) Less(other *EmptyStep) bool {
	if s.step < other.step {
		return true
	}
	if bytes.Compare(s.parentHash[:], other.parentHash[:]) < 0 {
		return true
	}
	if bytes.Compare(s.signature, other.signature) < 0 {
		return true
	}
	return false
}

// Returns `true` if the message has a valid signature by the expected proposer in the message's step.
func (s *EmptyStep) verify(validators ValidatorSet) (bool, error) { //nolint
	//sRlp, err := EmptyStepRlp(s.step, s.parentHash)
	//if err != nil {
	//	return false, err
	//}
	//message := crypto.Keccak256(sRlp)

	/*
		let correct_proposer = step_proposer(validators, &self.parent_hash, self.step);

		publickey::verify_address(&correct_proposer, &self.signature.into(), &message)
		.map_err(|e| e.into())
	*/
	return true, nil
}

func (s *EmptyStep) author() (common.Address, error) {
	sRlp, err := EmptyStepRlp(s.step, s.parentHash)
	if err != nil {
		return common.Address{}, err
	}
	message := crypto.Keccak256(sRlp)
	public, err := secp256k1.RecoverPubkey(message, s.signature)
	if err != nil {
		return common.Address{}, err
	}
	ecdsa, err := crypto.UnmarshalPubkey(public)
	if err != nil {
		return common.Address{}, err
	}
	return crypto.PubkeyToAddress(*ecdsa), nil
}

type EmptyStepSet struct {
	lock sync.Mutex
	list []*EmptyStep
}

func (s *EmptyStepSet) Less(i, j int) bool { return s.list[i].Less(s.list[j]) }
func (s *EmptyStepSet) Swap(i, j int)      { s.list[i], s.list[j] = s.list[j], s.list[i] }
func (s *EmptyStepSet) Len() int           { return len(s.list) }

func (s *EmptyStepSet) Sort() {
	s.lock.Lock()
	defer s.lock.Unlock()
	sort.Stable(s)
}

func (s *EmptyStepSet) ForEach(f func(int, *EmptyStep)) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, el := range s.list {
		f(i, el)
	}
}

func EmptyStepFullRlp(signature []byte, emptyStepRlp []byte) ([]byte, error) {
	type A struct {
		s []byte
		r []byte
	}

	return rlp.EncodeToBytes(A{s: signature, r: emptyStepRlp})
}

func EmptyStepRlp(step uint64, parentHash common.Hash) ([]byte, error) {
	type A struct {
		s uint64
		h common.Hash
	}
	return rlp.EncodeToBytes(A{s: step, h: parentHash})
}
