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

package aura

import (
	"bytes"
	"encoding/json"
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

/*
Not implemented features from OS:
 - two_thirds_majority_transition - because no chains in OE where this is != MaxUint64 - means 1/2 majority used everywhere
 - emptyStepsTransition - same

*/

type StepDurationInfo struct {
	TransitionStep      uint64
	TransitionTimestamp uint64
	StepDuration        uint64
}

type EpochTransition struct {
	/// Block hash at which the transition occurred.
	blockHash common.Hash
	/// Block number at which the transition occurred.
	blockNumber uint64
	/// "transition/epoch" proof from the engine combined with a finality proof.
	proof []byte
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

// zoomValidators - Zooms to the epoch after the header with the given hash. Returns true if succeeded, false otherwise.
// It's analog of zoom_to_after function in OE, but doesn't require external locking
//nolint
func (e *EpochManager) zoom(chain consensus.ChainHeaderReader, validators ValidatorSet, h *types.Header) (RollingFinality, uint64, bool) {
	var lastWasParent bool
	if e.finalityChecker.lastPushed != nil {
		lastWasParent = *e.finalityChecker.lastPushed == h.Hash()
	}

	// early exit for current target == chain head, but only if the epochs are
	// the same.
	if lastWasParent && !e.force {
		return e.finalityChecker, e.epochTransitionNumber, true
	}
	e.force = false

	// epoch_transition_for can be an expensive call, but in the absence of
	// forks it will only need to be called for the block directly after
	// epoch transition, in which case it will be O(1) and require a single
	// DB lookup.
	last_transition_hash, last_transition_num, ok := epochTransitionFor(chain, h.ParentHash)
	if !ok {
		return e.finalityChecker, e.epochTransitionNumber, false
	}
	// extract other epoch set if it's not the same as the last.
	if last_transition_hash != e.epochTransitionHash {
		/*
		   let (signal_number, set_proof, _) = destructure_proofs(&last_transition.proof)
		       .expect("proof produced by this engine; therefore it is valid; qed");

		   trace!(
		       target: "engine",
		       "extracting epoch validator set for epoch ({}, {}) signalled at #{}",
		       last_transition.block_number, last_transition.block_hash, signal_number
		   );

		   let first = signal_number == 0;
		   let (list, _) = validators
		       .epoch_set(
		           first,
		           machine,
		           signal_number, // use signal number so multi-set first calculation is correct.
		           set_proof,
		       )
		       .expect("proof produced by this engine; therefore it is valid; qed");
		   trace!(
		       target: "engine",
		       "Updating finality checker with new validator set extracted from epoch ({}, {}): {:?}",
		       last_transition.block_number, last_transition.block_hash, &list
		   );
		   let epoch_set = list.into_inner();
		   self.finality_checker =
		       RollingFinality::blank(epoch_set);
		*/
	}
	e.epochTransitionHash = last_transition_hash
	e.epochTransitionNumber = last_transition_num
	return e.finalityChecker, e.epochTransitionNumber, true
}

/// Get the transition to the epoch the given parent hash is part of
/// or transitions to.
/// This will give the epoch that any children of this parent belong to.
///
/// The block corresponding the the parent hash must be stored already.
//nolint
func epochTransitionFor(chain consensus.ChainHeaderReader, parentHash common.Hash) (common.Hash, uint64, bool) {
	// slow path: loop back block by block
	for {
		h := chain.GetHeaderByHash(parentHash)
		if h == nil {
			return parentHash, 0, false
		}

		// look for transition in database.
		transitionHash, transitionNum, ok := epochTransition(h.Number.Uint64(), h.Hash())
		if ok {
			return transitionHash, transitionNum, true
		}

		// canonical hash -> fast breakout:
		// get the last epoch transition up to this block.
		//
		// if `block_hash` is canonical it will only return transitions up to
		// the parent.
		canonical := chain.GetHeaderByNumber(h.Number.Uint64())
		if canonical == nil {
			return parentHash, h.Number.Uint64() - 1, false
		}
		//nolint
		if canonical.Hash() == parentHash {
			/* TODO: whaaaat????
			   return self
			       .epoch_transitions()
			       .map(|(_, t)| t)
			       .take_while(|t| t.block_number <= details.number)
			       .last();
			*/
		}

		parentHash = h.Hash()
	}
}

// epochTransition get a specific epoch transition by block number and provided block hash.
//nolint
func epochTransition(blockNum uint64, blockHash common.Hash) (common.Hash, uint64, bool) {
	return blockHash, blockNum, true
	/*
		pub fn epoch_transition(&self, block_num: u64, block_hash: H256) -> Option<EpochTransition> {
		   trace!(target: "blockchain", "Loading epoch transition at block {}, {}",
		    block_num, block_hash);

		   self.db
		       .key_value()
		       .read(db::COL_EXTRA, &block_num)
		       .and_then(|transitions: EpochTransitions| {
		           transitions
		               .candidates
		               .into_iter()
		               .find(|c| c.block_hash == block_hash)
		       })
		}
	*/
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
	EpochManager      EpochManager // Mutex<EpochManager>,

	//Validators                     ValidatorSet
	//ValidateScoreTransition        uint64
	//ValidateStepTransition         uint64
	//immediateTransitions           bool
	//blockReward                    map[uint64]*uint256.Int
	//blockRewardContractTransitions BlockRewardContractList
	//maximumUncleCountTransition    uint64
	//maximumUncleCount              uint
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

func NewAuRa(config *params.AuRaConfig, db ethdb.RwKV, ourSigningAddress common.Address, engineParamsJson []byte) (*AuRa, error) {
	spec := JsonSpec{}
	err := json.Unmarshal(engineParamsJson, &spec)
	if err != nil {
		return nil, err
	}
	auraParams, err := FromJson(spec)
	if err != nil {
		return nil, err
	}

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
		        epoch_manager: Mutex::new(EpochManager::blank()),
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

// FinalizeAndAssemble implements consensus.Engine
func (c *AuRa) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, receipts []*types.Receipt, syscall consensus.SystemCall) (*types.Block, error) {
	c.Finalize(chainConfig, header, state, txs, uncles, syscall)

	// Assemble and return the final block for sealing
	return types.NewBlock(header, txs, uncles, receipts), nil
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

func stepProposer(validators ValidatorSet, blockHash common.Hash, step uint64) (common.Address, error) {
	c, err := validators.defaultCaller(blockHash)
	if err != nil {
		return common.Address{}, err
	}
	return validators.getWithCaller(blockHash, uint(step), c)
}

// GenerateSeal - Attempt to seal the block internally.
//
// This operation is synchronous and may (quite reasonably) not be available, in which case
// `Seal::None` will be returned.
func (c *AuRa) GenerateSeal(chain consensus.ChainHeaderReader, current, parent *types.Header) []rlp.RawValue {
	// first check to avoid generating signature most of the time
	// (but there's still a race to the `compare_exchange`)
	if !c.step.canPropose.Load() {
		log.Trace("[engine] Aborting seal generation. Can't propose.")
		return nil
	}
	parentStep, err := headerStep(parent)
	if err != nil {
		panic(err)
	}
	step := c.step.inner.inner.Load()

	// filter messages from old and future steps and different parents
	expectedDiff := calculateScore(parentStep, step, 0)
	if current.Difficulty.Cmp(expectedDiff.ToBig()) != 0 {
		log.Debug(fmt.Sprintf("[engine] Aborting seal generation. The step or empty_steps have changed in the meantime. %d != %d", current.Difficulty, expectedDiff))
		return nil
	}

	if parentStep > step {
		log.Warn(fmt.Sprintf("[engine] Aborting seal generation for invalid step: %d > %d", parentStep, step))
		return nil
	}

	validators, setNumber, err := c.epochSet(chain, current)
	if err != nil {
		log.Warn("[engine] Unable to generate seal", "err", err)
		return nil
	}

	stepProposerAddr, err := stepProposer(validators, current.ParentHash, step)
	if err != nil {
		log.Warn("[engine] Unable to get stepProposer", "err", err)
		return nil
	}
	if stepProposerAddr != current.Coinbase {
		return nil
	}

	// this is guarded against by `can_propose` unless the block was signed
	// on the same step (implies same key) and on a different node.
	if parentStep == step {
		log.Warn("Attempted to seal block on the same step as parent. Is this authority sealing with more than one node?")
		return nil
	}

	_ = setNumber
	/*
		signature, err := c.sign(current.bareHash())
			if err != nil {
				log.Warn("[engine] generate_seal: FAIL: Accounts secret key unavailable.", "err", err)
				return nil
			}
	*/

	/*
		  // only issue the seal if we were the first to reach the compare_exchange.
		  if self
			  .step
			  .can_propose
			  .compare_exchange(true, false, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
			  .is_ok()
		  {
			  // we can drop all accumulated empty step messages that are
			  // older than the parent step since we're including them in
			  // the seal
			  self.clear_empty_steps(parent_step);

			  // report any skipped primaries between the parent block and
			  // the block we're sealing, unless we have empty steps enabled
			  if header.number() < self.empty_steps_transition {
				  self.report_skipped(header, step, parent_step, &*validators, set_number);
			  }

			  let mut fields =
				  vec![encode(&step), encode(&(H520::from(signature).as_bytes()))];

			  if let Some(empty_steps_rlp) = empty_steps_rlp {
				  fields.push(empty_steps_rlp);
			  }

			  return Seal::Regular(fields);
		  }
	*/
	return nil
}

// epochSet fetch correct validator set for epoch at header, taking into account
// finality of previous transitions.
func (c *AuRa) epochSet(chain consensus.ChainHeaderReader, h *types.Header) (ValidatorSet, uint64, error) {
	if c.cfg.ImmediateTransitions {
		return c.cfg.Validators, h.Number.Uint64(), nil
	}

	//TODO: hardcode for now
	if h.Number.Uint64() <= 671 {
		return &SimpleList{validators: []common.Address{
			common.HexToAddress("0xe8ddc5c7a2d2f0d7a9798459c0104fdf5e987aca"),
		}}, 0, nil
	}
	return &SimpleList{validators: []common.Address{
		common.HexToAddress("0xe8ddc5c7a2d2f0d7a9798459c0104fdf5e987aca"),
		common.HexToAddress("0x82e4e61e7f5139ff0a4157a5bc687ef42294c248"),
	}}, 672, nil
	/*
		finalityChecker, epochTransitionNumber, ok := c.EpochManager.zoom(chain, c.cfg.Validators, h)
		if !ok {
			return nil, 0, fmt.Errorf("Unable to zoom to epoch.")
		}
		return finalityChecker.validators(), epochTransitionNumber
	*/

	/*
			fn epoch_set<'a>(
		        &'a self,
		        header: &Header,
		    ) -> Result<(CowLike<dyn ValidatorSet, SimpleList>, BlockNumber), Error> {
		        Ok(if self.immediate_transitions {
		            (CowLike::Borrowed(&*self.validators), header.number())
		        } else {
		            let mut epoch_manager = self.epoch_manager.lock();
		            let client = self.upgrade_client_or("Unable to verify sig")?;

		            if !epoch_manager.zoom_to_after(
		                &*client,
		                &self.machine,
		                &*self.validators,
		                *header.parent_hash(),
		            ) {
		                debug!(target: "engine", "Unable to zoom to epoch.");
		                return Err(EngineError::RequiresClient.into());
		            }

		            (
		                CowLike::Owned(epoch_manager.validators().clone()),
		                epoch_manager.epoch_transition_number,
		            )
		        })
		    }
	*/
}

//nolint
func headerStep(current *types.Header) (val uint64, err error) {
	if len(current.Seal) < 1 {
		panic("was either checked with verify_block_basic or is genesis; has 2 fields; qed (Make sure the spec file has a correct genesis seal)")
	}
	err = rlp.Decode(bytes.NewReader(current.Seal[0]), &val)
	if err != nil {
		return val, err
	}
	return val, err
}

func (c *AuRa) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentHash, parentUncleHash common.Hash, parentSeal []rlp.RawValue) *big.Int {
	var parentStep uint64
	err := rlp.Decode(bytes.NewReader(parentSeal[0]), &parentStep)
	if err != nil {
		panic(err)
	}
	currentStep := c.step.inner.inner.Load()
	currentEmptyStepsLen := 0
	return calculateScore(parentStep, currentStep, uint64(currentEmptyStepsLen)).ToBig()

	/* TODO: do I need gasLimit override logic here ?
	if let Some(gas_limit) = self.gas_limit_override(header) {
		trace!(target: "engine", "Setting gas limit to {} for block {}.", gas_limit, header.number());
		let parent_gas_limit = *parent.gas_limit();
		header.set_gas_limit(gas_limit);
		if parent_gas_limit != gas_limit {
			info!(target: "engine", "Block gas limit was changed from {} to {}.", parent_gas_limit, gas_limit);
		}
	}
	*/
}

// calculateScore - analog of PoW difficulty:
//    sqrt(U256::max_value()) + parent_step - current_step + current_empty_steps
func calculateScore(parentStep, currentStep, currentEmptySteps uint64) *uint256.Int {
	maxU128 := uint256.NewInt(0).SetAllOne()
	maxU128 = maxU128.Rsh(maxU128, 128)
	res := maxU128.Add(maxU128, uint256.NewInt(parentStep))
	res = res.Sub(res, uint256.NewInt(currentStep))
	res = res.Add(res, uint256.NewInt(currentEmptySteps))
	return res
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

//nolint
func (c *AuRa) emptySteps(fromStep, toStep uint64, parentHash common.Hash) []EmptyStep {
	from := EmptyStep{step: fromStep + 1, parentHash: parentHash}
	to := EmptyStep{step: toStep}
	res := []EmptyStep{}
	if to.LessOrEqual(&from) {
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
	beneficiaries = append(beneficiaries, header.Coinbase)
	rewardKind = append(rewardKind, aurainterfaces.RewardAuthor)

	var rewardContractAddress BlockRewardContract
	var foundContract bool
	for _, c := range aura.cfg.BlockRewardContractTransitions {
		if c.blockNum > header.Number.Uint64() {
			break
		}
		foundContract = true
		rewardContractAddress = c
	}
	if foundContract {
		beneficiaries, rewards = callBlockRewardAbi(rewardContractAddress.address, syscall, beneficiaries, rewardKind)
		rewardKind = rewardKind[:len(beneficiaries)]
		for i := 0; i < len(rewardKind); i++ {
			rewardKind[i] = aurainterfaces.RewardExternal
		}
	} else {
		// block_reward.iter.rev().find(|&(block, _)| *block <= number)
		var reward BlockReward
		var found bool
		for i := range aura.cfg.BlockReward {
			if aura.cfg.BlockReward[i].blockNum > header.Number.Uint64() {
				break
			}
			found = true
			reward = aura.cfg.BlockReward[i]
		}
		if !found {
			panic("Current block's reward is not found; this indicates a chain config error")
		}

		for range beneficiaries {
			rewards = append(rewards, reward.amount)
		}
	}

	//err = aura.cfg.Validators.onCloseBlock(header, aura.OurSigningAddress)
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
//nolint
type SealedEmptyStep struct {
	signature []byte // H520
	step      uint64
}

/*
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
*/

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
func (s *EmptyStep) LessOrEqual(other *EmptyStep) bool {
	if s.step <= other.step {
		return true
	}
	if bytes.Compare(s.parentHash[:], other.parentHash[:]) <= 0 {
		return true
	}
	if bytes.Compare(s.signature, other.signature) <= 0 {
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

//nolint
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
