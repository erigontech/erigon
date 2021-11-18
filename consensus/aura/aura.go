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
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/crypto"

	// "github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura/aurainterfaces"
	"github.com/ledgerwatch/erigon/consensus/aura/contracts"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/atomic"
)

const DEBUG_LOG_FROM = 999_999_999

const (
	epochLength = uint64(30000)          // Default number of blocks after which to checkpoint and reset the pending votes
	ExtraVanity = 32                     // Fixed number of extra-data prefix bytes reserved for signer vanity
	ExtraSeal   = crypto.SignatureLength // Fixed number of extra-data suffix bytes reserved for signer seal
	// warmupCacheSnapshots = 20

/*
	wiggleTime = 500 * time.Millisecond  //Random delay (per signer) to allow concurrent signers
*/
)

/*
var (
	NonceAuthVote = hexutil.MustDecode("0xffffffffffffffff") // Magic nonce number to vote on adding a new signer
	nonceDropVote = hexutil.MustDecode("0x0000000000000000") // Magic nonce number to vote on removing a signer.

	DiffInTurn = big.NewInt(2) // Block difficulty for in-turn signatures
	diffNoTurn = big.NewInt(1) // Block difficulty for out-of-turn signatures
)
*/

var (
	// errUnknownBlock is returned when the list of signers is requested for a block
	// that is not part of the local blockchain.
	errUnknownBlock = errors.New("unknown block")

	// errTooOldOfBlock is returned when the block number requested for is below 20
	errTooOldOfBlock = errors.New("the block requested is too old")

	// errMissingVanity is returned if a block's extra-data section is shorter than
	// 32 bytes, which is required to store the signer vanity.
	errMissingVanity = errors.New("extra-data 32 byte vanity prefix missing")

	// errMissingSignature is returned if a block's extra-data section doesn't seem
	// to contain a 65 byte secp256k1 signature.
	errMissingSignature = errors.New("extra-data 65 byte signature suffix missing")

	// errExtraSigners is returned if non-checkpoint block contain signer data in
	// their extra-data fields.
	errExtraSigners = errors.New("non-checkpoint block contains extra signer list")

	// errInvalidMixDigest is returned if a block's mix digest is non-zero.
	errInvalidMixDigest = errors.New("non-zero mix digest")

	// errFutureStep is returned if the parent step is a step after the supposed step
	errFutureStep = errors.New("current step is ahead of the supposed step")

	// errIncorrectStep is returned if the header step is not the current step
	errIncorrectStep = errors.New("header step is not equal to the current step")

	// errMultipleBlocksInStep is returned when		 multiple blocks are assigned at the same step
	errMultipleBlocksInStep = errors.New("multiple blocks were assigned at the same step")

	// errInvalidPrimary will be returned when the primary
	// validator of the step is not the correct one to assign the block
	errInvalidPrimary = errors.New("the primary validator of this block is not the correct for this step")
)

/*
Not implemented features from OS:
 - two_thirds_majority_transition - because no chains in OE where this is != MaxUint64 - means 1/2 majority used everywhere
 - emptyStepsTransition - same

Repo with solidity sources: https://github.com/poanetwork/posdao-contracts
*/

type StepDurationInfo struct {
	TransitionStep      uint64
	TransitionTimestamp uint64
	StepDuration        uint64
}

// EpochTransitionProof - Holds 2 proofs inside: ValidatorSetProof and FinalityProof
type EpochTransitionProof struct {
	SignalNumber  uint64
	SetProof      []byte
	FinalityProof []byte
}

// ValidatorSetProof - validator set proof
type ValidatorSetProof struct {
	Header   *types.Header
	Receipts types.Receipts
}

// FirstValidatorSetProof state-dependent proofs for the safe contract:
// only "first" proofs are such.
type FirstValidatorSetProof struct { // TODO: whaaat? here is no state!
	ContractAddress common.Address
	Header          *types.Header
}

type EpochTransition struct {
	/// Block hash at which the transition occurred.
	BlockHash common.Hash
	/// Block number at which the transition occurred.
	BlockNumber uint64
	/// "transition/epoch" proof from the engine combined with a finality proof.
	ProofRlp []byte
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

type ReceivedStepHashes map[uint64]map[common.Address]common.Hash //BTreeMap<(u64, Address), H256>

//nolint
/*
func (r ReceivedStepHashes) get(step uint64, author common.Address) (common.Hash, bool) {
	res, ok := r[step]
	if !ok {
		return common.Hash{}, false
	}
	result, ok := res[author]
	return result, ok
}
*/

//nolint
/*
func (r ReceivedStepHashes) insert(step uint64, author common.Address, blockHash common.Hash) {
	res, ok := r[step]
	if !ok {
		res = map[common.Address]common.Hash{}
		r[step] = res
	}
	res[author] = blockHash
}
*/

//nolint
/*
func (r ReceivedStepHashes) dropAncient(step uint64) {
	for i := range r {
		if i < step {
			delete(r, i)
		}
	}
}
*/

//nolint
type EpochManager struct {
	epochTransitionHash   common.Hash // H256,
	epochTransitionNumber uint64      // BlockNumber
	finalityChecker       *RollingFinality
	force                 bool
}

func NewEpochManager() *EpochManager {
	return &EpochManager{
		finalityChecker: NewRollingFinality([]common.Address{}),
		force:           true,
	}
}

func (e *EpochManager) noteNewEpoch() { e.force = true }

// zoomValidators - Zooms to the epoch after the header with the given hash. Returns true if succeeded, false otherwise.
// It's analog of zoom_to_after function in OE, but doesn't require external locking
//nolint
func (e *EpochManager) zoomToAfter(chain consensus.ChainHeaderReader, er consensus.EpochReader, validators ValidatorSet, hash common.Hash, call consensus.SystemCall) (*RollingFinality, uint64, bool) {
	var lastWasParent bool
	if e.finalityChecker.lastPushed != nil {
		lastWasParent = *e.finalityChecker.lastPushed == hash
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
	lastTransition, ok := epochTransitionFor2(chain, er, hash)
	if !ok {
		if lastTransition.BlockNumber > DEBUG_LOG_FROM {
			fmt.Printf("zoom1: %d\n", lastTransition.BlockNumber)
		}
		return e.finalityChecker, e.epochTransitionNumber, false
	}

	// extract other epoch set if it's not the same as the last.
	if lastTransition.BlockHash != e.epochTransitionHash {
		proof := &EpochTransitionProof{}
		if err := rlp.DecodeBytes(lastTransition.ProofRlp, proof); err != nil {
			panic(err)
		}
		first := proof.SignalNumber == 0
		if lastTransition.BlockNumber > DEBUG_LOG_FROM {
			fmt.Printf("zoom2: %d,%d\n", lastTransition.BlockNumber, len(proof.SetProof))
		}

		// use signal number so multi-set first calculation is correct.
		list, _, err := validators.epochSet(first, proof.SignalNumber, proof.SetProof, call)
		if err != nil {
			panic(fmt.Errorf("proof produced by this engine is invalid: %w", err))
		}
		epochSet := list.validators
		log.Trace("[aura] Updating finality checker with new validator set extracted from epoch", "num", lastTransition.BlockNumber)
		e.finalityChecker = NewRollingFinality(epochSet)
		if proof.SignalNumber >= DEBUG_LOG_FROM {
			fmt.Printf("new rolling finality: %d\n", proof.SignalNumber)
			for i := 0; i < len(epochSet); i++ {
				fmt.Printf("\t%x\n", epochSet[i])
			}
		}
	}

	e.epochTransitionHash = lastTransition.BlockHash
	e.epochTransitionNumber = lastTransition.BlockNumber
	return e.finalityChecker, e.epochTransitionNumber, true
}

/// Get the transition to the epoch the given parent hash is part of
/// or transitions to.
/// This will give the epoch that any children of this parent belong to.
///
/// The block corresponding the the parent hash must be stored already.
//nolint
func epochTransitionFor2(chain consensus.ChainHeaderReader, e consensus.EpochReader, parentHash common.Hash) (transition EpochTransition, ok bool) {
	//TODO: probably this version of func doesn't support non-canonical epoch transitions
	h := chain.GetHeaderByHash(parentHash)
	if h == nil {
		return transition, false
	}
	num, hash, transitionProof, err := e.FindBeforeOrEqualNumber(h.Number.Uint64())
	if err != nil {
		panic(err)
	}
	if transitionProof == nil {
		panic("genesis epoch transition must already be set")
	}
	return EpochTransition{BlockNumber: num, BlockHash: hash, ProofRlp: transitionProof}, true
}

//nolint
/*
func epochTransitionFor(chain consensus.ChainHeaderReader, e consensus.EpochReader, parentHash common.Hash) (transition EpochTransition, ok bool) {
	// slow path: loop back block by block
	for {
		h := chain.GetHeaderByHash(parentHash)
		if h == nil {
			return transition, false
		}

		// look for transition in database.
		transitionProof, err := e.GetEpoch(h.Hash(), h.Number.Uint64())
		if err != nil {
			panic(err)
		}

		if transitionProof != nil {
			return EpochTransition{
				BlockNumber: h.Number.Uint64(),
				BlockHash:   h.Hash(),
				ProofRlp:    transitionProof,
			}, true
		}

		// canonical hash -> fast breakout:
		// get the last epoch transition up to this block.
		//
		// if `block_hash` is canonical it will only return transitions up to
		// the parent.
		canonical := chain.GetHeaderByNumber(h.Number.Uint64())
		if canonical == nil {
			return transition, false
		}
		//nolint
		if canonical.Hash() == parentHash {
			return EpochTransition{
				BlockNumber: 0,
				BlockHash:   common.HexToHash("0x5b28c1bfd3a15230c9a46b399cd0f9a6920d432e85381cc6a140b06e8410112f"),
				ProofRlp:    params.SokolGenesisEpochProof,
			}, true
			//  TODO:
			//    return self
			//        .epoch_transitions()
			//        .map(|(_, t)| t)
			//        .take_while(|t| t.block_number <= details.number)
			//        .last();

		}

		parentHash = h.Hash()
	}
}
*/

// AuRa
//nolint
type AuRa struct {
	db   kv.RwDB      // Database to store and retrieve snapshot checkpoints
	lock sync.RWMutex // Protects the signer fields

	step PermissionedStep
	// History of step hashes recently received from peers.
	receivedStepHashes ReceivedStepHashes

	Etherbase     common.Address // Same as Etherbase in Mining
	cfg           AuthorityRoundParams
	EmptyStepsSet *EmptyStepSet
	EpochManager  *EpochManager // Mutex<EpochManager>,

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

func NewAuRa(config *params.AuRaConfig, db kv.RwDB, Etherbase common.Address, engineParamsJson []byte) (*AuRa, error) {
	spec := JsonSpec{}
	err := json.Unmarshal(engineParamsJson, &spec)
	if err != nil {
		return nil, err
	}
	auraParams, err := FromJson(spec)
	if err != nil {
		return nil, err
	}

	c := &AuRa{
		db:                 db,
		Etherbase:          Etherbase,
		cfg:                auraParams,
		receivedStepHashes: ReceivedStepHashes{},
		EpochManager:       NewEpochManager(),
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
	/*
				 let message = keccak(empty_step_rlp(self.step, &self.parent_hash));
		        let public = publickey::recover(&self.signature.into(), &message)?;
		        Ok(publickey::public_to_address(&public))
	*/
	return header.Coinbase, nil
}

// returns the current step the engine is at
func (c *AuRa) GetStep() uint64 {
	return c.step.inner.inner.Load()
}

func (c *AuRa) Initialize(config *params.ChainConfig, chain consensus.ChainHeaderReader, e consensus.EpochReader, header *types.Header, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
}

// TODO: retrieve validators from Database
/*func (c *AuRa) GetValidatorSet() ValidatorSet {
	return c.cfg.Validators
}
*/
// VerifyHeader checks whether a header conforms to the consensus rules.
func (c *AuRa) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, _ bool) error {
	return c.verifyHeader(chain, header, nil)
}

// verifies AuRas uncles
func (c *AuRa) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	for _, uncle := range uncles {
		err := c.verifyUncle(chain, uncle)

		if err != nil {
			return err
		}

	}
	return nil
}

// Gets the list of validators at a certain point in time
// TODO: Add support for SafeContract
func (c *AuRa) getValidators(number uint64) []common.Address {
	var currentBlockNumber uint64 = 0
	var currentValidatorList []common.Address = c.cfg.ValidatorSetChange[0]
	for blockNumber, validators := range c.cfg.ValidatorSetChange {
		if blockNumber > number {
			break
		}
		if blockNumber > currentBlockNumber {
			currentBlockNumber = blockNumber
			currentValidatorList = validators
		}
	}
	return currentValidatorList
}

//nolint
/*
func (c *AuRa) hasReceivedStepHashes(step uint64, author common.Address, newHash common.Hash) bool {

		self
			       .received_step_hashes
			       .read()
			       .get(&received_step_key)
			       .map_or(false, |h| *h != new_hash)
	return false
}
*/

// //nolint
// func (c *AuRa) insertReceivedStepHashes(step uint64, author common.Address, newHash common.Hash) {
// 	/*
// 	   	    self.received_step_hashes
// 	                      .write()
// 	                      .insert(received_step_key, new_hash);
// 	*/
// }

// //nolint
// func (c *AuRa) verifyFamily(chain consensus.ChainHeaderReader, e consensus.EpochReader, header *types.Header, call consensus.Call, syscall consensus.SystemCall) error {
// 	// TODO: I call it from Initialize - because looks like no much reason to have separated "verifyFamily" call

// 	//nolint
// 	step, err := headerStep(header)
// 	if err != nil {
// 		return err
// 	}
// 	parent := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
// 	//nolint
// 	parentStep, err := headerStep(parent)
// 	if err != nil {
// 		return err
// 	}
// 	//nolint
// 	validators, setNumber, err := c.epochSet(chain, e, header, syscall)
// 	if err != nil {
// 		return err
// 	}
// 	return nil

// 	// Ensure header is from the step after parent.
// 	//nolint
// 	if step == parentStep ||
// 		(header.Number.Uint64() >= c.cfg.ValidateStepTransition && step <= parentStep) {
// 		log.Trace("[aura] Multiple blocks proposed for step", "num", parentStep)
// 		_ = setNumber
// 		/*
// 			self.validators.report_malicious(
// 				header.author(),
// 				set_number,
// 				header.number(),
// 				Default::default(),
// 			);
// 			Err(EngineError::DoubleVote(*header.author()))?;
// 		*/
// 		return fmt.Errorf("double vote: %x", header.Coinbase)
// 	}

// 	// Report malice if the validator produced other sibling blocks in the same step.
// 	if !c.hasReceivedStepHashes(step, header.Coinbase, header.Hash()) {
// 		/*
// 		   trace!(target: "engine", "Validator {} produced sibling blocks in the same step", header.author());
// 		   self.validators.report_malicious(
// 		       header.author(),
// 		       set_number,
// 		       header.number(),
// 		       Default::default(),
// 		   );
// 		*/
// 	} else {
// 		c.insertReceivedStepHashes(step, header.Coinbase, header.Hash())
// 	}

// 	// Remove hash records older than two full rounds of steps (picked as a reasonable trade-off between
// 	// memory consumption and fault-tolerance).
// 	cnt, err := count(validators, parent.Hash(), call)
// 	if err != nil {
// 		return err
// 	}
// 	siblingMaliceDetectionPeriod := 2 * cnt
// 	oldestStep := uint64(0) //  let oldest_step = parent_step.saturating_sub(sibling_malice_detection_period);
// 	if parentStep > siblingMaliceDetectionPeriod {
// 		oldestStep = parentStep - siblingMaliceDetectionPeriod
// 	}
// 	//nolint
// 	if oldestStep > 0 {
// 		/*
// 		   let mut rsh = self.received_step_hashes.write();
// 		   let new_rsh = rsh.split_off(&(oldest_step, Address::zero()));
// 		   *rsh = new_rsh;
// 		*/
// 	}

// 	emptyStepLen := uint64(0)
// 	//self.report_skipped(header, step, parent_step, &*validators, set_number);

// 	/*
// 	   // If empty step messages are enabled we will validate the messages in the seal, missing messages are not
// 	   // reported as there's no way to tell whether the empty step message was never sent or simply not included.
// 	   let empty_steps_len = if header.number() >= self.empty_steps_transition {
// 	       let validate_empty_steps = || -> Result<usize, Error> {
// 	           let strict_empty_steps = header.number() >= self.strict_empty_steps_transition;
// 	           let empty_steps = header_empty_steps(header)?;
// 	           let empty_steps_len = empty_steps.len();
// 	           let mut prev_empty_step = 0;

// 	           for empty_step in empty_steps {
// 	               if empty_step.step <= parent_step || empty_step.step >= step {
// 	                   Err(EngineError::InsufficientProof(format!(
// 	                       "empty step proof for invalid step: {:?}",
// 	                       empty_step.step
// 	                   )))?;
// 	               }

// 	               if empty_step.parent_hash != *header.parent_hash() {
// 	                   Err(EngineError::InsufficientProof(format!(
// 	                       "empty step proof for invalid parent hash: {:?}",
// 	                       empty_step.parent_hash
// 	                   )))?;
// 	               }

// 	               if !empty_step.verify(&*validators).unwrap_or(false) {
// 	                   Err(EngineError::InsufficientProof(format!(
// 	                       "invalid empty step proof: {:?}",
// 	                       empty_step
// 	                   )))?;
// 	               }

// 	               if strict_empty_steps {
// 	                   if empty_step.step <= prev_empty_step {
// 	                       Err(EngineError::InsufficientProof(format!(
// 	                           "{} empty step: {:?}",
// 	                           if empty_step.step == prev_empty_step {
// 	                               "duplicate"
// 	                           } else {
// 	                               "unordered"
// 	                           },
// 	                           empty_step
// 	                       )))?;
// 	                   }

// 	                   prev_empty_step = empty_step.step;
// 	               }
// 	           }

// 	           Ok(empty_steps_len)
// 	       };

// 	       match validate_empty_steps() {
// 	           Ok(len) => len,
// 	           Err(err) => {
// 	               trace!(
// 	                   target: "engine",
// 	                   "Reporting benign misbehaviour (cause: invalid empty steps) \
// 	                   at block #{}, epoch set number {}. Own address: {}",
// 	                   header.number(), set_number, self.address().unwrap_or_default()
// 	               );
// 	               self.validators
// 	                   .report_benign(header.author(), set_number, header.number());
// 	               return Err(err);
// 	           }
// 	       }
// 	   } else {
// 	       self.report_skipped(header, step, parent_step, &*validators, set_number);

// 	       0
// 	   };
// 	*/
// 	if header.Number.Uint64() >= c.cfg.ValidateScoreTransition {
// 		expectedDifficulty := calculateScore(parentStep, step, emptyStepLen)
// 		if header.Difficulty.Cmp(expectedDifficulty.ToBig()) != 0 {
// 			return fmt.Errorf("invlid difficulty: expect=%s, found=%s\n", expectedDifficulty, header.Difficulty)
// 		}
// 	}
// 	return nil
// }

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers. The
// method returns a quit channel to abort the operations and a results channel to
// retrieve the async verifications (the order is that of the input slice).
func (c *AuRa) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, _ []bool) error {

	for _, header := range headers {

		err := c.verifyHeader(chain, header, nil)

		if err != nil {
			return err
		}
	}
	return nil
}

// VerifySeal implements consensus.Engine, checking whether the signature contained
// in the header satisfies the consensus protocol requirements.
func (c *AuRa) VerifySeal(chain consensus.ChainHeaderReader, header *types.Header) error {
	return nil
	// snap, err := c.Snapshot(chain, header.Number.Uint64(), header.Hash(), nil)

	// if err != nil {
	// 	return err
	// }
	// return c.verifySeal(chain, header, nil)
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

//word `signal epoch` == word `pending epoch`
func (c *AuRa) Finalize(config *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, r types.Receipts, e consensus.EpochReader, chain consensus.ChainHeaderReader, syscall consensus.SystemCall) error {

	return nil
}

func buildFinality(e *EpochManager, chain consensus.ChainHeaderReader, er consensus.EpochReader, validators ValidatorSet, header *types.Header, syscall consensus.SystemCall) []unAssembledHeader {
	// commit_block -> aura.build_finality
	_, _, ok := e.zoomToAfter(chain, er, validators, header.ParentHash, syscall)
	if !ok {
		return []unAssembledHeader{}
	}
	if e.finalityChecker.lastPushed == nil || *e.finalityChecker.lastPushed != header.ParentHash {
		if err := e.finalityChecker.buildAncestrySubChain(func(hash common.Hash) ([]common.Address, common.Hash, common.Hash, uint64, bool) {
			h := chain.GetHeaderByHash(hash)
			if h == nil {
				return nil, common.Hash{}, common.Hash{}, 0, false
			}
			return []common.Address{h.Coinbase}, h.Hash(), h.ParentHash, h.Number.Uint64(), true
		}, header.ParentHash, e.epochTransitionHash); err != nil {
			//log.Warn("[aura] buildAncestrySubChain", "err", err)
			return []unAssembledHeader{}
		}
	}

	res, err := e.finalityChecker.push(header.Hash(), header.Number.Uint64(), []common.Address{header.Coinbase})
	if err != nil {
		//log.Warn("[aura] finalityChecker.push", "err", err)
		return []unAssembledHeader{}
	}
	return res
}

func isEpochEnd(chain consensus.ChainHeaderReader, e consensus.EpochReader, finalized []unAssembledHeader, header *types.Header) ([]byte, error) {
	// commit_block -> aura.is_epoch_end
	for i := range finalized {
		pendingTransitionProof, err := e.GetPendingEpoch(finalized[i].hash, finalized[i].number)
		if err != nil {
			return nil, err
		}
		if pendingTransitionProof == nil {
			continue
		}
		if header.Number.Uint64() >= DEBUG_LOG_FROM {
			fmt.Printf("pending transition: %d,%x,len=%d\n", finalized[i].number, finalized[i].hash, len(pendingTransitionProof))
		}

		finalityProof := allHeadersUntil(chain, header, finalized[i].hash)
		var finalizedHeader *types.Header
		if finalized[i].hash == header.Hash() {
			finalizedHeader = header
		} else {
			finalizedHeader = chain.GetHeader(finalized[i].hash, finalized[i].number)
		}
		signalNumber := finalizedHeader.Number
		finalityProof = append(finalityProof, finalizedHeader)
		for i, j := 0, len(finalityProof)-1; i < j; i, j = i+1, j-1 { // reverse
			finalityProof[i], finalityProof[j] = finalityProof[j], finalityProof[i]
		}
		finalityProofRLP, err := rlp.EncodeToBytes(finalityProof)
		if err != nil {
			return nil, err
		}
		/*
			// We turn off can_propose here because upon validator set change there can
			// be two valid proposers for a single step: one from the old set and
			// one from the new.
			//
			// This way, upon encountering an epoch change, the proposer from the
			// new set will be forced to wait until the next step to avoid sealing a
			// block that breaks the invariant that the parent's step < the block's step.
			self.step.can_propose.store(false, AtomicOrdering::SeqCst);
		*/
		return rlp.EncodeToBytes(EpochTransitionProof{SignalNumber: signalNumber.Uint64(), SetProof: pendingTransitionProof, FinalityProof: finalityProofRLP})
	}
	return nil, nil
}

// allHeadersUntil walk the chain backwards from current head until finalized_hash
// to construct transition proof. author == ec_recover(sig) known
// since the blocks are in the DB.
func allHeadersUntil(chain consensus.ChainHeaderReader, from *types.Header, to common.Hash) (out []*types.Header) {
	var header = from
	for {
		header = chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
		if header == nil {
			panic("not found header")
		}
		if header.Number.Uint64() == 0 {
			break
		}
		if to == header.Hash() {
			break
		}
		out = append(out, header)
	}
	return out
}

//func (c *AuRa) check_epoch_end(cc *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
//}

// FinalizeAndAssemble implements consensus.Engine
func (c *AuRa) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, r types.Receipts,
	e consensus.EpochReader, chain consensus.ChainHeaderReader, syscall consensus.SystemCall, call consensus.Call) (*types.Block, error) {
	c.Finalize(chainConfig, header, state, txs, uncles, r, e, chain, syscall)

	// Assemble and return the final block for sealing
	return types.NewBlock(header, txs, uncles, r), nil
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (c *AuRa) Authorize(signer common.Address, signFn clique.SignerFn) {
	c.lock.Lock()
	defer c.lock.Unlock()

	//c.signer = signer
	//c.signFn = signFn
}

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

func stepProposer(validators ValidatorSet, blockHash common.Hash, step uint64, call consensus.Call) (common.Address, error) {
	//c, err := validators.defaultCaller(blockHash)
	//if err != nil {
	//	return common.Address{}, err
	//}
	return validators.getWithCaller(blockHash, uint(step), call)
}

// GenerateSeal - Attempt to seal the block internally.
//
// This operation is synchronous and may (quite reasonably) not be available, in which case
// `Seal::None` will be returned.
func (c *AuRa) GenerateSeal(chain consensus.ChainHeaderReader, current, parent *types.Header, call consensus.Call) []rlp.RawValue {
	return nil
}

//nolint
func HeaderStep(current *types.Header) (val uint64, err error) {
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
	return CalculateScore(parentStep, currentStep, uint64(currentEmptyStepsLen)).ToBig()

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
func CalculateScore(parentStep, currentStep, currentEmptySteps uint64) *uint256.Int {
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
/*
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
*/

// AccumulateRewards returns rewards for a given block. The mining reward consists
// of the static blockReward plus a reward for each included uncle (if any). Individual
// uncle rewards are also returned in an array.
func AccumulateRewards(_ *params.ChainConfig, aura *AuRa, header *types.Header, _ []*types.Header, syscall consensus.SystemCall) (beneficiaries []common.Address, rewardKind []aurainterfaces.RewardKind, rewards []*uint256.Int, err error) {

	// This needs to be re-implemented from scratch
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
// empty message is included.
//nolint
/*
type SealedEmptyStep struct {
	signature []byte // H520
	step      uint64
}
*/
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
/*
func (s *EmptyStep) verify(validators ValidatorSet) (bool, error) { //nolint
	//sRlp, err := EmptyStepRlp(s.step, s.parentHash)
	//if err != nil {
	//	return false, err
	//}
	//message := crypto.Keccak256(sRlp)


		// let correct_proposer = step_proposer(validators, &self.parent_hash, self.step);

		// publickey::verify_address(&correct_proposer, &self.signature.into(), &message)
		// .map_err(|e| e.into())

	return true, nil
}
*/

//nolint
/*
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
*/

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

//nolint
type unAssembledHeader struct {
	hash    common.Hash
	number  uint64
	signers []common.Address
}
type unAssembledHeaders struct {
	l *list.List
}

func (u unAssembledHeaders) PushBack(header *unAssembledHeader)  { u.l.PushBack(header) }
func (u unAssembledHeaders) PushFront(header *unAssembledHeader) { u.l.PushFront(header) }
func (u unAssembledHeaders) Pop() *unAssembledHeader {
	e := u.l.Front()
	if e == nil {
		return nil
	}
	u.l.Remove(e)
	return e.Value.(*unAssembledHeader)
}
func (u unAssembledHeaders) Front() *unAssembledHeader {
	e := u.l.Front()
	if e == nil {
		return nil
	}
	return e.Value.(*unAssembledHeader)
}

// RollingFinality checker for authority round consensus.
// Stores a chain of unfinalized hashes that can be pushed onto.
//nolint
type RollingFinality struct {
	headers    unAssembledHeaders //nolint
	signers    *SimpleList
	signCount  map[common.Address]uint
	lastPushed *common.Hash // Option<H256>,
}

// NewRollingFinality creates a blank finality checker under the given validator set.
func NewRollingFinality(signers []common.Address) *RollingFinality {
	return &RollingFinality{
		signers:   NewSimpleList(signers),
		headers:   unAssembledHeaders{l: list.New()},
		signCount: map[common.Address]uint{},
	}
}

// Clears the finality status, but keeps the validator set.
func (f *RollingFinality) print(num uint64) {
	if num > DEBUG_LOG_FROM {
		h := f.headers
		fmt.Printf("finality_heads: %d\n", num)
		i := 0
		for e := h.l.Front(); e != nil; e = e.Next() {
			i++
			a := e.Value.(*unAssembledHeader)
			fmt.Printf("\t%d,%x\n", a.number, a.signers[0])
		}
		if i == 0 {
			fmt.Printf("\tempty\n")
		}
	}
}

func (f *RollingFinality) clear() {
	f.headers = unAssembledHeaders{l: list.New()}
	f.signCount = map[common.Address]uint{}
	f.lastPushed = nil
}

// Push a hash onto the rolling finality checker (implying `subchain_head` == head.parent)
//
// Fails if `signer` isn't a member of the active validator set.
// Returns a list of all newly finalized headers.
func (f *RollingFinality) push(head common.Hash, num uint64, signers []common.Address) (newlyFinalized []unAssembledHeader, err error) {
	for i := range signers {
		if !f.hasSigner(signers[i]) {
			return nil, fmt.Errorf("unknown validator")
		}
	}

	f.addSigners(signers)
	f.headers.PushBack(&unAssembledHeader{hash: head, number: num, signers: signers})

	for f.isFinalized() {
		e := f.headers.Pop()
		if e == nil {
			panic("headers length always greater than sign count length")
		}
		f.removeSigners(e.signers)
		newlyFinalized = append(newlyFinalized, *e)
	}
	f.lastPushed = &head
	return newlyFinalized, nil
}

// isFinalized returns whether the first entry in `self.headers` is finalized.
func (f *RollingFinality) isFinalized() bool {
	e := f.headers.Front()
	if e == nil {
		return false
	}
	return len(f.signCount)*2 > len(f.signers.validators)
}
func (f *RollingFinality) hasSigner(signer common.Address) bool {
	for j := range f.signers.validators {
		if f.signers.validators[j] == signer {
			return true

		}
	}
	return false
}
func (f *RollingFinality) addSigners(signers []common.Address) bool {
	for i := range signers {
		count, ok := f.signCount[signers[i]]
		if ok {
			f.signCount[signers[i]] = count + 1
		} else {
			f.signCount[signers[i]] = 1
		}
	}
	return false
}
func (f *RollingFinality) removeSigners(signers []common.Address) {
	for i := range signers {
		count, ok := f.signCount[signers[i]]
		if !ok {
			panic("all hashes in `header` should have entries in `sign_count` for their signers")
			//continue
		}
		if count <= 1 {
			delete(f.signCount, signers[i])
		} else {
			f.signCount[signers[i]] = count - 1
		}
	}
}
func (f *RollingFinality) buildAncestrySubChain(get func(hash common.Hash) ([]common.Address, common.Hash, common.Hash, uint64, bool), parentHash, epochTransitionHash common.Hash) error { // starts from chainHeadParentHash
	f.clear()

	for {
		signers, blockHash, newParentHash, blockNum, ok := get(parentHash)
		if !ok {
			return nil
		}
		if blockHash == epochTransitionHash {
			return nil
		}
		for i := range signers {
			if !f.hasSigner(signers[i]) {
				return fmt.Errorf("unknown validator: blockNum=%d", blockNum)
			}
		}
		if f.lastPushed == nil {
			copyHash := parentHash
			f.lastPushed = &copyHash
		}
		f.addSigners(signers)
		f.headers.PushFront(&unAssembledHeader{hash: blockHash, number: blockNum, signers: signers})
		// break when we've got our first finalized block.
		if f.isFinalized() {
			e := f.headers.Pop()
			if e == nil {
				panic("we just pushed a block")
			}
			f.removeSigners(e.signers)
			//log.Info("[aura] finality encountered already finalized block", "hash", e.hash.String(), "number", e.number)
			break
		}

		parentHash = newParentHash
	}
	return nil
}
