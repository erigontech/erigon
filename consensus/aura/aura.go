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
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
)

const DEBUG_LOG_FROM = 999_999_999

/*
Not implemented features from OS:
 - two_thirds_majority_transition - because no chains in OE where this is != MaxUint64 - means 1/2 majority used everywhere
 - emptyStepsTransition - same

Repo with solidity sources: https://github.com/poanetwork/posdao-contracts
*/

type Step struct {
	calibrate bool // whether calibration is enabled.
	inner     atomic.Uint64
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
	now := time.Now().Unix()
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

type ReceivedStepHashes map[uint64]map[libcommon.Address]libcommon.Hash //BTreeMap<(u64, Address), H256>

// nolint
func (r ReceivedStepHashes) get(step uint64, author libcommon.Address) (libcommon.Hash, bool) {
	res, ok := r[step]
	if !ok {
		return libcommon.Hash{}, false
	}
	result, ok := res[author]
	return result, ok
}

// nolint
func (r ReceivedStepHashes) insert(step uint64, author libcommon.Address, blockHash libcommon.Hash) {
	res, ok := r[step]
	if !ok {
		res = map[libcommon.Address]libcommon.Hash{}
		r[step] = res
	}
	res[author] = blockHash
}

// nolint
func (r ReceivedStepHashes) dropAncient(step uint64) {
	for i := range r {
		if i < step {
			delete(r, i)
		}
	}
}

// nolint
type EpochManager struct {
	epochTransitionHash   libcommon.Hash // H256,
	epochTransitionNumber uint64         // BlockNumber
	finalityChecker       *RollingFinality
	force                 bool
}

func NewEpochManager() *EpochManager {
	return &EpochManager{
		finalityChecker: NewRollingFinality([]libcommon.Address{}),
		force:           true,
	}
}

func (e *EpochManager) noteNewEpoch() { e.force = true }

// zoomValidators - Zooms to the epoch after the header with the given hash. Returns true if succeeded, false otherwise.
// It's analog of zoom_to_after function in OE, but doesn't require external locking
// nolint
func (e *EpochManager) zoomToAfter(chain consensus.ChainHeaderReader, er *NonTransactionalEpochReader, validators ValidatorSet, hash libcommon.Hash, call consensus.SystemCall) (*RollingFinality, uint64, bool) {
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
	lastTransition, ok := epochTransitionFor(chain, er, hash)
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

// / Get the transition to the epoch the given parent hash is part of
// / or transitions to.
// / This will give the epoch that any children of this parent belong to.
// /
// / The block corresponding the the parent hash must be stored already.
// nolint
func epochTransitionFor(chain consensus.ChainHeaderReader, e *NonTransactionalEpochReader, parentHash libcommon.Hash) (transition EpochTransition, ok bool) {
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

// AuRa
// nolint
type AuRa struct {
	e      *NonTransactionalEpochReader
	exitCh chan struct{}
	lock   sync.RWMutex // Protects the signer fields

	step PermissionedStep
	// History of step hashes recently received from peers.
	receivedStepHashes ReceivedStepHashes

	cfg           AuthorityRoundParams
	EmptyStepsSet *EmptyStepSet
	EpochManager  *EpochManager // Mutex<EpochManager>,

	certifier     *libcommon.Address // certifies service transactions
	certifierLock sync.RWMutex
}

func NewAuRa(spec *chain.AuRaConfig, db kv.RwDB) (*AuRa, error) {
	auraParams, err := FromJson(spec)
	if err != nil {
		return nil, err
	}

	if _, ok := auraParams.StepDurations[0]; !ok {
		return nil, fmt.Errorf("authority Round step 0 duration is undefined")
	}
	for _, v := range auraParams.StepDurations {
		if v == 0 {
			return nil, fmt.Errorf("authority Round step duration cannot be 0")
		}
	}
	//shouldTimeout := auraParams.StartStep == nil
	initialStep := uint64(0)
	if auraParams.StartStep != nil {
		initialStep = *auraParams.StartStep
	}
	durations := make([]StepDurationInfo, 0, 1+len(auraParams.StepDurations))
	durInfo := StepDurationInfo{
		TransitionStep:      0,
		TransitionTimestamp: 0,
		StepDuration:        auraParams.StepDurations[0],
	}
	durations = append(durations, durInfo)
	times := libcommon.SortedKeys(auraParams.StepDurations)
	for i := 1; i < len(auraParams.StepDurations); i++ { // skip first
		time := times[i]
		dur := auraParams.StepDurations[time]
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
		calibrate: auraParams.StartStep == nil,
		durations: durations,
	}
	step.inner.Store(initialStep)
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
		e:                  newEpochReader(db),
		exitCh:             exitCh,
		step:               PermissionedStep{inner: step},
		cfg:                auraParams,
		receivedStepHashes: ReceivedStepHashes{},
		EpochManager:       NewEpochManager(),
	}
	c.step.canPropose.Store(true)

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

// Type returns underlying consensus engine
func (c *AuRa) Type() chain.ConsensusName {
	return chain.AuRaConsensus
}

// Author implements consensus.Engine, returning the Ethereum address recovered
// from the signature in the header's extra-data section.
// This is thread-safe (only access the Coinbase of the header)
func (c *AuRa) Author(header *types.Header) (libcommon.Address, error) {
	/*
				 let message = keccak(empty_step_rlp(self.step, &self.parent_hash));
		        let public = publickey::recover(&self.signature.into(), &message)?;
		        Ok(publickey::public_to_address(&public))
	*/
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules.
func (c *AuRa) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, _ bool) error {
	number := header.Number.Uint64()
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		log.Error("consensus.ErrUnknownAncestor", "parentNum", number-1, "hash", header.ParentHash.String())
		return consensus.ErrUnknownAncestor
	}
	return ethash.VerifyHeaderBasics(chain, header, parent, true /*checkTimestamp*/, c.HasGasLimitContract() /*skipGasLimit*/)
}

// nolint
func (c *AuRa) hasReceivedStepHashes(step uint64, author libcommon.Address, newHash libcommon.Hash) bool {
	/*
		self
			       .received_step_hashes
			       .read()
			       .get(&received_step_key)
			       .map_or(false, |h| *h != new_hash)
	*/
	return false
}

// nolint
func (c *AuRa) insertReceivedStepHashes(step uint64, author libcommon.Address, newHash libcommon.Hash) {
	/*
	   	    self.received_step_hashes
	                      .write()
	                      .insert(received_step_key, new_hash);
	*/
}

// nolint
func (c *AuRa) verifyFamily(chain consensus.ChainHeaderReader, e *NonTransactionalEpochReader, header *types.Header, call consensus.Call, syscall consensus.SystemCall) error {
	// TODO: I call it from Initialize - because looks like no much reason to have separated "verifyFamily" call

	step := header.AuRaStep
	parent := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
	parentStep := parent.AuRaStep
	//nolint
	validators, setNumber, err := c.epochSet(chain, e, header, syscall)
	if err != nil {
		return err
	}
	return nil
	// TODO(yperbasis): re-enable the rest

	// Ensure header is from the step after parent.
	//nolint
	if step == parentStep ||
		(header.Number.Uint64() >= c.cfg.ValidateStepTransition && step <= parentStep) {
		log.Trace("[aura] Multiple blocks proposed for step", "num", parentStep)
		_ = setNumber
		/*
			self.validators.report_malicious(
				header.author(),
				set_number,
				header.number(),
				Default::default(),
			);
			Err(EngineError::DoubleVote(*header.author()))?;
		*/
		return fmt.Errorf("double vote: %x", header.Coinbase)
	}

	// Report malice if the validator produced other sibling blocks in the same step.
	if !c.hasReceivedStepHashes(step, header.Coinbase, header.Hash()) {
		/*
		   trace!(target: "engine", "Validator {} produced sibling blocks in the same step", header.author());
		   self.validators.report_malicious(
		       header.author(),
		       set_number,
		       header.number(),
		       Default::default(),
		   );
		*/
	} else {
		c.insertReceivedStepHashes(step, header.Coinbase, header.Hash())
	}

	// Remove hash records older than two full rounds of steps (picked as a reasonable trade-off between
	// memory consumption and fault-tolerance).
	cnt, err := count(validators, parent.Hash(), call)
	if err != nil {
		return err
	}
	siblingMaliceDetectionPeriod := 2 * cnt
	oldestStep := uint64(0) //  let oldest_step = parent_step.saturating_sub(sibling_malice_detection_period);
	if parentStep > siblingMaliceDetectionPeriod {
		oldestStep = parentStep - siblingMaliceDetectionPeriod
	}
	//nolint
	if oldestStep > 0 {
		/*
		   let mut rsh = self.received_step_hashes.write();
		   let new_rsh = rsh.split_off(&(oldest_step, Address::zero()));
		   *rsh = new_rsh;
		*/
	}

	emptyStepLen := uint64(0)
	//self.report_skipped(header, step, parent_step, &*validators, set_number);

	/*
	   // If empty step messages are enabled we will validate the messages in the seal, missing messages are not
	   // reported as there's no way to tell whether the empty step message was never sent or simply not included.
	   let empty_steps_len = if header.number() >= self.empty_steps_transition {
	       let validate_empty_steps = || -> Result<usize, Error> {
	           let strict_empty_steps = header.number() >= self.strict_empty_steps_transition;
	           let empty_steps = header_empty_steps(header)?;
	           let empty_steps_len = empty_steps.len();
	           let mut prev_empty_step = 0;

	           for empty_step in empty_steps {
	               if empty_step.step <= parent_step || empty_step.step >= step {
	                   Err(EngineError::InsufficientProof(format!(
	                       "empty step proof for invalid step: {:?}",
	                       empty_step.step
	                   )))?;
	               }

	               if empty_step.parent_hash != *header.parent_hash() {
	                   Err(EngineError::InsufficientProof(format!(
	                       "empty step proof for invalid parent hash: {:?}",
	                       empty_step.parent_hash
	                   )))?;
	               }

	               if !empty_step.verify(&*validators).unwrap_or(false) {
	                   Err(EngineError::InsufficientProof(format!(
	                       "invalid empty step proof: {:?}",
	                       empty_step
	                   )))?;
	               }

	               if strict_empty_steps {
	                   if empty_step.step <= prev_empty_step {
	                       Err(EngineError::InsufficientProof(format!(
	                           "{} empty step: {:?}",
	                           if empty_step.step == prev_empty_step {
	                               "duplicate"
	                           } else {
	                               "unordered"
	                           },
	                           empty_step
	                       )))?;
	                   }

	                   prev_empty_step = empty_step.step;
	               }
	           }

	           Ok(empty_steps_len)
	       };

	       match validate_empty_steps() {
	           Ok(len) => len,
	           Err(err) => {
	               trace!(
	                   target: "engine",
	                   "Reporting benign misbehaviour (cause: invalid empty steps) \
	                   at block #{}, epoch set number {}. Own address: {}",
	                   header.number(), set_number, self.address().unwrap_or_default()
	               );
	               self.validators
	                   .report_benign(header.author(), set_number, header.number());
	               return Err(err);
	           }
	       }
	   } else {
	       self.report_skipped(header, step, parent_step, &*validators, set_number);

	       0
	   };
	*/
	if header.Number.Uint64() >= c.cfg.ValidateScoreTransition {
		expectedDifficulty := calculateScore(parentStep, step, emptyStepLen)
		if header.Difficulty.Cmp(expectedDifficulty.ToBig()) != 0 {
			return fmt.Errorf("invlid difficulty: expect=%s, found=%s\n", expectedDifficulty, header.Difficulty)
		}
	}
	return nil
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
func (c *AuRa) Prepare(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	return nil
	/// If the block isn't a checkpoint, cast a random vote (good enough for now)
	//header.Coinbase = libcommon.Address{}
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
	//	addresses := make([]libcommon.Address, 0, len(c.proposals))
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
	//header.MixDigest = libcommon.Hash{}
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

func (c *AuRa) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, syscallCustom consensus.SysCallCustom, logger log.Logger,
) {
	blockNum := header.Number.Uint64()

	//Check block gas limit from smart contract, if applicable
	c.verifyGasLimitOverride(config, chain, header, state, syscallCustom)

	for address, rewrittenCode := range c.cfg.RewriteBytecode[blockNum] {
		state.SetCode(address, rewrittenCode)
	}

	syscall := func(addr libcommon.Address, data []byte) ([]byte, error) {
		return syscallCustom(addr, data, state, header, false /* constCall */)
	}
	c.certifierLock.Lock()
	if c.cfg.Registrar != nil && c.certifier == nil && config.IsLondon(blockNum) {
		c.certifier = getCertifier(*c.cfg.Registrar, syscall)
	}
	c.certifierLock.Unlock()

	if blockNum == 1 {
		proof, err := c.GenesisEpochData(header, syscall)
		if err != nil {
			panic(err)
		}
		err = c.e.PutEpoch(header.ParentHash, 0, proof) //TODO: block 0 hardcoded - need fix it inside validators
		if err != nil {
			panic(err)
		}
	}

	//if err := c.verifyFamily(chain, e, header, call, syscall); err != nil { //TODO: OE has it as a separate engine call? why?
	//	panic(err)
	//}

	// check_and_lock_block -> check_epoch_end_signal

	epoch, err := c.e.GetEpoch(header.ParentHash, blockNum-1)
	if err != nil {
		logger.Warn("[aura] initialize block: on epoch begin", "err", err)
		return
	}
	isEpochBegin := epoch != nil
	if !isEpochBegin {
		return
	}
	err = c.cfg.Validators.onEpochBegin(isEpochBegin, header, syscall)
	if err != nil {
		logger.Warn("[aura] initialize block: on epoch begin", "err", err)
		return
	}
	// check_and_lock_block -> check_epoch_end_signal END (before enact)

}

func (c *AuRa) applyRewards(header *types.Header, state *state.IntraBlockState, syscall consensus.SystemCall) error {
	rewards, err := c.CalculateRewards(nil, header, nil, syscall)
	if err != nil {
		return err
	}
	for _, r := range rewards {
		state.AddBalance(r.Beneficiary, &r.Amount)
	}
	return nil
}

// word `signal epoch` == word `pending epoch`
func (c *AuRa) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState, txs types.Transactions,
	uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainReader, syscall consensus.SystemCall, logger log.Logger,
) (types.Transactions, types.Receipts, error) {
	if err := c.applyRewards(header, state, syscall); err != nil {
		return nil, nil, err
	}

	// check_and_lock_block -> check_epoch_end_signal (after enact)
	if header.Number.Uint64() >= DEBUG_LOG_FROM {
		fmt.Printf("finalize1: %d,%d\n", header.Number.Uint64(), len(receipts))
	}
	pendingTransitionProof, err := c.cfg.Validators.signalEpochEnd(header.Number.Uint64() == 0, header, receipts)
	if err != nil {
		return nil, nil, err
	}
	if pendingTransitionProof != nil {
		if header.Number.Uint64() >= DEBUG_LOG_FROM {
			fmt.Printf("insert_pending_transition: %d,receipts=%d, lenProof=%d\n", header.Number.Uint64(), len(receipts), len(pendingTransitionProof))
		}
		if err = c.e.PutPendingEpoch(header.Hash(), header.Number.Uint64(), pendingTransitionProof); err != nil {
			return nil, nil, err
		}
	}
	// check_and_lock_block -> check_epoch_end_signal END

	finalized := buildFinality(c.EpochManager, chain, c.e, c.cfg.Validators, header, syscall)
	c.EpochManager.finalityChecker.print(header.Number.Uint64())
	epochEndProof, err := isEpochEnd(chain, c.e, finalized, header)
	if err != nil {
		return nil, nil, err
	}
	if epochEndProof != nil {
		c.EpochManager.noteNewEpoch()
		logger.Info("[aura] epoch transition", "block_num", header.Number.Uint64())
		if err := c.e.PutEpoch(header.Hash(), header.Number.Uint64(), epochEndProof); err != nil {
			return nil, nil, err
		}
	}

	return txs, receipts, nil
}

func buildFinality(e *EpochManager, chain consensus.ChainHeaderReader, er *NonTransactionalEpochReader, validators ValidatorSet, header *types.Header, syscall consensus.SystemCall) []unAssembledHeader {
	// commit_block -> aura.build_finality
	_, _, ok := e.zoomToAfter(chain, er, validators, header.ParentHash, syscall)
	if !ok {
		return []unAssembledHeader{}
	}
	if e.finalityChecker.lastPushed == nil || *e.finalityChecker.lastPushed != header.ParentHash {
		if err := e.finalityChecker.buildAncestrySubChain(func(hash libcommon.Hash) ([]libcommon.Address, libcommon.Hash, libcommon.Hash, uint64, bool) {
			h := chain.GetHeaderByHash(hash)
			if h == nil {
				return nil, libcommon.Hash{}, libcommon.Hash{}, 0, false
			}
			return []libcommon.Address{h.Coinbase}, h.Hash(), h.ParentHash, h.Number.Uint64(), true
		}, header.ParentHash, e.epochTransitionHash); err != nil {
			//log.Warn("[aura] buildAncestrySubChain", "err", err)
			return []unAssembledHeader{}
		}
	}

	res, err := e.finalityChecker.push(header.Hash(), header.Number.Uint64(), []libcommon.Address{header.Coinbase})
	if err != nil {
		//log.Warn("[aura] finalityChecker.push", "err", err)
		return []unAssembledHeader{}
	}
	return res
}

func isEpochEnd(chain consensus.ChainHeaderReader, e *NonTransactionalEpochReader, finalized []unAssembledHeader, header *types.Header) ([]byte, error) {
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
func allHeadersUntil(chain consensus.ChainHeaderReader, from *types.Header, to libcommon.Hash) (out []*types.Header) {
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

//func (c *AuRa) check_epoch_end(cc *chain.Config, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
//}

// FinalizeAndAssemble implements consensus.Engine
func (c *AuRa) FinalizeAndAssemble(config *chain.Config, header *types.Header, state *state.IntraBlockState, txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chain consensus.ChainReader, syscall consensus.SystemCall, call consensus.Call, logger log.Logger) (*types.Block, types.Transactions, types.Receipts, error) {
	outTxs, outReceipts, err := c.Finalize(config, header, state, txs, uncles, receipts, withdrawals, chain, syscall, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	// Assemble and return the final block for sealing
	return types.NewBlock(header, outTxs, uncles, outReceipts, withdrawals), outTxs, outReceipts, nil
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (c *AuRa) Authorize(signer libcommon.Address, signFn clique.SignerFn) {
	c.lock.Lock()
	defer c.lock.Unlock()

	//c.signer = signer
	//c.signFn = signFn
}

func (c *AuRa) GenesisEpochData(header *types.Header, caller consensus.SystemCall) ([]byte, error) {
	setProof, err := c.cfg.Validators.genesisEpochData(header, caller)
	if err != nil {
		return nil, err
	}
	res, err := rlp.EncodeToBytes(EpochTransitionProof{SignalNumber: 0, SetProof: setProof, FinalityProof: []byte{}})
	if err != nil {
		panic(err)
	}
	//fmt.Printf("reere: %x\n", res)
	//f91a84f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808080b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f91871b914c26060604052600436106100fc576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff16806303aca79214610101578063108552691461016457806340a141ff1461019d57806340c9cdeb146101d65780634110a489146101ff57806345199e0a1461025757806349285b58146102c15780634d238c8e14610316578063752862111461034f578063900eb5a8146103645780639a573786146103c7578063a26a47d21461041c578063ae4b1b5b14610449578063b3f05b971461049e578063b7ab4db5146104cb578063d3e848f114610535578063fa81b2001461058a578063facd743b146105df575b600080fd5b341561010c57600080fd5b6101226004808035906020019091905050610630565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561016f57600080fd5b61019b600480803573ffffffffffffffffffffffffffffffffffffffff1690602001909190505061066f565b005b34156101a857600080fd5b6101d4600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610807565b005b34156101e157600080fd5b6101e9610bb7565b6040518082815260200191505060405180910390f35b341561020a57600080fd5b610236600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610bbd565b60405180831515151581526020018281526020019250505060405180910390f35b341561026257600080fd5b61026a610bee565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156102ad578082015181840152602081019050610292565b505050509050019250505060405180910390f35b34156102cc57600080fd5b6102d4610c82565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561032157600080fd5b61034d600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610d32565b005b341561035a57600080fd5b610362610fcc565b005b341561036f57600080fd5b61038560048080359060200190919050506110fc565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156103d257600080fd5b6103da61113b565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561042757600080fd5b61042f6111eb565b604051808215151515815260200191505060405180910390f35b341561045457600080fd5b61045c6111fe565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156104a957600080fd5b6104b1611224565b604051808215151515815260200191505060405180910390f35b34156104d657600080fd5b6104de611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b83811015610521578082015181840152602081019050610506565b505050509050019250505060405180910390f35b341561054057600080fd5b6105486112cb565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561059557600080fd5b61059d6112f1565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156105ea57600080fd5b610616600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050611317565b604051808215151515815260200191505060405180910390f35b60078181548110151561063f57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156106cb57600080fd5b600460019054906101000a900460ff161515156106e757600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff161415151561072357600080fd5b80600a60006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055506001600460016101000a81548160ff0219169083151502179055507f600bcf04a13e752d1e3670a5a9f1c21177ca2a93c6f5391d4f1298d098097c22600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390a150565b600080600061081461113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff1614151561084d57600080fd5b83600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff1615156108a957600080fd5b600960008673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101549350600160078054905003925060078381548110151561090857fe5b906000526020600020900160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1691508160078581548110151561094657fe5b906000526020600020900160006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555083600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506007838154811015156109e557fe5b906000526020600020900160006101000a81549073ffffffffffffffffffffffffffffffffffffffff02191690556000600780549050111515610a2757600080fd5b6007805480919060019003610a3c9190611370565b506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160006101000a81548160ff0219169083151502179055506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610ba257602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610b58575b50509250505060405180910390a25050505050565b60085481565b60096020528060005260406000206000915090508060000160009054906101000a900460ff16908060010154905082565b610bf661139c565b6007805480602002602001604051908101604052809291908181526020018280548015610c7857602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610c2e575b5050505050905090565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff166349285b586000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b1515610d1257600080fd5b6102c65a03f11515610d2357600080fd5b50505060405180519050905090565b610d3a61113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff16141515610d7357600080fd5b80600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff16151515610dd057600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168273ffffffffffffffffffffffffffffffffffffffff1614151515610e0c57600080fd5b6040805190810160405280600115158152602001600780549050815250600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008201518160000160006101000a81548160ff0219169083151502179055506020820151816001015590505060078054806001018281610ea991906113b0565b9160005260206000209001600084909190916101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff160217905550506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610fba57602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610f70575b50509250505060405180910390a25050565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480156110365750600460009054906101000a900460ff16155b151561104157600080fd5b6001600460006101000a81548160ff0219169083151502179055506007600690805461106e9291906113dc565b506006805490506008819055507f8564cd629b15f47dc310d45bcbfc9bcf5420b0d51bf0659a16c67f91d27632536110a4611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156110e75780820151818401526020810190506110cc565b505050509050019250505060405180910390a1565b60068181548110151561110b57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16639a5737866000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b15156111cb57600080fd5b6102c65a03f115156111dc57600080fd5b50505060405180519050905090565b600460019054906101000a900460ff1681565b600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460009054906101000a900460ff1681565b61123f61139c565b60068054806020026020016040519081016040528092919081815260200182805480156112c157602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311611277575b5050505050905090565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600960008373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff169050919050565b81548183558181151161139757818360005260206000209182019101611396919061142e565b5b505050565b602060405190810160405280600081525090565b8154818355818115116113d7578183600052602060002091820191016113d6919061142e565b5b505050565b82805482825590600052602060002090810192821561141d5760005260206000209182015b8281111561141c578254825591600101919060010190611401565b5b50905061142a9190611453565b5090565b61145091905b8082111561144c576000816000905550600101611434565b5090565b90565b61149391905b8082111561148f57600081816101000a81549073ffffffffffffffffffffffffffffffffffffffff021916905550600101611459565b5090565b905600a165627a7a7230582036ea35935c8246b68074adece2eab70c40e69a0193c08a6277ce06e5b25188510029b8f3f8f1a08023c0d95fc2364e0bf7593f5ff32e1db8ef9f4b41c0bd474eae62d1af896e99808080a0b47b4f0b3e73b5edc8f9a9da1cbcfed562eb06bf54619b6aefeadebf5b3604c280a0da6ec08940a924cb08c947dd56cdb40076b29a6f0ea4dba4e2d02d9a9a72431b80a030cc4138c9e74b6cf79d624b4b5612c0fd888e91f55316cfee7d1694e1a90c0b80a0c5d54b915b56a888eee4e6eeb3141e778f9b674d1d322962eed900f02c29990aa017256b36ef47f907c6b1378a2636942ce894c17075e56fc054d4283f6846659e808080a03340bbaeafcda3a8672eb83099231dbbfab8dae02a1e8ec2f7180538fac207e080b86bf869a033aa5d69545785694b808840be50c182dad2ec3636dfccbe6572fb69828742c0b846f8440101a0663ce0d171e545a26aa67e4ca66f72ba96bb48287dbcc03beea282867f80d44ba01f0e7726926cb43c03a0abf48197dba78522ec8ba1b158e2aa30da7d2a2c6f9eb838f7a03868bdfa8727775661e4ccf117824a175a33f8703d728c04488fbfffcafda9f99594e8ddc5c7a2d2f0d7a9798459c0104fdf5e987acaa3e2a02052222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d3f01b853f851808080a07bb75cabebdcbd1dbb4331054636d0c6d7a2b08483b9e04df057395a7434c9e080808080808080a0e61e567237b49c44d8f906ceea49027260b4010c10a547b38d8b131b9d3b6f848080808080b8d3f8d1a0dc277c93a9f9dcee99aac9b8ba3cfa4c51821998522469c37715644e8fbac0bfa0ab8cdb808c8303bb61fb48e276217be9770fa83ecf3f90f2234d558885f5abf1808080a0fe137c3a474fbde41d89a59dd76da4c55bf696b86d3af64a55632f76cf30786780808080a06301b39b2ea8a44df8b0356120db64b788e71f52e1d7a6309d0d2e5b86fee7cb80a0da5d8b08dea0c5a4799c0f44d8a24d7cdf209f9b7a5588c1ecafb5361f6b9f07a01b7779e149cadf24d4ffb77ca7e11314b8db7097e4d70b2a173493153ca2e5a0808080b853f851808080a0a87d9bb950836582673aa0eecc0ff64aac607870637a2dd2012b8b1b31981f698080a08da6d5c36a404670c553a2c9052df7cd604f04e3863c4c7b9e0027bfd54206d680808080808080808080b86bf869a02080c7b7ae81a58eb98d9c78de4a1fd7fd9535fc953ed2be602daaa41767312ab846f8448080a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470
	//f91a8c80b91a87f91a84f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808080b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f91871b914c26060604052600436106100fc576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff16806303aca79214610101578063108552691461016457806340a141ff1461019d57806340c9cdeb146101d65780634110a489146101ff57806345199e0a1461025757806349285b58146102c15780634d238c8e14610316578063752862111461034f578063900eb5a8146103645780639a573786146103c7578063a26a47d21461041c578063ae4b1b5b14610449578063b3f05b971461049e578063b7ab4db5146104cb578063d3e848f114610535578063fa81b2001461058a578063facd743b146105df575b600080fd5b341561010c57600080fd5b6101226004808035906020019091905050610630565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561016f57600080fd5b61019b600480803573ffffffffffffffffffffffffffffffffffffffff1690602001909190505061066f565b005b34156101a857600080fd5b6101d4600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610807565b005b34156101e157600080fd5b6101e9610bb7565b6040518082815260200191505060405180910390f35b341561020a57600080fd5b610236600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610bbd565b60405180831515151581526020018281526020019250505060405180910390f35b341561026257600080fd5b61026a610bee565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156102ad578082015181840152602081019050610292565b505050509050019250505060405180910390f35b34156102cc57600080fd5b6102d4610c82565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561032157600080fd5b61034d600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610d32565b005b341561035a57600080fd5b610362610fcc565b005b341561036f57600080fd5b61038560048080359060200190919050506110fc565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156103d257600080fd5b6103da61113b565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561042757600080fd5b61042f6111eb565b604051808215151515815260200191505060405180910390f35b341561045457600080fd5b61045c6111fe565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156104a957600080fd5b6104b1611224565b604051808215151515815260200191505060405180910390f35b34156104d657600080fd5b6104de611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b83811015610521578082015181840152602081019050610506565b505050509050019250505060405180910390f35b341561054057600080fd5b6105486112cb565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561059557600080fd5b61059d6112f1565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156105ea57600080fd5b610616600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050611317565b604051808215151515815260200191505060405180910390f35b60078181548110151561063f57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156106cb57600080fd5b600460019054906101000a900460ff161515156106e757600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff161415151561072357600080fd5b80600a60006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055506001600460016101000a81548160ff0219169083151502179055507f600bcf04a13e752d1e3670a5a9f1c21177ca2a93c6f5391d4f1298d098097c22600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390a150565b600080600061081461113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff1614151561084d57600080fd5b83600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff1615156108a957600080fd5b600960008673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101549350600160078054905003925060078381548110151561090857fe5b906000526020600020900160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1691508160078581548110151561094657fe5b906000526020600020900160006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555083600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506007838154811015156109e557fe5b906000526020600020900160006101000a81549073ffffffffffffffffffffffffffffffffffffffff02191690556000600780549050111515610a2757600080fd5b6007805480919060019003610a3c9190611370565b506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160006101000a81548160ff0219169083151502179055506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610ba257602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610b58575b50509250505060405180910390a25050505050565b60085481565b60096020528060005260406000206000915090508060000160009054906101000a900460ff16908060010154905082565b610bf661139c565b6007805480602002602001604051908101604052809291908181526020018280548015610c7857602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610c2e575b5050505050905090565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff166349285b586000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b1515610d1257600080fd5b6102c65a03f11515610d2357600080fd5b50505060405180519050905090565b610d3a61113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff16141515610d7357600080fd5b80600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff16151515610dd057600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168273ffffffffffffffffffffffffffffffffffffffff1614151515610e0c57600080fd5b6040805190810160405280600115158152602001600780549050815250600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008201518160000160006101000a81548160ff0219169083151502179055506020820151816001015590505060078054806001018281610ea991906113b0565b9160005260206000209001600084909190916101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff160217905550506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610fba57602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610f70575b50509250505060405180910390a25050565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480156110365750600460009054906101000a900460ff16155b151561104157600080fd5b6001600460006101000a81548160ff0219169083151502179055506007600690805461106e9291906113dc565b506006805490506008819055507f8564cd629b15f47dc310d45bcbfc9bcf5420b0d51bf0659a16c67f91d27632536110a4611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156110e75780820151818401526020810190506110cc565b505050509050019250505060405180910390a1565b60068181548110151561110b57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16639a5737866000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b15156111cb57600080fd5b6102c65a03f115156111dc57600080fd5b50505060405180519050905090565b600460019054906101000a900460ff1681565b600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460009054906101000a900460ff1681565b61123f61139c565b60068054806020026020016040519081016040528092919081815260200182805480156112c157602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311611277575b5050505050905090565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600960008373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff169050919050565b81548183558181151161139757818360005260206000209182019101611396919061142e565b5b505050565b602060405190810160405280600081525090565b8154818355818115116113d7578183600052602060002091820191016113d6919061142e565b5b505050565b82805482825590600052602060002090810192821561141d5760005260206000209182015b8281111561141c578254825591600101919060010190611401565b5b50905061142a9190611453565b5090565b61145091905b8082111561144c576000816000905550600101611434565b5090565b90565b61149391905b8082111561148f57600081816101000a81549073ffffffffffffffffffffffffffffffffffffffff021916905550600101611459565b5090565b905600a165627a7a7230582036ea35935c8246b68074adece2eab70c40e69a0193c08a6277ce06e5b25188510029b8f3f8f1a08023c0d95fc2364e0bf7593f5ff32e1db8ef9f4b41c0bd474eae62d1af896e99808080a0b47b4f0b3e73b5edc8f9a9da1cbcfed562eb06bf54619b6aefeadebf5b3604c280a0da6ec08940a924cb08c947dd56cdb40076b29a6f0ea4dba4e2d02d9a9a72431b80a030cc4138c9e74b6cf79d624b4b5612c0fd888e91f55316cfee7d1694e1a90c0b80a0c5d54b915b56a888eee4e6eeb3141e778f9b674d1d322962eed900f02c29990aa017256b36ef47f907c6b1378a2636942ce894c17075e56fc054d4283f6846659e808080a03340bbaeafcda3a8672eb83099231dbbfab8dae02a1e8ec2f7180538fac207e080b86bf869a033aa5d69545785694b808840be50c182dad2ec3636dfccbe6572fb69828742c0b846f8440101a0663ce0d171e545a26aa67e4ca66f72ba96bb48287dbcc03beea282867f80d44ba01f0e7726926cb43c03a0abf48197dba78522ec8ba1b158e2aa30da7d2a2c6f9eb838f7a03868bdfa8727775661e4ccf117824a175a33f8703d728c04488fbfffcafda9f99594e8ddc5c7a2d2f0d7a9798459c0104fdf5e987acaa3e2a02052222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d3f01b853f851808080a07bb75cabebdcbd1dbb4331054636d0c6d7a2b08483b9e04df057395a7434c9e080808080808080a0e61e567237b49c44d8f906ceea49027260b4010c10a547b38d8b131b9d3b6f848080808080b8d3f8d1a0dc277c93a9f9dcee99aac9b8ba3cfa4c51821998522469c37715644e8fbac0bfa0ab8cdb808c8303bb61fb48e276217be9770fa83ecf3f90f2234d558885f5abf1808080a0fe137c3a474fbde41d89a59dd76da4c55bf696b86d3af64a55632f76cf30786780808080a06301b39b2ea8a44df8b0356120db64b788e71f52e1d7a6309d0d2e5b86fee7cb80a0da5d8b08dea0c5a4799c0f44d8a24d7cdf209f9b7a5588c1ecafb5361f6b9f07a01b7779e149cadf24d4ffb77ca7e11314b8db7097e4d70b2a173493153ca2e5a0808080b853f851808080a0a87d9bb950836582673aa0eecc0ff64aac607870637a2dd2012b8b1b31981f698080a08da6d5c36a404670c553a2c9052df7cd604f04e3863c4c7b9e0027bfd54206d680808080808080808080b86bf869a02080c7b7ae81a58eb98d9c78de4a1fd7fd9535fc953ed2be602daaa41767312ab846f8448080a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47080
	return res, nil
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

func stepProposer(validators ValidatorSet, blockHash libcommon.Hash, step uint64, call consensus.Call) (libcommon.Address, error) {
	//c, err := validators.defaultCaller(blockHash)
	//if err != nil {
	//	return libcommon.Address{}, err
	//}
	return validators.getWithCaller(blockHash, uint(step), call)
}

// GenerateSeal - Attempt to seal the block internally.
//
// This operation is synchronous and may (quite reasonably) not be available, in which case
// `Seal::None` will be returned.
func (c *AuRa) GenerateSeal(chain consensus.ChainHeaderReader, current, parent *types.Header, call consensus.Call) []byte {
	// first check to avoid generating signature most of the time
	// (but there's still a race to the `compare_exchange`)
	if !c.step.canPropose.Load() {
		log.Trace("[aura] Aborting seal generation. Can't propose.")
		return nil
	}
	parentStep := parent.AuRaStep
	step := c.step.inner.inner.Load()

	// filter messages from old and future steps and different parents
	expectedDiff := calculateScore(parentStep, step, 0)
	if current.Difficulty.Cmp(expectedDiff.ToBig()) != 0 {
		log.Trace(fmt.Sprintf("[aura] Aborting seal generation. The step or empty_steps have changed in the meantime. %d != %d", current.Difficulty, expectedDiff))
		return nil
	}

	if parentStep > step {
		log.Warn(fmt.Sprintf("[aura] Aborting seal generation for invalid step: %d > %d", parentStep, step))
		return nil
	}

	validators, setNumber, err := c.epochSet(chain, nil, current, nil)
	if err != nil {
		log.Warn("[aura] Unable to generate seal", "err", err)
		return nil
	}

	stepProposerAddr, err := stepProposer(validators, current.ParentHash, step, call)
	if err != nil {
		log.Warn("[aura] Unable to get stepProposer", "err", err)
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

	// TODO(yperbasis) re-enable the rest

	_ = setNumber
	/*
		signature, err := c.sign(current.bareHash())
			if err != nil {
				log.Warn("[aura] generate_seal: FAIL: Accounts secret key unavailable.", "err", err)
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
func (c *AuRa) epochSet(chain consensus.ChainHeaderReader, e *NonTransactionalEpochReader, h *types.Header, call consensus.SystemCall) (ValidatorSet, uint64, error) {
	if c.cfg.ImmediateTransitions {
		return c.cfg.Validators, h.Number.Uint64(), nil
	}

	finalityChecker, epochTransitionNumber, ok := c.EpochManager.zoomToAfter(chain, e, c.cfg.Validators, h.ParentHash, call)
	if !ok {
		return nil, 0, fmt.Errorf("unable to zoomToAfter to epoch")
	}
	return finalityChecker.signers, epochTransitionNumber, nil
}

func (c *AuRa) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentHash, parentUncleHash libcommon.Hash, parentStep uint64) *big.Int {
	currentStep := c.step.inner.inner.Load()
	currentEmptyStepsLen := 0
	return calculateScore(parentStep, currentStep, uint64(currentEmptyStepsLen)).ToBig()
}

// calculateScore - analog of PoW difficulty:
//
//	sqrt(U256::max_value()) + parent_step - current_step + current_empty_steps
func calculateScore(parentStep, currentStep, currentEmptySteps uint64) *uint256.Int {
	maxU128 := uint256.NewInt(0).SetAllOne()
	maxU128 = maxU128.Rsh(maxU128, 128)
	res := maxU128.Add(maxU128, uint256.NewInt(parentStep))
	res = res.Sub(res, uint256.NewInt(currentStep))
	res = res.Add(res, uint256.NewInt(currentEmptySteps))
	return res
}

func (c *AuRa) SealHash(header *types.Header) libcommon.Hash {
	return clique.SealHash(header)
}

// See https://openethereum.github.io/Permissioning.html#gas-price
// This is thread-safe: it only accesses the `certifier` which is used behind a RWLock
func (c *AuRa) IsServiceTransaction(sender libcommon.Address, syscall consensus.SystemCall) bool {
	c.certifierLock.RLock()
	defer c.certifierLock.RUnlock()
	if c.certifier == nil {
		return false
	}
	packed, err := certifierAbi().Pack("certified", sender)
	if err != nil {
		panic(err)
	}
	out, err := syscall(*c.certifier, packed)
	if err != nil {
		panic(err)
	}
	res, err := certifierAbi().Unpack("certified", out)
	if err != nil {
		log.Warn("error while detecting service tx on AuRa", "err", err)
		return false
	}
	if len(res) == 0 {
		return false
	}
	if certified, ok := res[0].(bool); ok {
		return certified
	}
	return false
}

// Close implements consensus.Engine. It's a noop for clique as there are no background threads.
func (c *AuRa) Close() error {
	libcommon.SafeClose(c.exitCh)
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

// nolint
func (c *AuRa) emptySteps(fromStep, toStep uint64, parentHash libcommon.Hash) []EmptyStep {
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

func (c *AuRa) CalculateRewards(_ *chain.Config, header *types.Header, _ []*types.Header, syscall consensus.SystemCall,
) ([]consensus.Reward, error) {
	var rewardContractAddress BlockRewardContract
	var foundContract bool
	for _, c := range c.cfg.BlockRewardContractTransitions {
		if c.blockNum > header.Number.Uint64() {
			break
		}
		foundContract = true
		rewardContractAddress = c
	}
	if foundContract {
		beneficiaries := []libcommon.Address{header.Coinbase}
		rewardKind := []consensus.RewardKind{consensus.RewardAuthor}
		var amounts []*uint256.Int
		beneficiaries, amounts = callBlockRewardAbi(rewardContractAddress.address, syscall, beneficiaries, rewardKind)
		rewards := make([]consensus.Reward, len(amounts))
		for i, amount := range amounts {
			rewards[i].Beneficiary = beneficiaries[i]
			rewards[i].Kind = consensus.RewardExternal
			rewards[i].Amount = *amount
		}
		return rewards, nil
	}

	// block_reward.iter.rev().find(|&(block, _)| *block <= number)
	var reward BlockReward
	var found bool
	for i := range c.cfg.BlockReward {
		if c.cfg.BlockReward[i].blockNum > header.Number.Uint64() {
			break
		}
		found = true
		reward = c.cfg.BlockReward[i]
	}
	if !found {
		return nil, errors.New("Current block's reward is not found; this indicates a chain config error")
	}

	r := consensus.Reward{Beneficiary: header.Coinbase, Kind: consensus.RewardAuthor, Amount: *reward.amount}
	return []consensus.Reward{r}, nil
}

// See https://github.com/gnosischain/specs/blob/master/execution/withdrawals.md
func (c *AuRa) ExecuteSystemWithdrawals(withdrawals []*types.Withdrawal, syscall consensus.SystemCall) error {
	if c.cfg.WithdrawalContractAddress == nil {
		return nil
	}

	maxFailedWithdrawalsToProcess := big.NewInt(4)
	amounts := make([]uint64, 0, len(withdrawals))
	addresses := make([]libcommon.Address, 0, len(withdrawals))
	for _, w := range withdrawals {
		amounts = append(amounts, w.Amount)
		addresses = append(addresses, w.Address)
	}

	packed, err := withdrawalAbi().Pack("executeSystemWithdrawals", maxFailedWithdrawalsToProcess, amounts, addresses)
	if err != nil {
		return err
	}

	_, err = syscall(*c.cfg.WithdrawalContractAddress, packed)
	if err != nil {
		log.Warn("ExecuteSystemWithdrawals", "err", err)
	}
	return err
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

func newEmptyStepFromSealed(step SealedEmptyStep, parentHash libcommon.Hash) EmptyStep {
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
