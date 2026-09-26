// Copyright 2024 The Erigon Authors
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

package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type voluntaryExitService struct {
	operationsPool         pool.OperationsPool
	emitters               *beaconevents.EventEmitter
	syncedDataManager      synced_data.SyncedData
	beaconCfg              *clparams.BeaconChainConfig
	ethClock               eth_clock.EthereumClock
	batchSignatureVerifier *BatchSignatureVerifier
	seen                   *lru.Cache[uint64, *cltypes.SignedVoluntaryExit]
	immediateGates         [256]chan struct{}
	now                    func() time.Time
}

// SignedVoluntaryExitForGossip type represents SignedVoluntaryExit with the gossip data where it's coming from.
type SignedVoluntaryExitForGossip struct {
	SignedVoluntaryExit   *cltypes.SignedVoluntaryExit
	Receiver              *sentinelproto.Peer
	ImmediateVerification bool
}

func NewVoluntaryExitService(
	operationsPool pool.OperationsPool,
	emitters *beaconevents.EventEmitter,
	syncedDataManager synced_data.SyncedData,
	beaconCfg *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	batchSignatureVerifier *BatchSignatureVerifier,
) VoluntaryExitService {
	seen, err := lru.New[uint64, *cltypes.SignedVoluntaryExit]("voluntary_exit_seen", operationSeenCacheSize)
	if err != nil {
		panic(err)
	}
	service := &voluntaryExitService{
		operationsPool:         operationsPool,
		emitters:               emitters,
		syncedDataManager:      syncedDataManager,
		beaconCfg:              beaconCfg,
		ethClock:               ethClock,
		batchSignatureVerifier: batchSignatureVerifier,
		seen:                   seen,
		now:                    time.Now,
	}
	for i := range service.immediateGates {
		service.immediateGates[i] = make(chan struct{}, 1)
	}
	return service
}

func (s *voluntaryExitService) Names() []string {
	return []string{gossip.TopicNameVoluntaryExit}
}

func (s *voluntaryExitService) IsMyGossipMessage(name string) bool {
	return name == gossip.TopicNameVoluntaryExit
}

func (s *voluntaryExitService) DecodeGossipMessage(pid peer.ID, data []byte, version clparams.StateVersion) (*SignedVoluntaryExitForGossip, error) {
	obj := &SignedVoluntaryExitForGossip{
		Receiver:            &sentinelproto.Peer{Pid: pid.String()},
		SignedVoluntaryExit: &cltypes.SignedVoluntaryExit{},
	}
	if err := obj.SignedVoluntaryExit.DecodeSSZ(data, int(version)); err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *voluntaryExitService) ProcessMessage(ctx context.Context, subnet *uint64, msg *SignedVoluntaryExitForGossip) error {
	if msg == nil || msg.SignedVoluntaryExit == nil || msg.SignedVoluntaryExit.VoluntaryExit == nil {
		return errors.New("invalid voluntary exit")
	}
	// ref: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#voluntary_exit
	voluntaryExit := msg.SignedVoluntaryExit.VoluntaryExit
	var immediateGate chan struct{}
	immediateGateHeld := false
	releaseImmediateGate := func() {
		if immediateGateHeld {
			<-immediateGate
			immediateGateHeld = false
		}
	}
	defer releaseImmediateGate()
	if msg.ImmediateVerification {
		immediateGate = s.immediateGates[voluntaryExit.ValidatorIndex%uint64(len(s.immediateGates))]
		select {
		case immediateGate <- struct{}{}:
			immediateGateHeld = true
		case <-ctx.Done():
			return fmt.Errorf("%w: voluntary exit validation canceled: %w", ErrIgnore, ctx.Err())
		}
		if seen, matches := s.operationsPool.PreviouslySeenVoluntaryExitMatches(msg.SignedVoluntaryExit); seen && !matches {
			return fmt.Errorf("%w: validator already has a different voluntary exit", ErrIgnore)
		}
	}

	// [IGNORE] The voluntary exit is the first valid voluntary exit received for the validator with index signed_voluntary_exit.message.validator_index.
	if !msg.ImmediateVerification {
		if _, ok := s.seen.Get(voluntaryExit.ValidatorIndex); ok {
			return ErrIgnore
		}
		if s.operationsPool.VoluntaryExitsPool.Has(voluntaryExit.ValidatorIndex) {
			return ErrIgnore
		}
		if s.operationsPool.HasSeenVoluntaryExit(voluntaryExit.ValidatorIndex) {
			return ErrIgnore
		}
	}

	currentEpoch := uint64(0)
	now := s.now().Add(maximumGossipClockDisparity)
	if !now.Before(s.ethClock.GetSlotTime(0)) {
		currentEpoch = s.ethClock.GetEpochAtSlot(s.ethClock.GetSlotByTime(now))
	}
	if voluntaryExit.Epoch > currentEpoch {
		return ErrIgnore
	}

	var (
		signingRoot common.Hash
		pk          common.Bytes48
		domain      []byte
	)

	// ref: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#voluntary-exits
	// def process_voluntary_exit(state: BeaconState, signed_voluntary_exit: SignedVoluntaryExit) -> None:
	if err := s.syncedDataManager.ViewHeadState(func(state *state.CachingBeaconState) error {
		val, err := state.ValidatorForValidatorIndex(int(voluntaryExit.ValidatorIndex))
		if err != nil {
			return ErrIgnore
		}
		curEpoch := state.Slot() / s.beaconCfg.SlotsPerEpoch

		if val.ExitEpoch() != s.beaconCfg.FarFutureEpoch {
			return ErrIgnore
		}

		// Verify the validator is active
		// assert is_active_validator(validator, get_current_epoch(state))
		if !val.Active(curEpoch) {
			if !msg.ImmediateVerification && val.Active(currentEpoch) {
				return fmt.Errorf("%w: validator activity is waiting for head epoch", ErrIgnore)
			}
			return errors.New("validator is not active")
		}

		// Verify the validator has been active long enough
		// assert get_current_epoch(state) >= validator.activation_epoch + SHARD_COMMITTEE_PERIOD
		eligibleEpoch := val.ActivationEpoch() + s.beaconCfg.ShardCommitteePeriod
		if curEpoch < eligibleEpoch {
			if !msg.ImmediateVerification && currentEpoch >= eligibleEpoch {
				return fmt.Errorf("%w: validator tenure is waiting for head epoch", ErrIgnore)
			}
			return errors.New("verify the validator has been active long enough")
		}

		// Verify signature
		// domain = get_domain(state, DOMAIN_VOLUNTARY_EXIT, voluntary_exit.epoch)
		// signing_root = compute_signing_root(voluntary_exit, domain)
		// assert bls.Verify(validator.pubkey, signing_root, signed_voluntary_exit.signature)
		pk = val.PublicKey()
		domainType := s.beaconCfg.DomainVoluntaryExit
		if state.Version() < clparams.DenebVersion {
			domain, err = state.GetDomain(domainType, voluntaryExit.Epoch)
		} else if state.Version() >= clparams.DenebVersion {
			domain, err = fork.ComputeDomain(domainType[:], utils.Uint32ToBytes4(uint32(s.beaconCfg.CapellaForkVersion)), state.GenesisValidatorsRoot())
		}
		if err != nil {
			return err
		}
		signingRoot, err = computeSigningRoot(voluntaryExit, domain)
		return err
	}); err != nil {
		return err
	}
	signingRoot, err := computeSigningRoot(voluntaryExit, domain)
	if err != nil {
		return err
	}
	if msg.ImmediateVerification {
		verified, ok := s.seen.Get(voluntaryExit.ValidatorIndex)
		if !ok {
			verified, ok = s.operationsPool.VoluntaryExitsPool.Get(voluntaryExit.ValidatorIndex)
		}
		if ok {
			return s.ensureVerifiedExitStored(verified, msg.SignedVoluntaryExit)
		}
	}

	var (
		storeErr error
		emitExit bool
	)
	aggregateVerificationData := &AggregateVerificationData{
		Signatures:  [][]byte{msg.SignedVoluntaryExit.Signature[:]},
		SignRoots:   [][]byte{signingRoot[:]},
		Pks:         [][]byte{pk[:]},
		SendingPeer: msg.Receiver,
		F: func() {
			emitExit, storeErr = s.storeVerifiedExit(msg.SignedVoluntaryExit, immediateGate != nil)
			if !msg.ImmediateVerification && emitExit {
				s.emitters.Operation().SendVoluntaryExit(copySignedVoluntaryExit(msg.SignedVoluntaryExit))
			}
		},
	}

	if msg.ImmediateVerification {
		if err := s.batchSignatureVerifier.ImmediateVerification(aggregateVerificationData); err != nil {
			return err
		}
		releaseImmediateGate()
		if emitExit {
			s.emitters.Operation().SendVoluntaryExit(copySignedVoluntaryExit(msg.SignedVoluntaryExit))
		}
		return storeErr
	}

	// push the signatures to verify asynchronously and run final functions after that.
	s.batchSignatureVerifier.AsyncVerifyVoluntaryExit(aggregateVerificationData)

	// As the logic goes, if we return ErrIgnore there will be no peer banning and further publishing
	// gossip data into the network by the gossip manager. That's what we want because we will be doing that ourselves
	// in BatchSignatureVerifier service. After validating signatures, if they are valid we will publish the
	// gossip ourselves or ban the peer which sent that particular invalid signature.
	return nil
}

func (s *voluntaryExitService) storeVerifiedExit(exit *cltypes.SignedVoluntaryExit, gateHeld bool) (bool, error) {
	stored := copySignedVoluntaryExit(exit)
	if !gateHeld {
		gate := s.immediateGates[stored.VoluntaryExit.ValidatorIndex%uint64(len(s.immediateGates))]
		gate <- struct{}{}
		defer func() { <-gate }()
	}
	verified, ok := s.seen.Get(stored.VoluntaryExit.ValidatorIndex)
	if !ok {
		verified, ok = s.operationsPool.VoluntaryExitsPool.Get(stored.VoluntaryExit.ValidatorIndex)
	}
	if !ok {
		seen, matches := s.operationsPool.PreviouslySeenVoluntaryExitMatches(stored)
		if seen && !matches {
			return false, fmt.Errorf("%w: validator already has a different voluntary exit", ErrIgnore)
		}
		if matches {
			verified, ok = stored, true
		}
	}
	if ok {
		return false, s.ensureVerifiedExitStored(verified, stored)
	}
	s.seen.Add(stored.VoluntaryExit.ValidatorIndex, stored)
	s.operationsPool.RestoreVoluntaryExitIfMissing(stored.VoluntaryExit.ValidatorIndex, stored)
	return true, nil
}

func (s *voluntaryExitService) ensureVerifiedExitStored(verified, submitted *cltypes.SignedVoluntaryExit) error {
	if !equalSignedVoluntaryExits(verified, submitted) {
		return fmt.Errorf("%w: validator already has a different voluntary exit", ErrIgnore)
	}
	index := verified.VoluntaryExit.ValidatorIndex
	if pooled, ok := s.operationsPool.VoluntaryExitsPool.Get(index); ok {
		if !equalSignedVoluntaryExits(pooled, verified) {
			return fmt.Errorf("%w: validator already has a different voluntary exit", ErrIgnore)
		}
		return nil
	}
	s.operationsPool.RestoreVoluntaryExitIfMissing(index, verified)
	return nil
}

func equalSignedVoluntaryExits(a, b *cltypes.SignedVoluntaryExit) bool {
	return a != nil && b != nil && a.VoluntaryExit != nil && b.VoluntaryExit != nil &&
		a.VoluntaryExit.Epoch == b.VoluntaryExit.Epoch &&
		a.VoluntaryExit.ValidatorIndex == b.VoluntaryExit.ValidatorIndex &&
		a.Signature == b.Signature
}

func copySignedVoluntaryExit(exit *cltypes.SignedVoluntaryExit) *cltypes.SignedVoluntaryExit {
	if exit == nil || exit.VoluntaryExit == nil {
		return nil
	}
	message := *exit.VoluntaryExit
	copy := *exit
	copy.VoluntaryExit = &message
	return &copy
}
