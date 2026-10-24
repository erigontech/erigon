// Copyright 2026 The Erigon Authors
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

package handler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common/hexutil"
)

// StartPayloadAttributes emits Gloas payload attributes for the next slot when the head changes and
// at each slot's payload decision.
func (a *ApiHandler) StartPayloadAttributes(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	events := make(chan *beaconevents.EventStream, 128)
	subscription := a.emitters.State().Subscribe(events)
	// Feed sends wait for every subscriber, so state work runs off the receiving goroutine.
	wake := make(chan struct{}, 1)
	notify := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	receiverDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		defer subscription.Unsubscribe()
		decision := time.NewTimer(a.untilPayloadDecision())
		defer decision.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-subscription.Err():
				return
			case event := <-events:
				if event.Event == beaconevents.StateHeadV2 {
					notify()
				}
			case <-decision.C:
				notify()
				decision.Reset(a.untilPayloadDecision())
			}
		}
	}()
	go func() {
		defer func() {
			<-receiverDone
			close(done)
		}()
		failureLog := rate.Sometimes{Interval: time.Minute}
		for {
			select {
			case <-ctx.Done():
				return
			case <-wake:
				if err := a.emitPayloadAttributes(); err != nil && !errors.Is(err, errForkChoiceHeadChanged) {
					failureLog.Do(func() {
						a.logger.Debug("Failed to emit payload attributes", "err", err)
					})
				}
			}
		}
	}()
	return done
}

func (a *ApiHandler) untilPayloadDecision() time.Duration {
	due := time.Duration(a.beaconChainCfg.PayloadAttestationDueMs()) * time.Millisecond
	slot := a.ethClock.GetCurrentSlot()
	next := a.ethClock.GetSlotTime(slot).Add(due)
	if !time.Now().Before(next) {
		next = a.ethClock.GetSlotTime(slot + 1).Add(due)
	}
	return time.Until(next)
}

func (a *ApiHandler) emitPayloadAttributes() error {
	if !a.emitters.HasPayloadAttributesSubscribers() {
		return nil
	}
	head, headSlot, err := a.forkchoiceStore.GetHeadNode()
	if err != nil {
		return fmt.Errorf("payload attributes event head: %w", err)
	}
	proposalSlot := max(a.ethClock.GetCurrentSlot(), headSlot) + 1
	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(proposalSlot / a.beaconChainCfg.SlotsPerEpoch)
	if stateVersion.Before(clparams.GloasVersion) || a.proposerLookupTooFarAhead(headSlot, proposalSlot) {
		return nil
	}
	baseState, err := a.forkchoiceStore.GetOwnedStateAtBlockRoot(head.Root)
	if err != nil {
		return fmt.Errorf("payload attributes event state: %w", err)
	}
	if baseState == nil {
		return errors.New("payload attributes event state not found")
	}
	if err := transition.DefaultMachine.ProcessSlots(baseState, proposalSlot); err != nil {
		return fmt.Errorf("payload attributes event process slots: %w", err)
	}
	payloadSource, err := a.resolveExecutionPayloadSource(baseState, head.Root, proposalSlot, stateVersion)
	if err != nil {
		return err
	}
	withdrawalsState, err := withdrawalsStateForExecutionPayloadSource(baseState, payloadSource)
	if err != nil {
		return fmt.Errorf("payload attributes event withdrawals state: %w", err)
	}
	withdrawals, err := a.expectedWithdrawals(baseState, withdrawalsState, proposalSlot)
	if err != nil {
		return fmt.Errorf("payload attributes event withdrawals: %w", err)
	}
	proposerIndex, err := baseState.GetBeaconProposerIndexForSlot(proposalSlot)
	if err != nil {
		return fmt.Errorf("payload attributes event proposer: %w", err)
	}
	preference := a.proposerPreferencesForProposal(baseState, proposalSlot, proposerIndex, stateVersion)
	registeredRecipient, registered := a.validatorParams.GetFeeRecipient(proposerIndex)
	feeRecipient, _ := resolveFeeRecipient(preference, registeredRecipient, registered)
	targetGasLimit := a.targetGasLimitForProposal(baseState, proposalSlot, proposerIndex, stateVersion)
	slotNumber := hexutil.Uint64(proposalSlot)
	attributes := a.payloadBuildAttributes(
		baseState, head.Root, proposalSlot, feeRecipient, withdrawals, &slotNumber, targetGasLimit, stateVersion,
	)
	finalizedHash, safeHash := a.payloadBuildFinalityHashes(baseState, payloadSource.head)
	if current, _, err := a.forkchoiceStore.GetHeadNode(); err != nil || current != head || proposalSlot <= a.ethClock.GetCurrentSlot() {
		return err
	}
	a.emitters.State().SendPayloadAttributes(&beaconevents.PayloadAttributesData{
		Version: stateVersion.String(),
		Data: beaconevents.PayloadAttributesContent{
			ProposerIndex:      proposerIndex,
			ProposalSlot:       proposalSlot,
			ParentBlockRoot:    head.Root,
			ParentBlockHash:    payloadSource.head,
			SafeBlockHash:      &safeHash,
			FinalizedBlockHash: &finalizedHash,
			PayloadAttributes:  beaconevents.NewPayloadAttributes(stateVersion, attributes),
		},
	})
	return nil
}
