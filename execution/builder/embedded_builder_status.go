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

package builder

import "sync"

const (
	BuilderPhaseDisabled = "disabled"
	BuilderPhaseStarting = "starting"
	BuilderPhaseRunning  = "running"
	BuilderPhaseStopped  = "stopped"

	BuilderDisabledNotConfigured       = "not_configured"
	BuilderDisabledPendingPayloadStore = "pending_payload_store_unavailable"
	BuilderStoppedRuntimeError         = "runtime_error"
	BuilderStoppedNode                 = "node_stopping"

	BuilderOutcomePublished           = "published"
	BuilderOutcomeStaleInput          = "stale_input"
	BuilderOutcomeInputUnavailable    = "input_unavailable"
	BuilderOutcomeExecutionBusy       = "execution_busy"
	BuilderOutcomePayloadNotReady     = "payload_not_ready"
	BuilderOutcomeAlreadyTracked      = "already_tracked"
	BuilderOutcomeBidRejected         = "bid_rejected"
	BuilderOutcomeNoBid               = "no_bid"
	BuilderOutcomeOutbid              = "outbid"
	BuilderOutcomeBelowMinProfit      = "below_min_profit"
	BuilderOutcomeCollateralExhausted = "collateral_exhausted"
	BuilderOutcomeFailed              = "failed"
)

type EmbeddedBuilderStatusSnapshot struct {
	Enabled                 bool
	Phase                   string
	Reason                  string
	LastAttemptSlot         uint64
	LastBidSlot             uint64
	LastBidValueGwei        uint64
	LastOutcomeSlot         uint64
	LastOutcome             string
	AvailableCollateralGwei uint64
}

type EmbeddedBuilderStatus struct {
	mu       sync.RWMutex
	snapshot EmbeddedBuilderStatusSnapshot
}

func NewEmbeddedBuilderStatus(enabled bool) *EmbeddedBuilderStatus {
	status := &EmbeddedBuilderStatus{}
	if enabled {
		status.snapshot = EmbeddedBuilderStatusSnapshot{Enabled: true, Phase: BuilderPhaseStarting}
	} else {
		status.snapshot = EmbeddedBuilderStatusSnapshot{Phase: BuilderPhaseDisabled, Reason: BuilderDisabledNotConfigured}
	}
	return status
}

func (s *EmbeddedBuilderStatus) Snapshot() EmbeddedBuilderStatusSnapshot {
	if s == nil {
		return EmbeddedBuilderStatusSnapshot{Phase: BuilderPhaseDisabled, Reason: BuilderDisabledNotConfigured}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}

func (s *EmbeddedBuilderStatus) MarkRunning() {
	s.setPhase(BuilderPhaseRunning, "")
}

func (s *EmbeddedBuilderStatus) MarkDisabled(reason string) {
	s.setPhase(BuilderPhaseDisabled, reason)
}

func (s *EmbeddedBuilderStatus) MarkStopped(reason string) {
	s.setPhase(BuilderPhaseStopped, reason)
}

func (s *EmbeddedBuilderStatus) setPhase(phase, reason string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Phase = phase
	s.snapshot.Reason = reason
}

func (s *EmbeddedBuilderStatus) RecordAttempt(slot uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot >= s.snapshot.LastAttemptSlot {
		s.snapshot.LastAttemptSlot = slot
	}
}

func (s *EmbeddedBuilderStatus) RecordBid(slot, valueGwei uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot >= s.snapshot.LastBidSlot {
		s.snapshot.LastBidSlot = slot
		s.snapshot.LastBidValueGwei = valueGwei
	}
	if slot >= s.snapshot.LastOutcomeSlot {
		s.snapshot.LastOutcomeSlot = slot
		s.snapshot.LastOutcome = BuilderOutcomePublished
	}
}

func (s *EmbeddedBuilderStatus) RecordOutcome(slot uint64, outcome string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot == s.snapshot.LastOutcomeSlot && s.snapshot.LastOutcome == BuilderOutcomePublished {
		return
	}
	if slot >= s.snapshot.LastOutcomeSlot {
		s.snapshot.LastOutcomeSlot = slot
		s.snapshot.LastOutcome = outcome
	}
}

func (s *EmbeddedBuilderStatus) RecordAvailableCollateral(valueGwei uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.AvailableCollateralGwei = valueGwei
}
