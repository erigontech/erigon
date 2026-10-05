// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	executionbuilder "github.com/erigontech/erigon/execution/builder"
)

var ErrPendingPayloadStore = errors.New("epbs/runtime: pending payload storage unavailable")

type RuntimeDependencies struct {
	BeaconConfig     *clparams.BeaconChainConfig
	PendingDirectory string
	Clock            LiveSlotClock
	Head             LiveHeadStateSource
	Forkchoice       LiveForkchoiceSource
	Assembler        PayloadAssembler
	Publisher        GossipPublisher
	ColumnStorage    DataColumnWriter
	BidProcessor     BidProcessor
	PayloadProcessor PayloadProcessor
	AcceptedBlocks   AcceptedBlockReader
	HighestBids      HighestBidReader
	Events           *beaconevents.EventEmitter
	Status           *executionbuilder.EmbeddedBuilderStatus
}

type Runtime struct {
	coordinator *Coordinator
	runner      *ValidatedPreferencesRunner
	reveals     *revealRunner
	events      *beaconevents.EventEmitter
	status      *executionbuilder.EmbeddedBuilderStatus
}

func NewRuntime(cfg epbscfg.Config, deps RuntimeDependencies) (*Runtime, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	resolvedCfg, signer, err := prepareRuntimeConfig(cfg, deps.BeaconConfig)
	if err != nil {
		return nil, err
	}
	cfg = resolvedCfg
	if isNilDependency(deps.Clock) || isNilDependency(deps.Head) || isNilDependency(deps.Forkchoice) ||
		isNilDependency(deps.Assembler) || isNilDependency(deps.Publisher) || isNilDependency(deps.ColumnStorage) || isNilDependency(deps.BidProcessor) ||
		isNilDependency(deps.PayloadProcessor) || isNilDependency(deps.AcceptedBlocks) || isNilDependency(deps.HighestBids) || deps.Events == nil {
		return nil, errors.New("epbs/runtime: missing dependency")
	}
	bidPublisher := newValidatedBidPublisher(deps.BidProcessor, deps.Publisher, cfg.RetryInterval)
	coordinator := NewCoordinator(
		deps.BeaconConfig,
		signer,
		FixedMarginStrategy{Margin: cfg.BidMargin},
		deps.Assembler,
		bidPublisher,
		cfg.MaxRetained,
	)
	store, err := OpenPendingPayloadStore(deps.PendingDirectory, deps.BeaconConfig, cfg.MaxRetained)
	if err != nil {
		return nil, fmt.Errorf("%w: open: %w", ErrPendingPayloadStore, err)
	}
	coordinator.pendingStore = store
	if err := coordinator.RecoverPending(deps.Clock.GetCurrentSlot()); err != nil {
		return nil, fmt.Errorf("%w: recover: %w", ErrPendingPayloadStore, err)
	}
	coordinator.bidPublishLead = cfg.BidPublishLead
	coordinator.retryInterval = cfg.RetryInterval
	coordinator.slotTime = deps.Clock.GetSlotTime
	coordinator.maxBidMargin = cfg.MaxBidMargin
	coordinator.minProfitGwei = cfg.MinProfitGwei
	coordinator.highestBids = deps.HighestBids
	coordinator.status = deps.Status
	resolver := NewLiveSlotInputResolver(deps.BeaconConfig, signer, deps.Clock, deps.Head, deps.Forkchoice)
	live := NewLiveCoordinator(coordinator, resolver, resolver)
	live.collateralWarningGwei = cfg.CollateralWarningGwei
	runner, err := newValidatedPreferencesRunnerWithTiming(
		live,
		deps.Clock,
		cfg.MaxPending,
		cfg.RetryInterval,
		cfg.BidDelay,
		func(interval time.Duration) runnerTicker { return systemRunnerTicker{Ticker: time.NewTicker(interval)} },
		time.Now,
		deps.Clock.GetSlotTime,
	)
	if err != nil {
		return nil, fmt.Errorf("epbs/runtime: create preferences runner: %w", err)
	}
	runner.observeOutcome = func(slot uint64, err error) {
		deps.Status.RecordOutcome(slot, builderAttemptOutcome(err))
	}
	reveals := newRevealRunner(
		deps.BeaconConfig, deps.Clock, signer, coordinator, deps.AcceptedBlocks, deps.PayloadProcessor,
		deps.Publisher, deps.Forkchoice, deps.Forkchoice, cfg.RetryInterval, cfg.MaxRetained,
	)
	reveals.blobData = newBlobDataPreparer(deps.BeaconConfig, deps.ColumnStorage, deps.Publisher)
	return &Runtime{coordinator: coordinator, runner: runner, reveals: reveals, events: deps.Events, status: deps.Status}, nil
}

func builderAttemptOutcome(err error) string {
	switch {
	case errors.Is(err, ErrSlotInputStale):
		return executionbuilder.BuilderOutcomeStaleInput
	case errors.Is(err, ErrBuilderCollateralExhausted):
		return executionbuilder.BuilderOutcomeCollateralExhausted
	case errors.Is(err, ErrSlotInputUnavailable):
		return executionbuilder.BuilderOutcomeInputUnavailable
	case errors.Is(err, eladapter.ErrExecutionBusy):
		return executionbuilder.BuilderOutcomeExecutionBusy
	case errors.Is(err, ErrPayloadNotReady):
		return executionbuilder.BuilderOutcomePayloadNotReady
	case errors.Is(err, ErrAuctionAlreadyTracked):
		return executionbuilder.BuilderOutcomeAlreadyTracked
	case errors.Is(err, ErrBidOutbid):
		return executionbuilder.BuilderOutcomeOutbid
	case errors.Is(err, ErrBidBelowMinProfit):
		return executionbuilder.BuilderOutcomeBelowMinProfit
	case errors.Is(err, errLocalBidNotAccepted):
		return executionbuilder.BuilderOutcomeBidRejected
	case errors.Is(err, errValidatedPreferencesAttemptNoBid):
		return executionbuilder.BuilderOutcomeNoBid
	default:
		return executionbuilder.BuilderOutcomeFailed
	}
}

func ValidateRuntimeConfig(cfg epbscfg.Config, beaconCfg *clparams.BeaconChainConfig) error {
	if !cfg.Enabled {
		return nil
	}
	_, _, err := prepareRuntimeConfig(cfg, beaconCfg)
	return err
}

func prepareRuntimeConfig(cfg epbscfg.Config, beaconCfg *clparams.BeaconChainConfig) (epbscfg.Config, Signer, error) {
	if cfg.KeyPath == "" {
		return cfg, nil, errors.New("epbs/runtime: builder key path is required")
	}
	if math.IsNaN(cfg.BidMargin) || math.IsInf(cfg.BidMargin, 0) || cfg.BidMargin < 0 || cfg.BidMargin > 1 {
		return cfg, nil, errors.New("epbs/runtime: bid margin must be between zero and one")
	}
	if math.IsNaN(cfg.MaxBidMargin) || math.IsInf(cfg.MaxBidMargin, 0) || cfg.MaxBidMargin < cfg.BidMargin || cfg.MaxBidMargin > 1 {
		return cfg, nil, errors.New("epbs/runtime: maximum bid margin must be between the bid margin and one")
	}
	if cfg.MaxPending < 0 || cfg.MaxRetained <= 0 {
		return cfg, nil, errors.New("epbs/runtime: capacities must not be negative and retained capacity must be positive")
	}
	if cfg.RetryInterval < minValidatedPreferencesRetryInterval {
		return cfg, nil, errors.New("epbs/runtime: retry interval is too short")
	}
	if beaconCfg == nil {
		return cfg, nil, errors.New("epbs/runtime: missing beacon config")
	}
	if beaconCfg.SlotsPerEpoch == 0 {
		return cfg, nil, errors.New("epbs/runtime: slots per epoch must be positive")
	}
	if beaconCfg.NumberOfColumns == 0 || beaconCfg.DataColumnSidecarSubnetCount == 0 {
		return cfg, nil, errors.New("epbs/runtime: data column and subnet counts must be positive")
	}
	if beaconCfg.PayloadDueBps > clparams.BpsFactor {
		return cfg, nil, errors.New("epbs/runtime: payload deadline must be within the slot")
	}
	if beaconCfg.SecondsPerSlot == 0 || beaconCfg.SecondsPerSlot > uint64(math.MaxInt64/int64(time.Second)) {
		return cfg, nil, errors.New("epbs/runtime: slot duration is outside the supported range")
	}
	slotDuration := time.Duration(beaconCfg.SecondsPerSlot) * time.Second
	if cfg.BidPublishLead < 0 {
		return cfg, nil, errors.New("epbs/runtime: bid publish lead must not be negative")
	}
	if cfg.BidDelay == 0 {
		cfg.BidDelay = max(0, slotDuration/4*3-cfg.BidPublishLead)
	}
	if cfg.BidDelay < 0 || cfg.BidDelay >= slotDuration {
		return cfg, nil, errors.New("epbs/runtime: bid delay must be within the preceding slot")
	}
	if cfg.BidDelay >= slotDuration-cfg.BidPublishLead {
		return cfg, nil, errors.New("epbs/runtime: first bid attempt must precede the bid publish time")
	}
	if cfg.BidDelay > 0 && (cfg.RetryInterval >= slotDuration || cfg.BidDelay >= slotDuration-cfg.RetryInterval) {
		return cfg, nil, errors.New("epbs/runtime: bid delay and retry cadence must fit within the preceding slot")
	}
	if cfg.MaxPending == 0 {
		if beaconCfg.SlotsPerEpoch > uint64(^uint(0)>>1) {
			return cfg, nil, errors.New("epbs/runtime: slots per epoch exceeds pending capacity range")
		}
		cfg.MaxPending = int(beaconCfg.SlotsPerEpoch)
	}
	if uint64(cfg.MaxPending) < beaconCfg.SlotsPerEpoch {
		return cfg, nil, errors.New("epbs/runtime: pending capacity must cover one epoch")
	}
	if beaconCfg.GloasForkEpoch == beaconCfg.FarFutureEpoch {
		return cfg, nil, errors.New("epbs/runtime: Gloas is not configured")
	}
	signer, err := NewLocalSignerFromFile(cfg.KeyPath)
	if err != nil {
		return cfg, nil, fmt.Errorf("epbs/runtime: load builder key: %w", err)
	}
	return cfg, signer, nil
}

func (r *Runtime) SubmitValidatedPreferences(preferences *cltypes.SignedProposerPreferences) {
	if r == nil || r.runner == nil {
		return
	}
	r.runner.SubmitValidatedPreferences(preferences)
}

func (r *Runtime) Run(ctx context.Context) (resultErr error) {
	if r == nil || r.runner == nil || r.reveals == nil || r.events == nil {
		return errors.New("epbs/runtime: not initialized")
	}
	if ctx == nil {
		return errors.New("epbs/runtime: nil context")
	}
	r.status.MarkRunning()
	defer func() {
		if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(resultErr, ctxErr) {
			r.status.MarkStopped(executionbuilder.BuilderStoppedNode)
			return
		}
		if resultErr != nil {
			r.status.MarkStopped(executionbuilder.BuilderStoppedRuntimeError)
		}
	}()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events := make(chan *beaconevents.EventStream, r.reveals.maxQueue())
	subscription := r.events.State().Subscribe(events)
	defer subscription.Unsubscribe()
	runnerDone := make(chan error, 1)
	revealsDone := make(chan struct{})
	runnerExited := false
	go func() { runnerDone <- r.runner.Run(runCtx) }()
	go func() {
		defer close(revealsDone)
		r.reveals.Run(runCtx)
	}()
	defer func() {
		cancel()
		if !runnerExited {
			<-runnerDone
		}
		<-revealsDone
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-runnerDone:
			runnerExited = true
			if err == nil {
				return errors.New("epbs/runtime: preferences runner stopped")
			}
			return err
		case err, ok := <-subscription.Err():
			if !ok || err == nil {
				return errors.New("epbs/runtime: block event subscription stopped")
			}
			return fmt.Errorf("epbs/runtime: block event subscription: %w", err)
		case event, ok := <-events:
			if !ok {
				return errors.New("epbs/runtime: block event stream stopped")
			}
			if event == nil {
				continue
			}
			switch event.Event {
			case beaconevents.StateBlock:
				data, ok := event.Data.(*beaconevents.BlockData)
				if ok && data != nil {
					r.reveals.SubmitAcceptedBlock(data.Block)
				}
			case beaconevents.StateBlockGossip:
				data, ok := event.Data.(*beaconevents.BlockGossipData)
				if ok && data != nil {
					r.reveals.SubmitGossipValidatedBlock(data.Block, data.SignedBlock)
				}
			}
		}
	}
}
