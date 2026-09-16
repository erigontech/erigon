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
	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common/log/v3"
)

type RuntimeDependencies struct {
	BeaconConfig     *clparams.BeaconChainConfig
	Clock            LiveSlotClock
	Head             LiveHeadStateSource
	Forkchoice       LiveForkchoiceSource
	Assembler        PayloadAssembler
	Publisher        GossipPublisher
	ColumnStorage    DataColumnWriter
	BidProcessor     BidProcessor
	PayloadProcessor PayloadProcessor
	AcceptedBlocks   AcceptedBlockReader
	Events           *beaconevents.EventEmitter
}

type Runtime struct {
	coordinator *Coordinator
	runner      *ValidatedPreferencesRunner
	shadow      *shadowValueCurveRunner
	reveals     *revealRunner
	events      *beaconevents.EventEmitter
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
		isNilDependency(deps.PayloadProcessor) || isNilDependency(deps.AcceptedBlocks) || deps.Events == nil {
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
	coordinator.privateOrderflowWindow = cfg.PrivateOrderflowWindow
	resolver := NewLiveSlotInputResolver(deps.BeaconConfig, signer, deps.Clock, deps.Head, deps.Forkchoice)
	live := NewLiveCoordinator(coordinator, resolver, resolver)
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
	var shadow *shadowValueCurveRunner
	if cfg.ShadowValueCurve {
		discarder, ok := deps.Assembler.(payloadDiscarder)
		if !ok || !discarder.CanDiscardPayload() {
			return nil, errors.New("epbs/runtime: shadow value curve requires disposable payload assembly")
		}
		shadow, err = newShadowValueCurveRunner(
			live,
			deps.Clock,
			cfg.MaxPending,
			cfg.RetryInterval,
			cfg.BidDelay,
			[]time.Duration{cfg.BidDelay + 2*time.Second, cfg.BidDelay + 4*time.Second},
			func(interval time.Duration) runnerTicker { return systemRunnerTicker{Ticker: time.NewTicker(interval)} },
			time.Now,
			deps.Clock.GetSlotTime,
		)
		if err != nil {
			return nil, fmt.Errorf("epbs/runtime: create shadow value curve: %w", err)
		}
		coordinator.onPayloadMeasured = func(
			preferences *cltypes.SignedProposerPreferences,
			parent PayloadParentIdentity,
			measurement PayloadMeasurement,
		) {
			if !shadow.Submit(preferences, parent, measurement) {
				log.Warn("Embedded builder shadow payload baseline dropped", "slot", parent.Slot,
					"parentBlockRoot", parent.ParentBlockRoot, "parentBlockHash", parent.ParentBlockHash)
			}
		}
	}
	reveals := newRevealRunner(
		deps.BeaconConfig, deps.Clock, signer, coordinator, deps.AcceptedBlocks, deps.PayloadProcessor,
		deps.Publisher, deps.Forkchoice, deps.Forkchoice, cfg.RetryInterval, cfg.MaxRetained,
	)
	reveals.blobData = newBlobDataPreparer(deps.BeaconConfig, deps.ColumnStorage, deps.Publisher)
	return &Runtime{coordinator: coordinator, runner: runner, shadow: shadow, reveals: reveals, events: deps.Events}, nil
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
	if cfg.BidDelay < 0 || cfg.BidDelay >= slotDuration {
		return cfg, nil, errors.New("epbs/runtime: bid delay must be within the preceding slot")
	}
	if cfg.PrivateOrderflowWindow < 0 || cfg.PrivateOrderflowWindow >= slotDuration {
		return cfg, nil, errors.New("epbs/runtime: private orderflow window must be within the preceding slot")
	}
	if cfg.BidDelay > 0 || cfg.PrivateOrderflowWindow > 0 {
		if cfg.RetryInterval >= slotDuration {
			return cfg, nil, errors.New("epbs/runtime: retry cadence must fit within the preceding slot")
		}
		remaining := slotDuration - cfg.RetryInterval
		if cfg.PrivateOrderflowWindow >= remaining || cfg.BidDelay >= remaining-cfg.PrivateOrderflowWindow {
			return cfg, nil, errors.New("epbs/runtime: bid delay, private orderflow window, and retry cadence must fit within the preceding slot")
		}
	}
	if cfg.ShadowValueCurve {
		const shadowOffset = 4 * time.Second
		if cfg.BidDelay <= 0 || slotDuration <= shadowOffset || cfg.RetryInterval >= slotDuration-shadowOffset ||
			cfg.BidDelay >= slotDuration-shadowOffset-cfg.RetryInterval {
			return cfg, nil, errors.New("epbs/runtime: shadow value curve must fit within the preceding slot")
		}
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

func (r *Runtime) Run(ctx context.Context) error {
	if r == nil || r.runner == nil || r.reveals == nil || r.events == nil {
		return errors.New("epbs/runtime: not initialized")
	}
	if ctx == nil {
		return errors.New("epbs/runtime: nil context")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events := make(chan *beaconevents.EventStream, r.reveals.maxQueue())
	subscription := r.events.State().Subscribe(events)
	defer subscription.Unsubscribe()
	runnerDone := make(chan error, 1)
	var shadowDone chan error
	revealsDone := make(chan struct{})
	runnerExited := false
	shadowExited := false
	go func() { runnerDone <- r.runner.Run(runCtx) }()
	if r.shadow != nil {
		shadowDone = make(chan error, 1)
		go func() { shadowDone <- r.shadow.Run(runCtx) }()
	}
	go func() {
		defer close(revealsDone)
		r.reveals.Run(runCtx)
	}()
	defer func() {
		cancel()
		if !runnerExited {
			<-runnerDone
		}
		if shadowDone != nil && !shadowExited {
			<-shadowDone
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
		case err := <-shadowDone:
			shadowExited = true
			if err == nil {
				return errors.New("epbs/runtime: shadow value curve stopped")
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
