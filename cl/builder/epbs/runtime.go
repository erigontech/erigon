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

	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

type RuntimeDependencies struct {
	BeaconConfig *clparams.BeaconChainConfig
	Clock        LiveSlotClock
	Head         LiveHeadStateSource
	Forkchoice   LiveForkchoiceSource
	Assembler    PayloadAssembler
	Publisher    GossipPublisher
}

type Runtime struct {
	coordinator *Coordinator
	runner      *ValidatedPreferencesRunner
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
		isNilDependency(deps.Assembler) || isNilDependency(deps.Publisher) {
		return nil, errors.New("epbs/runtime: missing dependency")
	}
	coordinator := NewCoordinator(
		deps.BeaconConfig,
		signer,
		FixedMarginStrategy{Margin: cfg.BidMargin},
		deps.Assembler,
		deps.Publisher,
		cfg.MaxRetained,
	)
	resolver := NewLiveSlotInputResolver(deps.BeaconConfig, signer, deps.Clock, deps.Head, deps.Forkchoice)
	live := NewLiveCoordinator(coordinator, resolver, resolver)
	runner, err := NewValidatedPreferencesRunner(live, deps.Clock, cfg.MaxPending, cfg.RetryInterval)
	if err != nil {
		return nil, fmt.Errorf("epbs/runtime: create preferences runner: %w", err)
	}
	return &Runtime{coordinator: coordinator, runner: runner}, nil
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
	if r == nil || r.runner == nil {
		return errors.New("epbs/runtime: not initialized")
	}
	return r.runner.Run(ctx)
}
