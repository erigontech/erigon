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

// Package fork owns the runtime chain-config transition (parent ↔
// fork child) driven by both the debug_setFork RPC and the
// integration binary. Callers implement Runtime; the Controller
// validates the target, unwinds to CutBlock, walks the Restartable /
// Reconfigurable captor list, and swaps chain.Config in place.
package fork

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// Runtime is what the Controller drives during a transition. Callers
// implement this — Ethereum (backend.go) for the RPC path, a tailored
// wrapper for the integration binary. Nil-return getters mean "nothing
// wired": Restartables()/Reconfigurables() may return empty maps for a
// minimal runtime without derailing the transition.
type Runtime interface {
	// ChainDB returns the DB handle used to read the current head
	// block number.
	ChainDB() kv.RwDB
	// CurrentChainConfig returns the chain.Config currently in
	// effect. Read at the start of Transition to compute the parent
	// relationship + CutBlock.
	CurrentChainConfig() *chain.Config
	// DataDir returns the datadir root — used to resolve target
	// chain specs stashed as fork.toml under the datadir.
	DataDir() string
	// SetHead performs the unwind to the fork's CutBlock. Blocks
	// until execution + downstream captors have applied it.
	SetHead(ctx context.Context, block uint64) error
	// Restartables returns the captors that implement Stop /
	// SetChainConfig / Start (sentry, storage, caplin). Ordering
	// matters for Start — Controller reads from this map and
	// executes Start in a fixed sequence.
	Restartables() map[string]rpchelper.ChainConfigRestartable
	// Reconfigurables returns the captors that support single-call
	// Reconfigure(ctx, cfg) — TxPool, Downloader, manifest_exchange.
	Reconfigurables() map[string]rpchelper.ChainConfigReconfigurable
	// SwapChainConfig updates the runtime's own chain.Config
	// reference (e.g. Ethereum.chainConfig) after captors have been
	// stopped + reconfigured but before they Start on the new
	// config. Nil-return; Controller does not surface an error path
	// because the pointer swap itself cannot fail.
	SwapChainConfig(target *chain.Config)
	// ApplyPostSwapHooks runs runtime-specific fixups that the
	// captor interfaces don't cover — Downloader.SetChainIdentity,
	// manifest_exchange filters, Caplin launch closure rebind.
	// Called after SwapChainConfig, before the Restartables' Start
	// sequence, so post-transition Starts see the target chain
	// identity everywhere.
	ApplyPostSwapHooks(target *chain.Config)
	// BackgroundCtx returns the runtime's long-lived shutdown ctx
	// (backend.sentryCtx in the in-process case). The Controller
	// uses it — NOT the caller's Transition ctx — when calling
	// Restartable.Start on post-swap. The Transition ctx cancels as
	// soon as the RPC responds; parenting Caplin's goroutine on it
	// would kill Caplin immediately after the transition returns.
	BackgroundCtx() context.Context
	// Logger returns the runtime's logger for diagnostic output.
	Logger() log.Logger
}

// Controller drives a single fork transition against a Runtime.
type Controller struct {
	rt Runtime
}

// New constructs a Controller bound to the given Runtime.
func New(rt Runtime) *Controller {
	return &Controller{rt: rt}
}

// Transition validates the target chain name (must be a direct
// parent/child of Runtime.CurrentChainConfig()), unwinds to the
// CutBlock via Runtime.SetHead, then walks the Restartable and
// Reconfigurable captor lists to swap chain.Config in-process.
//
// Returns RestartRequired=false on success; RestartRequired=true when
// a post-swap Start fails — the caller restarts erigon with
// --chain=<target> to complete the transition manually.
func (c *Controller) Transition(ctx context.Context, targetChainName string) (*rpchelper.SetForkResult, error) {
	if targetChainName == "" {
		return nil, errors.New("targetChainName is required")
	}
	current := c.rt.CurrentChainConfig()
	currentName := current.ChainName
	if targetChainName == currentName {
		return nil, fmt.Errorf("target chain %q is the currently-loaded chain; no transition needed", targetChainName)
	}

	targetSpec, err := chainspec.ChainSpecByNameOrForkDatadir(targetChainName, c.rt.DataDir())
	if err != nil {
		return nil, fmt.Errorf("looking up target chain %q: %w", targetChainName, err)
	}
	target := targetSpec.Config
	if target == nil {
		return nil, fmt.Errorf("target chain %q has no config", targetChainName)
	}

	var cutBlock uint64
	switch {
	case target.Parent == currentName:
		cutBlock = target.CutBlock
	case current.Parent == targetChainName:
		cutBlock = current.CutBlock
	default:
		return nil, fmt.Errorf(
			"target chain %q has no direct parent relationship with current %q "+
				"(target.Parent=%q, current.Parent=%q)",
			targetChainName, currentName, target.Parent, current.Parent,
		)
	}
	if cutBlock == 0 {
		return nil, fmt.Errorf(
			"transition target %q resolves to CutBlock=0; refusing (root-chain transitions not supported)",
			targetChainName,
		)
	}

	unwoundFrom, err := readCurrentBlockNumber(ctx, c.rt.ChainDB())
	if err != nil {
		return nil, fmt.Errorf("reading current head: %w", err)
	}
	if unwoundFrom < cutBlock {
		return nil, fmt.Errorf(
			"current head %d is already at or below CutBlock %d for target %q",
			unwoundFrom, cutBlock, targetChainName,
		)
	}

	if err := c.rt.SetHead(ctx, cutBlock); err != nil {
		return nil, fmt.Errorf("SetHead(%d): %w", cutBlock, err)
	}

	restartRequired, swapErr := c.applyChainConfigSwap(ctx, target)
	message := ""
	if swapErr != nil {
		message = fmt.Sprintf("Unwound OK but chain-config swap failed: %v. Restart erigon with --chain=%s to complete the transition.", swapErr, targetChainName)
		c.rt.Logger().Error("[fork] chain-config swap failed; restart required", "err", swapErr)
	} else if restartRequired {
		message = fmt.Sprintf("Unwound OK and partial swap completed. Restart erigon with --chain=%s to complete the transition.", targetChainName)
	}

	return &rpchelper.SetForkResult{
		FromChain:       currentName,
		ToChain:         targetChainName,
		UnwoundFrom:     unwoundFrom,
		UnwoundTo:       cutBlock,
		RestartRequired: restartRequired || swapErr != nil,
		Message:         message,
	}, nil
}

func (c *Controller) applyChainConfigSwap(ctx context.Context, target *chain.Config) (restartRequired bool, err error) {
	restartables := c.rt.Restartables()
	reconfigurables := c.rt.Reconfigurables()

	for name, r := range restartables {
		if stopErr := r.Stop(); stopErr != nil {
			return true, fmt.Errorf("stop %s: %w", name, stopErr)
		}
	}

	for name, r := range reconfigurables {
		if rcErr := r.Reconfigure(ctx, target); rcErr != nil {
			return true, fmt.Errorf("reconfigure %s: %w", name, rcErr)
		}
	}

	for _, r := range restartables {
		r.SetChainConfig(target)
	}

	c.rt.SwapChainConfig(target)
	c.rt.ApplyPostSwapHooks(target)

	// Start uses BackgroundCtx (node lifetime), NOT the caller's ctx.
	// The Transition ctx is bound to the RPC handler and cancels as
	// soon as the response is sent — parenting a long-lived component
	// goroutine (Caplin, sentry) on it would kill the goroutine
	// immediately after Transition returns.
	startCtx := c.rt.BackgroundCtx()
	if startCtx == nil {
		startCtx = ctx
	}
	var startErrs []string
	for _, name := range []string{"storage", "sentry", "caplin"} {
		r, ok := restartables[name]
		if !ok {
			continue
		}
		if startErr := r.Start(startCtx); startErr != nil {
			startErrs = append(startErrs, fmt.Sprintf("start %s: %v", name, startErr))
		}
	}
	if len(startErrs) > 0 {
		return true, fmt.Errorf("post-swap start failures: %s", strings.Join(startErrs, "; "))
	}
	return false, nil
}

func readCurrentBlockNumber(ctx context.Context, db kv.RwDB) (uint64, error) {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	return stages.GetStageProgress(tx, stages.Finish)
}
