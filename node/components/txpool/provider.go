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

// Package txpool provides the TxPoolProvider — the transaction pool component
// extracted from backend.go as part of the Erigon componentization effort.
//
// The TxPool manages pending transactions and feeds them to the block builder.
// When disabled (--txpool.disable), the provider returns a GrpcDisabled server
// so callers don't need to nil-check.
//
// The Shutter encrypted mempool wrapper (shutter.Pool) is not included here
// because it depends on jsonrpc.EthAPI which is initialized after the base pool.
// Shutter wiring stays in backend.go until EthAPI is also componentized.
package txpool

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/chain"
	remoteproto "github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	sentryproto "github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	txpoolproto "github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// Provider holds the TxPool's runtime state. It implements:
//
//	Configure → Initialize → Start
//
// After Initialize, GrpcServer is ready for use by EmbeddedServices and
// Pool is available for consumers that need direct access (e.g. Shutter).
type Provider struct {
	// Public — accessible by consumers via dependency graph.
	Pool       *txpool.TxPool           // nil when disabled
	GrpcServer txpoolproto.TxpoolServer // always set (GrpcDisabled when disabled)

	// Configuration
	cfg         txpoolcfg.Config
	chainConfig *chain.Config
	logger      log.Logger
}

// Configure applies configuration. Call before Initialize.
// Enforces Bor minimum fee cap if applicable.
func (p *Provider) Configure(
	cfg txpoolcfg.Config,
	chainConfig *chain.Config,
	logger log.Logger,
) {
	p.cfg = cfg
	p.chainConfig = chainConfig
	p.logger = logger

	// Enforce Bor minimum fee cap.
	if chainConfig != nil && chainConfig.Bor != nil && cfg.MinFeeCap != txpoolcfg.BorDefaultTxPoolPriceLimit {
		logger.Warn("Sanitizing invalid bor min fee cap",
			"provided", cfg.MinFeeCap,
			"updated", txpoolcfg.BorDefaultTxPoolPriceLimit,
		)
		p.cfg.MinFeeCap = txpoolcfg.BorDefaultTxPoolPriceLimit
	}
}

// Deps are the external dependencies needed to assemble the pool.
// They come from other components initialized earlier in backend.go.
type Deps struct {
	ChainDB            kv.TemporalRoDB
	Sentries           []sentryproto.SentryClient
	StateChangesClient txpool.StateChangesClient
	EthBackend         remoteproto.ETHBACKENDClient
	BlockBuilderNotify func() // called when new txns arrive (wakes block builder)
}

// Initialize creates the txpool. After this call, GrpcServer and Pool are ready.
func (p *Provider) Initialize(ctx context.Context, deps Deps) error {
	if p.cfg.Disable {
		p.GrpcServer = &txpool.GrpcDisabled{}
		return nil
	}

	pool, grpcServer, err := txpool.Assemble(
		ctx,
		p.cfg,
		deps.ChainDB,
		kvcache.NewDummy(),
		deps.Sentries,
		deps.StateChangesClient,
		deps.BlockBuilderNotify,
		p.logger,
		deps.EthBackend,
	)
	if err != nil {
		return fmt.Errorf("assemble txpool: %w", err)
	}
	p.Pool = pool
	p.GrpcServer = grpcServer
	return nil
}

// ErrGroup is satisfied by errgroup.Group and similar constructs.
type ErrGroup interface {
	Go(func() error)
}

// Start launches the txpool background goroutine.
// Should be called after all initialization is complete.
func (p *Provider) Start(ctx context.Context, eg ErrGroup) {
	if p.Pool == nil {
		return
	}
	eg.Go(func() error {
		defer p.logger.Info("[devp2p] txn pool goroutine terminated")
		if err := p.Pool.Run(ctx); err != nil && ctx.Err() == nil {
			p.logger.Error("[devp2p] Run error", "err", err)
		}
		return nil
	})
}

// IsEnabled returns true if the pool is active (not disabled).
func (p *Provider) IsEnabled() bool {
	return p.Pool != nil
}
