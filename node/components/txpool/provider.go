// Copyright 2025 The Erigon Authors
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

package txpool

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type StateChangesClient = txpool.StateChangesClient

type Provider struct {
	Pool                 *txpool.TxPool
	Server               txpoolproto.TxpoolServer
	cfg                  txpoolcfg.Config
	chainDB              kv.TemporalRoDB
	chainCfg             *chain.Config
	ethBackend           remoteproto.ETHBACKENDClient
	stateChanges         StateChangesClient
	logger               log.Logger
	sentryClients        []sentryproto.SentryClient
	cache                kvcache.Cache
	builderNotifyNewTxns func()
	runCtx               context.Context
	runCancel            context.CancelFunc
	runWg                sync.WaitGroup
	initialized          bool
	active               bool
	mu                   sync.RWMutex
}

func NewProvider() *Provider {
	return &Provider{}
}

func (p *Provider) Configure(
	cfg txpoolcfg.Config,
	chainDB kv.TemporalRoDB,
	chainCfg *chain.Config,
	ethBackend remoteproto.ETHBACKENDClient,
	stateChanges StateChangesClient,
	logger log.Logger,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.initialized {
		return fmt.Errorf("provider already initialized")
	}

	p.cfg = cfg
	p.chainDB = chainDB
	p.chainCfg = chainCfg
	p.ethBackend = ethBackend
	p.stateChanges = stateChanges
	p.logger = logger

	return nil
}

func (p *Provider) SetSentryClients(clients []sentryproto.SentryClient) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.initialized {
		return fmt.Errorf("provider already initialized")
	}
	p.sentryClients = clients
	return nil
}

func (p *Provider) SetCache(cache kvcache.Cache) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.initialized {
		return fmt.Errorf("provider already initialized")
	}
	p.cache = cache
	return nil
}

func (p *Provider) SetBuilderNotifyNewTxns(fn func()) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.initialized {
		return fmt.Errorf("provider already initialized")
	}
	p.builderNotifyNewTxns = fn
	return nil
}

func (p *Provider) Initialize(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.initialized {
		return fmt.Errorf("provider already initialized")
	}

	if p.chainDB == nil || p.chainCfg == nil || p.ethBackend == nil || p.stateChanges == nil {
		return fmt.Errorf("provider not configured: call Configure() first")
	}

	// If disabled, set to disabled stubs
	if p.cfg.Disable {
		p.Pool = nil
		p.Server = &txpool.GrpcDisabled{}
		p.initialized = true
		return nil
	}

	if p.cache == nil {
		cacheConfig := kvcache.DefaultCoherentConfig
		cacheConfig.MetricsLabel = "txpool"
		p.cache = kvcache.New(cacheConfig)
	}
	if p.builderNotifyNewTxns == nil {
		p.builderNotifyNewTxns = func() {}
	}

	pool, server, err := txpool.Assemble(
		ctx,
		p.cfg,
		p.chainDB,
		p.cache,
		p.sentryClients,
		p.stateChanges,
		p.builderNotifyNewTxns,
		p.logger,
		p.ethBackend,
	)
	if err != nil {
		return err
	}

	p.Pool = pool
	p.Server = server
	p.runCtx = ctx
	p.initialized = true
	p.logger.Info("TxPool Provider initialized", "disabled", p.cfg.Disable)

	return nil
}

func (p *Provider) Activate() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.initialized {
		return fmt.Errorf("provider not initialized: call Initialize() first")
	}

	if p.active {
		return nil // already active
	}

	if p.cfg.Disable {
		p.active = true
		return nil
	}

	if p.Pool == nil {
		return fmt.Errorf("provider not initialized: pool is nil")
	}

	runCtx := p.runCtx
	if runCtx == nil {
		runCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(runCtx)
	p.runCancel = cancel
	p.runWg.Add(1)
	go func() {
		defer p.runWg.Done()
		if err := p.Pool.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			p.logger.Error("[txpool] Run error", "err", err)
		}
	}()
	p.active = true
	p.logger.Info("TxPool Provider activated")
	return nil
}

func (p *Provider) Deactivate() error {
	p.mu.Lock()

	if !p.active {
		p.mu.Unlock()
		return nil // already inactive
	}

	p.active = false
	cancel := p.runCancel
	p.runCancel = nil
	p.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	p.runWg.Wait()
	p.logger.Info("TxPool Provider deactivated")
	return nil
}

func (p *Provider) IsInitialized() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.initialized
}

func (p *Provider) IsActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active
}

func (p *Provider) IsDisabled() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cfg.Disable
}
