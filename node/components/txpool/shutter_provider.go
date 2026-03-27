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

package txpool

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

// ShutterProvider wraps an existing TxnProvider with the Shutter encrypted mempool.
// When shutter is disabled it is a transparent pass-through: TxnProvider == BaseTxnProvider.
//
// Must be initialized AFTER jsonrpc.EthAPI is available (it depends on ContractBackend
// which wraps EthAPI). Initialize is called after TxPoolProvider and EmbeddedServices.
type ShutterProvider struct {
	// Pool is the underlying shutter.Pool (nil when shutter is disabled).
	Pool *shutter.Pool

	// TxnProvider is the effective provider for the block builder:
	//   - shutter enabled: Pool
	//   - shutter disabled: BaseTxnProvider passed to Initialize
	TxnProvider txnprovider.TxnProvider

	cfg    shuttercfg.Config
	logger log.Logger
}

// ShutterDeps are external dependencies for shutter pool assembly.
// ContractBackend is typically rpc/contracts.NewDirectBackend(ethApi).
// StateChangesClient is satisfied by *direct.StateDiffClientDirect.
// CurrentBlock reads the current canonical block number from chainDB.
type ShutterDeps struct {
	BaseTxnProvider    txnprovider.TxnProvider
	ContractBackend    bind.ContractBackend
	StateChangesClient txpool.StateChangesClient
	CurrentBlock       func(ctx context.Context) (*uint64, error)
}

// Configure applies the shutter config. Call before Initialize.
func (p *ShutterProvider) Configure(cfg shuttercfg.Config, logger log.Logger) {
	p.cfg = cfg
	p.logger = logger
}

// Initialize assembles the shutter pool (or is a no-op when disabled).
// After this call, TxnProvider is ready for the block builder.
func (p *ShutterProvider) Initialize(_ context.Context, deps ShutterDeps) error {
	if deps.BaseTxnProvider == nil {
		return fmt.Errorf("shutter: BaseTxnProvider must not be nil")
	}
	p.TxnProvider = deps.BaseTxnProvider

	if !p.cfg.Enabled {
		return nil
	}

	p.Pool = shutter.NewPool(
		p.logger,
		p.cfg,
		deps.BaseTxnProvider,
		deps.ContractBackend,
		deps.StateChangesClient,
		deps.CurrentBlock,
	)
	p.TxnProvider = p.Pool
	return nil
}

// Start launches the shutter pool background goroutine (no-op when disabled).
func (p *ShutterProvider) Start(ctx context.Context, eg ErrGroup) {
	if p.Pool == nil {
		return
	}
	eg.Go(func() error {
		defer p.logger.Info("[shutter] pool goroutine terminated")
		if err := p.Pool.Run(ctx); err != nil && ctx.Err() == nil {
			p.logger.Error("[shutter] Run error", "err", err)
		}
		return nil
	})
}

// IsEnabled returns true if the shutter pool is active.
func (p *ShutterProvider) IsEnabled() bool {
	return p.Pool != nil
}
