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

// Package txpool provides the TxPool Provider, which assembles the devp2p
// transaction pool and its gRPC server for backend.go and cmd/txpool.
package txpool

import (
	"context"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// Provider holds the assembled pool. Pool is nil when the pool is disabled.
type Provider struct {
	Pool       *txpool.TxPool
	GrpcServer txpoolproto.TxpoolServer
}

type Deps struct {
	Ctx           context.Context
	Config        txpoolcfg.Config
	ChainDB       kv.TemporalRoDB
	Cache         kvcache.Cache
	Sentries      []sentryproto.SentryClient
	StateChanges  txpool.StateChangesClient
	NotifyNewTxns func()
	EthBackend    remoteproto.ETHBACKENDClient
	Logger        log.Logger
	Options       []txpool.Option
}

func (p *Provider) Initialize(deps Deps) error {
	if deps.Config.Disable {
		p.GrpcServer = &txpool.GrpcDisabled{}
		return nil
	}
	pool, grpcServer, err := txpool.Assemble(
		deps.Ctx,
		deps.Config,
		deps.ChainDB,
		deps.Cache,
		deps.Sentries,
		deps.StateChanges,
		deps.NotifyNewTxns,
		deps.Logger,
		deps.EthBackend,
		deps.Options...,
	)
	if err != nil {
		return err
	}
	p.Pool = pool
	p.GrpcServer = grpcServer
	return nil
}
