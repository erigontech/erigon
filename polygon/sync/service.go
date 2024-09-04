// Copyright 2024 The Erigon Authors
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

package sync

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/p2p"
)

type Service interface {
	Run(ctx context.Context) error
}

type service struct {
	sync            *Sync
	p2pService      p2p.Service
	store           Store
	events          *TipEvents
	heimdallService heimdall.Service
	bridgeService   bridge.Service
}

func NewService(
	logger log.Logger,
	chainConfig *chain.Config,
	sentryClient sentryproto.SentryClient,
	maxPeers int,
	statusDataProvider *sentry.StatusDataProvider,
	executionClient executionproto.ExecutionClient,
	blockLimit uint,
	bridgeService bridge.Service,
	heimdallService heimdall.Service,
) Service {
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	checkpointVerifier := VerifyCheckpointHeaders
	milestoneVerifier := VerifyMilestoneHeaders
	blocksVerifier := VerifyBlocks
	p2pService := p2p.NewService(maxPeers, logger, sentryClient, statusDataProvider.GetStatusData)
	execution := NewExecutionClient(executionClient)
	store := NewStore(logger, execution, bridgeService)
	blockDownloader := NewBlockDownloader(
		logger,
		p2pService,
		heimdallService,
		checkpointVerifier,
		milestoneVerifier,
		blocksVerifier,
		store,
		blockLimit,
	)
	ccBuilderFactory := NewCanonicalChainBuilderFactory(chainConfig, borConfig, heimdallService)
	events := NewTipEvents(logger, p2pService, heimdallService)
	sync := NewSync(
		store,
		execution,
		milestoneVerifier,
		blocksVerifier,
		p2pService,
		blockDownloader,
		ccBuilderFactory,
		heimdallService,
		bridgeService,
		events.Events(),
		logger,
	)
	return &service{
		sync:            sync,
		p2pService:      p2pService,
		store:           store,
		events:          events,
		heimdallService: heimdallService,
		bridgeService:   bridgeService,
	}
}

func (s *service) Run(parentCtx context.Context) error {
	group, ctx := errgroup.WithContext(parentCtx)

	group.Go(func() error { return s.p2pService.Run(ctx) })
	group.Go(func() error { return s.store.Run(ctx) })
	group.Go(func() error { return s.events.Run(ctx) })
	group.Go(func() error { return s.heimdallService.Run(ctx) })
	group.Go(func() error { return s.bridgeService.Run(ctx) })
	group.Go(func() error { return s.sync.Run(ctx) })

	return group.Wait()
}
