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

package caplin1

import (
	"context"
	"sync"

	clp2p "github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
)

// NetworkSession owns the Caplin p2p stack for the life of the process: the
// libp2p host with its peer set, the pubsub router and the discv5 table.
//
// An unwind restarts the Caplin runtime, which takes seconds; rediscovering a
// full consensus peer set takes the better part of an hour, and a restart that
// dropped it left the node unable to backfill. The session is therefore built
// from the process context, never from a launch context, so no restart can
// cancel it.
type NetworkSession struct {
	ctx context.Context

	mu   sync.Mutex
	p2p  clp2p.P2PManager
	stop func()
}

func NewNetworkSession(ctx context.Context) *NetworkSession {
	return &NetworkSession{ctx: ctx}
}

// P2P returns the process's p2p manager, starting it on the first call. Only
// that call reads cfg, logger and ethClock: later runtimes join the stack the
// first one opened.
func (s *NetworkSession) P2P(cfg *clp2p.P2PConfig, logger log.Logger, ethClock eth_clock.EthereumClock) (clp2p.P2PManager, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.p2p != nil {
		logger.Info("[Caplin] reusing the p2p stack from the previous runtime",
			"peers", len(s.p2p.Host().Network().Peers()))
		return s.p2p, nil
	}
	manager, stop, err := clp2p.NewP2Pmanager(s.ctx, cfg, logger, ethClock)
	if err != nil {
		return nil, err
	}
	s.p2p, s.stop = manager, stop
	return s.p2p, nil
}

func (s *NetworkSession) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop != nil {
		s.stop()
		s.stop = nil
	}
	s.p2p = nil
}
