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

package bscsync

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func TestFollowTipHandsOverFromCatchUpToTipMode(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		network := fakeChain(1000)
		head := network[999]
		chainRW := newFakeChainRW(network[:101])
		reg := newFakeTipRegistrar()
		svc := &fakeP2P{peers: 2, chain: network, fakeTipRegistrar: reg}
		cfg := Config{ChainRW: chainRW, Svc: svc, FcuInterval: 10_000}
		logger := testlog.Logger(t, log.LvlCrit)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		te := NewTipEvents(logger, svc)
		go func() { _ = te.Run(ctx) }()
		for range 3 {
			<-reg.registered
		}
		ts := newTipSync(logger, chainRW, svc, newFakeNetwork(network).fetchBackwards, te, fakeParliaDB{chainRW}, newFakeVerifier())
		done := make(chan error, 1)
		go func() { done <- followTip(ctx, logger, cfg, ts) }()

		pkt := eth.NewBlockHashesPacket{{Hash: head.Hash(), Number: head.NumberU64()}}
		reg.sendHashes(1, pkt)
		reg.sendHashes(2, pkt)
		synctest.Wait()

		require.Equal(t, head.Hash(), chainRW.CurrentHeader(ctx).Hash())
		require.Equal(t, network[999-tipHandoverDistance].Hash(), chainRW.fcus[0].head)
		require.Equal(t, head.Hash(), chainRW.lastFcu().head)
		require.Len(t, chainRW.fcus, 2)
		require.Empty(t, done)
	})
}
