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

package libsentry_test

import (
	"context"
	"strconv"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

type peersStub struct {
	sentryproto.SentryClient
	reply *sentryproto.PeersReply
}

func (c *peersStub) Peers(context.Context, *emptypb.Empty, ...grpc.CallOption) (*sentryproto.PeersReply, error) {
	return c.reply, nil
}

func BenchmarkMultiplexerPeers(b *testing.B) {
	const clients, peersPerClient = 8, 250
	stubs := make([]sentryproto.SentryClient, clients)
	for i := range stubs {
		peers := make([]*typesproto.PeerInfo, peersPerClient)
		for j := range peers {
			peers[j] = &typesproto.PeerInfo{Id: strconv.Itoa(i*peersPerClient + j)}
		}
		stubs[i] = &peersStub{reply: &sentryproto.PeersReply{Peers: peers}}
	}
	mux := libsentry.NewSentryMultiplexer(stubs)
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if _, err := mux.Peers(ctx, &emptypb.Empty{}); err != nil {
			b.Fatal(err)
		}
	}
}
