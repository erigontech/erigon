// Copyright 2021 The Erigon Authors
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
	"sync"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/gointerfaces"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
)

type MockSentry struct {
	ctx context.Context
	*sentry.MockSentryServer
	streams      map[sentry.MessageId][]sentry.Sentry_MessagesServer
	peersStreams []sentry.Sentry_PeerEventsServer
	StreamWg     sync.WaitGroup
	lock         sync.RWMutex
}

func NewMockSentry(ctx context.Context, sentryServer *sentry.MockSentryServer) *MockSentry {
	return &MockSentry{
		ctx:              ctx,
		MockSentryServer: sentryServer,
	}
}

var peerID PeerID = gointerfaces.ConvertHashToH512([64]byte{0x12, 0x34, 0x50}) // "12345"

func (ms *MockSentry) Send(req *sentry.InboundMessage) (errs []error) {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	for _, stream := range ms.streams[req.Id] {
		if err := stream.Send(req); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (ms *MockSentry) SetStatus(context.Context, *sentry.StatusData) (*sentry.SetStatusReply, error) {
	return &sentry.SetStatusReply{}, nil
}
func (ms *MockSentry) HandShake(context.Context, *emptypb.Empty) (*sentry.HandShakeReply, error) {
	return &sentry.HandShakeReply{Protocol: sentry.Protocol_ETH68}, nil
}
func (ms *MockSentry) Messages(req *sentry.MessagesRequest, stream sentry.Sentry_MessagesServer) error {
	ms.lock.Lock()
	if ms.streams == nil {
		ms.streams = map[sentry.MessageId][]sentry.Sentry_MessagesServer{}
	}
	for _, id := range req.Ids {
		ms.streams[id] = append(ms.streams[id], stream)
	}
	ms.lock.Unlock()
	ms.StreamWg.Done()
	select {
	case <-ms.ctx.Done():
		return nil
	case <-stream.Context().Done():
		return nil
	}
}

func (ms *MockSentry) PeerEvents(req *sentry.PeerEventsRequest, stream sentry.Sentry_PeerEventsServer) error {
	ms.lock.Lock()
	ms.peersStreams = append(ms.peersStreams, stream)
	ms.lock.Unlock()
	ms.StreamWg.Done()
	select {
	case <-ms.ctx.Done():
		return nil
	case <-stream.Context().Done():
		return nil
	}
}

func testRlps(num int) [][]byte {
	rlps := make([][]byte, num)
	for i := 0; i < num; i++ {
		rlps[i] = []byte{1}
	}
	return rlps
}

func toPeerIDs(h ...byte) (out []PeerID) {
	for i := range h {
		hash := [64]byte{h[i]}
		out = append(out, gointerfaces.ConvertHashToH512(hash))
	}
	return out
}
