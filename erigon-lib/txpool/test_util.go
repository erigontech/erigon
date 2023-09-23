/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/types"
	"google.golang.org/protobuf/types/known/emptypb"
)

//go:generate moq -stub -out mocks_test.go . Pool

type MockSentry struct {
	ctx context.Context
	*sentry.SentryServerMock
	streams      map[sentry.MessageId][]sentry.Sentry_MessagesServer
	peersStreams []sentry.Sentry_PeerEventsServer
	StreamWg     sync.WaitGroup
	lock         sync.RWMutex
}

func NewMockSentry(ctx context.Context) *MockSentry {
	return &MockSentry{ctx: ctx, SentryServerMock: &sentry.SentryServerMock{}}
}

var peerID types.PeerID = gointerfaces.ConvertHashToH512([64]byte{0x12, 0x34, 0x50}) // "12345"

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

func toHashes(h ...byte) (out types.Hashes) {
	for i := range h {
		hash := [32]byte{h[i]}
		out = append(out, hash[:]...)
	}
	return out
}

func testRlps(num int) [][]byte {
	rlps := make([][]byte, num)
	for i := 0; i < num; i++ {
		rlps[i] = []byte{1}
	}
	return rlps
}

func toPeerIDs(h ...byte) (out []types.PeerID) {
	for i := range h {
		hash := [64]byte{h[i]}
		out = append(out, gointerfaces.ConvertHashToH512(hash))
	}
	return out
}
