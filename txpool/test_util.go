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
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
)

//go:generate moq -stub -out mocks_test.go . Pool

type MockSentry struct {
	*sentry.SentryServerMock
	streams      map[sentry.MessageId][]sentry.Sentry_MessagesServer
	peersStreams []sentry.Sentry_PeersServer
	StreamWg     sync.WaitGroup
	ctx          context.Context
	lock         sync.RWMutex
}

func NewMockSentry(ctx context.Context) *MockSentry {
	return &MockSentry{ctx: ctx, SentryServerMock: &sentry.SentryServerMock{}}
}

var PeerId PeerID = gointerfaces.ConvertBytesToH512([]byte("12345"))

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
	return &sentry.SetStatusReply{Protocol: sentry.Protocol_ETH66}, nil
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

func (ms *MockSentry) Peers(req *sentry.PeersRequest, stream sentry.Sentry_PeersServer) error {
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

func toHashes(h ...[32]byte) (out Hashes) {
	for i := range h {
		out = append(out, h[i][:]...)
	}
	return out
}

func toPeerIDs(h ...int) (out []PeerID) {
	for i := range h {
		out = append(out, gointerfaces.ConvertBytesToH512([]byte(fmt.Sprintf("%x", h[i]))))
	}
	return out
}
