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

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MockSentry struct {
}

func NewMockSentry() *MockSentry {
	return &MockSentry{}
}

func (m *MockSentry) PenalizePeer(ctx context.Context, in *sentry.PenalizePeerRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, nil
}

func (m *MockSentry) PeerMinBlock(ctx context.Context, in *sentry.PeerMinBlockRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, nil
}

func (m *MockSentry) SendMessageByMinBlock(ctx context.Context, in *sentry.SendMessageByMinBlockRequest, opts ...grpc.CallOption) (*sentry.SentPeers, error) {
	return nil, nil
}

func (m *MockSentry) SendMessageById(ctx context.Context, in *sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentry.SentPeers, error) {
	return nil, nil
}

func (m *MockSentry) SendMessageToRandomPeers(ctx context.Context, in *sentry.SendMessageToRandomPeersRequest, opts ...grpc.CallOption) (*sentry.SentPeers, error) {
	return nil, nil
}

func (m *MockSentry) SendMessageToAll(ctx context.Context, in *sentry.OutboundMessageData, opts ...grpc.CallOption) (*sentry.SentPeers, error) {
	return nil, nil
}

func (m *MockSentry) SetStatus(ctx context.Context, in *sentry.StatusData, opts ...grpc.CallOption) (*sentry.SetStatusReply, error) {
	return nil, nil
}

// Subscribe to receive messages.
// Calling multiple times with a different set of ids starts separate streams.
// It is possible to subscribe to the same set if ids more than once.
func (m *MockSentry) Messages(ctx context.Context, in *sentry.MessagesRequest, opts ...grpc.CallOption) (sentry.Sentry_MessagesClient, error) {
	return nil, nil
}

func (m *MockSentry) PeerCount(ctx context.Context, in *sentry.PeerCountRequest, opts ...grpc.CallOption) (*sentry.PeerCountReply, error) {
	return nil, nil
}

func (m *MockSentry) Peers(ctx context.Context, in *sentry.PeersRequest, opts ...grpc.CallOption) (sentry.Sentry_PeersClient, error) {
	return nil, nil
}

type MockSentryMessageClient struct {
	grpc.ClientStream
}

func (m *MockSentryMessageClient) 