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

package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/sentinel"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

func TestPeerRequestUsesCallerContext(t *testing.T) {
	requestCancelled := make(chan struct{})
	server := &SentinelServer{peerRequestBackend: peerRequestBackendStub{
		handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			<-request.Context().Done()
			close(requestCancelled)
		}),
	}}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := server.requestPeer(ctx, peer.ID("peer"), &sentinelproto.RequestData{Data: []byte("request")})
		done <- err
	}()

	cancel()
	require.ErrorIs(t, receivePeerRequestTestValue(t, done), context.Canceled)
	receivePeerRequestTestValue(t, requestCancelled)
}

type peerRequestBackendStub struct {
	handler http.Handler
}

func (p peerRequestBackendStub) GetPeersCount() (int, int, int) { return 0, 0, 0 }
func (p peerRequestBackendStub) Config() *sentinel.SentinelConfig {
	config := &sentinel.SentinelConfig{}
	config.MaxPeerCount = 1
	return config
}
func (p peerRequestBackendStub) ReqRespHandler() http.Handler { return p.handler }

func receivePeerRequestTestValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for peer request")
		var zero T
		return zero
	}
}
