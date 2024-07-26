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

package rpc

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
)

// This test checks whether calls exceeding the request size limit are rejected.
func BenchmarkWebsocketEmptyCall(b *testing.B) {
	logger := log.New()

	var (
		srv     = newTestServer(logger)
		httpsrv = httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
		wsURL   = "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")
	)
	defer srv.Stop()
	defer httpsrv.Close()

	client, err := DialWebsocket(context.Background(), wsURL, "", logger)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Call(nil, "test_ping"); err != nil {
			panic(err)
		}
	}
}
func BenchmarkWebsocket16kb(b *testing.B) {
	logger := log.New()

	var (
		srv     = newTestServer(logger)
		httpsrv = httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
		wsURL   = "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")
	)
	defer srv.Stop()
	defer httpsrv.Close()

	client, err := DialWebsocket(context.Background(), wsURL, "", logger)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	payload16kb := strings.Repeat("x", 4096*4)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := client.Call(nil, "test_echo", payload16kb, 5, nil)
		if err != nil {
			panic(err)
		}
	}
}
