package rpc

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ledgerwatch/log/v3"
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
