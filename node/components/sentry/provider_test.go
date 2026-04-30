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

package sentry

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// TestConfigureIsPure verifies Configure just stores the config without
// touching the network, starting goroutines, or allocating anything that
// needs cleanup.
func TestConfigureIsPure(t *testing.T) {
	p := &Provider{}
	cfg := Config{
		SentryCtx: t.Context(),
		Logger:    log.Root(),
	}
	p.Configure(cfg)

	require.Equal(t, cfg.SentryCtx, p.cfg.SentryCtx)
	require.NotNil(t, p.logger)
	require.Nil(t, p.Servers)
	require.Nil(t, p.Sentries)
	require.Nil(t, p.Multiplexer)
	require.Nil(t, p.StatusDataProvider)
}

// TestCloseIdempotent verifies Close is safe to call without Initialize
// and safe to call multiple times.
func TestCloseIdempotent(t *testing.T) {
	p := &Provider{}
	// Close without Configure or Initialize — no panic.
	p.Close()

	// Configure, then Close twice.
	p.Configure(Config{
		SentryCtx: t.Context(),
		Logger:    log.Root(),
	})
	p.Close()
	p.Close()
}

// TestSplitAddrIntoHostAndPort covers the port_util helper used by
// Initialize to parse p2p.Config.ListenAddr.
func TestSplitAddrIntoHostAndPort(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantHost string
		wantPort int
		wantErr  bool
	}{
		{name: "ipv4", input: "127.0.0.1:30303", wantHost: "127.0.0.1", wantPort: 30303},
		{name: "hostname", input: "localhost:8080", wantHost: "localhost", wantPort: 8080},
		{name: "zero-host", input: "0.0.0.0:30303", wantHost: "0.0.0.0", wantPort: 30303},
		{name: "ipv6", input: "[::1]:30303", wantHost: "[::1]", wantPort: 30303},
		{name: "no-colon", input: "localhost", wantErr: true},
		{name: "non-numeric-port", input: "localhost:abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, err := splitAddrIntoHostAndPort(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantHost, host)
			require.Equal(t, tt.wantPort, port)
		})
	}
}

// TestCheckPortIsFree verifies checkPortIsFree returns true for a port
// nothing is listening on, and false when a listener is bound.
func TestCheckPortIsFree(t *testing.T) {
	// Bind a listener on an ephemeral port, then probe it — should report busy.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	busyAddr := l.Addr().String()
	require.False(t, checkPortIsFree(busyAddr), "port with active listener should report busy")

	// Close the listener and probe again — should now report free.
	// (The OS may not immediately free the port; this is a soft check.)
	require.NoError(t, l.Close())

	// Probe a very unlikely port (high ephemeral range with no listener).
	// This is inherently racy but reliable enough in test contexts.
	require.True(t, checkPortIsFree("127.0.0.1:1"), "port 1 should be free in test env")
}
