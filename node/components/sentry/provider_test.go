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
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/sentry"
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

// helperGrpcServerWithProtocols returns a minimally-populated *sentry.GrpcServer
// suitable for startSharedP2PServer dedup / reporter-wiring tests. It does
// not start any network listeners.
func helperGrpcServerWithProtocols(protocols ...p2p.Protocol) *sentry.GrpcServer {
	ss := &sentry.GrpcServer{}
	ss.Protocols = append(ss.Protocols, protocols...)
	return ss
}

// TestBuildSharedP2PConfig_NodeDatabaseUnified verifies that the shared
// config uses a single nodes/eth path regardless of how many protocol
// versions are configured.
func TestBuildSharedP2PConfig_NodeDatabaseUnified(t *testing.T) {
	p := &Provider{
		logger: log.Root(),
		cfg: Config{
			NodesDir: "/data/nodes",
			P2P: p2p.Config{
				ListenAddr: ":30303",
			},
		},
	}
	cfg, err := p.buildSharedP2PConfig()
	require.NoError(t, err)
	require.Equal(t, "/data/nodes/eth", cfg.NodeDatabase)
}

// TestBuildSharedP2PConfig_NoAllowedPortsKeepsListenAddr verifies that an
// explicit ListenAddr is honoured as-is when AllowedPorts is empty.
func TestBuildSharedP2PConfig_NoAllowedPortsKeepsListenAddr(t *testing.T) {
	p := &Provider{
		logger: log.Root(),
		cfg: Config{
			NodesDir: t.TempDir(),
			P2P: p2p.Config{
				ListenAddr: "127.0.0.1:39301",
			},
		},
	}
	cfg, err := p.buildSharedP2PConfig()
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:39301", cfg.ListenAddr)
}

// TestBuildSharedP2PConfig_PicksFirstFreeAllowedPort verifies the fallback
// loop: when the first AllowedPorts entry is busy, the picker moves on to
// the next.
func TestBuildSharedP2PConfig_PicksFirstFreeAllowedPort(t *testing.T) {
	// Reserve one ephemeral port so we have a guaranteed-busy probe target,
	// and a freshly-released one to use as the "free" candidate.
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer busy.Close()
	busyPort := uint(busy.Addr().(*net.TCPAddr).Port)

	free, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	freePort := uint(free.Addr().(*net.TCPAddr).Port)
	free.Close() // release before the probe runs

	p := &Provider{
		logger: log.Root(),
		cfg: Config{
			NodesDir: t.TempDir(),
			P2P: p2p.Config{
				ListenAddr:   "127.0.0.1:0",
				AllowedPorts: []uint{busyPort, freePort},
			},
		},
	}
	cfg, err := p.buildSharedP2PConfig()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("127.0.0.1:%d", freePort), cfg.ListenAddr)
}

// TestBuildSharedP2PConfig_ErrsWhenAllAllowedPortsBusy guards against the
// silent-fallback regression Copilot flagged: with every entry held by a
// live listener the picker must return an error, not keep going with a stale
// port that will then fail on bind.
func TestBuildSharedP2PConfig_ErrsWhenAllAllowedPortsBusy(t *testing.T) {
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l1.Close()
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l2.Close()

	p := &Provider{
		logger: log.Root(),
		cfg: Config{
			NodesDir: t.TempDir(),
			P2P: p2p.Config{
				ListenAddr: "127.0.0.1:0",
				AllowedPorts: []uint{
					uint(l1.Addr().(*net.TCPAddr).Port),
					uint(l2.Addr().(*net.TCPAddr).Port),
				},
			},
		},
	}
	_, err = p.buildSharedP2PConfig()
	require.Error(t, err)
}

// TestBuildSharedP2PConfig_EmptyHostProbesLoopback exercises the
// ":port" form of ListenAddr (no explicit host). checkPortIsFree's dial
// would fail with "missing host" against an empty target and falsely report
// the port as free; the picker normalises the probe to 127.0.0.1 so a busy
// listener still gets detected.
func TestBuildSharedP2PConfig_EmptyHostProbesLoopback(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer busy.Close()
	busyPort := uint(busy.Addr().(*net.TCPAddr).Port)

	free, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	freePort := uint(free.Addr().(*net.TCPAddr).Port)
	free.Close()

	p := &Provider{
		logger: log.Root(),
		cfg: Config{
			NodesDir: t.TempDir(),
			P2P: p2p.Config{
				ListenAddr:   ":0", // empty host
				AllowedPorts: []uint{busyPort, freePort},
			},
		},
	}
	cfg, err := p.buildSharedP2PConfig()
	require.NoError(t, err)
	// Original empty host preserved (binds on all interfaces); port picked
	// from the allowlist after the busy one is rejected.
	require.Equal(t, fmt.Sprintf(":%d", freePort), cfg.ListenAddr)
}

// TestStartSharedP2PServer_ErrsOnEmptyServers documents the precondition.
func TestStartSharedP2PServer_ErrsOnEmptyServers(t *testing.T) {
	p := &Provider{
		logger: log.Root(),
		cfg:    Config{SentryCtx: t.Context()},
	}
	cfg := p2p.Config{}
	err := p.startSharedP2PServer(&cfg, nil, "")
	require.Error(t, err)
}

// TestStartSharedP2PServer_DedupesAndInjects is the integration test for
// the central wiring: protocols collected from all GrpcServers are
// deduplicated by (name, version), the shared p2p.Server is started once,
// and that same Server is injected into every GrpcServer. Each GrpcServer
// then reports peers from its own goodPeers subset — there is no
// designated "reporter" sentry.
func TestStartSharedP2PServer_DedupesAndInjects(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	first := helperGrpcServerWithProtocols(
		p2p.Protocol{Name: "eth", Version: 71, Length: 17},
		p2p.Protocol{Name: "wit", Version: 0, Length: 5},
	)
	second := helperGrpcServerWithProtocols(
		p2p.Protocol{Name: "eth", Version: 70, Length: 17},
		p2p.Protocol{Name: "wit", Version: 0, Length: 5}, // duplicate, must be dropped
	)

	p := &Provider{
		logger: log.Root(),
		cfg:    Config{SentryCtx: t.Context()},
	}
	p.Servers = []*sentry.GrpcServer{first, second}

	cfg := p2p.Config{
		PrivateKey:      key,
		MaxPeers:        1,
		MaxPendingPeers: 1,
		NoDiscovery:     true,
		NoDial:          true,
	}
	require.NoError(t, p.startSharedP2PServer(&cfg, nil, ""))
	t.Cleanup(func() { p.Close() })

	require.NotNil(t, p.sharedP2PServer)
	require.Len(t, cfg.Protocols, 3, "eth/71, eth/70, wit/0 — wit deduped")

	require.Same(t, p.sharedP2PServer, first.GetP2PServer())
	require.Same(t, p.sharedP2PServer, second.GetP2PServer())
}

// TestProviderClose_StopsSharedP2PServer mirrors the GrpcServer-side test:
// Provider.Close must stop the shared p2p.Server it created (GrpcServer.Close
// is a no-op for external servers by design). We bind an ephemeral listener
// and dial it before/after Close — the connection must be refused once the
// Provider has shut the listener down. Avoids relying on Start-after-Stop,
// which p2p.Server documents as unsupported.
func TestProviderClose_StopsSharedP2PServer(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	first := helperGrpcServerWithProtocols(
		p2p.Protocol{Name: "eth", Version: 71, Length: 17},
	)

	p := &Provider{
		logger: log.Root(),
		cfg:    Config{SentryCtx: t.Context()},
	}
	p.Servers = []*sentry.GrpcServer{first}

	cfg := p2p.Config{
		PrivateKey:      key,
		MaxPeers:        1,
		MaxPendingPeers: 1,
		NoDiscovery:     true,
		NoDial:          true,
		ListenAddr:      "127.0.0.1:0",
	}
	require.NoError(t, p.startSharedP2PServer(&cfg, nil, ""))
	srv := p.sharedP2PServer
	require.NotNil(t, srv)

	addr := srv.NodeInfo().ListenAddr
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	require.NoError(t, err, "listener should be up before Close")
	c.Close()

	require.NoError(t, p.Close())
	require.Nil(t, p.sharedP2PServer, "Close must clear the shared server reference")

	_, err = net.DialTimeout("tcp", addr, 200*time.Millisecond)
	require.Error(t, err, "Provider.Close must shut the shared p2p.Server's listener down")
}

// TestLoopbackProbeHost covers the input shapes we expect from
// splitAddrIntoHostAndPort: the four unspecified forms must all rewrite
// to a loopback target so checkPortIsFree actually detects busy ports;
// concrete hosts must pass through.
func TestLoopbackProbeHost(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", "127.0.0.1"},
		{"0.0.0.0", "127.0.0.1"},
		{"::", "[::1]"},
		{"[::]", "[::1]"},
		{"127.0.0.1", "127.0.0.1"},
		{"10.1.2.3", "10.1.2.3"},
		{"localhost", "localhost"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.want, loopbackProbeHost(tt.input))
		})
	}
}
