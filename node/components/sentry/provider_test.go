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
	"path/filepath"
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
	// t.TempDir() gives an OS-native path; build the expected suffix with
	// filepath.Join so the assertion holds on Windows (\) as well as Unix.
	dir := t.TempDir()
	p := &Provider{
		logger: log.Root(),
		cfg: Config{
			NodesDir: dir,
			P2P: p2p.Config{
				ListenAddr: ":30303",
			},
		},
	}
	cfg := p.buildSharedP2PConfig()
	require.Equal(t, filepath.Join(dir, "eth"), cfg.NodeDatabase)
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
