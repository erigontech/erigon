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

package main

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/rpc"
)

// l1Configs returns the configuration of an L1 node of chain id, as the
// command line builds it.
func l1Configs(t *testing.T, id uint64) (*nodecfg.Config, *ethconfig.Config) {
	t.Helper()
	n := nodecfg.DefaultConfig
	n.Dirs = datadir.New(t.TempDir())
	n.P2P.MaxPeers = 32
	n.PrivateApiAddr = "127.0.0.1:9090"
	n.Http.HttpPort, n.Http.WebsocketPort = 8545, 8546
	n.Http.HttpURL, n.Http.HttpsURL = "tcp://127.0.0.1:8545", "tcp://127.0.0.1:8908"
	n.Http.HttpsServerEnabled = true
	n.Http.API = []string{"eth", "erigon", "engine"}
	n.Http.Dirs = n.Dirs
	e := ethconfig.Defaults
	e.Dirs = n.Dirs
	e.Genesis = &types.Genesis{Config: &chain.Config{ChainID: uint256.NewInt(id)}}
	e.NetworkID = id
	e.Snapshot.ChainName = "l1"
	e.Downloader = &downloadercfg.Cfg{}
	e.InternalCL = true
	e.MCPAddress = "127.0.0.1:8553"
	e.Ethstats = "node:secret@stats"
	e.ExtraAPIs = []rpc.API{{Namespace: "l1"}}
	return &n, &e
}

// TestAllInOneTracer: the EEZ chain of an all-in-one node traces with its own
// instance of the --vmtrace tracer, never the L1's.
func TestAllInOneTracer(t *testing.T) {
	for _, args := range [][]string{{"--vmtrace=4byteTracer"}, nil} {
		cmd := &cli.Command{Name: "erigon", Flags: debug.Flags, Action: func(_ context.Context, cmd *cli.Command) error {
			l1, err := debug.SetupTracerCtx(cmd)
			require.NoError(t, err)
			l2, err := allInOneTracer(cmd)
			require.NoError(t, err)
			if args == nil {
				require.Nil(t, l1)
				require.Nil(t, l2)
				return nil
			}
			require.NotNil(t, l2)
			require.NotSame(t, l1, l2)
			require.NotSame(t, l1.Hooks, l2.Hooks)
			return nil
		}}
		require.NoError(t, cmd.Run(t.Context(), append([]string{"erigon"}, args...)))
	}
}

// TestAllInOneConfigs: the EEZ chain of an all-in-one node takes the L1's
// settings but none of its chain, directories, ports or components, and
// leaves the L1 configuration as it was.
func TestAllInOneConfigs(t *testing.T) {
	eez.Register()
	l2 := networks.Devnet.Config.L2.(*config.L2Config)
	cfg := &eez.Config{Chain: networks.Devnet.Name, HTTPPort: 8645, HTTPAPI: []string{"eth", "debug", "eez"}}
	l1Node, l1Eth := l1Configs(t, l2.L1ChainID)
	l1NodeBefore, l1EthBefore := *l1Node, *l1Eth

	n, e, err := allInOneConfigs(l1Node, l1Eth, cfg)
	require.NoError(t, err)
	require.Equal(t, l1NodeBefore, *l1Node)
	require.Equal(t, l1EthBefore, *l1Eth)

	require.Equal(t, eez.AllInOneDirs(l1Node.Dirs), n.Dirs)
	require.Equal(t, n.Dirs, e.Dirs)
	require.Equal(t, n.Dirs, n.Http.Dirs)
	require.True(t, n.DisableSentry)
	require.Zero(t, n.P2P.MaxPeers)
	require.Empty(t, n.PrivateApiAddr)
	require.Equal(t, 8645, n.Http.HttpPort)
	require.Equal(t, 8645, n.Http.WebsocketPort)
	require.Empty(t, n.Http.HttpURL)
	require.Empty(t, n.Http.HttpsURL)
	require.False(t, n.Http.HttpsServerEnabled)
	require.Equal(t, cfg.HTTPAPI, n.Http.API)

	require.Same(t, networks.Devnet.Genesis, e.Genesis)
	require.Equal(t, networks.Devnet.Config.ChainID.Uint64(), e.NetworkID)
	require.Equal(t, networks.Devnet.Name, e.Snapshot.ChainName)
	require.Equal(t, e.Snapshot, n.Http.Snap)
	require.True(t, e.Snapshot.NoDownloader)
	require.Nil(t, e.Downloader)
	require.False(t, e.InternalCL)
	require.True(t, e.NoEngineAPI)
	require.True(t, e.TxPool.Disable)
	require.Empty(t, e.MCPAddress)
	require.Empty(t, e.Ethstats)
	require.Empty(t, e.ExtraAPIs)

	for name, mutate := range map[string]func(*eez.Config, *ethconfig.Config){
		"testing API":   func(c *eez.Config, _ *ethconfig.Config) { c.HTTPAPI = []string{"eth", "testing"} },
		"unknown chain": func(c *eez.Config, _ *ethconfig.Config) { c.Chain = "eez-nowhere" },
		"L1 chain":      func(c *eez.Config, _ *ethconfig.Config) { c.Chain = "mainnet" },
		"EEZ as L1":     func(_ *eez.Config, e *ethconfig.Config) { e.Genesis = networks.Devnet.Genesis },
	} {
		c := *cfg
		l1Node, l1Eth := l1Configs(t, l2.L1ChainID)
		mutate(&c, l1Eth)
		_, _, err := allInOneConfigs(l1Node, l1Eth, &c)
		require.ErrorIs(t, err, eez.ErrConfig, name)
	}
}
