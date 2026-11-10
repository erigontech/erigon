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
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	erigonnode "github.com/erigontech/erigon/cmd/erigon/node"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	erigoncli "github.com/erigontech/erigon/node/cli"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
)

var (
	bootnode = "enode://a972e9d344f799985aad905fe9e460a5e11c6507a30f37e85ad508af27f8da6e1a641305466947462f782db70c08c3a9931e6f5f3c3891ff54c4a07d72e40155@10.0.0.1:30303"
	static   = "enode://d860a01f9722d78051619d1e2351aba3f43f943f6f00718d1b9baa4101932a1f5011f16bb2b1bb35db20d6fe28fa0bf09636d26a87d31de9ec6203eeedb1f666@10.0.0.2:30303"
	trusted  = "enode://22a8232c3abc76a16ae9d6c3b164f98775fe226f0917b0ca871128a74a8e9630b458460865bab457221f1d448dd9791d24c4e5d88786180ac185df813a68d4de@10.0.0.3:30303"
)

// TestConfigureFollowerPeers: the follower of an R0 chain takes L2 peers
// over eth/68 and eth/69 and dials its trusted peers; discovery without
// bootnodes or a DNS tree is refused. A RollupX chain takes no peers
// whatever the flags.
func TestConfigureFollowerPeers(t *testing.T) {
	eez.Register()
	r0, rollupX := networks.Devnet.Config, networks.Chiado699169.Config
	boot, s, tr := enode.MustParse(bootnode), enode.MustParse(static), enode.MustParse(trusted)
	protocols := []uint{direct.ETH68, direct.ETH69}
	defaults := func() p2p.Config { return nodecfg.DefaultConfig.P2P }

	p := defaults()
	p.BootstrapNodes, p.BootstrapNodesV5 = []*enode.Node{boot}, []*enode.Node{boot}
	require.NoError(t, configureFollowerPeers(&p, nil, r0))
	require.False(t, p.NoDiscovery)
	require.Equal(t, nodecfg.DefaultConfig.P2P.MaxPeers, p.MaxPeers)
	require.Equal(t, protocols, p.ProtocolVersion)

	p = defaults()
	require.NoError(t, configureFollowerPeers(&p, []string{"enrtree://AM5FCQLWIZX2QFPNJAP7VUERCCRNGRHWZG3YYHIUV7BVDQ5FDPRT2@nodes.example.org"}, r0))

	p = defaults()
	p.NoDiscovery = true
	p.StaticNodes, p.TrustedNodes = []*enode.Node{s}, []*enode.Node{s, tr}
	require.NoError(t, configureFollowerPeers(&p, nil, r0))
	require.Equal(t, []*enode.Node{s, tr}, p.StaticNodes, "trusted peers dialled")
	require.Equal(t, []*enode.Node{s, tr}, p.TrustedNodes)
	require.Equal(t, protocols, p.ProtocolVersion)

	for name, chainConfig := range map[string]*chain.Config{"R0": r0, "unknown chain": nil} {
		p = defaults()
		p.StaticNodes = []*enode.Node{s}
		err := configureFollowerPeers(&p, nil, chainConfig)
		require.ErrorIs(t, err, eez.ErrConfig, name)
		require.ErrorContains(t, err, "--bootnodes", name)
	}

	p = defaults()
	p.BootstrapNodes, p.BootstrapNodesV5 = []*enode.Node{boot}, []*enode.Node{boot}
	p.StaticNodes, p.TrustedNodes = []*enode.Node{s}, []*enode.Node{tr}
	require.NoError(t, configureFollowerPeers(&p, nil, rollupX))
	require.Zero(t, p.MaxPeers)
	require.True(t, p.NoDiscovery)
	require.True(t, p.NoDial)
	require.Empty(t, p.StaticNodes)
	require.Empty(t, p.TrustedNodes)
}

// freshFlags copies flags, which keep what a parse set.
func freshFlags(flags []cli.Flag) []cli.Flag {
	out := make([]cli.Flag, len(flags))
	for i, f := range flags {
		v := reflect.ValueOf(f).Elem()
		c := reflect.New(v.Type())
		c.Elem().Set(v)
		out[i] = c.Interface().(cli.Flag)
	}
	return out
}

// followerConfigs returns the configuration runErigon builds for a
// standalone follower in dir from args.
func followerConfigs(t *testing.T, dir string, args ...string) (*nodecfg.Config, *ethconfig.Config, error) {
	t.Helper()
	eez.Register()
	var n *nodecfg.Config
	var e *ethconfig.Config
	var err error
	cmd := &cli.Command{Name: "erigon", Flags: freshFlags(slices.Concat(erigoncli.DefaultFlags, eez.Flags)), Action: func(ctx context.Context, cmd *cli.Command) error {
		n, err = erigonnode.NewNodConfigUrfave(cmd, nil, log.New())
		require.NoError(t, err)
		require.NoError(t, configureFollowerNode(n))
		e = erigonnode.NewEthConfigUrfave(ctx, cmd, n, log.New())
		err = configureFollowerNetwork(ctx, cmd, n, e, log.New())
		return nil
	}}
	require.NoError(t, cmd.Run(t.Context(), append([]string{"erigon", "--eez.enabled", "--datadir", dir}, args...)))
	return n, e, err
}

// initialised returns a datadir whose chain database holds spec's genesis
// and chain config, as init writes them from a genesis file.
func initialised(t *testing.T, spec chainspec.Spec) string {
	t.Helper()
	eez.Register()
	dir := t.TempDir()
	db, err := mdbx.New(dbcfg.ChainDB, log.New()).Path(eez.Dirs(datadir.New(dir)).Chaindata).Open(t.Context())
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := rawdb.WriteCanonicalHash(tx, spec.GenesisHash, 0); err != nil {
			return err
		}
		return rawdb.WriteChainConfig(tx, spec.GenesisHash, spec.Config)
	}))
	return dir
}

// TestConfigureFollowerNetwork: a follower runs on the network of the chain
// its datadir holds. A chain initialised from a genesis file runs under the
// default --chain, mainnet, and takes none of its network id, bootnodes or
// DNS tree unless set on the command line; a chain whose --chain spec has
// its genesis keeps the spec's. RollupX chains take no peers whatever the
// flags.
func TestConfigureFollowerNetwork(t *testing.T) {
	devnet := networks.Devnet.Config.ChainID.Uint64()
	file := initialised(t, networks.Devnet)

	n, e, err := followerConfigs(t, file, "--nodiscover", "--staticpeers", static, "--trustedpeers", trusted)
	require.NoError(t, err)
	require.Equal(t, devnet, e.NetworkID)
	require.Empty(t, n.P2P.BootstrapNodes)
	require.Empty(t, n.P2P.BootstrapNodesV5)
	require.Empty(t, e.EthDiscoveryURLs)
	require.Equal(t, []uint{direct.ETH68, direct.ETH69}, n.P2P.ProtocolVersion)
	require.Equal(t, []*enode.Node{enode.MustParse(static), enode.MustParse(trusted)}, n.P2P.StaticNodes)

	_, _, err = followerConfigs(t, file)
	require.ErrorIs(t, err, eez.ErrConfig, "mainnet's bootnodes dropped")

	n, _, err = followerConfigs(t, file, "--bootnodes", bootnode)
	require.NoError(t, err)
	require.Equal(t, []*enode.Node{enode.MustParse(bootnode)}, n.P2P.BootstrapNodes)

	dns := "enrtree://AM5FCQLWIZX2QFPNJAP7VUERCCRNGRHWZG3YYHIUV7BVDQ5FDPRT2@nodes.example.org"
	_, e, err = followerConfigs(t, file, "--discovery.dns", dns)
	require.NoError(t, err)
	require.Equal(t, []string{dns}, e.EthDiscoveryURLs)

	_, e, err = followerConfigs(t, file, "--nodiscover", "--networkid", "77")
	require.NoError(t, err)
	require.Equal(t, uint64(77), e.NetworkID)

	_, e, err = followerConfigs(t, t.TempDir(), "--chain", networks.Devnet.Name, "--nodiscover")
	require.NoError(t, err)
	require.Equal(t, devnet, e.NetworkID)

	listed := networks.Devnet
	listed.Name, listed.Bootnodes, listed.DNSNetwork, listed.NetworkID = "eez-devnet-listed", []string{bootnode}, dns, 4242
	chainspec.RegisterChainSpec(listed.Name, listed)
	n, e, err = followerConfigs(t, t.TempDir(), "--chain", listed.Name)
	require.NoError(t, err, "discovery from the spec's bootnodes")
	require.Equal(t, uint64(4242), e.NetworkID)
	require.Equal(t, []*enode.Node{enode.MustParse(bootnode)}, n.P2P.BootstrapNodes)
	require.Equal(t, []string{dns}, e.EthDiscoveryURLs)

	for _, dir := range []string{t.TempDir(), initialised(t, networks.Chiado699169)} {
		n, e, err = followerConfigs(t, dir, "--chain", networks.Chiado699169.Name, "--staticpeers", static, "--trustedpeers", trusted, "--bootnodes", bootnode)
		require.NoError(t, err)
		require.Zero(t, n.P2P.MaxPeers)
		require.True(t, n.P2P.NoDiscovery)
		require.True(t, n.P2P.NoDial)
		require.Empty(t, n.P2P.StaticNodes)
		require.Empty(t, n.P2P.TrustedNodes)
		require.Equal(t, networks.Chiado699169.Config.ChainID.Uint64(), e.NetworkID)
	}
	n, _, err = followerConfigs(t, initialised(t, networks.Chiado699169))
	require.NoError(t, err, "a RollupX genesis file under the default --chain")
	require.True(t, n.P2P.NoDial)
}

// TestAllInOneNetwork: the EEZ chain of an all-in-one node runs its own
// sentry once an --eez.* devp2p flag is set: on --eez.port, with a node key
// generated once in <datadir>/eez/nodekey and its node database under
// <datadir>/eez, discovery only from --eez.bootnodes, trusted peers dialled,
// the L1's node name, NAT, netrestrict and metrics, Erigon's defaults
// otherwise, and none of the L1's port, key, peers, discovery, protocols or
// external sentry. Without those flags, and on a RollupX chain, it runs no
// sentry. --eez.port must differ from the L1's --port. A chain initialised
// from a genesis file takes its chain id as network id.
func TestAllInOneNetwork(t *testing.T) {
	eez.Register()
	boot, s, tr := enode.MustParse(bootnode), enode.MustParse(static), enode.MustParse(trusted)
	l1Only := true
	// Spare capacity: the node's peer list must not share the flags' array.
	cfg := &eez.Config{
		Chain: networks.Devnet.Name, HTTPPort: 8645, SyncL1Only: &l1Only,
		StaticPeers: append(make([]*enode.Node, 0, 4), s), TrustedPeers: []*enode.Node{tr},
	}
	l1Node, l1Eth := l1Configs(t, 1337)
	keyFile := filepath.Join(l1Node.Dirs.DataDir, "eez", "nodekey")

	n, e, err := allInOneConfigs(t.Context(), l1Node, l1Eth, cfg, log.New())
	require.NoError(t, err)
	require.False(t, n.DisableSentry)
	require.Equal(t, ":30403", n.P2P.ListenAddr, "default port")
	key, err := crypto.LoadECDSA(keyFile)
	require.NoError(t, err)
	require.True(t, key.Equal(n.P2P.PrivateKey))
	require.False(t, key.Equal(l1Node.P2P.PrivateKey))
	require.Equal(t, filepath.Join(l1Node.Dirs.DataDir, "eez", "nodes"), n.Dirs.Nodes)
	require.Equal(t, n.Dirs.Tmp, n.P2P.TmpDir)
	require.True(t, n.P2P.NoDiscovery)
	require.Empty(t, n.P2P.BootstrapNodes)
	require.Empty(t, n.P2P.BootstrapNodesV5)
	require.Equal(t, []*enode.Node{s, tr}, n.P2P.StaticNodes, "trusted peers dialled")
	require.Equal(t, []*enode.Node{tr}, n.P2P.TrustedNodes)
	require.Equal(t, []*enode.Node{s}, cfg.StaticPeers)
	require.Equal(t, []uint{direct.ETH68, direct.ETH69}, n.P2P.ProtocolVersion)
	require.Equal(t, nodecfg.DefaultConfig.P2P.MaxPeers, n.P2P.MaxPeers)
	require.Empty(t, n.P2P.SentryAddr)
	require.Equal(t, l1Node.P2P.Name, n.P2P.Name)
	require.Equal(t, l1Node.P2P.NAT, n.P2P.NAT)
	require.Equal(t, l1Node.P2P.NATSpec, n.P2P.NATSpec)
	require.Same(t, l1Node.P2P.NetRestrict, n.P2P.NetRestrict)
	require.True(t, n.P2P.MetricsEnabled)
	require.Equal(t, nodecfg.DefaultConfig.P2P.DiscoveryV4, n.P2P.DiscoveryV4)
	require.Equal(t, nodecfg.DefaultConfig.P2P.DiscoveryV5, n.P2P.DiscoveryV5)
	require.NotEqual(t, l1Node.P2P.DiscoveryV5, n.P2P.DiscoveryV5)
	require.Empty(t, e.EthDiscoveryURLs)
	require.Equal(t, networks.Devnet.Config.ChainID.Uint64(), e.NetworkID)
	n.P2P.StaticNodes[0] = boot
	require.Equal(t, []*enode.Node{s}, cfg.StaticPeers, "the node's peers alias the flags")

	n, _, err = allInOneConfigs(t.Context(), l1Node, l1Eth, cfg, log.New())
	require.NoError(t, err)
	require.True(t, key.Equal(n.P2P.PrivateKey), "the key is kept")

	port := eez.Config{Chain: networks.Devnet.Name, SyncL1Only: &l1Only, Port: 30404}
	n, _, err = allInOneConfigs(t.Context(), l1Node, l1Eth, &port, log.New())
	require.NoError(t, err)
	require.False(t, n.DisableSentry, "--eez.port alone")
	require.Equal(t, ":30404", n.P2P.ListenAddr)
	port.Port = l1Node.P2P.ListenPort()
	_, _, err = allInOneConfigs(t.Context(), l1Node, l1Eth, &port, log.New())
	require.ErrorIs(t, err, eez.ErrConfig)
	require.ErrorContains(t, err, "--eez.port 30303 is the L1's --port")

	l1Node, l1Eth = l1Configs(t, 1337)
	n, _, err = allInOneConfigs(t.Context(), l1Node, l1Eth, &eez.Config{Chain: networks.Devnet.Name, SyncL1Only: &l1Only}, log.New())
	require.NoError(t, err)
	require.True(t, n.DisableSentry, "no --eez.* devp2p flag")
	require.NoFileExists(t, filepath.Join(l1Node.Dirs.DataDir, "eez", "nodekey"))

	discovery := *cfg
	discovery.Bootnodes = []*enode.Node{boot}
	n, _, err = allInOneConfigs(t.Context(), l1Node, l1Eth, &discovery, log.New())
	require.NoError(t, err)
	require.False(t, n.P2P.NoDiscovery)
	require.Equal(t, []*enode.Node{boot}, n.P2P.BootstrapNodes)
	require.Equal(t, []*enode.Node{boot}, n.P2P.BootstrapNodesV5)

	rollupX := discovery
	rollupX.Chain = networks.Chiado699169.Name
	for name, dir := range map[string]string{"spec": "", "stored": initialised(t, networks.Chiado699169)} {
		l1Node, l1Eth := l1Configs(t, networks.Chiado699169.Config.L2.(*config.L2Config).L1ChainID)
		c := rollupX
		if dir != "" {
			l1Node.Dirs, c.Chain = datadir.New(dir), networks.Devnet.Name
		}
		n, _, err := allInOneConfigs(t.Context(), l1Node, l1Eth, &c, log.New())
		require.NoError(t, err, name)
		require.True(t, n.DisableSentry, name)
		require.Zero(t, n.P2P.MaxPeers, name)
		require.NoFileExists(t, filepath.Join(l1Node.Dirs.DataDir, "eez", "nodekey"), name)
	}

	file := networks.Devnet
	file.GenesisHash = common.HexToHash("0x01")
	file.Config = networks.Devnet.Config.Copy()
	file.Config.ChainID = uint256.NewInt(4242)
	l1Node, l1Eth = l1Configs(t, 1337)
	l1Node.Dirs = datadir.New(initialised(t, file))
	n, e, err = allInOneConfigs(t.Context(), l1Node, l1Eth, cfg, log.New())
	require.NoError(t, err)
	require.Equal(t, uint64(4242), e.NetworkID)
	require.False(t, n.DisableSentry)
}
