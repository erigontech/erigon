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

package eez

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

func parse(t *testing.T, args ...string) (*Config, error) {
	t.Helper()
	var cfg *Config
	var err error
	cmd := &cli.Command{Name: "erigon", Flags: freshFlags(), Action: func(_ context.Context, cmd *cli.Command) error {
		cfg, err = ConfigFromCLI(cmd)
		return nil
	}}
	require.NoError(t, cmd.Run(t.Context(), append([]string{"erigon"}, args...)))
	return cfg, err
}

// freshFlags copies Flags, which keep what a parse set.
func freshFlags() []cli.Flag {
	out := make([]cli.Flag, len(Flags))
	for i, f := range Flags {
		switch f := f.(type) {
		case *cli.BoolFlag:
			c := *f
			out[i] = &c
		case *cli.StringFlag:
			c := *f
			out[i] = &c
		case *cli.DurationFlag:
			c := *f
			out[i] = &c
		case *cli.Uint64Flag:
			c := *f
			out[i] = &c
		case *cli.IntFlag:
			c := *f
			out[i] = &c
		default:
			panic(f)
		}
	}
	return out
}

func TestConfigFromCLI(t *testing.T) {
	cfg, err := parse(t)
	require.NoError(t, err)
	require.Nil(t, cfg, "EEZ off")

	cfg, err = parse(t, "--eez.enabled", "--eez.l1.kv=127.0.0.1:9090", "--eez.l1.blobs=/blobs")
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9090", cfg.L1KV)
	require.Equal(t, "/blobs", cfg.L1Blobs)
	require.NoError(t, cfg.CheckNode())

	cfg, err = parse(t, "--eez.enabled", "--eez.l1.rpc=http://l1:8545", "--eez.l1.poll=1s",
		"--eez.registry=0x5fbdb2315678afecb367f032d93f642f64180aa3", "--eez.rollupid=1", "--eez.l1.blocktime=12s", "--eez.l2.blocktime=2s")
	require.NoError(t, err)
	one, twelve, two := uint64(1), uint64(12), uint64(2)
	registry := common.HexToAddress("0x5fbdb2315678afecb367f032d93f642f64180aa3")
	require.Equal(t, &Config{
		L1RPC:        "http://l1:8545",
		L1Poll:       time.Second,
		L1LogChunk:   100_000,
		L1ReorgDepth: 62,
		L2:           L2Flags{Registry: &registry, RollupID: &one, L1BlockTime: &twelve, BlockInterval: &two},
	}, cfg)

	for _, args := range [][]string{
		{"--eez.l1.rpc=http://l1:8545"},
		{"--eez.enabled", "--eez.registry=0x12"},
		{"--eez.enabled", "--eez.l2.blocktime=1500ms"},
		{"--eez.enabled", "--eez.l1.reorgdepth=0"},
	} {
		_, err := parse(t, args...)
		require.ErrorIs(t, err, ErrConfig, "%v", args)
	}

	cfg, err = parse(t, "--eez.enabled", "--eez.chain=eez-devnet")
	require.NoError(t, err)
	require.Equal(t, "eez-devnet", cfg.Chain)
	require.Equal(t, 8645, cfg.HTTPPort)
	require.Equal(t, []string{"eth", "debug", "eez"}, cfg.HTTPAPI)
	require.True(t, cfg.AllInOne())
	require.NoError(t, cfg.CheckNode())
	cfg, err = parse(t, "--eez.enabled", "--eez.chain=eez-devnet", "--eez.http.port=9645", "--eez.http.api=eth,eez")
	require.NoError(t, err)
	require.Equal(t, 9645, cfg.HTTPPort)
	require.Equal(t, []string{"eth", "eez"}, cfg.HTTPAPI)
	cfg, err = parse(t, "--eez.enabled", "--eez.chain=eez-devnet", "--eez.http.api= eth, debug,,eez ")
	require.NoError(t, err)
	require.Equal(t, []string{"eth", "debug", "eez"}, cfg.HTTPAPI, "spaces and empty entries")
	_, err = parse(t, "--eez.enabled", "--eez.l1.rpc=http://l1:8545", "--eez.http.port=9645")
	require.ErrorIs(t, err, ErrConfig, "--eez.http.port without --eez.chain")

	// init needs no L1 source; a running node needs exactly one.
	for _, args := range [][]string{
		{"--eez.enabled"},
		{"--eez.enabled", "--eez.l1.rpc=http://l1:8545", "--eez.l1.kv=l1:9090"},
		{"--eez.enabled", "--eez.l1.rpc=http://l1:8545", "--eez.chain=eez-devnet"},
	} {
		cfg, err := parse(t, args...)
		require.NoError(t, err, "%v", args)
		require.ErrorIs(t, cfg.CheckNode(), ErrConfig, "%v", args)
	}
}

// TestSyncMode: --eez.sync.l1only picks where the history below L1's
// finalized root comes from. Unset, rollupX chains derive it from L1 alone
// and R0 chains take it from L2 peers, unless they run beside their L1.
// RollupX chains, and chains beside their L1, refuse L2 peers.
func TestSyncMode(t *testing.T) {
	r0, rollupX := networks.Devnet.Config, networks.Chiado699169.Config
	standalone := []string{"--eez.enabled", "--eez.l1.rpc=http://l1:8545"}
	beside := []string{"--eez.enabled", "--eez.chain=eez-devnet"}
	for _, c := range []struct {
		args  []string
		chain *chain.Config
		want  bool
	}{
		{standalone, r0, false},
		{standalone, rollupX, true},
		{append(standalone, "--eez.sync.l1only"), r0, true},
		{append(standalone, "--eez.sync.l1only=false"), r0, false},
		{append(standalone, "--eez.sync.l1only=true"), rollupX, true},
		{beside, r0, true},
		{append(beside, "--eez.sync.l1only"), r0, true},
	} {
		cfg, err := parse(t, c.args...)
		require.NoError(t, err, "%v", c.args)
		require.NoError(t, cfg.CheckNode(), "%v", c.args)
		got, err := cfg.L1Only(c.chain)
		require.NoError(t, err, "%v", c.args)
		require.Equal(t, c.want, got, "%v on %s", c.args, c.chain.ChainName)
	}

	cfg, err := parse(t, append(standalone, "--eez.sync.l1only=false")...)
	require.NoError(t, err)
	_, err = cfg.L1Only(rollupX)
	require.ErrorIs(t, err, ErrConfig, "L2 peers on a rollupX chain")
	cfg, err = parse(t, append(beside, "--eez.sync.l1only=false")...)
	require.NoError(t, err)
	require.ErrorIs(t, cfg.CheckNode(), ErrConfig, "L2 peers beside the L1")
	_, err = parse(t, "--eez.sync.l1only")
	require.ErrorIs(t, err, ErrConfig, "without --eez.enabled")
}

func TestOverlayFlags(t *testing.T) {
	const (
		listen   = "--eez.p2p.listen=/ip4/0.0.0.0/tcp/9100"
		address  = "--eez.sequencer.address=0x5fbdb2315678afecb367f032d93f642f64180aa3"
		peerID   = "16Uiu2HAm8kegYGp6XeybmZAuNcnLosyjsRwZ44yLgfEuqLqYL9zt"
		peerAddr = "/ip4/10.0.0.1/tcp/9100/p2p/" + peerID
	)
	follower := []string{"--eez.enabled", "--eez.l1.rpc=http://l1:8545"}

	cfg, err := parse(t, append(follower, listen, address, "--eez.p2p.peers="+peerAddr)...)
	require.NoError(t, err)
	require.Equal(t, "/ip4/0.0.0.0/tcp/9100", cfg.P2PListen.String())
	require.Len(t, cfg.P2PPeers, 1)
	require.Equal(t, peerID, cfg.P2PPeers[0].ID.String())
	require.Equal(t, "/ip4/10.0.0.1/tcp/9100", cfg.P2PPeers[0].Addrs[0].String())
	require.Equal(t, common.HexToAddress("0x5fbdb2315678afecb367f032d93f642f64180aa3"), *cfg.SequencerAddress)
	require.NoError(t, cfg.CheckNode())

	cfg, err = parse(t, follower...)
	require.NoError(t, err)
	require.Nil(t, cfg.P2PListen, "no overlay")
	require.Nil(t, cfg.SequencerAddress)

	for name, args := range map[string][]string{
		"listen without address":   {listen},
		"address without listen":   {address},
		"peers without listen":     {address, "--eez.p2p.peers=" + peerAddr},
		"with the sequencer RPC":   {listen, address, "--eez.sequencer.rpc=http://seq:8545"},
		"peers with sequencer RPC": {"--eez.p2p.peers=" + peerAddr, "--eez.sequencer.rpc=http://seq:8545"},
		"listen port 0":            {"--eez.p2p.listen=/ip4/0.0.0.0/tcp/0", address},
		"listen not TCP":           {"--eez.p2p.listen=/ip4/0.0.0.0/udp/9100", address},
		"peer without id":          {listen, address, "--eez.p2p.peers=/ip4/10.0.0.1/tcp/9100"},
		"DNS peer":                 {listen, address, "--eez.p2p.peers=/dns4/seq/tcp/9100/p2p/" + peerID},
		"bad address":              {listen, "--eez.sequencer.address=0x12"},
	} {
		_, err := parse(t, append(follower, args...)...)
		require.ErrorIs(t, err, ErrConfig, name)
	}

	_, err = parse(t, append(follower, "--eez.p2p.peers="+peerAddr)...)
	require.ErrorIs(t, err, ErrConfig)
	require.ErrorContains(t, err, "--eez.p2p.peers requires --eez.p2p.listen")

	// The EEZ chain of an all-in-one node has no overlay yet.
	for _, args := range [][]string{{listen, address}, {listen}, {address}, {"--eez.p2p.peers=" + peerAddr}} {
		_, err := parse(t, append([]string{"--eez.enabled", "--eez.chain=eez-devnet"}, args...)...)
		require.ErrorIs(t, err, ErrConfig, "all-in-one %v", args)
	}
}

func TestCheckChainRefusesOverlay(t *testing.T) {
	overlay, err := parse(t, "--eez.enabled", "--eez.l1.rpc=http://l1:8545",
		"--eez.p2p.listen=/ip4/0.0.0.0/tcp/9100", "--eez.sequencer.address=0x5fbdb2315678afecb367f032d93f642f64180aa3")
	require.NoError(t, err)
	plain, err := parse(t, "--eez.enabled", "--eez.l1.rpc=http://l1:8545")
	require.NoError(t, err)

	signedHeads := networks.Devnet.Genesis.Config
	require.Equal(t, config.Rollup0, signedHeads.L2.(*config.L2Config).RuleSet())
	require.NoError(t, overlay.CheckChain(signedHeads))
	require.NoError(t, plain.CheckChain(signedHeads))

	for _, ruleSet := range []*config.RollupXConfig{{}, {FeeCollector: &common.Address{1}}} {
		l2 := *signedHeads.L2.(*config.L2Config)
		l2.RollupX = ruleSet
		cc := *signedHeads
		cc.L2 = &l2
		require.True(t, config.IsRollupX(l2.RuleSet()))
		require.ErrorIs(t, overlay.CheckChain(&cc), ErrConfig, "rule set %d", l2.RuleSet())
		require.NoError(t, plain.CheckChain(&cc), "rule set %d", l2.RuleSet())
	}
}

// initGenesis runs InitGenesis on genesis under the flags args.
func initGenesis(t *testing.T, genesis *types.Genesis, args ...string) error {
	t.Helper()
	var err error
	cmd := &cli.Command{Name: "erigon", Flags: freshFlags(), Action: func(_ context.Context, cmd *cli.Command) error {
		err = InitGenesis(cmd, genesis)
		return nil
	}}
	require.NoError(t, cmd.Run(t.Context(), append([]string{"erigon"}, args...)))
	return err
}

func TestInitGenesis(t *testing.T) {
	plain := func() *types.Genesis {
		return &types.Genesis{Config: &chain.Config{ChainName: "plain"}, GasLimit: 30_000_000}
	}

	require.ErrorIs(t, initGenesis(t, plain()), ErrConfig, "EEZ off")
	require.ErrorIs(t, initGenesis(t, plain(), "--eez.enabled"), ErrConfig, "no l2 section")

	g := plain()
	require.NoError(t, initGenesis(t, g, "--eez.enabled", "--eez.l1.chainid=7331", "--eez.registry=0x5fbdb2315678afecb367f032d93f642f64180aa3",
		"--eez.registry.deployblock=12", "--eez.rollupid=1", "--eez.l1.blocktime=12s", "--eez.l2.blocktime=2s"))
	require.IsType(t, &config.L2Config{}, g.Config.L2)
	require.Equal(t, uint64(30_000_000), g.Config.L2.(*config.L2Config).BlockGasLimit)
}

func TestApplyGenesis(t *testing.T) {
	devnet := networks.Devnet.Genesis.Config.L2.(*config.L2Config)
	cfg, err := parse(t, "--eez.enabled", "--eez.l1.chainid=7331", "--eez.registry=0x5fbdb2315678afecb367f032d93f642f64180aa3",
		"--eez.registry.deployblock=12", "--eez.rollupid=1", "--eez.l1.blocktime=12s", "--eez.l2.blocktime=2s")
	require.NoError(t, err)

	// A vanilla genesis gets the section, with its own gas limit.
	vanilla := &chain.Config{ChainName: "vanilla"}
	require.NoError(t, cfg.L2.ApplyGenesis(vanilla, devnet.BlockGasLimit))
	require.Equal(t, devnet, vanilla.L2)
	require.NoError(t, cfg.L2.Check(vanilla))

	// A section the flags leave incomplete is refused.
	partial, err := parse(t, "--eez.enabled", "--eez.rollupid=1")
	require.NoError(t, err)
	require.ErrorIs(t, partial.L2.ApplyGenesis(&chain.Config{}, 30_000_000), ErrConfig)

	// Flags override a section the genesis carries.
	carried := &chain.Config{L2: devnet}
	require.NoError(t, partial.L2.ApplyGenesis(carried, 0))
	moved, err := parse(t, "--eez.enabled", "--eez.registry.deployblock=99")
	require.NoError(t, err)
	require.NoError(t, moved.L2.ApplyGenesis(carried, 0))
	require.Equal(t, uint64(99), carried.L2.(*config.L2Config).RegistryDeployBlock)
	require.Equal(t, uint64(12), devnet.RegistryDeployBlock, "the carried section is copied")

	// Without flags the genesis is unchanged.
	none, err := parse(t, "--eez.enabled")
	require.NoError(t, err)
	empty := &chain.Config{}
	require.NoError(t, none.L2.ApplyGenesis(empty, 30_000_000))
	require.Nil(t, empty.L2)

	// A running node only checks the flags against the section.
	require.ErrorIs(t, moved.L2.Check(networks.Devnet.Genesis.Config), ErrConfig)
	require.NoError(t, partial.L2.Check(networks.Devnet.Genesis.Config))
	require.ErrorIs(t, none.L2.Check(&chain.Config{}), ErrConfig, "not an EEZ chain")
}
