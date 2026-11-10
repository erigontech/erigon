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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/overlay"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/enode"
)

// ErrConfig reports EEZ flags that do not make a configuration.
var ErrConfig = errors.New("invalid EEZ configuration")

var (
	L1RPCFlag = cli.StringFlag{
		Name:  "eez.l1.rpc",
		Usage: "JSON-RPC endpoint of the L1 node the follower reads",
	}
	L1KVFlag = cli.StringFlag{
		Name:  "eez.l1.kv",
		Usage: "Private API address (host:port) of the Erigon L1 node the follower reads",
	}
	L1BeaconFlag = cli.StringFlag{
		Name:  "eez.l1.beacon",
		Usage: "REST endpoint of the L1 beacon node that serves the blobs of blob-backed batches",
	}
	L1BlobsFlag = cli.StringFlag{
		Name:  "eez.l1.blobs",
		Usage: "Directory of blobs named by versioned hash, read before the beacon node; blobs the beacon node serves are stored in it",
	}
	L1PollFlag = cli.DurationFlag{
		Name:  "eez.l1.poll",
		Usage: "Interval between reads of the L1 head",
		Value: 2 * time.Second,
	}
	L1LogChunkFlag = cli.Uint64Flag{
		Name:  "eez.l1.logchunk",
		Usage: "Widest block range of one L1 log query, halved when the node refuses it",
		Value: l1.LogChunk,
	}
	L1ReorgDepthFlag = cli.IntFlag{
		Name:  "eez.l1.reorgdepth",
		Usage: "Deepest L1 reorg the follower survives",
		Value: l1.DefaultReorgDepth,
	}
	SequencerRPCFlag = cli.StringFlag{
		Name:  "eez.sequencer.rpc",
		Usage: "JSON-RPC endpoint of the sequencer, whose blocks the follower takes as its unsafe head",
	}
	SyncL1OnlyFlag = cli.BoolFlag{
		Name:  "eez.sync.l1only",
		Usage: "Derive the whole history from L1 rather than take the history below L1's finalized root from L2 peers; unset, true on rollupX networks and false on rollup0 networks",
	}
	P2PListenFlag = cli.StringFlag{
		Name:  "eez.p2p.listen",
		Usage: "TCP multiaddr with a fixed port on which the node joins the signed-head overlay, such as /ip4/0.0.0.0/tcp/9100",
	}
	P2PPeersFlag = cli.StringFlag{
		Name:  "eez.p2p.peers",
		Usage: "Comma-separated static overlay peers, each an IP multiaddr ending in /p2p/<peer id>",
	}
	SequencerAddressFlag = cli.StringFlag{
		Name:  "eez.sequencer.address",
		Usage: "Address of the sequencer, whose signed heads the overlay accepts",
	}
	ChainFlag = cli.StringFlag{
		Name:  "eez.chain",
		Usage: "EEZ network to run in the same process as the L1 chain of --chain, whose database the follower reads",
	}
	HTTPPortFlag = cli.IntFlag{
		Name:  "eez.http.port",
		Usage: "HTTP-RPC port of the EEZ chain of --eez.chain",
		Value: 8645,
	}
	HTTPAPIFlag = cli.StringFlag{
		Name:  "eez.http.api",
		Usage: "API namespaces the HTTP-RPC of the EEZ chain of --eez.chain serves",
		Value: "eth,debug,eez",
	}
	PortFlag = cli.IntFlag{
		Name:  "eez.port",
		Usage: "devp2p listening port of the EEZ chain of --eez.chain",
		Value: 30403,
	}
	BootnodesFlag = cli.StringFlag{
		Name:  "eez.bootnodes",
		Usage: "Comma-separated enode URLs from which the EEZ chain of --eez.chain discovers its L2 peers; unset, it runs no discovery",
	}
	StaticPeersFlag = cli.StringFlag{
		Name:  "eez.staticpeers",
		Usage: "Comma-separated enode URLs of L2 peers the EEZ chain of --eez.chain keeps connected",
	}
	TrustedPeersFlag = cli.StringFlag{
		Name:  "eez.trustedpeers",
		Usage: "Comma-separated enode URLs of L2 peers the EEZ chain of --eez.chain keeps connected, even above its peer limit",
	}

	// The l2 section of the chain config; init writes them into the
	// section, a running node checks them against it.
	L1ChainIDFlag = cli.Uint64Flag{
		Name:  "eez.l1.chainid",
		Usage: "Chain id of the L1 the EEZ chain settles on",
	}
	RegistryFlag = cli.StringFlag{
		Name:  "eez.registry",
		Usage: "Address of the EEZ registry on L1",
	}
	RegistryDeployBlockFlag = cli.Uint64Flag{
		Name:  "eez.registry.deployblock",
		Usage: "L1 block the EEZ registry was deployed in",
	}
	RollupIDFlag = cli.Uint64Flag{
		Name:  "eez.rollupid",
		Usage: "Rollup id of the EEZ chain in the registry",
	}
	L1BlockTimeFlag = cli.DurationFlag{
		Name:  "eez.l1.blocktime",
		Usage: "L1 block time, whole seconds",
	}
	L2BlockTimeFlag = cli.DurationFlag{
		Name:  "eez.l2.blocktime",
		Usage: "EEZ block time, whole seconds",
	}
)

// Flags lists the EEZ command-line flags.
var Flags = []cli.Flag{
	&EnabledFlag, &L1RPCFlag, &L1KVFlag, &L1BeaconFlag, &L1BlobsFlag, &L1PollFlag, &L1LogChunkFlag, &L1ReorgDepthFlag, &SequencerRPCFlag, &SyncL1OnlyFlag,
	&P2PListenFlag, &P2PPeersFlag, &SequencerAddressFlag,
	&ChainFlag, &HTTPPortFlag, &HTTPAPIFlag, &PortFlag, &BootnodesFlag, &StaticPeersFlag, &TrustedPeersFlag, &L1ChainIDFlag, &RegistryFlag, &RegistryDeployBlockFlag, &RollupIDFlag, &L1BlockTimeFlag, &L2BlockTimeFlag,
}

// Config is a node's EEZ configuration.
type Config struct {
	L1RPC        string
	L1KV         string
	L1Beacon     string
	L1Blobs      string
	L1Poll       time.Duration
	L1LogChunk   uint64
	L1ReorgDepth int
	SequencerRPC string
	// SyncL1Only, when set, chooses L1-only derivation or the history below
	// L1's finalized root from L2 peers; unset, the rule set chooses.
	SyncL1Only *bool
	// P2PListen is where the node joins the signed-head overlay, nil for
	// none; P2PPeers are its static peers and SequencerAddress the signer of
	// the heads it accepts.
	P2PListen        ma.Multiaddr
	P2PPeers         []peer.AddrInfo
	SequencerAddress *common.Address
	// Chain is the EEZ network an all-in-one node runs beside its L1 chain,
	// served on HTTPPort with the namespaces HTTPAPI. Its devp2p listens on
	// Port, zero unless set, and takes its peers from Bootnodes, StaticPeers
	// and TrustedPeers.
	Chain        string
	HTTPPort     int
	HTTPAPI      []string
	Port         int
	Bootnodes    []*enode.Node
	StaticPeers  []*enode.Node
	TrustedPeers []*enode.Node
	// L2 holds the l2 section fields the flags set.
	L2 L2Flags
}

// L2Flags are the l2 section fields set on the command line.
type L2Flags struct {
	L1ChainID           *uint64
	Registry            *common.Address
	RegistryDeployBlock *uint64
	RollupID            *uint64
	L1BlockTime         *uint64
	BlockInterval       *uint64
}

// ConfigFromCLI reads the EEZ flags of cmd; nil when EEZ is not enabled.
func ConfigFromCLI(cmd *cli.Command) (*Config, error) {
	if !cmd.Bool(EnabledFlag.Name) {
		for _, f := range Flags {
			if name := f.Names()[0]; name != EnabledFlag.Name && cmd.IsSet(name) {
				return nil, fmt.Errorf("%w: --%s requires --%s", ErrConfig, name, EnabledFlag.Name)
			}
		}
		return nil, nil
	}
	c := &Config{
		L1RPC:        cmd.String(L1RPCFlag.Name),
		L1KV:         cmd.String(L1KVFlag.Name),
		L1Beacon:     cmd.String(L1BeaconFlag.Name),
		L1Blobs:      cmd.String(L1BlobsFlag.Name),
		L1Poll:       cmd.Duration(L1PollFlag.Name),
		L1LogChunk:   cmd.Uint64(L1LogChunkFlag.Name),
		L1ReorgDepth: int(cmd.Int(L1ReorgDepthFlag.Name)),
		SequencerRPC: cmd.String(SequencerRPCFlag.Name),
		Chain:        cmd.String(ChainFlag.Name),
	}
	if cmd.IsSet(SyncL1OnlyFlag.Name) {
		l1Only := cmd.Bool(SyncL1OnlyFlag.Name)
		c.SyncL1Only = &l1Only
	}
	if c.Chain != "" {
		c.HTTPPort = int(cmd.Int(HTTPPortFlag.Name))
		c.HTTPAPI = common.CliString2Array(cmd.String(HTTPAPIFlag.Name))
		if cmd.IsSet(PortFlag.Name) {
			if c.Port = int(cmd.Int(PortFlag.Name)); c.Port < 1 || c.Port > 65535 {
				return nil, fmt.Errorf("%w: --%s %d is out of range", ErrConfig, PortFlag.Name, c.Port)
			}
		}
		for _, p := range []struct {
			flag *cli.StringFlag
			out  *[]*enode.Node
		}{{&BootnodesFlag, &c.Bootnodes}, {&StaticPeersFlag, &c.StaticPeers}, {&TrustedPeersFlag, &c.TrustedPeers}} {
			urls := common.CliString2Array(cmd.String(p.flag.Name))
			if len(urls) == 0 {
				continue
			}
			nodes, err := enode.ParseNodesFromURLs(urls)
			if err != nil {
				return nil, fmt.Errorf("%w: --%s: %w", ErrConfig, p.flag.Name, err)
			}
			*p.out = nodes
		}
	} else {
		for _, f := range []cli.Flag{&HTTPPortFlag, &HTTPAPIFlag, &PortFlag, &BootnodesFlag, &StaticPeersFlag, &TrustedPeersFlag} {
			if name := f.Names()[0]; cmd.IsSet(name) {
				return nil, fmt.Errorf("%w: --%s requires --%s", ErrConfig, name, ChainFlag.Name)
			}
		}
	}
	if err := c.readOverlay(cmd); err != nil {
		return nil, err
	}
	uintFlag := func(f *cli.Uint64Flag) *uint64 {
		if !cmd.IsSet(f.Name) {
			return nil
		}
		v := cmd.Uint64(f.Name)
		return &v
	}
	c.L2.L1ChainID = uintFlag(&L1ChainIDFlag)
	c.L2.RegistryDeployBlock = uintFlag(&RegistryDeployBlockFlag)
	c.L2.RollupID = uintFlag(&RollupIDFlag)
	if cmd.IsSet(RegistryFlag.Name) {
		s := cmd.String(RegistryFlag.Name)
		if !common.IsHexAddress(s) {
			return nil, fmt.Errorf("%w: --%s %q is not an address", ErrConfig, RegistryFlag.Name, s)
		}
		a := common.HexToAddress(s)
		c.L2.Registry = &a
	}
	for _, d := range []struct {
		flag *cli.DurationFlag
		out  **uint64
	}{{&L1BlockTimeFlag, &c.L2.L1BlockTime}, {&L2BlockTimeFlag, &c.L2.BlockInterval}} {
		if !cmd.IsSet(d.flag.Name) {
			continue
		}
		v := cmd.Duration(d.flag.Name)
		if v <= 0 || v%time.Second != 0 {
			return nil, fmt.Errorf("%w: --%s %s is not a whole number of seconds", ErrConfig, d.flag.Name, v)
		}
		s := uint64(v / time.Second)
		*d.out = &s
	}
	if c.L1Poll <= 0 || c.L1LogChunk == 0 || c.L1ReorgDepth <= 0 {
		return nil, fmt.Errorf("%w: --%s, --%s and --%s must be positive", ErrConfig, L1PollFlag.Name, L1LogChunkFlag.Name, L1ReorgDepthFlag.Name)
	}
	return c, nil
}

// readOverlay reads the flags of the signed-head overlay.
func (c *Config) readOverlay(cmd *cli.Command) error {
	var set []string
	for _, f := range []cli.Flag{&P2PListenFlag, &P2PPeersFlag, &SequencerAddressFlag} {
		if name := f.Names()[0]; cmd.IsSet(name) {
			set = append(set, name)
		}
	}
	listen, address := cmd.IsSet(P2PListenFlag.Name), cmd.IsSet(SequencerAddressFlag.Name)
	switch {
	case len(set) == 0:
		return nil
	case c.Chain != "":
		return fmt.Errorf("%w: --%s is not available with --%s", ErrConfig, set[0], ChainFlag.Name)
	case c.SequencerRPC != "":
		return fmt.Errorf("%w: --%s and --%s both set the unsafe head", ErrConfig, set[0], SequencerRPCFlag.Name)
	case listen != address:
		return fmt.Errorf("%w: --%s and --%s require each other", ErrConfig, P2PListenFlag.Name, SequencerAddressFlag.Name)
	case !listen:
		return fmt.Errorf("%w: --%s requires --%s", ErrConfig, P2PPeersFlag.Name, P2PListenFlag.Name)
	}
	var err error
	if c.P2PListen, err = overlay.ParseListen(cmd.String(P2PListenFlag.Name)); err != nil {
		return fmt.Errorf("%w: --%s: %w", ErrConfig, P2PListenFlag.Name, err)
	}
	if c.P2PPeers, err = overlay.ParsePeers(cmd.String(P2PPeersFlag.Name)); err != nil {
		return fmt.Errorf("%w: --%s: %w", ErrConfig, P2PPeersFlag.Name, err)
	}
	s := cmd.String(SequencerAddressFlag.Name)
	if !common.IsHexAddress(s) {
		return fmt.Errorf("%w: --%s %q is not an address", ErrConfig, SequencerAddressFlag.Name, s)
	}
	a := common.HexToAddress(s)
	c.SequencerAddress = &a
	return nil
}

// InitGenesis writes the l2 section the EEZ flags set into the genesis init
// commits, which must then be an EEZ genesis. It requires --eez.enabled.
func InitGenesis(cmd *cli.Command, genesis *types.Genesis) error {
	cfg, err := ConfigFromCLI(cmd)
	if err != nil {
		return err
	}
	if cfg == nil {
		return fmt.Errorf("%w: --%s is not set", ErrConfig, EnabledFlag.Name)
	}
	if err := cfg.L2.ApplyGenesis(genesis.Config, genesis.GasLimit); err != nil {
		return err
	}
	if _, ok := genesis.Config.L2.(*config.L2Config); !ok {
		return fmt.Errorf("%w: --%s needs a genesis with an EEZ l2 section, or the flags that write one", ErrConfig, EnabledFlag.Name)
	}
	return nil
}

// CheckNode reports what a running node lacks: its follower reads exactly
// one L1 source, an L1 node or the L1 chain of the same process.
func (c *Config) CheckNode() error {
	sources := 0
	for _, s := range []string{c.L1RPC, c.L1KV, c.Chain} {
		if s != "" {
			sources++
		}
	}
	if sources != 1 {
		return fmt.Errorf("%w: the follower needs one L1 source, --%s, --%s or --%s", ErrConfig, L1RPCFlag.Name, L1KVFlag.Name, ChainFlag.Name)
	}
	if c.AllInOne() && c.SyncL1Only != nil && !*c.SyncL1Only {
		return fmt.Errorf("%w: --%s=false: the EEZ chain beside its L1 (--%s) takes no L2 peers", ErrConfig, SyncL1OnlyFlag.Name, ChainFlag.Name)
	}
	return nil
}

// L1Only reports whether the follower of chainConfig derives its whole
// history from L1.
func (c *Config) L1Only(chainConfig *chain.Config) (bool, error) {
	l2, ok := chainConfig.L2.(*config.L2Config)
	rollupX := ok && l2.RollupX != nil
	if c.SyncL1Only == nil {
		return rollupX || c.AllInOne(), nil
	}
	if !*c.SyncL1Only && rollupX {
		return false, fmt.Errorf("%w: --%s=false: rollupX chains derive their history from L1 alone", ErrConfig, SyncL1OnlyFlag.Name)
	}
	return *c.SyncL1Only, nil
}

// CheckChain refuses the flags cc's rule set does not run with: a chain whose
// sequencer signs no heads has no overlay.
func (c *Config) CheckChain(cc *chain.Config) error {
	l2, ok := cc.L2.(*config.L2Config)
	if !ok || !config.IsRollupX(l2.RuleSet()) || c.P2PListen == nil {
		return nil
	}
	return fmt.Errorf("%w: --%s, --%s and --%s: the chain's sequencer signs no heads", ErrConfig, P2PListenFlag.Name, P2PPeersFlag.Name, SequencerAddressFlag.Name)
}

// AllInOne reports whether the node runs its L1 chain in the same process.
func (c *Config) AllInOne() bool { return c.Chain != "" }

// fields pairs each flag-set field with its l2 section field.
func (f *L2Flags) fields(l2 *config.L2Config) []struct {
	flag string
	set  bool
	eq   func() bool
	put  func()
} {
	type field = struct {
		flag string
		set  bool
		eq   func() bool
		put  func()
	}
	u := func(flag string, v *uint64, dst *uint64) field {
		return field{flag, v != nil, func() bool { return *v == *dst }, func() { *dst = *v }}
	}
	return []field{
		u(L1ChainIDFlag.Name, f.L1ChainID, &l2.L1ChainID),
		{RegistryFlag.Name, f.Registry != nil, func() bool { return *f.Registry == l2.Registry }, func() { l2.Registry = *f.Registry }},
		u(RegistryDeployBlockFlag.Name, f.RegistryDeployBlock, &l2.RegistryDeployBlock),
		u(RollupIDFlag.Name, f.RollupID, &l2.RollupID),
		u(L1BlockTimeFlag.Name, f.L1BlockTime, &l2.L1BlockTime),
		u(L2BlockTimeFlag.Name, f.BlockInterval, &l2.BlockInterval),
	}
}

// ApplyGenesis writes the l2 section fields set on the command line into
// cfg, the config of a genesis about to be written, creating the section
// with gasLimit as block gas limit when cfg has none.
func (f *L2Flags) ApplyGenesis(cfg *chain.Config, gasLimit uint64) error {
	if err := cfg.ResolveL2(); err != nil {
		return err
	}
	var l2 config.L2Config
	switch cur := cfg.L2.(type) {
	case *config.L2Config:
		l2 = *cur
	case nil:
		l2.BlockGasLimit = gasLimit
	default:
		return fmt.Errorf("%w: the genesis carries a %q l2 section", ErrConfig, cur.Name())
	}
	fields := f.fields(&l2)
	var set bool
	for _, x := range fields {
		if x.set {
			x.put()
			set = true
		}
	}
	if !set {
		return nil
	}
	if err := l2.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}
	cfg.L2, cfg.L2JSON = &l2, nil
	return nil
}

// Check returns an error naming each flag that differs from cfg's l2
// section, which only a genesis sets.
func (f *L2Flags) Check(cfg *chain.Config) error {
	l2, ok := cfg.L2.(*config.L2Config)
	if !ok {
		return fmt.Errorf("%w: chain %s has no EEZ l2 section", ErrConfig, cfg.ChainName)
	}
	var differ []string
	for _, x := range f.fields(l2) {
		if x.set && !x.eq() {
			differ = append(differ, "--"+x.flag)
		}
	}
	if len(differ) > 0 {
		return fmt.Errorf("%w: %s differ from the chain's l2 section, which init writes", ErrConfig, strings.Join(differ, ", "))
	}
	return nil
}
