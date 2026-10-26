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

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
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
	&EnabledFlag, &L1RPCFlag, &L1KVFlag, &L1PollFlag, &L1LogChunkFlag, &L1ReorgDepthFlag, &SequencerRPCFlag,
	&L1ChainIDFlag, &RegistryFlag, &RegistryDeployBlockFlag, &RollupIDFlag, &L1BlockTimeFlag, &L2BlockTimeFlag,
}

// Config is a node's EEZ configuration.
type Config struct {
	L1RPC        string
	L1KV         string
	L1Poll       time.Duration
	L1LogChunk   uint64
	L1ReorgDepth int
	SequencerRPC string
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
		L1Poll:       cmd.Duration(L1PollFlag.Name),
		L1LogChunk:   cmd.Uint64(L1LogChunkFlag.Name),
		L1ReorgDepth: int(cmd.Int(L1ReorgDepthFlag.Name)),
		SequencerRPC: cmd.String(SequencerRPCFlag.Name),
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
// one L1 source.
func (c *Config) CheckNode() error {
	if (c.L1RPC == "") == (c.L1KV == "") {
		return fmt.Errorf("%w: the follower needs one L1 source, --%s or --%s", ErrConfig, L1RPCFlag.Name, L1KVFlag.Name)
	}
	return nil
}

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
