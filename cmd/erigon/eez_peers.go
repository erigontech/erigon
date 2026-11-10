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
	"fmt"
	"path/filepath"
	"slices"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
)

// configureFollowerNetwork gives the node of a standalone follower the
// network of the chain it runs, before the node starts its sentry: the
// network id and the L2 peers.
func configureFollowerNetwork(ctx context.Context, cmd *cli.Command, n *nodecfg.Config, e *ethconfig.Config, logger log.Logger) error {
	chainConfig, own, err := followerChain(ctx, n.Dirs, e, logger)
	if err != nil {
		return err
	}
	// A chain initialised from a genesis file runs under the default
	// --chain, whose network id, bootnodes and DNS tree are mainnet's.
	if !own {
		if !cmd.IsSet(utils.NetworkIdFlag.Name) && chainConfig != nil {
			e.NetworkID = chainConfig.ChainID.Uint64()
		}
		if !cmd.IsSet(utils.BootnodesFlag.Name) {
			n.P2P.BootstrapNodes, n.P2P.BootstrapNodesV5 = nil, nil
		}
		if !cmd.IsSet(utils.DNSDiscoveryFlag.Name) {
			e.EthDiscoveryURLs = nil
		}
	}
	return configureFollowerPeers(&n.P2P, e.EthDiscoveryURLs, chainConfig)
}

// configureFollowerPeers gives the follower of an R0 chain its L2 peers over
// eth/68 and eth/69: discovery from bootnodes or the DNS tree dns, or static
// and trusted peers without discovery. A RollupX chain takes no peers.
func configureFollowerPeers(p *p2p.Config, dns []string, chainConfig *chain.Config) error {
	if chainConfig != nil {
		if l2, ok := chainConfig.L2.(*config.L2Config); ok && l2.RollupX != nil {
			p.MaxPeers, p.NoDiscovery, p.NoDial = 0, true, true
			p.StaticNodes, p.TrustedNodes = nil, nil
			return nil
		}
	}
	if !p.NoDiscovery && len(p.BootstrapNodes) == 0 && len(dns) == 0 {
		return fmt.Errorf("%w: discovery finds no L2 peers without --%s; set it, or --%s with --%s or --%s",
			eez.ErrConfig, utils.BootnodesFlag.Name, utils.NoDiscoverFlag.Name, utils.StaticPeersFlag.Name, utils.TrustedPeersFlag.Name)
	}
	p.ProtocolVersion = []uint{direct.ETH68, direct.ETH69}
	// Trusted peers are dialled, not only admitted past the peer limit.
	for _, t := range p.TrustedNodes {
		if !slices.ContainsFunc(p.StaticNodes, func(s *enode.Node) bool { return s.ID() == t.ID() }) {
			p.StaticNodes = append(p.StaticNodes, t)
		}
	}
	return nil
}

// followerChain returns the chain config the node of dirs runs, the one its
// chain database stores or e's genesis before the first start, and whether
// the --chain spec of e has that chain's genesis.
func followerChain(ctx context.Context, dirs datadir.Dirs, e *ethconfig.Config, logger log.Logger) (*chain.Config, bool, error) {
	spec, _ := chainspec.ChainSpecByName(e.Snapshot.ChainName)
	ok, err := dir.FileExist(filepath.Join(dirs.Chaindata, "mdbx.dat"))
	if err != nil {
		return nil, false, err
	}
	if ok {
		db, err := mdbx.New(dbcfg.ChainDB, logger).Path(dirs.Chaindata).Accede(true).Readonly(true).
			WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.TableCfg{kv.HeaderCanonical: {}, kv.ConfigTable: {}} }).
			Open(ctx)
		if err != nil {
			return nil, false, fmt.Errorf("chain config of %s: %w", dirs.Chaindata, err)
		}
		defer db.Close()
		var genesis common.Hash
		var stored *chain.Config
		if err := db.View(ctx, func(tx kv.Tx) error {
			if genesis, err = rawdb.ReadCanonicalHash(tx, 0); err != nil || genesis == (common.Hash{}) {
				return err
			}
			stored, err = rawdb.ReadChainConfig(tx, genesis)
			return err
		}); err != nil {
			return nil, false, fmt.Errorf("chain config of %s: %w", dirs.Chaindata, err)
		}
		if stored != nil {
			return stored, spec.GenesisHash == genesis, nil
		}
	}
	if e.Genesis == nil {
		return nil, false, nil
	}
	return e.Genesis.Config, e.Genesis == spec.Genesis, nil
}
