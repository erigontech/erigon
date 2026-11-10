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
	"fmt"

	"github.com/erigontech/erigon/cmd/erigon/node"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/overlay"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/execution/chain"
)

// startOverlay registers on n the signed-head overlay of cfg, on the
// identity and discv5 table of n's devp2p server, and returns the heads it
// accepts.
func startOverlay(n *node.ErigonNode, cfg *eez.Config, chainConfig *chain.Config, logger log.Logger) (<-chan *seqsig.VerifiedHead, error) {
	srv := n.Backend().P2PServer()
	if srv == nil {
		return nil, fmt.Errorf("%w: --%s needs the node's own sentry", eez.ErrConfig, eez.P2PListenFlag.Name)
	}
	l2, ok := chainConfig.L2.(*config.L2Config)
	if !ok {
		return nil, fmt.Errorf("%w: chain %s has no EEZ l2 section", eez.ErrConfig, chainConfig.ChainName)
	}
	o, err := overlay.New(overlay.Config{
		Listen: cfg.P2PListen,
		Peers:  cfg.P2PPeers,
		Key:    srv.PrivateKey,
		Domain: seqsig.Domain{ChainID: chainConfig.ChainID.Uint64(), Registry: l2.Registry, RollupID: l2.RollupID},
		Signer: *cfg.SequencerAddress,
		Devp2p: srv,
	}, logger)
	if err != nil {
		return nil, err
	}
	n.RegisterLifecycle(o)
	return o.Heads(), nil
}
