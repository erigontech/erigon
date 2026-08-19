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

// Package bscsync drives BSC (Parlia) block acquisition over devp2p without a
// consensus layer. This first cut only observes block announcements and logs
// number+hash — no verification, no execution, no persistence.
package bscsync

import (
	"context"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	polygonp2p "github.com/erigontech/erigon/polygon/p2p"
)

// RunBlockPrinter registers announcement observers, then runs the p2p service.
// It blocks until ctx is cancelled or the service fails.
func RunBlockPrinter(ctx context.Context, logger log.Logger, svc *polygonp2p.Service) error {
	unregHashes := svc.RegisterNewBlockHashesObserver(func(msg *p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]) {
		for _, h := range *msg.Decoded {
			logger.Info("[bsc] new block hash", "number", h.Number, "hash", h.Hash, "peer", msg.PeerId.String())
		}
	})
	defer unregHashes()

	unregBlock := svc.RegisterNewBlockObserver(func(msg *p2p.DecodedInboundMessage[*eth.NewBlockPacket]) {
		b := msg.Decoded.Block
		logger.Info("[bsc] new block", "number", b.NumberU64(), "hash", b.Hash(), "txs", len(b.Transactions()), "peer", msg.PeerId.String())
	})
	defer unregBlock()

	logger.Info("[bsc] block printer started — observing devp2p announcements")
	return svc.Run(ctx)
}
