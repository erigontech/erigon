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

package bscsync

import (
	"context"
	"errors"
	"fmt"

	bscp2p "github.com/erigontech/erigon/bsc/p2p"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

var errNoPeers = errors.New("no peers available for range")

// fetchForwardRange downloads headers [from,to] and their bodies from a single
// peer and assembles forward-ordered blocks. It tries peers that may have the
// range until one returns the full set.
func fetchForwardRange(ctx context.Context, svc *bscp2p.Service, from, to uint64) ([]*types.Block, error) {
	peers := svc.ListPeersMayHaveBlockNum(to)
	if len(peers) == 0 {
		return nil, errNoPeers
	}

	var lastErr error
	for _, peerID := range peers {
		blocks, err := fetchRangeFromPeer(ctx, svc, from, to, peerID)
		if err != nil {
			lastErr = err
			continue
		}
		return blocks, nil
	}
	return nil, fmt.Errorf("all %d peers failed for [%d,%d]: %w", len(peers), from, to, lastErr)
}

func fetchRangeFromPeer(ctx context.Context, svc *bscp2p.Service, from, to uint64, peerID *p2p.PeerId) ([]*types.Block, error) {
	hResp, err := svc.FetchHeaders(ctx, from, to+1, peerID)
	if err != nil {
		return nil, err
	}
	headers := hResp.Data
	if len(headers) == 0 {
		return nil, fmt.Errorf("peer %s returned no headers for [%d,%d]", peerID, from, to)
	}

	bResp, err := svc.FetchBodies(ctx, headers, peerID)
	if err != nil {
		return nil, err
	}
	bodies := bResp.Data
	if len(bodies) != len(headers) {
		return nil, fmt.Errorf("peer %s returned %d bodies for %d headers", peerID, len(bodies), len(headers))
	}

	blocks := make([]*types.Block, len(headers))
	for i, h := range headers {
		blocks[i] = types.NewBlockFromNetwork(h, bodies[i], nil)
	}
	return blocks, nil
}
