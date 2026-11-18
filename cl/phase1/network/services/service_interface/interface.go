// Copyright 2024 The Erigon Authors
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

package serviceinterface

import (
	"context"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Note: BlobSidecarService and BlockService are tested in spectests

type Service[T any] interface {
	Names() []string
	DecodeGossipMessage(pid peer.ID, data []byte, version clparams.StateVersion) (T, error)
	ProcessMessage(ctx context.Context, subnet *uint64, msg T) error
}

type gossipPeerKey struct{}

// WithGossipPeer returns ctx carrying the peer that forwarded the gossip message.
func WithGossipPeer(ctx context.Context, pid peer.ID) context.Context {
	return context.WithValue(ctx, gossipPeerKey{}, pid)
}

// GossipPeer returns the peer that forwarded the gossip message, if known.
func GossipPeer(ctx context.Context) (peer.ID, bool) {
	pid, _ := ctx.Value(gossipPeerKey{}).(peer.ID)
	return pid, pid != ""
}
