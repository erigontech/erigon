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

package jsonrpc

import (
	"context"
	"errors"
	"fmt"

	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/p2p"

	"github.com/erigontech/erigon/turbo/rpchelper"
)

// AdminAPI the interface for the admin_* RPC commands.
type AdminAPI interface {
	// NodeInfo returns a collection of metadata known about the host.
	NodeInfo(ctx context.Context) (*p2p.NodeInfo, error)

	// Peers returns information about the connected remote nodes.
	// https://geth.ethereum.org/docs/rpc/ns-admin#admin_peers
	Peers(ctx context.Context) ([]*p2p.PeerInfo, error)

	// AddPeer requests connecting to a remote node.
	AddPeer(ctx context.Context, url string) (bool, error)
}

// AdminAPIImpl data structure to store things needed for admin_* commands.
type AdminAPIImpl struct {
	ethBackend rpchelper.ApiBackend
}

// NewAdminAPI returns AdminAPIImpl instance.
func NewAdminAPI(eth rpchelper.ApiBackend) *AdminAPIImpl {
	return &AdminAPIImpl{
		ethBackend: eth,
	}
}

func (api *AdminAPIImpl) NodeInfo(ctx context.Context) (*p2p.NodeInfo, error) {
	nodes, err := api.ethBackend.NodeInfo(ctx, 1)
	if err != nil {
		return nil, fmt.Errorf("node info request error: %w", err)
	}

	if len(nodes) == 0 {
		return nil, errors.New("empty nodesInfo response")
	}

	return &nodes[0], nil
}

func (api *AdminAPIImpl) Peers(ctx context.Context) ([]*p2p.PeerInfo, error) {
	return api.ethBackend.Peers(ctx)
}

func (api *AdminAPIImpl) AddPeer(ctx context.Context, url string) (bool, error) {
	result, err := api.ethBackend.AddPeer(ctx, &remote.AddPeerRequest{Url: url})
	if err != nil {
		return false, err
	}
	if result == nil {
		return false, errors.New("nil addPeer response")
	}
	return result.Success, nil
}
