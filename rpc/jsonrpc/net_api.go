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
	"fmt"
	"strconv"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// NetAPI the interface for the net_ RPC commands
type NetAPI interface {
	Listening(_ context.Context) (bool, error)
	Version(_ context.Context) (string, error)
	PeerCount(_ context.Context) (hexutil.Uint, error)
}

// NetAPIImpl data structure to store things needed for net_ commands
type NetAPIImpl struct {
	ethBackend rpchelper.ApiBackend
}

// NewNetAPIImpl returns NetAPIImplImpl instance
func NewNetAPIImpl(eth rpchelper.ApiBackend) *NetAPIImpl {
	return &NetAPIImpl{
		ethBackend: eth,
	}
}

// Listening implements net_listening. Returns true if client is actively listening for network connections.
// If we can get peers info, it means the network interface is up and listening
func (api *NetAPIImpl) Listening(ctx context.Context) (bool, error) {
	_, err := api.ethBackend.Peers(ctx)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// Version implements net_version. Returns the current network id.
func (api *NetAPIImpl) Version(ctx context.Context) (string, error) {
	if api.ethBackend == nil {
		// We're running in --datadir mode or otherwise cannot get the backend
		return "", fmt.Errorf(NotAvailableChainData, "net_version")
	}

	res, err := api.ethBackend.NetVersion(ctx)
	if err != nil {
		return "", err
	}

	return strconv.FormatUint(res, 10), nil
}

// PeerCount implements net_peerCount. Returns number of peers currently
// connected to the first sentry server.
func (api *NetAPIImpl) PeerCount(ctx context.Context) (hexutil.Uint, error) {
	if api.ethBackend == nil {
		// We're running in --datadir mode or otherwise cannot get the backend
		return 0, fmt.Errorf(NotAvailableChainData, "net_peerCount")
	}

	res, err := api.ethBackend.NetPeerCount(ctx)
	if err != nil {
		return 0, err
	}

	return hexutil.Uint(res), nil
}
