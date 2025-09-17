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

	"github.com/erigontech/erigon/rpc/rpchelper"
)

// NetAPIArb is the interface for the net_ RPC commands.
type NetAPIArb interface {
	Version(_ context.Context) (string, error)
}

// NetAPIArbImpl is a data structure to store things needed for net_ commands.
type NetAPIArbImpl struct {
	ethBackend rpchelper.ApiBackend
}

// NewNetAPIArbImpl returns a NetAPIArbImpl instance.
func NewNetAPIArbImpl(eth rpchelper.ApiBackend) *NetAPIArbImpl {
	return &NetAPIArbImpl{
		ethBackend: eth,
	}
}

// Version implements net_version. Returns the current network ID.
func (api *NetAPIArbImpl) Version(ctx context.Context) (string, error) {
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
