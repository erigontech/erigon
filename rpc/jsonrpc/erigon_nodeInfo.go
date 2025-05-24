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

	p2p "github.com/erigontech/erigon-p2p"
)

const (
	// allNodesInfo used in NodeInfo request to receive meta data from all running sentries.
	allNodesInfo = 0
)

func (api *ErigonImpl) NodeInfo(ctx context.Context) ([]p2p.NodeInfo, error) {
	return api.ethBackend.NodeInfo(ctx, allNodesInfo)
}
