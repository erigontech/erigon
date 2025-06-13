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

package node_utils

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/erigontech/erigon-p2p/enode"
	"github.com/erigontech/erigon/cmd/observer/database"
)

func NodeID(node *enode.Node) (database.NodeID, error) {
	if node.Incomplete() {
		return "", errors.New("NodeID not implemented for incomplete nodes")
	}
	nodeURL, err := url.Parse(node.URLv4())
	if err != nil {
		return "", fmt.Errorf("failed to parse node URL: %w", err)
	}
	id := nodeURL.User.Username()
	return database.NodeID(id), nil
}
