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

package jsonrpc

import "github.com/erigontech/erigon/rpc"

// Connections holds the shared BaseAPI and an open registry of rpc.API entries.
//
// Create with NewConnections, populate with PopulateConnections (for the
// standard JSON-RPC API set) and/or Register (for additional namespaces from
// other components), then retrieve the complete list with APIs().
//
// This design allows each component to contribute its own RPC namespaces
// without any coupling between components at the code level.
type Connections struct {
	BaseApi *BaseAPI
	apis    []rpc.API
}

// NewConnections creates a Connections wrapping the given BaseAPI.
func NewConnections(base *BaseAPI) *Connections {
	return &Connections{BaseApi: base}
}

// Register adds API entries to the registry.
// Call before APIs() — insertion order is preserved.
func (c *Connections) Register(apis ...rpc.API) {
	c.apis = append(c.apis, apis...)
}

// APIs returns a copy of all registered API entries.
func (c *Connections) APIs() []rpc.API {
	out := make([]rpc.API, len(c.apis))
	copy(out, c.apis)
	return out
}
