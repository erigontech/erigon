// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package rpc

import (
	"context"
	"net"

	"github.com/erigontech/erigon/erigon-lib/log/v3"
)

// DialInProc attaches an in-process connection to the given RPC server.
func DialInProc(handler *Server, logger log.Logger) *Client {
	initctx := context.Background()
	c, _ := newClient(initctx, func(context.Context) (ServerCodec, error) {
		p1, p2 := net.Pipe()
		go handler.ServeCodec(NewCodec(p1), 0)
		return NewCodec(p2), nil
	}, logger)
	return c
}
