// Copyright 2015 The go-ethereum Authors
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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/netutil"
)

// ServeListener accepts connections on l, serving JSON-RPC on them.
func (s *Server) ServeListener(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if netutil.IsTemporaryError(err) {
			log.Warn("RPC accept error", "err", err)
			continue
		} else if err != nil {
			return err
		}
		log.Trace("Accepted RPC connection", "conn", conn.RemoteAddr())
		go s.ServeCodec(NewCodec(&heldConn{Conn: conn}), 0)
	}
}

// DialIPC creates a new client that connects to the unix socket at endpoint.
//
// The context is used for the initial connection establishment. It does not
// affect subsequent interactions with the client.
func DialIPC(ctx context.Context, endpoint string, logger log.Logger) (*Client, error) {
	return newClient(ctx, new(clientConfig), newClientTransportIPC(endpoint), logger)
}

func newClientTransportIPC(endpoint string) reconnectFunc {
	return func(ctx context.Context) (ServerCodec, error) {
		conn, err := new(net.Dialer).DialContext(ctx, "unix", endpoint)
		if err != nil {
			return nil, err
		}
		return NewCodec(conn), nil
	}
}
