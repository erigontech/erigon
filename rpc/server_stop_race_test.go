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

package rpc

import (
	"net"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/erigontech/erigon/common/log/v3"
)

// stopBeforeAdd runs Server.Stop right before a codec joins the set, which is the
// interleaving where Stop's close pass misses a connection that passed the run check.
type stopBeforeAdd struct {
	mapset.Set[ServerCodec]
	srv *Server
}

func (s stopBeforeAdd) Add(c ServerCodec) bool {
	s.srv.Stop()
	return s.Set.Add(c)
}

func TestServeCodecRegisteredAfterStopIsClosed(t *testing.T) {
	t.Parallel()
	server := NewServer(1, false, false, true, log.New(), 0)
	server.codecs = stopBeforeAdd{Set: server.codecs, srv: server}

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	done := make(chan struct{})
	go func() {
		server.ServeCodec(NewCodec(serverConn), 0)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("connection registered during Stop is still served after Stop returned")
	}
}
