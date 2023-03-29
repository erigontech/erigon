// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rpc

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	mapset "github.com/deckarep/golang-set"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/log/v3"
)

const MetadataApi = "rpc"

// CodecOption specifies which type of messages a codec supports.
//
// Deprecated: this option is no longer honored by Server.
type CodecOption int

const (
	// OptionMethodInvocation is an indication that the codec supports RPC method calls
	OptionMethodInvocation CodecOption = 1 << iota

	// OptionSubscriptions is an indication that the codec supports RPC notifications
	OptionSubscriptions = 1 << iota // support pub sub
)

// Server is an RPC server.
type Server struct {
	services        serviceRegistry
	methodAllowList AllowList
	idgen           func() ID
	run             int32
	codecs          mapset.Set

	batchConcurrency uint
	disableStreaming bool
	traceRequests    bool // Whether to print requests at INFO level
	batchLimit       int  // Maximum number of requests in a batch
}

// NewServer creates a new server instance with no registered handlers.
func NewServer(batchConcurrency uint, traceRequests, disableStreaming bool) *Server {
	server := &Server{idgen: randomIDGenerator(), codecs: mapset.NewSet(), run: 1, batchConcurrency: batchConcurrency, disableStreaming: disableStreaming, traceRequests: traceRequests}
	// Register the default service providing meta information about the RPC service such
	// as the services and methods it offers.
	rpcService := &RPCService{server: server}
	server.RegisterName(MetadataApi, rpcService)
	return server
}

// SetAllowList sets the allow list for methods that are handled by this server
func (s *Server) SetAllowList(allowList AllowList) {
	s.methodAllowList = allowList
}

// SetBatchLimit sets limit of number of requests in a batch
func (s *Server) SetBatchLimit(limit int) {
	s.batchLimit = limit
}

// RegisterName creates a service for the given receiver type under the given name. When no
// methods on the given receiver match the criteria to be either a RPC method or a
// subscription an error is returned. Otherwise a new service is created and added to the
// service collection this server provides to clients.
func (s *Server) RegisterName(name string, receiver interface{}) error {
	return s.services.registerName(name, receiver)
}

// ServeCodec reads incoming requests from codec, calls the appropriate callback and writes
// the response back using the given codec. It will block until the codec is closed or the
// server is stopped. In either case the codec is closed.
//
// Note that codec options are no longer supported.
func (s *Server) ServeCodec(codec ServerCodec, options CodecOption) {
	defer codec.close()

	// Don't serve if server is stopped.
	if atomic.LoadInt32(&s.run) == 0 {
		return
	}

	// Add the codec to the set so it can be closed by Stop.
	s.codecs.Add(codec)
	defer s.codecs.Remove(codec)

	c := initClient(codec, s.idgen, &s.services)
	<-codec.closed()
	c.Close()
}

// serveSingleRequest reads and processes a single RPC request from the given codec. This
// is used to serve HTTP connections. Subscriptions and reverse calls are not allowed in
// this mode.
func (s *Server) serveSingleRequest(ctx context.Context, codec ServerCodec, stream *jsoniter.Stream) {
	// Don't serve if server is stopped.
	if atomic.LoadInt32(&s.run) == 0 {
		return
	}

	h := newHandler(ctx, codec, s.idgen, &s.services, s.methodAllowList, s.batchConcurrency, s.traceRequests)
	h.allowSubscribe = false
	defer h.close(io.EOF, nil)

	reqs, batch, err := codec.readBatch()
	if err != nil {
		if err != io.EOF {
			codec.writeJSON(ctx, errorMessage(&invalidMessageError{"parse error"}))
		}
		return
	}
	if batch {
		if s.batchLimit > 0 && len(reqs) > s.batchLimit {
			codec.writeJSON(ctx, errorMessage(fmt.Errorf("batch limit %d exceeded (can increase by --rpc.batch.limit). Requested batch of size: %d", s.batchLimit, len(reqs))))
		} else {
			h.handleBatch(reqs)
		}
	} else {
		h.handleMsg(reqs[0], stream)
	}
}

// Stop stops reading new requests, waits for stopPendingRequestTimeout to allow pending
// requests to finish, then closes all codecs which will cancel pending requests and
// subscriptions.
func (s *Server) Stop() {
	if atomic.CompareAndSwapInt32(&s.run, 1, 0) {
		log.Info("RPC server shutting down")
		s.codecs.Each(func(c interface{}) bool {
			c.(ServerCodec).close()
			return true
		})
	}
}

// RPCService gives meta information about the server.
// e.g. gives information about the loaded modules.
type RPCService struct {
	server *Server
}

// Modules returns the list of RPC services with their version number
func (s *RPCService) Modules() map[string]string {
	s.server.services.mu.Lock()
	defer s.server.services.mu.Unlock()

	modules := make(map[string]string)
	for name := range s.server.services.services {
		modules[name] = "1.0"
	}
	return modules
}
