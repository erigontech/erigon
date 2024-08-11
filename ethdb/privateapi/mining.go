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

package privateapi

import (
	"bytes"
	"context"
	"sync"

	"google.golang.org/protobuf/types/known/emptypb"

	proto_txpool "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rlp"
)

// MiningAPIVersion
// 2.0.0 - move all mining-related methods to 'txpool/mining' server
var MiningAPIVersion = &types2.VersionReply{Major: 1, Minor: 0, Patch: 0}

type MiningServer struct {
	proto_txpool.UnimplementedMiningServer
	ctx                 context.Context
	pendingLogsStreams  PendingLogsStreams
	pendingBlockStreams PendingBlockStreams
	minedBlockStreams   MinedBlockStreams
	isMining            IsMining
	logger              log.Logger
}

type IsMining interface {
	IsMining() bool
}

func NewMiningServer(ctx context.Context, isMining IsMining, logger log.Logger) *MiningServer {
	return &MiningServer{ctx: ctx, isMining: isMining, logger: logger}
}

func (s *MiningServer) Version(context.Context, *emptypb.Empty) (*types2.VersionReply, error) {
	return MiningAPIVersion, nil
}

func (s *MiningServer) OnPendingLogs(req *proto_txpool.OnPendingLogsRequest, reply proto_txpool.Mining_OnPendingLogsServer) error {
	remove := s.pendingLogsStreams.Add(reply)
	defer remove()
	<-reply.Context().Done()
	return reply.Context().Err()
}

func (s *MiningServer) BroadcastPendingLogs(l types.Logs) error {
	b, err := rlp.EncodeToBytes(l)
	if err != nil {
		return err
	}
	reply := &proto_txpool.OnPendingBlockReply{RplBlock: b}
	s.pendingBlockStreams.Broadcast(reply, s.logger)
	return nil
}

func (s *MiningServer) OnPendingBlock(req *proto_txpool.OnPendingBlockRequest, reply proto_txpool.Mining_OnPendingBlockServer) error {
	remove := s.pendingBlockStreams.Add(reply)
	defer remove()
	select {
	case <-s.ctx.Done():
		return nil
	case <-reply.Context().Done():
		return nil
	}
}

func (s *MiningServer) BroadcastPendingBlock(block *types.Block) error {
	var buf bytes.Buffer
	if err := block.EncodeRLP(&buf); err != nil {
		return err
	}
	reply := &proto_txpool.OnPendingBlockReply{RplBlock: buf.Bytes()}
	s.pendingBlockStreams.Broadcast(reply, s.logger)
	return nil
}

func (s *MiningServer) OnMinedBlock(req *proto_txpool.OnMinedBlockRequest, reply proto_txpool.Mining_OnMinedBlockServer) error {
	remove := s.minedBlockStreams.Add(reply)
	defer remove()
	<-reply.Context().Done()
	return reply.Context().Err()
}

func (s *MiningServer) BroadcastMinedBlock(block *types.Block) error {
	s.logger.Debug("BroadcastMinedBlock", "block hash", block.Hash(), "block number", block.Number(), "root", block.Root(), "gas", block.GasUsed())
	var buf bytes.Buffer
	if err := block.EncodeRLP(&buf); err != nil {
		return err
	}
	reply := &proto_txpool.OnMinedBlockReply{RplBlock: buf.Bytes()}
	s.minedBlockStreams.Broadcast(reply, s.logger)
	return nil
}

// MinedBlockStreams - it's safe to use this class as non-pointer
type MinedBlockStreams struct {
	chans  map[uint]proto_txpool.Mining_OnMinedBlockServer
	id     uint
	mu     sync.Mutex
	logger log.Logger
}

func (s *MinedBlockStreams) Add(stream proto_txpool.Mining_OnMinedBlockServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chans == nil {
		s.chans = make(map[uint]proto_txpool.Mining_OnMinedBlockServer)
	}
	s.id++
	id := s.id
	s.chans[id] = stream
	return func() { s.remove(id) }
}

func (s *MinedBlockStreams) Broadcast(reply *proto_txpool.OnMinedBlockReply, logger log.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, stream := range s.chans {
		err := stream.Send(reply)
		if err != nil {
			logger.Trace("failed send to mined block stream", "err", err)
			select {
			case <-stream.Context().Done():
				delete(s.chans, id)
			default:
			}
		}
	}
}

func (s *MinedBlockStreams) remove(id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chans[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.chans, id)
}

// PendingBlockStreams - it's safe to use this class as non-pointer
type PendingBlockStreams struct {
	chans map[uint]proto_txpool.Mining_OnPendingBlockServer
	mu    sync.Mutex
	id    uint
}

func (s *PendingBlockStreams) Add(stream proto_txpool.Mining_OnPendingBlockServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chans == nil {
		s.chans = make(map[uint]proto_txpool.Mining_OnPendingBlockServer)
	}
	s.id++
	id := s.id
	s.chans[id] = stream
	return func() { s.remove(id) }
}

func (s *PendingBlockStreams) Broadcast(reply *proto_txpool.OnPendingBlockReply, logger log.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, stream := range s.chans {
		err := stream.Send(reply)
		if err != nil {
			logger.Trace("failed send to mined block stream", "err", err)
			select {
			case <-stream.Context().Done():
				delete(s.chans, id)
			default:
			}
		}
	}
}

func (s *PendingBlockStreams) remove(id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chans[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.chans, id)
}

// PendingLogsStreams - it's safe to use this class as non-pointer
type PendingLogsStreams struct {
	chans map[uint]proto_txpool.Mining_OnPendingLogsServer
	mu    sync.Mutex
	id    uint
}

func (s *PendingLogsStreams) Add(stream proto_txpool.Mining_OnPendingLogsServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chans == nil {
		s.chans = make(map[uint]proto_txpool.Mining_OnPendingLogsServer)
	}
	s.id++
	id := s.id
	s.chans[id] = stream
	return func() { s.remove(id) }
}

func (s *PendingLogsStreams) Broadcast(reply *proto_txpool.OnPendingLogsReply, logger log.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, stream := range s.chans {
		err := stream.Send(reply)
		if err != nil {
			logger.Trace("failed send to mined block stream", "err", err)
			select {
			case <-stream.Context().Done():
				delete(s.chans, id)
			default:
			}
		}
	}
}

func (s *PendingLogsStreams) remove(id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chans[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.chans, id)
}
