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
	"errors"
	"sync"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/consensus/ethash"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

// MiningAPIVersion
// 2.0.0 - move all mining-related methods to 'txpool/mining' server
var MiningAPIVersion = &typesproto.VersionReply{Major: 1, Minor: 0, Patch: 0}

type MiningServer struct {
	txpoolproto.UnimplementedMiningServer
	ctx                 context.Context
	pendingLogsStreams  PendingLogsStreams
	pendingBlockStreams PendingBlockStreams
	minedBlockStreams   MinedBlockStreams
	ethash              *ethash.API
	isMining            IsMining
	logger              log.Logger
}

type IsMining interface {
	IsMining() bool
}

func NewMiningServer(ctx context.Context, isMining IsMining, ethashApi *ethash.API, logger log.Logger) *MiningServer {
	return &MiningServer{ctx: ctx, isMining: isMining, ethash: ethashApi, logger: logger}
}

func (s *MiningServer) Version(context.Context, *emptypb.Empty) (*typesproto.VersionReply, error) {
	return MiningAPIVersion, nil
}

func (s *MiningServer) GetWork(context.Context, *txpoolproto.GetWorkRequest) (*txpoolproto.GetWorkReply, error) {
	if s.ethash == nil {
		return nil, errors.New("not supported, consensus engine is not ethash")
	}
	res, err := s.ethash.GetWork()
	if err != nil {
		return nil, err
	}
	return &txpoolproto.GetWorkReply{HeaderHash: res[0], SeedHash: res[1], Target: res[2], BlockNumber: res[3]}, nil
}

func (s *MiningServer) SubmitWork(_ context.Context, req *txpoolproto.SubmitWorkRequest) (*txpoolproto.SubmitWorkReply, error) {
	if s.ethash == nil {
		return nil, errors.New("not supported, consensus engine is not ethash")
	}
	var nonce types.BlockNonce
	copy(nonce[:], req.BlockNonce)
	ok := s.ethash.SubmitWork(nonce, common.BytesToHash(req.PowHash), common.BytesToHash(req.Digest))
	return &txpoolproto.SubmitWorkReply{Ok: ok}, nil
}

func (s *MiningServer) SubmitHashRate(_ context.Context, req *txpoolproto.SubmitHashRateRequest) (*txpoolproto.SubmitHashRateReply, error) {
	if s.ethash == nil {
		return nil, errors.New("not supported, consensus engine is not ethash")
	}
	ok := s.ethash.SubmitHashRate(hexutil.Uint64(req.Rate), common.BytesToHash(req.Id))
	return &txpoolproto.SubmitHashRateReply{Ok: ok}, nil
}

func (s *MiningServer) GetHashRate(_ context.Context, req *txpoolproto.HashRateRequest) (*txpoolproto.HashRateReply, error) {
	if s.ethash == nil {
		return nil, errors.New("not supported, consensus engine is not ethash")
	}
	return &txpoolproto.HashRateReply{HashRate: s.ethash.GetHashrate()}, nil
}

func (s *MiningServer) Mining(_ context.Context, req *txpoolproto.MiningRequest) (*txpoolproto.MiningReply, error) {
	if s.ethash == nil {
		return nil, errors.New("not supported, consensus engine is not ethash")
	}
	return &txpoolproto.MiningReply{Enabled: s.isMining.IsMining(), Running: true}, nil
}

func (s *MiningServer) OnPendingLogs(req *txpoolproto.OnPendingLogsRequest, reply txpoolproto.Mining_OnPendingLogsServer) error {
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
	reply := &txpoolproto.OnPendingBlockReply{RplBlock: b}
	s.pendingBlockStreams.Broadcast(reply, s.logger)
	return nil
}

func (s *MiningServer) OnPendingBlock(req *txpoolproto.OnPendingBlockRequest, reply txpoolproto.Mining_OnPendingBlockServer) error {
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
	reply := &txpoolproto.OnPendingBlockReply{RplBlock: buf.Bytes()}
	s.pendingBlockStreams.Broadcast(reply, s.logger)
	return nil
}

func (s *MiningServer) OnMinedBlock(req *txpoolproto.OnMinedBlockRequest, reply txpoolproto.Mining_OnMinedBlockServer) error {
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
	reply := &txpoolproto.OnMinedBlockReply{RplBlock: buf.Bytes()}
	s.minedBlockStreams.Broadcast(reply, s.logger)
	return nil
}

// MinedBlockStreams - it's safe to use this class as non-pointer
type MinedBlockStreams struct {
	chans  map[uint]txpoolproto.Mining_OnMinedBlockServer
	id     uint
	mu     sync.Mutex
	logger log.Logger
}

func (s *MinedBlockStreams) Add(stream txpoolproto.Mining_OnMinedBlockServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chans == nil {
		s.chans = make(map[uint]txpoolproto.Mining_OnMinedBlockServer)
	}
	s.id++
	id := s.id
	s.chans[id] = stream
	return func() { s.remove(id) }
}

func (s *MinedBlockStreams) Broadcast(reply *txpoolproto.OnMinedBlockReply, logger log.Logger) {
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
	chans map[uint]txpoolproto.Mining_OnPendingBlockServer
	mu    sync.Mutex
	id    uint
}

func (s *PendingBlockStreams) Add(stream txpoolproto.Mining_OnPendingBlockServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chans == nil {
		s.chans = make(map[uint]txpoolproto.Mining_OnPendingBlockServer)
	}
	s.id++
	id := s.id
	s.chans[id] = stream
	return func() { s.remove(id) }
}

func (s *PendingBlockStreams) Broadcast(reply *txpoolproto.OnPendingBlockReply, logger log.Logger) {
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
	chans map[uint]txpoolproto.Mining_OnPendingLogsServer
	mu    sync.Mutex
	id    uint
}

func (s *PendingLogsStreams) Add(stream txpoolproto.Mining_OnPendingLogsServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chans == nil {
		s.chans = make(map[uint]txpoolproto.Mining_OnPendingLogsServer)
	}
	s.id++
	id := s.id
	s.chans[id] = stream
	return func() { s.remove(id) }
}

func (s *PendingLogsStreams) Broadcast(reply *txpoolproto.OnPendingLogsReply, logger log.Logger) {
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
