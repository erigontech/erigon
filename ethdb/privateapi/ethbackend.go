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

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/builder"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
)

type bridgeReader interface {
	Events(ctx context.Context, blockNum uint64) ([]*types.Message, error)
	EventTxnLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error)
}

// EthBackendAPIVersion
// 2.0.0 - move all mining-related methods to 'txpool/mining' server
// 2.1.0 - add NetPeerCount function
// 2.2.0 - add NodesInfo function
// 3.0.0 - adding PoS interfaces
// 3.1.0 - add Subscribe to logs
// 3.2.0 - add EngineGetBlobsBundleV1
// 3.3.0 - merge EngineGetBlobsBundleV1 into EngineGetPayload
var EthBackendAPIVersion = &types2.VersionReply{Major: 3, Minor: 3, Patch: 0}

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	ctx                   context.Context
	eth                   EthBackend
	events                *shards.Events
	db                    kv.RoDB
	blockReader           services.FullBlockReader
	bridgeReader          bridgeReader
	latestBlockBuiltStore *builder.LatestBlockBuiltStore

	logsFilter *LogsFilterAggregator
	logger     log.Logger
}

type EthBackend interface {
	Etherbase() (libcommon.Address, error)
	NetVersion() (uint64, error)
	NetPeerCount() (uint64, error)
	NodesInfo(limit int) (*remote.NodesInfoReply, error)
	Peers(ctx context.Context) (*remote.PeersReply, error)
	AddPeer(ctx context.Context, url *remote.AddPeerRequest) (*remote.AddPeerReply, error)
}

func NewEthBackendServer(ctx context.Context, eth EthBackend, db kv.RwDB, events *shards.Events, blockReader services.FullBlockReader,
	logger log.Logger, latestBlockBuiltStore *builder.LatestBlockBuiltStore, bridgeReader bridgeReader,
) *EthBackendServer {
	s := &EthBackendServer{
		ctx:                   ctx,
		eth:                   eth,
		events:                events,
		db:                    db,
		blockReader:           blockReader,
		bridgeReader:          bridgeReader,
		logsFilter:            NewLogsFilterAggregator(events),
		logger:                logger,
		latestBlockBuiltStore: latestBlockBuiltStore,
	}

	ch, clean := s.events.AddLogsSubscription()
	go func() {
		var err error
		defer clean()
		logger.Info("new subscription to logs established")
		defer func() {
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Warn("subscription to logs closed", "reason", err)
				}
			} else {
				logger.Warn("subscription to logs closed")
			}
		}()
		for {
			select {
			case <-s.ctx.Done():
				err = s.ctx.Err()
				return
			case logs := <-ch:
				s.logsFilter.distributeLogs(logs)
			}
		}
	}()
	return s
}

func (s *EthBackendServer) Version(context.Context, *emptypb.Empty) (*types2.VersionReply, error) {
	return EthBackendAPIVersion, nil
}

func (s *EthBackendServer) PendingBlock(ctx context.Context, _ *emptypb.Empty) (*remote.PendingBlockReply, error) {
	pendingBlock := s.latestBlockBuiltStore.BlockBuilt()
	if pendingBlock == nil {
		tx, err := s.db.BeginRo(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		// use latest
		pendingBlock, err = s.blockReader.CurrentBlock(tx)
		if err != nil {
			return nil, err
		}
	}

	blockRlp, err := rlp.EncodeToBytes(pendingBlock)
	if err != nil {
		return nil, err
	}

	return &remote.PendingBlockReply{BlockRlp: blockRlp}, nil
}

func (s *EthBackendServer) Etherbase(_ context.Context, _ *remote.EtherbaseRequest) (*remote.EtherbaseReply, error) {
	out := &remote.EtherbaseReply{Address: gointerfaces.ConvertAddressToH160(libcommon.Address{})}

	base, err := s.eth.Etherbase()
	if err != nil {
		return out, err
	}

	out.Address = gointerfaces.ConvertAddressToH160(base)
	return out, nil
}

func (s *EthBackendServer) NetVersion(_ context.Context, _ *remote.NetVersionRequest) (*remote.NetVersionReply, error) {
	id, err := s.eth.NetVersion()
	if err != nil {
		return &remote.NetVersionReply{}, err
	}
	return &remote.NetVersionReply{Id: id}, nil
}

func (s *EthBackendServer) NetPeerCount(_ context.Context, _ *remote.NetPeerCountRequest) (*remote.NetPeerCountReply, error) {
	id, err := s.eth.NetPeerCount()
	if err != nil {
		return &remote.NetPeerCountReply{}, err
	}
	return &remote.NetPeerCountReply{Count: id}, nil
}

func (s *EthBackendServer) Subscribe(r *remote.SubscribeRequest, subscribeServer remote.ETHBACKEND_SubscribeServer) (err error) {
	s.logger.Debug("Establishing event subscription channel with the RPC daemon ...")
	ch, clean := s.events.AddHeaderSubscription()
	defer clean()
	newSnCh, newSnClean := s.events.AddNewSnapshotSubscription()
	defer newSnClean()
	s.logger.Info("new subscription to newHeaders established")
	defer func() {
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.logger.Warn("subscription to newHeaders closed", "reason", err)
			}
		} else {
			s.logger.Warn("subscription to newHeaders closed")
		}
	}()
	_ = subscribeServer.Send(&remote.SubscribeReply{Type: remote.Event_NEW_SNAPSHOT})
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-subscribeServer.Context().Done():
			return subscribeServer.Context().Err()
		case headersRlp := <-ch:
			for _, headerRlp := range headersRlp {
				if err = subscribeServer.Send(&remote.SubscribeReply{
					Type: remote.Event_HEADER,
					Data: headerRlp,
				}); err != nil {
					return err
				}
			}
		case <-newSnCh:
			if err = subscribeServer.Send(&remote.SubscribeReply{Type: remote.Event_NEW_SNAPSHOT}); err != nil {
				return err
			}
		}
	}
}

func (s *EthBackendServer) ProtocolVersion(_ context.Context, _ *remote.ProtocolVersionRequest) (*remote.ProtocolVersionReply, error) {
	return &remote.ProtocolVersionReply{Id: direct.ETH66}, nil
}

func (s *EthBackendServer) ClientVersion(_ context.Context, _ *remote.ClientVersionRequest) (*remote.ClientVersionReply, error) {
	return &remote.ClientVersionReply{NodeName: common.MakeName("erigon", params.Version)}, nil
}

func (s *EthBackendServer) TxnLookup(ctx context.Context, req *remote.TxnLookupRequest) (*remote.TxnLookupReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, ok, err := s.blockReader.TxnLookup(ctx, tx, gointerfaces.ConvertH256ToHash(req.TxnHash))
	if err != nil {
		return nil, err
	}
	if !ok {
		// Not a perfect solution, assumes there are no transactions in block 0
		return &remote.TxnLookupReply{BlockNumber: 0}, nil
	}
	return &remote.TxnLookupReply{BlockNumber: blockNum}, nil
}

func (s *EthBackendServer) Block(ctx context.Context, req *remote.BlockRequest) (*remote.BlockReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block, senders, err := s.blockReader.BlockWithSenders(ctx, tx, gointerfaces.ConvertH256ToHash(req.BlockHash), req.BlockHeight)
	if err != nil {
		return nil, err
	}
	blockRlp, err := rlp.EncodeToBytes(block)
	if err != nil {
		return nil, err
	}
	sendersBytes := make([]byte, 20*len(senders))
	for i, sender := range senders {
		copy(sendersBytes[i*20:], sender[:])
	}
	return &remote.BlockReply{BlockRlp: blockRlp, Senders: sendersBytes}, nil
}

func (s *EthBackendServer) CanonicalBodyForStorage(ctx context.Context, req *remote.CanonicalBodyForStorageRequest) (*remote.CanonicalBodyForStorageReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bd, err := s.blockReader.CanonicalBodyForStorage(ctx, tx, req.BlockNumber)
	if err != nil {
		return nil, err
	}
	if bd == nil {
		return &remote.CanonicalBodyForStorageReply{}, nil
	}
	b := bytes.Buffer{}
	if err := bd.EncodeRLP(&b); err != nil {
		return nil, err
	}
	return &remote.CanonicalBodyForStorageReply{Body: b.Bytes()}, nil
}

func (s *EthBackendServer) CanonicalHash(ctx context.Context, req *remote.CanonicalHashRequest) (*remote.CanonicalHashReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hash, err := s.blockReader.CanonicalHash(ctx, tx, req.BlockNumber)
	if err != nil {
		return nil, err
	}
	return &remote.CanonicalHashReply{Hash: gointerfaces.ConvertHashToH256(hash)}, nil
}

func (s *EthBackendServer) HeaderNumber(ctx context.Context, req *remote.HeaderNumberRequest) (*remote.HeaderNumberReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	headerNum, err := s.blockReader.HeaderNumber(ctx, tx, gointerfaces.ConvertH256ToHash(req.Hash))
	if err != nil {
		return nil, err
	}

	if headerNum == nil {
		return &remote.HeaderNumberReply{}, nil
	}
	return &remote.HeaderNumberReply{Number: headerNum}, nil
}

func (s *EthBackendServer) NodeInfo(_ context.Context, r *remote.NodesInfoRequest) (*remote.NodesInfoReply, error) {
	nodesInfo, err := s.eth.NodesInfo(int(r.Limit))
	if err != nil {
		return nil, err
	}
	return nodesInfo, nil
}

func (s *EthBackendServer) Peers(ctx context.Context, _ *emptypb.Empty) (*remote.PeersReply, error) {
	return s.eth.Peers(ctx)
}

func (s *EthBackendServer) AddPeer(ctx context.Context, req *remote.AddPeerRequest) (*remote.AddPeerReply, error) {
	return s.eth.AddPeer(ctx, req)
}

func (s *EthBackendServer) SubscribeLogs(server remote.ETHBACKEND_SubscribeLogsServer) (err error) {
	if s.logsFilter != nil {
		return s.logsFilter.subscribeLogs(server)
	}
	return errors.New("no logs filter available")
}

func (s *EthBackendServer) BorTxnLookup(ctx context.Context, req *types2.BorTxnLookupRequest) (*types2.BorTxnLookupReply, error) {
	if s.bridgeReader != nil {
		blockNum, ok, err := s.bridgeReader.EventTxnLookup(ctx, gointerfaces.ConvertH256ToHash(req.BorTxHash))
		if err != nil {
			return nil, err
		}

		return &types2.BorTxnLookupReply{
			Present:     ok,
			BlockNumber: blockNum,
		}, nil
	}

	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, ok, err := s.blockReader.EventLookup(ctx, tx, gointerfaces.ConvertH256ToHash(req.BorTxHash))
	if err != nil {
		return nil, err
	}
	return &types2.BorTxnLookupReply{
		BlockNumber: blockNum,
		Present:     ok,
	}, nil
}

func (s *EthBackendServer) BorEvents(ctx context.Context, req *types2.BorEventsRequest) (*types2.BorEventsReply, error) {
	if s.bridgeReader != nil {
		events, err := s.bridgeReader.Events(ctx, req.BlockNum)
		if err != nil {
			return nil, err
		}

		eventsRaw := make([][]byte, len(events))
		for i, event := range events {
			eventsRaw[i] = event.Data()
		}

		return &types2.BorEventsReply{
			EventRlps: eventsRaw,
		}, nil
	}

	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	events, err := s.blockReader.EventsByBlock(ctx, tx, gointerfaces.ConvertH256ToHash(req.BlockHash), req.BlockNum)
	if err != nil {
		return nil, err
	}

	eventsRaw := make([][]byte, len(events))
	for i, event := range events {
		eventsRaw[i] = event
	}

	return &types2.BorEventsReply{
		EventRlps: eventsRaw,
	}, nil
}
