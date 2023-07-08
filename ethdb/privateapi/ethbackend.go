package privateapi

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

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
}

func NewEthBackendServer(ctx context.Context, eth EthBackend, db kv.RwDB, events *shards.Events, blockReader services.FullBlockReader,
	logger log.Logger, latestBlockBuiltStore *builder.LatestBlockBuiltStore,
) *EthBackendServer {
	s := &EthBackendServer{ctx: ctx, eth: eth, events: events, db: db, blockReader: blockReader,
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

func (s *EthBackendServer) PendingBlock(_ context.Context, _ *emptypb.Empty) (*remote.PendingBlockReply, error) {
	pendingBlock := s.latestBlockBuiltStore.BlockBuilt()
	if pendingBlock == nil {
		return nil, nil
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

func (s *EthBackendServer) SubscribeLogs(server remote.ETHBACKEND_SubscribeLogsServer) (err error) {
	if s.logsFilter != nil {
		return s.logsFilter.subscribeLogs(server)
	}
	return fmt.Errorf("no logs filter available")
}
