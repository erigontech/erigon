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
	"math"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/aa"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

// EthBackendAPIVersion
// 2.0.0 - move all mining-related methods to 'txpool/mining' server
// 2.1.0 - add NetPeerCount function
// 2.2.0 - add NodesInfo function
// 3.0.0 - adding PoS interfaces
// 3.1.0 - add Subscribe to logs
// 3.2.0 - add EngineGetBlobsBundleV1k
// 3.3.0 - merge EngineGetBlobsBundleV1 into EngineGetPayload
var EthBackendAPIVersion = &types2.VersionReply{Major: 3, Minor: 3, Patch: 0}

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	ctx                   context.Context
	eth                   EthBackend
	notifications         *shards.Notifications
	db                    kv.RoDB
	blockReader           services.FullBlockReader
	bridgeStore           bridge.Store
	latestBlockBuiltStore *builder.LatestBlockBuiltStore

	logsFilter  *LogsFilterAggregator
	logger      log.Logger
	chainConfig *chain.Config
}

type EthBackend interface {
	Etherbase() (common.Address, error)
	NetVersion() (uint64, error)
	NetPeerCount() (uint64, error)
	NodesInfo(limit int) (*remote.NodesInfoReply, error)
	Peers(ctx context.Context) (*remote.PeersReply, error)
	AddPeer(ctx context.Context, url *remote.AddPeerRequest) (*remote.AddPeerReply, error)
}

func NewEthBackendServer(ctx context.Context, eth EthBackend, db kv.RwDB, notifications *shards.Notifications, blockReader services.FullBlockReader,
	bridgeStore bridge.Store, logger log.Logger, latestBlockBuiltStore *builder.LatestBlockBuiltStore, chainConfig *chain.Config,
) *EthBackendServer {
	s := &EthBackendServer{
		ctx:                   ctx,
		eth:                   eth,
		notifications:         notifications,
		db:                    db,
		blockReader:           blockReader,
		bridgeStore:           bridgeStore,
		logsFilter:            NewLogsFilterAggregator(notifications.Events),
		logger:                logger,
		latestBlockBuiltStore: latestBlockBuiltStore,
		chainConfig:           chainConfig,
	}

	ch, clean := s.notifications.Events.AddLogsSubscription()
	go func() {
		var err error
		defer clean()
		defer func() {
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Warn("[rpc] terminated subscription to `logs` events", "reason", err)
				}
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

func (s *EthBackendServer) Syncing(ctx context.Context, _ *emptypb.Empty) (*remote.SyncingReply, error) {
	highestBlock := s.notifications.LastNewBlockSeen.Load()
	frozenBlocks := s.blockReader.FrozenBlocks()

	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	currentBlock, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return nil, err
	}

	if highestBlock < frozenBlocks {
		highestBlock = frozenBlocks
	}

	reply := &remote.SyncingReply{
		CurrentBlock:     currentBlock,
		FrozenBlocks:     frozenBlocks,
		LastNewBlockSeen: highestBlock,
		Syncing:          true,
	}

	// Maybe it is still downloading snapshots. Impossible to determine the highest block.
	if highestBlock == 0 {
		return reply, nil
	}

	// If the distance between the current block and the highest block is less than the reorg range, we are not syncing. abs(highestBlock - currentBlock) < reorgRange
	reorgRange := 8
	if math.Abs(float64(highestBlock)-float64(currentBlock)) < float64(reorgRange) {
		reply.Syncing = false
		return reply, nil
	}

	reply.Stages = make([]*remote.SyncingReply_StageProgress, len(stages.AllStages))
	for i, stage := range stages.AllStages {
		progress, err := stages.GetStageProgress(tx, stage)
		if err != nil {
			return nil, err
		}
		reply.Stages[i] = &remote.SyncingReply_StageProgress{}
		reply.Stages[i].StageName = string(stage)
		reply.Stages[i].BlockNumber = progress
	}

	return reply, nil
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
	out := &remote.EtherbaseReply{Address: gointerfaces.ConvertAddressToH160(common.Address{})}

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
	s.logger.Debug("[rpc] new subscription to `newHeaders` events")
	ch, clean := s.notifications.Events.AddHeaderSubscription()
	defer clean()
	newSnCh, newSnClean := s.notifications.Events.AddNewSnapshotSubscription()
	defer newSnClean()
	defer func() {
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.logger.Warn("[rpc] terminated subscription to `newHeaders` events", "reason", err)
			}
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
	return &remote.ProtocolVersionReply{Id: direct.ETH67}, nil
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

	blockNum, txNum, ok, err := s.blockReader.TxnLookup(ctx, tx, gointerfaces.ConvertH256ToHash(req.TxnHash))
	if err != nil {
		return nil, err
	}
	if !ok {
		// Not a perfect solution, assumes there are no transactions in block 0
		return &remote.TxnLookupReply{BlockNumber: 0, TxNumber: txNum}, nil
	}
	return &remote.TxnLookupReply{BlockNumber: blockNum, TxNumber: txNum}, nil
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

	hash, ok, err := s.blockReader.CanonicalHash(ctx, tx, req.BlockNumber)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
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

func (s *EthBackendServer) BorTxnLookup(ctx context.Context, req *remote.BorTxnLookupRequest) (*remote.BorTxnLookupReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// otherwise this may be a bor state sync transaction - check
	blockNum, ok, err := s.bridgeStore.EventTxnToBlockNum(ctx, gointerfaces.ConvertH256ToHash(req.BorTxHash))
	if err != nil {
		return nil, err
	}
	return &remote.BorTxnLookupReply{
		BlockNumber: blockNum,
		Present:     ok,
	}, nil
}

func (s *EthBackendServer) BorEvents(ctx context.Context, req *remote.BorEventsRequest) (*remote.BorEventsReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	events, err := s.bridgeStore.EventsByBlock(ctx, gointerfaces.ConvertH256ToHash(req.BlockHash), req.BlockNum)
	if err != nil {
		return nil, err
	}

	eventsRaw := make([][]byte, len(events))
	for i, event := range events {
		eventsRaw[i] = event
	}

	return &remote.BorEventsReply{
		EventRlps: eventsRaw,
	}, nil
}

func (s *EthBackendServer) AAValidation(ctx context.Context, req *remote.AAValidationRequest) (*remote.AAValidationReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	currentBlock, err := s.blockReader.CurrentBlock(tx)
	if err != nil {
		return nil, err
	}
	header := currentBlock.HeaderNoCopy()

	aaTxn := types.FromProto(req.Tx)
	stateReader := state.NewHistoryReaderV3()
	stateReader.SetTx(tx.(kv.TemporalTx))

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, s.blockReader))
	maxTxNum, err := txNumsReader.Max(tx, header.Number.Uint64())
	if err != nil {
		return nil, err
	}

	stateReader.SetTxNum(maxTxNum)
	ibs := state.New(stateReader)

	blockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, nil), nil, &common.Address{}, s.chainConfig)

	senderCodeSize, err := ibs.GetCodeSize(*aaTxn.SenderAddress)
	if err != nil {
		return nil, err
	}

	validationTracer := aa.NewValidationRulesTracer(*aaTxn.SenderAddress, senderCodeSize != 0)
	evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, s.chainConfig, vm.Config{Tracer: validationTracer.Hooks(), ReadOnly: true})
	ibs.SetHooks(validationTracer.Hooks())

	vmConfig := evm.Config()
	rules := s.chainConfig.Rules(header.Number.Uint64(), header.Time)
	hasEIP3860 := vmConfig.HasEip3860(rules)

	preTxCost, err := aaTxn.PreTransactionGasCost(rules, hasEIP3860)
	if err != nil {
		return nil, err
	}

	totalGasLimit := preTxCost + aaTxn.ValidationGasLimit + aaTxn.PaymasterValidationGasLimit + aaTxn.GasLimit + aaTxn.PostOpGasLimit
	_, _, err = aa.ValidateAATransaction(aaTxn, ibs, new(core.GasPool).AddGas(totalGasLimit), header, evm, s.chainConfig)
	if err != nil {
		log.Info("RIP-7560 validation err", "err", err.Error())
		return &remote.AAValidationReply{Valid: false}, nil
	}

	return &remote.AAValidationReply{Valid: validationTracer.Err() == nil}, nil
}
