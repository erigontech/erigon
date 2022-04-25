package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ApiBackend - interface which must be used by API layer
// implementation can work with local Ethereum object or with Remote (grpc-based) one
// this is reason why all methods are accepting context and returning error
type ApiBackend interface {
	Etherbase(ctx context.Context) (common.Address, error)
	NetVersion(ctx context.Context) (uint64, error)
	NetPeerCount(ctx context.Context) (uint64, error)
	ProtocolVersion(ctx context.Context) (uint64, error)
	ClientVersion(ctx context.Context) (string, error)
	Subscribe(ctx context.Context, cb func(*remote.SubscribeReply)) error
	SubscribeLogs(ctx context.Context, cb func(*remote.SubscribeLogsReply), requestor *atomic.Value) error
	BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error)
	EngineNewPayloadV1(ctx context.Context, payload *types2.ExecutionPayload) (*remote.EnginePayloadStatus, error)
	EngineForkchoiceUpdatedV1(ctx context.Context, request *remote.EngineForkChoiceUpdatedRequest) (*remote.EngineForkChoiceUpdatedReply, error)
	EngineGetPayloadV1(ctx context.Context, payloadId uint64) (*types2.ExecutionPayload, error)
	NodeInfo(ctx context.Context, limit uint32) ([]p2p.NodeInfo, error)
	Peers(ctx context.Context) ([]*p2p.PeerInfo, error)
}

type RemoteBackend struct {
	remoteEthBackend remote.ETHBACKENDClient
	log              log.Logger
	version          gointerfaces.Version
	db               kv.RoDB
	blockReader      interfaces.BlockAndTxnReader
}

func NewRemoteBackend(client remote.ETHBACKENDClient, db kv.RoDB, blockReader interfaces.BlockAndTxnReader) *RemoteBackend {
	return &RemoteBackend{
		remoteEthBackend: client,
		version:          gointerfaces.VersionFromProto(privateapi.EthBackendAPIVersion),
		log:              log.New("remote_service", "eth_backend"),
		db:               db,
		blockReader:      blockReader,
	}
}

func (back *RemoteBackend) EnsureVersionCompatibility() bool {
	versionReply, err := back.remoteEthBackend.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {

		back.log.Error("getting Version", "err", err)
		return false
	}
	if !gointerfaces.EnsureVersion(back.version, versionReply) {
		back.log.Error("incompatible interface versions", "client", back.version.String(),
			"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
		return false
	}
	back.log.Info("interfaces compatible", "client", back.version.String(),
		"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
	return true
}

func (back *RemoteBackend) Etherbase(ctx context.Context) (common.Address, error) {
	res, err := back.remoteEthBackend.Etherbase(ctx, &remote.EtherbaseRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return common.Address{}, errors.New(s.Message())
		}
		return common.Address{}, err
	}

	return gointerfaces.ConvertH160toAddress(res.Address), nil
}

func (back *RemoteBackend) NetVersion(ctx context.Context) (uint64, error) {
	res, err := back.remoteEthBackend.NetVersion(ctx, &remote.NetVersionRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return 0, errors.New(s.Message())
		}
		return 0, err
	}

	return res.Id, nil
}

func (back *RemoteBackend) NetPeerCount(ctx context.Context) (uint64, error) {
	res, err := back.remoteEthBackend.NetPeerCount(ctx, &remote.NetPeerCountRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return 0, errors.New(s.Message())
		}
		return 0, err
	}

	return res.Count, nil
}

func (back *RemoteBackend) ProtocolVersion(ctx context.Context) (uint64, error) {
	res, err := back.remoteEthBackend.ProtocolVersion(ctx, &remote.ProtocolVersionRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return 0, errors.New(s.Message())
		}
		return 0, err
	}

	return res.Id, nil
}

func (back *RemoteBackend) ClientVersion(ctx context.Context) (string, error) {
	res, err := back.remoteEthBackend.ClientVersion(ctx, &remote.ClientVersionRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return "", errors.New(s.Message())
		}
		return "", err
	}

	return res.NodeName, nil
}

func (back *RemoteBackend) Subscribe(ctx context.Context, onNewEvent func(*remote.SubscribeReply)) error {
	subscription, err := back.remoteEthBackend.Subscribe(ctx, &remote.SubscribeRequest{}, grpc.WaitForReady(true))
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return errors.New(s.Message())
		}
		return err
	}
	for {
		event, err := subscription.Recv()
		if errors.Is(err, io.EOF) {
			log.Info("rpcdaemon: the subscription channel was closed")
			break
		}
		if err != nil {
			return err
		}

		onNewEvent(event)
	}
	return nil
}

func (back *RemoteBackend) SubscribeLogs(ctx context.Context, onNewLogs func(reply *remote.SubscribeLogsReply), requestor *atomic.Value) error {
	subscription, err := back.remoteEthBackend.SubscribeLogs(ctx, grpc.WaitForReady(true))
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return errors.New(s.Message())
		}
		return err
	}
	requestor.Store(subscription.Send)
	for {
		logs, err := subscription.Recv()
		if errors.Is(err, io.EOF) {
			log.Info("rpcdaemon: the logs subscription channel was closed")
			break
		}
		if err != nil {
			return err
		}
		onNewLogs(logs)
	}
	return nil
}

func (back *RemoteBackend) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
	return back.blockReader.TxnLookup(ctx, tx, txnHash)
}
func (back *RemoteBackend) BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	return back.blockReader.BlockWithSenders(ctx, tx, hash, blockHeight)
}

func (back *RemoteBackend) EngineNewPayloadV1(ctx context.Context, payload *types2.ExecutionPayload) (res *remote.EnginePayloadStatus, err error) {
	return back.remoteEthBackend.EngineNewPayloadV1(ctx, payload)
}

func (back *RemoteBackend) EngineForkchoiceUpdatedV1(ctx context.Context, request *remote.EngineForkChoiceUpdatedRequest) (*remote.EngineForkChoiceUpdatedReply, error) {
	return back.remoteEthBackend.EngineForkChoiceUpdatedV1(ctx, request)
}

func (back *RemoteBackend) EngineGetPayloadV1(ctx context.Context, payloadId uint64) (res *types2.ExecutionPayload, err error) {
	return back.remoteEthBackend.EngineGetPayloadV1(ctx, &remote.EngineGetPayloadRequest{
		PayloadId: payloadId,
	})
}

func (back *RemoteBackend) NodeInfo(ctx context.Context, limit uint32) ([]p2p.NodeInfo, error) {
	nodes, err := back.remoteEthBackend.NodeInfo(ctx, &remote.NodesInfoRequest{Limit: limit})
	if err != nil {
		return nil, fmt.Errorf("nodes info request error: %w", err)
	}

	if nodes == nil || len(nodes.NodesInfo) == 0 {
		return nil, errors.New("empty nodesInfo response")
	}

	ret := make([]p2p.NodeInfo, 0, len(nodes.NodesInfo))
	for _, node := range nodes.NodesInfo {
		var rawProtocols map[string]json.RawMessage
		if err = json.Unmarshal(node.Protocols, &rawProtocols); err != nil {
			return nil, fmt.Errorf("cannot decode protocols metadata: %w", err)
		}

		protocols := make(map[string]interface{}, len(rawProtocols))
		for k, v := range rawProtocols {
			protocols[k] = v
		}

		ret = append(ret, p2p.NodeInfo{
			Enode:      node.Enode,
			ID:         node.Id,
			IP:         node.Enode,
			ENR:        node.Enr,
			ListenAddr: node.ListenerAddr,
			Name:       node.Name,
			Ports: struct {
				Discovery int `json:"discovery"`
				Listener  int `json:"listener"`
			}{
				Discovery: int(node.Ports.Discovery),
				Listener:  int(node.Ports.Listener),
			},
			Protocols: protocols,
		})
	}

	return ret, nil
}

func (back *RemoteBackend) Peers(ctx context.Context) ([]*p2p.PeerInfo, error) {
	rpcPeers, err := back.remoteEthBackend.Peers(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("ETHBACKENDClient.Peers() error: %w", err)
	}

	peers := make([]*p2p.PeerInfo, 0, len(rpcPeers.Peers))

	for _, rpcPeer := range rpcPeers.Peers {
		peer := p2p.PeerInfo{
			ENR:   rpcPeer.Enr,
			Enode: rpcPeer.Enode,
			ID:    rpcPeer.Id,
			Name:  rpcPeer.Name,
			Caps:  rpcPeer.Caps,
			Network: struct {
				LocalAddress  string `json:"localAddress"`
				RemoteAddress string `json:"remoteAddress"`
				Inbound       bool   `json:"inbound"`
				Trusted       bool   `json:"trusted"`
				Static        bool   `json:"static"`
			}{
				LocalAddress:  rpcPeer.ConnLocalAddr,
				RemoteAddress: rpcPeer.ConnRemoteAddr,
				Inbound:       rpcPeer.ConnIsInbound,
				Trusted:       rpcPeer.ConnIsTrusted,
				Static:        rpcPeer.ConnIsStatic,
			},
			Protocols: nil,
		}

		peers = append(peers, &peer)
	}

	return peers, nil
}
