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

package rpcservices

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/turbo/privateapi"
	"github.com/erigontech/erigon/turbo/services"
)

var _ services.FullBlockReader = &RemoteBackend{}

type RemoteBackend struct {
	remoteEthBackend remote.ETHBACKENDClient
	log              log.Logger
	version          gointerfaces.Version
	db               kv.RoDB
	blockReader      services.FullBlockReader
}

func NewRemoteBackend(client remote.ETHBACKENDClient, db kv.RoDB, blockReader services.FullBlockReader) *RemoteBackend {
	return &RemoteBackend{
		remoteEthBackend: client,
		version:          gointerfaces.VersionFromProto(privateapi.EthBackendAPIVersion),
		log:              log.New("remote_service", "eth_backend"),
		db:               db,
		blockReader:      blockReader,
	}
}

func (back *RemoteBackend) CanPruneTo(currentBlockInDB uint64) (canPruneBlocksTo uint64) {
	return back.blockReader.CanPruneTo(currentBlockInDB)
}
func (back *RemoteBackend) HeadersRange(ctx context.Context, walker func(header *types.Header) error) error {
	panic("not implemented")
}

func (back *RemoteBackend) Integrity(_ context.Context) error {
	panic("not implemented")
}

func (back *RemoteBackend) CurrentBlock(db kv.Tx) (*types.Block, error) {
	panic("not implemented")
}
func (back *RemoteBackend) RawTransactions(ctx context.Context, tx kv.Getter, fromBlock, toBlock uint64) (txs [][]byte, err error) {
	panic("not implemented")
}

func (back *RemoteBackend) FirstTxnNumNotInSnapshots() uint64 {
	panic("not implemented")
}

func (back *RemoteBackend) ReadAncestor(db kv.Getter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
	panic("not implemented")
}
func (back *RemoteBackend) BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error) {
	hash, ok, err := back.CanonicalHash(ctx, db, number)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if !ok || hash == (common.Hash{}) {
		return nil, nil
	}
	block, _, err := back.BlockWithSenders(ctx, db, hash, number)
	return block, err
}
func (back *RemoteBackend) BlockByHash(ctx context.Context, db kv.Tx, hash common.Hash) (*types.Block, error) {
	number := rawdb.ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil
	}
	block, _, err := back.BlockWithSenders(ctx, db, hash, *number)
	return block, err
}
func (back *RemoteBackend) TxsV3Enabled() bool { panic("not implemented") }
func (back *RemoteBackend) Snapshots() snapshotsync.BlockSnapshots {
	return back.blockReader.Snapshots()
}
func (back *RemoteBackend) BorSnapshots() snapshotsync.BlockSnapshots { panic("not implemented") }

func (back *RemoteBackend) Ready(ctx context.Context) <-chan error {
	return back.blockReader.Ready(ctx)
}

func (back *RemoteBackend) AllTypes() []snaptype.Type { panic("not implemented") }
func (back *RemoteBackend) FrozenBlocks() uint64      { return back.blockReader.FrozenBlocks() }
func (back *RemoteBackend) FrozenBorBlocks(align bool) uint64 {
	return back.blockReader.FrozenBorBlocks(align)
}
func (back *RemoteBackend) FrozenFiles() (list []string) { return back.blockReader.FrozenFiles() }
func (back *RemoteBackend) CanonicalBodyForStorage(ctx context.Context, tx kv.Getter, blockNum uint64) (body *types.BodyForStorage, err error) {
	return back.blockReader.CanonicalBodyForStorage(ctx, tx, blockNum)
}
func (back *RemoteBackend) FreezingCfg() ethconfig.BlocksFreezing {
	return back.blockReader.FreezingCfg()
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

func (back *RemoteBackend) Syncing(ctx context.Context) (*remote.SyncingReply, error) {
	res, err := back.remoteEthBackend.Syncing(ctx, &emptypb.Empty{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return nil, errors.New(s.Message())
		}
		return nil, err
	}

	return res, nil
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

func (back *RemoteBackend) PendingBlock(ctx context.Context) (*types.Block, error) {
	blockRlp, err := back.remoteEthBackend.PendingBlock(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("ETHBACKENDClient.PendingBlock() error: %w", err)
	}
	if blockRlp == nil {
		return nil, nil
	}

	var block types.Block
	err = rlp.DecodeBytes(blockRlp.BlockRlp, &block)
	if err != nil {
		return nil, fmt.Errorf("decoding block from %x: %w", blockRlp.BlockRlp, err)
	}

	return &block, nil
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
			log.Debug("rpcdaemon: the subscription channel was closed")
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

func (back *RemoteBackend) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, uint64, bool, error) {
	return back.blockReader.TxnLookup(ctx, tx, txnHash)
}
func (back *RemoteBackend) HasSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bool, error) {
	panic("HasSenders is low-level method, don't use it in RPCDaemon")
}
func (back *RemoteBackend) BadHeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (blockNum *uint64, err error) {
	return back.blockReader.BadHeaderNumber(ctx, tx, hash)
}
func (back *RemoteBackend) BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (block *types.Block, senders []common.Address, err error) {
	return back.blockReader.BlockWithSenders(ctx, tx, hash, blockNum)
}

func (back *RemoteBackend) IterateFrozenBodies(_ func(blockNum uint64, baseTxNum uint64, txCount uint64) error) error {
	panic("not implemented")
}

func (back *RemoteBackend) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (body *types.Body, err error) {
	return back.blockReader.BodyWithTransactions(ctx, tx, hash, blockNum)
}
func (back *RemoteBackend) BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bodyRlp rlp.RawValue, err error) {
	return back.blockReader.BodyRlp(ctx, tx, hash, blockNum)
}
func (back *RemoteBackend) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (body *types.Body, txCount uint32, err error) {
	return back.blockReader.Body(ctx, tx, hash, blockNum)
}
func (back *RemoteBackend) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Header, error) {
	return back.blockReader.Header(ctx, tx, hash, blockNum)
}
func (back *RemoteBackend) HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error) {
	return back.blockReader.HeaderByNumber(ctx, tx, blockNum)
}
func (back *RemoteBackend) HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error) {
	return back.blockReader.HeaderByHash(ctx, tx, hash)
}
func (back *RemoteBackend) CanonicalHash(ctx context.Context, tx kv.Getter, blockNum uint64) (common.Hash, bool, error) {
	return back.blockReader.CanonicalHash(ctx, tx, blockNum)
}
func (back *RemoteBackend) HeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (*uint64, error) {
	return back.blockReader.HeaderNumber(ctx, tx, hash)
}
func (back *RemoteBackend) IsCanonical(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bool, error) {
	return back.blockReader.IsCanonical(ctx, tx, hash, blockNum)
}
func (back *RemoteBackend) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (types.Transaction, error) {
	return back.blockReader.TxnByIdxInBlock(ctx, tx, blockNum, i)
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

func (back *RemoteBackend) AddPeer(ctx context.Context, request *remote.AddPeerRequest) (*remote.AddPeerReply, error) {
	result, err := back.remoteEthBackend.AddPeer(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("ETHBACKENDClient.AddPeer() error: %w", err)
	}
	return result, nil
}

func (back *RemoteBackend) RemovePeer(ctx context.Context, request *remote.RemovePeerRequest) (*remote.RemovePeerReply, error) {
	result, err := back.remoteEthBackend.RemovePeer(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("ETHBACKENDClient.RemovePeer() error: %w", err)
	}
	return result, nil
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

func (back *RemoteBackend) TxnumReader(ctx context.Context) rawdbv3.TxNumsReader {
	return back.blockReader.TxnumReader(ctx)
}

func (back *RemoteBackend) BlockForTxNum(ctx context.Context, tx kv.Tx, txNum uint64) (uint64, bool, error) {
	return back.blockReader.BlockForTxNum(ctx, tx, txNum)
}
