package rpchelper

import (
	"context"
	"sync/atomic"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p"
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
	EngineNewPayloadV2(ctx context.Context, payload *types2.ExecutionPayloadV2) (*remote.EnginePayloadStatus, error)
	EngineForkchoiceUpdatedV1(ctx context.Context, request *remote.EngineForkChoiceUpdatedRequest) (*remote.EngineForkChoiceUpdatedReply, error)
	EngineForkchoiceUpdatedV2(ctx context.Context, request *remote.EngineForkChoiceUpdatedRequestV2) (*remote.EngineForkChoiceUpdatedReply, error)
	EngineGetPayloadV1(ctx context.Context, payloadId uint64) (*types2.ExecutionPayload, error)
	EngineGetPayloadV2(ctx context.Context, payloadId uint64) (*types2.ExecutionPayloadV2, error)
	NodeInfo(ctx context.Context, limit uint32) ([]p2p.NodeInfo, error)
	Peers(ctx context.Context) ([]*p2p.PeerInfo, error)
	PendingBlock(ctx context.Context) (*types.Block, error)
}
