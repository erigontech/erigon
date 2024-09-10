package direct

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
)

type BridgeClientDirect struct {
	server remote.BridgeBackendServer
}

func NewBridgeClientDirect(server remote.BridgeBackendServer) *BridgeClientDirect {
	return &BridgeClientDirect{server: server}
}

func (b *BridgeClientDirect) BorTxnLookup(ctx context.Context, in *types.BorTxnLookupRequest, opts ...grpc.CallOption) (*types.BorTxnLookupReply, error) {
	return b.server.BorTxnLookup(ctx, in)
}

func (b *BridgeClientDirect) BorEvents(ctx context.Context, in *types.BorEventsRequest, opts ...grpc.CallOption) (*types.BorEventsReply, error) {
	return b.server.BorEvents(ctx, in)
}

func (b *BridgeClientDirect) Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types.VersionReply, error) {
	return b.server.Version(ctx, in)
}
