package shutter

import (
	"context"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
)

type stateChangesClient interface {
	StateChanges(ctx context.Context, in *remoteproto.StateChangeRequest, opts ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error)
}
