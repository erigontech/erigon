package rpc

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

var _ BeaconRpc = (*BeaconRpcP2P)(nil)

type BeaconRpc interface {
	SendBeaconBlocksByRangeReq(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error)
	SendBeaconBlocksByRootReq(ctx context.Context, roots [][32]byte) ([]*cltypes.SignedBeaconBlock, string, error)
	SendBlobsSidecarByIdentifierReq(ctx context.Context, req *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error)
	SendBlobsSidecarByRangerReq(ctx context.Context, start, count uint64) ([]*cltypes.BlobSidecar, string, error)
	SendColumnSidecarsByRootIdentifierReq(ctx context.Context, req *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier]) ([]*cltypes.DataColumnSidecar, string, error)
	SendColumnSidecarsByRangeReqV1(ctx context.Context, start, count uint64, columns []uint64) ([]*cltypes.DataColumnSidecar, string, error)
	Peers() (uint64, error)
	SetStatus(finalizedRoot common.Hash, finalizedEpoch uint64, headRoot common.Hash, headSlot uint64) error
	BanPeer(pid string)
}
