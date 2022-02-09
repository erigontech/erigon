package downloadergrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader"
	"github.com/ledgerwatch/erigon/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

func NewClient(ctx context.Context, downloaderAddr string) (proto_downloader.DownloaderClient, error) {
	// creating grpc client connection
	var dialOpts []grpc.DialOption

	backoffCfg := backoff.DefaultConfig
	backoffCfg.BaseDelay = 500 * time.Millisecond
	backoffCfg.MaxDelay = 10 * time.Second
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoffCfg, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(16 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	}

	dialOpts = append(dialOpts, grpc.WithInsecure())
	conn, err := grpc.DialContext(ctx, downloaderAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client connection to sentry P2P: %w", err)
	}
	return proto_downloader.NewDownloaderClient(conn), nil
}

func InfoHashes2Proto(in []metainfo.Hash) []*prototypes.H160 {
	infoHashes := make([]*prototypes.H160, len(in))
	i := 0
	for _, h := range in {
		infoHashes[i] = gointerfaces.ConvertAddressToH160(h)
		i++
	}
	return infoHashes
}

func Strings2Proto(in []string) []*prototypes.H160 {
	infoHashes := make([]*prototypes.H160, len(in))
	i := 0
	for _, h := range in {
		infoHashes[i] = String2Proto(h)
		i++
	}
	return infoHashes
}

func String2Proto(in string) *prototypes.H160 {
	var infoHash [20]byte
	copy(infoHash[:], common.FromHex(in))
	return gointerfaces.ConvertAddressToH160(infoHash)
}

type ClientDirect struct {
	server *downloader.GrpcServer
}

func NewClientDirect(server *downloader.GrpcServer) *ClientDirect {
	return &ClientDirect{server: server}
}

func (c *ClientDirect) Download(ctx context.Context, in *proto_downloader.DownloadRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Download(ctx, in)
}
func (c *ClientDirect) Stats(ctx context.Context, in *proto_downloader.StatsRequest, opts ...grpc.CallOption) (*proto_downloader.StatsReply, error) {
	return c.server.Stats(ctx, in)
}
