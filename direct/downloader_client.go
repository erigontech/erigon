package direct

import (
	"context"

	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type DownloaderClientDirect struct {
	server proto_downloader.DownloaderServer
}

func NewClientDirect(server proto_downloader.DownloaderServer) *DownloaderClientDirect {
	return &DownloaderClientDirect{server: server}
}

func (c *DownloaderClientDirect) Download(ctx context.Context, in *proto_downloader.DownloadRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Download(ctx, in)
}
func (c *DownloaderClientDirect) Stats(ctx context.Context, in *proto_downloader.StatsRequest, opts ...grpc.CallOption) (*proto_downloader.StatsReply, error) {
	return c.server.Stats(ctx, in)
}
