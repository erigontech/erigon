// Copyright 2021 The Erigon Authors
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

package direct

import (
	"context"

	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type DownloaderClient struct {
	server proto_downloader.DownloaderServer
}

func NewDownloaderClient(server proto_downloader.DownloaderServer) *DownloaderClient {
	return &DownloaderClient{server: server}
}

func (c *DownloaderClient) Add(ctx context.Context, in *proto_downloader.AddRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Add(ctx, in)
}

func (c *DownloaderClient) ProhibitNewDownloads(ctx context.Context, in *proto_downloader.ProhibitNewDownloadsRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.ProhibitNewDownloads(ctx, in)
}
func (c *DownloaderClient) Delete(ctx context.Context, in *proto_downloader.DeleteRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Delete(ctx, in)
}
func (c *DownloaderClient) Verify(ctx context.Context, in *proto_downloader.VerifyRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.Verify(ctx, in)
}
func (c *DownloaderClient) Stats(ctx context.Context, in *proto_downloader.StatsRequest, opts ...grpc.CallOption) (*proto_downloader.StatsReply, error) {
	return c.server.Stats(ctx, in)
}
